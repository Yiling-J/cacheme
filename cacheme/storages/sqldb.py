from typing import List, cast

from sqlalchemy import (Column, DateTime, Integer, LargeBinary, MetaData,
                        String, Table, create_mock_engine)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateIndex, CreateTable
from sqlalchemy.sql.functions import now

from cacheme.storages.base import BaseStorage


@compiles(now, "sqlite")
def sl_now(element, compiler, **kw):
    return "strftime('%Y-%m-%d %H:%M:%f', 'now')"


@compiles(DateTime, "mysql")
def compile_datetime_mysql(type_, compiler, **kw):
    return "DATETIME(6)"


@compiles(now, "mysql")  # type: ignore
def sl_now(element, compiler, **kw):
    return "now(6)"


class SQLStorage(BaseStorage):
    def __init__(self, address: str, initialize: bool):
        super().__init__(address=address)
        self._initialize = initialize

    async def _connect(self):
        raise NotImplemented()

    async def connect(self):
        await self._connect()
        if self._initialize:
            ddl = self._create_table_ddl()
            await self.execute_ddl(ddl["create_table"])
            for iddl in ddl["create_indexes"]:
                await self.execute_ddl(iddl)

    def _create_table_ddl(self) -> dict:
        meta = MetaData()
        tb = Table(
            "cacheme_data",
            meta,
            Column("id", Integer, primary_key=True),
            Column("key", String(512), unique=True),
            Column("value", LargeBinary),
            Column("expire", DateTime(timezone=True), index=True),
            Column(
                "updated_at",
                DateTime(timezone=True),
                server_default=now(),
                server_onupdate=now(),
            ),
        )
        engine = create_mock_engine(self.address, None)
        ddl = {
            "create_table": str(CreateTable(tb).compile(engine)),
            "create_indexes": [],
        }
        for index in tb.indexes:
            cast(List, ddl["create_indexes"]).append(
                str(CreateIndex(index).compile(engine))
            )
        return ddl

    async def execute_ddl(self, ddl: str):
        raise NotImplementedError()
