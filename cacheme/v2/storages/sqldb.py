from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    LargeBinary,
    DateTime,
    create_mock_engine,
)
from sqlalchemy.schema import CreateTable, CreateIndex
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import now

from cacheme.v2.storages.base import BaseStorage


@compiles(now, "sqlite")
def sl_now(element, compiler, **kw):
    return "strftime('%Y-%m-%d %H:%M:%f', 'now')"


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
        ddl = {"create_table": CreateTable(tb).compile(engine), "create_indexes": []}
        for index in tb.indexes:
            ddl["create_indexes"].append(CreateIndex(index).compile(engine))
        return ddl

    async def execute_ddl(self, ddl: str):
        raise NotImplemented()
