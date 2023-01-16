import re
from typing import List, cast

from cacheme.storages.base import BaseStorage


class SQLStorage(BaseStorage):
    def __init__(self, address: str, table: str):
        match = re.fullmatch(r".\w+", table)
        if match is None:
            raise Exception("invalid table name")
        self.address = address
        self.table = table
        super().__init__(address=address, table=table)

    async def _connect(self):
        raise NotImplemented()

    async def connect(self):
        await self._connect()

    async def execute_ddl(self, ddl):
        raise NotImplementedError()
