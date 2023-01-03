import motor.motor_asyncio as mongo
from datetime import datetime, timezone, timedelta
from typing import Optional, Any
from cacheme.v2.storages.base import BaseStorage


class MongoStorage(BaseStorage):
    def __init__(self, address: str, initialize: bool = False):
        super().__init__(address=address)
        self.address = address
        self.initialize = initialize

    async def connect(self):
        client = mongo.AsyncIOMotorClient(self.address)
        self.table = client.cacheme.data
        if self.initialize:
            await self.table.create_index("key", unique=True)
            await self.table.create_index("expire")

    async def get_by_key(self, key: str) -> Any:
        return await self.table.find_one({"key": key})

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        expire = None
        if ttl != None:
            expire = datetime.now(timezone.utc) + ttl
        await self.table.update_one(
            {"key": key},
            {
                "$set": {
                    "value": value,
                    "updated_at": datetime.now(timezone.utc),
                    "expire": expire,
                }
            },
            True,
        )

    async def remove_by_key(self, key: str):
        await self.table.delete_one({"key": key})
