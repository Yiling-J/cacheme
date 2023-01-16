from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import motor.motor_asyncio as mongo

from cacheme.storages.base import BaseStorage


class MongoStorage(BaseStorage):
    def __init__(self, address: str, database: str, collection: str):
        super().__init__(address=address)
        self.address = address
        self.database = database
        self.collection = collection

    async def connect(self):
        client = mongo.AsyncIOMotorClient(self.address)
        self.table = client[self.database][self.collection]

    async def get_by_key(self, key: str) -> Any:
        return await self.table.find_one({"key": key})

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        expire = None
        if ttl is not None:
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

    async def get_by_keys(self, keys: List[str]) -> Dict[str, Any]:
        results = await self.table.find({"key": {"$in": keys}}).to_list(None)
        return {r["key"]: r for r in results}
