from typing import Any
from urllib.parse import urlparse
from cacheme.storages.mongo import MongoStorage
from cacheme.storages.sqldb import SQLStorage
import motor.motor_asyncio as mongo


async def setup_storage(storage: Any):
    if isinstance(storage, SQLStorage):
        url = urlparse(storage.address)
        with open(f"cacheme/storages/scripts/{url.scheme}.sql", "r") as f:
            sql = f.read()
        ddls = sql.split(";")
        for ddl in ddls:
            if ddl.strip() == "":
                continue
            ddl = ddl.replace("cacheme_data", storage.table)
            await storage.execute_ddl(ddl)

    if isinstance(storage, MongoStorage):
        client = mongo.AsyncIOMotorClient(storage.collection)
        table = client[storage.database][storage.table]
        await table.create_index("key", unique=True)
        await table.create_index("expire")
