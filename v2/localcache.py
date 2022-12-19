import datetime
from collections import OrderedDict
from typing import Any


class LocalCache:
    def __init__(
        self,
        enable: bool = False,
        maxsize: int = 500,
        ttl: datetime.timedelta = datetime.timedelta(seconds=5),
    ):
        self.enable = enable
        self.cache = OrderedDict()
        self.maxsize = maxsize
        self.ttl = ttl

    def get(self, key: str) -> Any:
        print(len(self.cache))
        if key in self.cache:
            dt, result = self.cache[key]
            if datetime.datetime.utcnow() - dt <= self.ttl:
                return result
            return None

    def set(self, key: str, value: Any):
        self.cache.pop(key, None)
        self.cache[key] = datetime.datetime.utcnow(), value
        if len(self.cache) > self.maxsize:
            self.cache.popitem()
