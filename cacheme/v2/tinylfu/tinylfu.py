from datetime import datetime, timedelta, timezone
from typing import Optional
from cacheme.v2.tinylfu.lru import LRU, SLRU
from cacheme.v2.tinylfu.sketch import CountMinSketch

from cacheme.v2.models import CacheKey, Element, Item, CachedData


class Cache:
    def __init__(self, size: int):
        admission_ratio = 0.01
        lru_size = int(size * admission_ratio) or 1
        slru_size = size - lru_size
        self.cache_dict: dict[str, Element] = {}
        self.lru = LRU(lru_size, self.cache_dict)
        self.slru = SLRU(slru_size, self.cache_dict)
        self.sketch = CountMinSketch(size)

    def set(self, key: CacheKey, value, ttl: Optional[timedelta]):
        item = Item(key, value, ttl)
        candidate = self.lru.set(key.full_key, item)
        if candidate == None:
            return None
        victim = self.slru.victim()
        if victim == None:
            self.slru.set(candidate.key.full_key, candidate)
            return
        candidate_count = self.sketch.estimate(candidate.key.hash)
        victim_count = self.sketch.estimate(victim.item.key.hash)
        if candidate_count > victim_count:
            self.slru.set(candidate.key.full_key, candidate)

    def remove(self, element: Element):
        if element.list != None:
            element.list.remove(element)
        self.cache_dict.pop(element.item.key.full_key, None)

    def get(self, key: CacheKey) -> Optional[CachedData]:
        self.sketch.add(key.hash)
        e = self.cache_dict.get(key.full_key, None)
        if e != None:
            if e.item.expire == None or (e.item.expire > datetime.now(timezone.utc)):
                return CachedData(data=e.item.value, updated_at=e.item.updated_at)
            self.remove(e)
        return None
