from datetime import datetime, timedelta, timezone
from typing import Optional, Dict
from cacheme.interfaces import CachedData

from cacheme.models import Element, Item
from cacheme.tinylfu.lru import LRU, SLRU
from cacheme.tinylfu.sketch import CountMinSketch
from cacheme.utils import hash_string


class Cache:
    def __init__(self, size: int):
        admission_ratio = 0.01
        lru_size = int(size * admission_ratio) or 1
        slru_size = size - lru_size
        self.cache_dict: Dict[str, Element] = {}
        self.lru = LRU(lru_size, self.cache_dict)
        self.slru = SLRU(slru_size, self.cache_dict)
        self.sketch = CountMinSketch(size)

    def set(self, key: str, value, ttl: Optional[timedelta]) -> bool:
        item = Item(key, value, ttl)
        candidate = self.lru.set(key, item)
        if candidate is None:
            return False
        victim = self.slru.victim()
        if victim is None:
            self.slru.set(candidate.key, candidate)
            return False
        candidate_count = self.sketch.estimate(candidate.keyh)
        victim_count = self.sketch.estimate(victim.item.keyh)
        if candidate_count > victim_count:
            self.slru.set(candidate.key, candidate)
        return True

    def remove(self, key: str):
        element = self.cache_dict.pop(key, None)
        if element is None:
            return
        if element.list is not None:
            element.list.remove(element)

    def get(self, key: str) -> Optional[CachedData]:
        self.sketch.add(hash_string(key))
        e = self.cache_dict.get(key, None)
        if e is not None:
            if e.item.expire is None or (e.item.expire > datetime.now(timezone.utc)):
                return CachedData(data=e.item.value, updated_at=e.item.updated_at)
            self.remove(e.item.key)
        return None
