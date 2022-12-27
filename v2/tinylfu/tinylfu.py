from datetime import timedelta
from tinylfu.lru import LRU, SLRU
from tinylfu.sketch import CountMinSketch

from tinylfu.linkedlist import Element
from tinylfu.linkedlist import Item
from data_types import CacheKey


class Cache:
    def __init__(self, size: int):
        admission_ratio = 0.01
        lru_size = int(size * admission_ratio) or 1
        slru_size = size - lru_size
        self.cache_dict: dict[str, Element] = {}
        self.lru = LRU(lru_size, self.cache_dict)
        self.slru = SLRU(slru_size, self.cache_dict)
        self.sketch = CountMinSketch(size)

    def set(self, key: CacheKey, value, ttl: timedelta):
        item = Item(key, value, ttl)
        element = Element(item)
        candidate = self.lru.set(key.full_key, element)
        if candidate == None:
            return None
        victim = self.slru.victim()
        if victim == None:
            self.slru.set(key.full_key, element)
            return
        candidate_count = self.sketch.estimate(candidate.keyh)
        victim_count = self.sketch.estimate(victim.keyh)
        if candidate_count > victim_count:
            self.slru.set(candidate.item.key.full_key, candidate)

    def get(self, key: CacheKey):
        self.sketch.add(key.hash)
        e = self.cache_dict.get(key.full_key, None)
        if e != None:
            return e.item.value
        return None
