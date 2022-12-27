from datetime import timedelta
from tinylfu.lru import LRU, SLRU
from tinylfu.sketch import CountMinSketch
from tinylfu.hash import hash_string
from typing import Any

from tinylfu.linkedlist import Element
from tinylfu.linkedlist import Item


class Cache:
    def __init__(self, size: int):
        admission_ratio = 0.01
        lru_size = int(size * admission_ratio) or 1
        slru_size = size - lru_size
        self.cache_dict: dict[str, Element] = {}
        self.lru = LRU(lru_size, self.cache_dict)
        self.slru = SLRU(slru_size, self.cache_dict)
        self.sketch = CountMinSketch(size)

    def set(self, key, value, ttl: timedelta):
        item = Item(key, value, ttl)
        element = Element(item)
        candidate = self.lru.set(key, element)
        if candidate == None:
            return None
        victim = self.slru.victim()
        if victim == None:
            self.slru.set(key, element)
            return
        candidate_count = self.sketch.estimate(candidate.keyh)
        victim_count = self.sketch.estimate(victim.keyh)
        if candidate_count > victim_count:
            self.slru.set(candidate.item.key, candidate)

    def get(self, key: str):
        keyh = hash_string(key)
        self.sketch.add(keyh)
        e = self.cache_dict.get(key, None)
        if e != None:
            return e.item.value
        return None
