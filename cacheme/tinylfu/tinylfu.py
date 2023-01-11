from typing import Optional, Dict
from cacheme.tinylfu.linkedlist import Element

from cacheme.tinylfu.lru import LRU, SLRU
from cacheme.tinylfu.sketch import CountMinSketch
from cacheme.utils import hash_string


class Cache:
    def __init__(self, size: int):
        admission_ratio = 0.01
        lru_size = int(size * admission_ratio) or 1
        slru_size = size - lru_size
        self.key_mapping: Dict[str, Element] = {}
        self.lru = LRU(lru_size, self.key_mapping)
        self.slru = SLRU(slru_size, self.key_mapping)
        self.sketch = CountMinSketch(size)

    def set(self, key: str) -> Optional[str]:
        candidate = self.lru.set(key)
        if candidate is None:
            return None
        victim = self.slru.victim()
        if victim is None:
            self.slru.set(candidate)
            return None
        evicated: Optional[str] = candidate
        candidate_count = self.sketch.estimate(hash_string(candidate))
        victim_count = self.sketch.estimate(hash_string(victim))
        if candidate_count > victim_count:
            evicated = self.slru.set(candidate)
        return evicated

    def remove(self, key: str):
        element = self.key_mapping.pop(key, None)
        if element is None:
            return
        if element.list is not None:
            element.list.remove(element)
