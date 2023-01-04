from cacheme.tinylfu.linkedlist import LinkedList
from cacheme.models import Element, Item
from typing import Optional, Dict


class LRU:
    def __init__(self, maxsize, cache: Dict[str, Element]):
        self.maxsize = maxsize
        self.cache = cache
        self.ls = LinkedList()

    def set(self, key: str, value: Item) -> Optional[Item]:
        if len(self.ls) < self.maxsize:
            new = self.ls.push_front(value)
            self.cache[key] = new
            return None
        last = self.ls.back()
        if last != None:
            self.cache.pop(last.item.key.full_key)
            old = last.item
            last.item = value
            self.ls.move_to_front(last)
            self.cache[key] = last
            return old


class SLRU:
    def __init__(self, maxsize, cache):
        self.maxsize = maxsize
        self.cache = cache
        self.protected = LinkedList()
        self.probation = LinkedList()
        self.protected_cap = int(maxsize * 0.8)
        self.probation_cap = maxsize - self.protected_cap

    def set(self, key: str, value: Item) -> Optional[Item]:
        value.list_id = 1
        if (len(self.probation) < self.probation_cap) or (
            len(self.probation) + len(self.protected) < self.maxsize
        ):
            new = self.probation.push_front(value)
            self.cache[key] = new
            return None
        last = self.probation.back()
        if last != None:
            self.cache.pop(last.item.key.full_key)
            old = last.item
            last.item = value
            self.probation.move_to_front(last)
            self.cache[key] = last
            return old

    def victim(self) -> Optional[Element]:
        if len(self.probation) + len(self.protected) < self.maxsize:
            return None
        return self.probation.back()

    def access(self, element: Element):
        if element.item.list_id is None:
            return
        if element.item.list_id == 2:
            self.protected.move_to_front(element)
            return
        element.item.list_id = 2
        self.probation.remove(element)
        self.protected.move_to_front(element)
        if len(self.protected) > self.protected_cap:
            el = self.protected.back()
            if el is not None:
                self.protected.remove(el)
                el.item.list_id = 1
                self.probation.push_front(el.item)
