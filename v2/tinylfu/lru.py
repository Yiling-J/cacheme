from tinylfu.linkedlist import Element, LinkedList
from typing import Optional


class LRU:
    def __init__(self, maxsize, cache: dict[str, Element]):
        self.maxsize = maxsize
        self.cache = cache
        self.ls = LinkedList()

    def set(self, key: str, value: Element):
        item = self.cache.pop(key, None)
        if item != None:
            self.cache[key] = item
            self.ls.move_to_front(item)
            return
        self.ls.push_front(value.item)
        self.cache[key] = value
        if len(self.ls) > self.maxsize:
            last = self.ls.back()
            if last == None:
                return
            self.ls.remove(last)
            return last


class SLRU:
    def __init__(self, maxsize, cache):
        self.maxsize = maxsize
        self.cache = cache
        self.protected = LinkedList()
        self.probation = LinkedList()
        self.protected_cap = int(maxsize * 0.8)
        self.probation_cap = maxsize - self.protected_cap

    def set(self, key: str, value: Element):
        value.item.list_id = 1
        self.cache[key] = value
        if (len(self.probation) < self.probation_cap) or (
            len(self.probation) + len(self.protected) < self.maxsize
        ):
            self.probation.push_front(value.item)
            return
        last = self.probation.back()
        if last != None:
            last.item = value.item
            self.probation.move_to_front(last)
            self.cache.pop(last.item.key, None)

    def victim(self) -> Optional[Element]:
        if len(self.probation) + len(self.protected) < self.maxsize:
            return None
        return self.probation.back()

    def access(self, element: Element):
        if element.item.list_id == None:
            return
        if element.item.list_id == 2:
            self.protected.move_to_front(element)
            return
        element.item.list_id = 2
        self.probation.remove(element)
        self.protected.move_to_front(element)
        if len(self.protected) > self.protected_cap:
            el = self.protected.back()
            if el != None:
                self.protected.remove(el)
                el.item.list_id = 1
                self.probation.push_front(el.item)
