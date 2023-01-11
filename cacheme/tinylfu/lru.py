from typing import Dict, Optional

from cacheme.tinylfu.linkedlist import LinkedList, Element


class LRU:
    def __init__(self, maxsize, key_mapping: Dict[str, Element]):
        self.maxsize = maxsize
        self.key_mapping = key_mapping
        self.ls = LinkedList(id=0)

    def set(self, key: str) -> Optional[str]:
        if len(self.ls) < self.maxsize:
            new = self.ls.push_front(key)
            self.key_mapping[key] = new
            return None
        last = self.ls.back()
        if last is not None:
            evicated = last.key
            last.key = key
            self.key_mapping.pop(evicated)
            self.ls.move_to_front(last)
            self.key_mapping[key] = last
            return evicated
        return None


class SLRU:
    def __init__(self, maxsize, key_mapping: Dict[str, Element]):
        self.maxsize = maxsize
        self.key_mapping = key_mapping
        self.protected = LinkedList(id=1)
        self.probation = LinkedList(id=2)
        self.protected_cap = int(maxsize * 0.8)
        self.probation_cap = maxsize - self.protected_cap

    def set(self, key: str) -> Optional[str]:
        if (len(self.probation) < self.probation_cap) or (
            len(self.probation) + len(self.protected) < self.maxsize
        ):
            new = self.probation.push_front(key)
            self.key_mapping[key] = new
            return None
        last = self.probation.back()
        if last is not None:
            evicated = last.key
            last.key = key
            self.probation.move_to_front(last)
            return evicated
        return None

    def victim(self) -> Optional[str]:
        if len(self.probation) + len(self.protected) < self.maxsize:
            return None
        victim = self.probation.back()
        if victim is None:
            return None
        return victim.key

    def access(self, element: Element):
        if element.list is None:
            return
        # already in protected
        if element.list.id == 1:
            self.protected.move_to_front(element)
            return
        # move from probation to protected
        self.probation.remove(element)
        element.list = self.protected
        self.protected.move_to_front(element)
        # evicate one and add back to probation
        if len(self.protected) > self.protected_cap:
            evicated = self.protected.back()
            if evicated is not None:
                self.protected.remove(evicated)
                evicated.list = self.probation
                self.probation.move_to_front(evicated)
