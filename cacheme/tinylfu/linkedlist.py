from typing import Optional, cast


class Element:
    prev: Optional["Element"]
    next: Optional["Element"]
    list: Optional["LinkedList"]
    key: str

    def __init__(self, key: str):
        self.key = key


class LinkedList:
    def __init__(self, id: int):
        self.root = Element(key="__root__")
        self.root.list = self
        self.root.prev = self.root
        self.root.next = self.root
        self.len = 0
        self.id = id

    def __len__(self):
        return self.len

    def __insert(self, e: Element, at: Element):
        e.prev = at
        e.next = at.next
        e.prev.next = e
        if e.next is not None:
            e.next.prev = e
        e.list = self
        self.len += 1

    def __move(self, e: Element, at: Element):
        if e == at:
            return
        if e.prev is not None:
            e.prev.next = e.next
        if e.next is not None:
            e.next.prev = e.prev
        e.prev = at
        e.next = at.next
        e.prev.next = e
        if e.next is not None:
            e.next.prev = e

    def front(self) -> Optional[Element]:
        if self.len == 0:
            return None
        return self.root.next

    def back(self) -> Optional[Element]:
        if self.len == 0:
            return None
        return self.root.prev

    def remove(self, e: Element):
        if e.list == self:
            if e.prev is not None:
                e.prev.next = e.next
            if e.next is not None:
                e.next.prev = e.prev
            e.next = None
            e.prev = None
            e.list = None
            self.len -= 1

    def push_front(self, key: str) -> Element:
        e = Element(key)
        self.__insert(e, self.root)
        return e

    def push_back(self, key: str) -> Element:
        e = Element(key)
        self.__insert(e, cast(Element, self.root.prev))
        return e

    def insert_before(self, at: Element, key: str) -> Element:
        e = Element(key)
        self.__insert(e, cast(Element, at.prev))
        return e

    def insert_after(self, at: Element, key: str) -> Element:
        e = Element(key)
        self.__insert(e, at)
        return e

    def move_to_front(self, e: Element):
        if e.list != self:
            return
        self.__move(e, self.root)

    def move_to_back(self, e: Element):
        if e.list != self:
            return
        self.__move(e, cast(Element, self.root.prev))

    def move_before(self, e: Element, at: Element):
        if e.list != self or at.list != self or e == at:
            return
        self.__move(e, cast(Element, at.prev))

    def move_after(self, e: Element, at: Element):
        if e.list != self or at.list != self or e == at:
            return
        self.__move(e, at)
