import datetime

from typing import Any


from data_types import CacheKey


class Item:
    key: CacheKey
    value: Any
    list_id: int | None
    expire: datetime.datetime

    def __init__(
        self,
        key: CacheKey,
        value: Any,
        ttl: datetime.timedelta,
        list_id: int | None = None,
    ):
        self.expire = datetime.datetime.now() + ttl
        self.key = key
        self.value = value
        self.list_id = list_id


class Element:
    prev: Any
    next: Any
    list: Any
    item: Item

    def __init__(self, item: Item):
        self.item = item

    @property
    def keyh(self) -> int:
        return self.item.key.hash


class LinkedList:
    def __init__(self):
        self.root = Element(
            Item(
                key=CacheKey(node="", prefix="", key="root", version="0", tags=[]),
                value=0,
                ttl=datetime.timedelta(days=600),
            )
        )
        self.root.list = self
        self.root.prev = self.root
        self.root.next = self.root
        self.len = 0

    def __len__(self):
        return self.len

    def __insert(self, e: Element, at: Element):
        e.prev = at
        e.next = at.next
        e.prev.next = e
        e.next.prev = e
        e.list = self
        self.len += 1

    def __move(self, e: Element, at: Element):
        if e == at:
            return
        e.prev.next = e.next
        e.next.prev = e.prev
        e.prev = at
        e.next = at.next
        e.prev.next = e
        e.next.prev = e

    def front(self) -> Element | None:
        if self.len == 0:
            return None
        return self.root.next

    def back(self) -> Element | None:
        if self.len == 0:
            return None
        return self.root.prev

    def remove(self, e: Element):
        if e.list == self:
            e.prev.next = e.next
            e.next.prev = e.prev
            e.next = None
            e.prev = None
            e.list = None
            self.len -= 1

    def push_front(self, item: Item) -> Element:
        e = Element(item)
        self.__insert(e, self.root)
        return e

    def push_back(self, item: Item) -> Element:
        e = Element(item)
        self.__insert(e, self.root.prev)
        return e

    def insert_before(self, at: Element, item: Item) -> Element:
        e = Element(item)
        e.item = item
        self.__insert(e, at.prev)
        return e

    def insert_after(self, at: Element, item: Item) -> Element:
        e = Element(item)
        e.item = item
        self.__insert(e, at)
        return e

    def move_to_front(self, e: Element):
        if e.list != self:
            return
        self.__move(e, self.root)

    def move_to_back(self, e: Element):
        if e.list != self:
            return
        self.__move(e, self.root.prev)

    def move_before(self, e: Element, at: Element):
        if e.list != self or at.list != self or e == at:
            return
        self.__move(e, at.prev)

    def move_after(self, e: Element, at: Element):
        if e.list != self or at.list != self or e == at:
            return
        self.__move(e, at)
