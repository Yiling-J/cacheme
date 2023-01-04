from datetime import timedelta

from cacheme.models import CacheKey, Item
from cacheme.tinylfu.linkedlist import LinkedList


def assert_list(order: str, l: LinkedList):
    el = l.root
    r = []
    while True:
        if el != l.root:
            r.append(el.item.key.key)
        el = el.next
        if el == l.root:
            break
        if el is None:
            break
    assert "-".join(r) == order


def test_linkedlist():
    l = LinkedList()
    for i in ["A", "B", "C", "D", "E"]:
        l.push_back(
            Item(
                key=CacheKey(node="", prefix="", key=i, version="", tags=[]),
                value="",
                ttl=timedelta(days=10),
            )
        )
    assert_list("A-B-C-D-E", l)
    last = l.back()
    assert last is not None
    l.move_to_front(last)
    assert_list("E-A-B-C-D", l)
    front = l.front()
    assert front is not None
    # E-F-A-B-C-D
    l.insert_after(
        front,
        Item(
            key=CacheKey(node="", prefix="", key="F", version="", tags=[]),
            value="",
            ttl=timedelta(days=10),
        ),
    )
    assert_list("E-F-A-B-C-D", l)
    # G-E-F-A-B-C-D
    l.push_front(
        Item(
            key=CacheKey(node="", prefix="", key="G", version="", tags=[]),
            value="",
            ttl=timedelta(days=10),
        ),
    )
    assert_list("G-E-F-A-B-C-D", l)
