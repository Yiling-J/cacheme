from datetime import timedelta

from cacheme.tinylfu.linkedlist import LinkedList


def assert_list(order: str, l: LinkedList):
    el = l.root
    r = []
    while True:
        if el != l.root:
            r.append(el.key)
        if el.next is None:
            break
        el = el.next
        if el == l.root:
            break
        if el is None:
            break
    assert "-".join(r) == order


def test_linkedlist():
    l = LinkedList(id=1)
    for i in ["A", "B", "C", "D", "E"]:
        l.push_back(i)
    assert_list("A-B-C-D-E", l)
    last = l.back()
    assert last is not None
    l.move_to_front(last)
    assert_list("E-A-B-C-D", l)
    front = l.front()
    assert front is not None
    # E-F-A-B-C-D
    l.insert_after(front, "F")
    assert_list("E-F-A-B-C-D", l)
    # G-E-F-A-B-C-D
    l.push_front("G")
    assert_list("G-E-F-A-B-C-D", l)
