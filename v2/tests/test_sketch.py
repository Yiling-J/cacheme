from v2.tinylfu.sketch import CountMinSketch
from v2.utils import hash_string


def test_sketch():
    sketch = CountMinSketch(100)
    # 512 counters per row, 2048 bits per row, 32 uint64 per row
    assert sketch.row_counter_size == 512
    # 512 - 1
    assert sketch.row_mask == 511
    # 32 uint64 * 4 rows
    assert len(sketch.table) == 128

    # index is the uint64 array index
    # offset is the start index of 64 counters if that index
    for i in range(1000):
        key = f"foo:bar:{i}"
        h = hash_string(key)
        index, offset = sketch.index_of(h, i % 4)
        assert index < 128
        assert offset < 64

    # on evicate, we always compare two key's estimate count
    failed = 0
    for i in range(1000):
        key = f"foo:bar:{i}"
        h = hash_string(key)
        sketch.add(h)
        sketch.add(h)
        sketch.add(h)
        sketch.add(h)
        sketch.add(h)
        key2 = f"foo:bar:{i}:t2"
        h2 = hash_string(key2)
        sketch.add(h2)
        sketch.add(h2)
        sketch.add(h2)
        es1 = sketch.estimate(h)
        es2 = sketch.estimate(h2)
        if es2 >= es1:
            failed += 1
    # call add 8 times each loop, so total additions is 8000
    assert failed / 8000 < 0.1
