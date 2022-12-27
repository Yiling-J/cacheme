import math
from ctypes import c_uint64


def next_power_of_two(i: int) -> int:
    n = i - 1
    n |= n >> 1
    n |= n >> 2
    n |= n >> 4
    n |= n >> 8
    n |= n >> 16
    n += 1
    return n


class BloomFilter:
    def __init__(self, insertions: int, fpp: float):
        self.insertions = insertions
        ln2 = math.log(2)
        factor = -math.log(fpp) / (ln2 * ln2)
        bits = next_power_of_two(int(insertions * factor))
        if bits == 0:
            bits = 1
        self.bits_mask = bits - 1
        self.slice_count = int(ln2 * bits / insertions)
        int64_size = int((bits + 63) / 64)
        self.bits = (c_uint64 * int64_size)()
        self.additions = 0

    def put(self, keyh: int) -> bool:
        self.additions += 1
        if self.additions == self.insertions:
            self.reset()
        h1 = keyh & 0xFFFFFFFF
        h2 = keyh >> 32
        o = True
        for i in range(self.slice_count):
            o &= self.set((h1 + (i * h2)) & self.bits_mask)
        return o

    def contains(self, keyh: int) -> bool:
        h1 = keyh & 0xFFFFFFFF
        h2 = keyh >> 32
        o = True
        for i in range(self.slice_count):
            o &= self.get((h1 + (i * h2)) & self.bits_mask)
        return o

    def set(self, h: int) -> bool:
        idx = h >> 6
        offset = h & 63
        val = self.bits[idx]
        mask = 1 << offset
        self.bits[idx] |= mask
        return bool((val & mask) >> offset)

    def get(self, h: int) -> bool:
        idx = h >> 6
        offset = h & 63
        val = self.bits[idx]
        mask = 1 << offset
        return bool((val & mask) >> offset)

    def reset(self):
        for i in range(len(self.bits)):
            self.bits[i] = 0
