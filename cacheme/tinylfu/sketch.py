from typing import Tuple


def next_power_of_two(i: int) -> int:
    n = i - 1
    n |= n >> 1
    n |= n >> 2
    n |= n >> 4
    n |= n >> 8
    n |= n >> 16
    n += 1
    return n


class CountMinSketch:
    def __init__(self, width: int):
        self.row_counter_size = next_power_of_two(width * 3)
        self.row_i64_size = int(self.row_counter_size / 16)
        self.row_mask = self.row_counter_size - 1
        self.table = [0] * (self.row_counter_size >> 2)
        self.additions = 0
        self.sample_size = 10 * self.row_counter_size

    def index_of(self, h: int, offset: int) -> Tuple[int, int]:
        h1 = h & 0xFFFFFFFF
        h1 += offset * (h >> 32)
        i = h1 & self.row_mask
        index = offset * self.row_i64_size + (i >> 4)
        offset = (i & 0xF) << 2
        return index, offset

    def add(self, keyh: int):
        index0, offset0 = self.index_of(keyh, 0)
        index1, offset1 = self.index_of(keyh, 1)
        index2, offset2 = self.index_of(keyh, 2)
        index3, offset3 = self.index_of(keyh, 3)

        added = self.inc(index0, offset0)
        added |= self.inc(index1, offset1)
        added |= self.inc(index2, offset2)
        added |= self.inc(index3, offset3)

        if added:
            self.additions += 1
            if self.additions == self.sample_size:
                self.reset()

    def inc(self, i: int, offset: int) -> bool:
        mask = 0xF << offset
        if self.table[i] & mask != mask:
            self.table[i] += 1 << offset
            return True
        return False

    def reset(self):
        self.table = [((counter >> 1) & 0x7777777777777777) for counter in self.table]
        self.additions = self.additions >> 1

    def __get_count(self, keyh: int, depth: int) -> int:
        index, offset = self.index_of(keyh, depth)
        return (self.table[index] >> offset) & 0xF

    def estimate(self, keyh: int) -> int:
        count0 = self.__get_count(keyh, 0)
        count1 = self.__get_count(keyh, 1)
        count2 = self.__get_count(keyh, 2)
        count3 = self.__get_count(keyh, 3)
        return min(count0, count1, count2, count3)
