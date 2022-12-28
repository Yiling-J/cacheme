import xxhash
from ctypes import c_uint64


def bc():
    data = []
    for i in range(100):
        k = f"hhhh:ffdd:fff:{i}"
        data.append(xxhash.xxh64_intdigest(k))


def bc2():
    data = (c_uint64 * 100)()
    for i in range(100):
        k = f"hhhh:ffdd:fff:{i}"
        data[i] = xxhash.xxh64_intdigest(k)


if __name__ == "__main__":
    bc()
