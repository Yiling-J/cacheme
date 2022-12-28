import xxhash


# generate 64bits hash of  key string
def hash_string(s: str) -> int:
    return xxhash.xxh64_intdigest(s)
