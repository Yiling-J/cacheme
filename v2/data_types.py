from functools import cached_property

from tinylfu.hash import hash_string

from dataclasses import dataclass


@dataclass
class CacheKey:
    node: str
    prefix: str
    key: str
    version: str
    tags: list[str]

    @property
    def full_key(self) -> str:
        return f"{self.prefix}:{self.key}:{self.version}"

    @cached_property
    def hash(self) -> int:
        return hash_string(self.full_key)
