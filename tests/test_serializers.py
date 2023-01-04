from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest
from pydantic import BaseModel

from cacheme.serializer import *

TUPLE_TO_LIST = 1
JSON_ONLY = 2
PICKLE = 3
JSON = 4
MSGPACK = 5


class Foo:
    a = 1
    b = 2

    def __init__(self, q):
        self.c = q

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.a}:{self.b}:{self.c}")


@dataclass
class Bar:
    a: int
    b: str


class FooBar(BaseModel):
    id: int
    name = "foo bar"


@pytest.mark.parametrize(
    "data",
    [
        {"d": None, "s": [PICKLE, JSON, MSGPACK]},
        {"d": True, "s": [PICKLE, JSON, MSGPACK]},
        {"d": False, "s": [PICKLE, JSON, MSGPACK]},
        {"d": [], "s": [PICKLE, JSON, MSGPACK]},
        {"d": {}, "s": [PICKLE, JSON, MSGPACK]},
        {"d": (), "s": [PICKLE, JSON, MSGPACK]},
        {"d": 1, "s": [PICKLE, JSON, MSGPACK]},
        {"d": 1.23, "s": [PICKLE, JSON, MSGPACK]},
        {"d": "foo", "s": [PICKLE, JSON, MSGPACK]},
        {"d": [1, 2, 3], "s": [PICKLE, JSON, MSGPACK]},
        {"d": (1, 2, 3), "s": [PICKLE, JSON, MSGPACK]},
        {"d": datetime.now(), "s": [PICKLE, MSGPACK]},
        {"d": timedelta(seconds=20), "s": [PICKLE, MSGPACK]},
        {
            "d": {
                "a": "a",
                "b": 2,
                "ll": [1, 2, "3", {"a": "b"}],
            },
            "s": [PICKLE, JSON, MSGPACK],
        },
        {"d": Foo(10), "s": [PICKLE]},
        {"d": Bar(a=1, b="12"), "s": [PICKLE, JSON, MSGPACK]},
        {"d": FooBar(id=12), "s": [PICKLE, JSON, MSGPACK]},
    ],
)
@pytest.mark.parametrize(
    "serializer_data",
    [
        {
            "n": PICKLE,
            "s": [PickleSerializer(), CompressedPickleSerializer()],
            "tags": [],
        },
        {
            "n": MSGPACK,
            "s": [MsgPackSerializer(), CompressedMsgPackSerializer()],
            "tags": [TUPLE_TO_LIST],
        },
        {
            "n": JSON,
            "s": [JSONSerializer(), CompressedJSONSerializer()],
            "tags": [TUPLE_TO_LIST],
        },
    ],
)
def test_serializers(data, serializer_data):
    if serializer_data["n"] not in data["s"]:
        return
    value = data["d"]
    for serializer in serializer_data["s"]:
        deserialized = serializer.dumps(value)
        serialized = serializer.loads(deserialized)
        if TUPLE_TO_LIST in serializer_data["tags"] and isinstance(value, tuple):
            value = list(value)
        assert serialized == value
