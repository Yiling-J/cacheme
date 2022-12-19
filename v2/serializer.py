import importlib
import base64
import pickle
import json
import lzma
from typing import Any, Protocol, cast
import msgpack

import pydantic
from pydantic.json import pydantic_encoder


def to_qualified_name(obj: Any) -> str:
    return obj.__module__ + "." + obj.__qualname__


def from_qualified_name(name: str) -> Any:
    try:
        module = importlib.import_module(name)
        return module
    except ImportError:
        # If no subitem was included raise the import error
        if "." not in name:
            raise

    # Otherwise, we'll try to load it as an attribute of a module
    mod_name, attr_name = name.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    return getattr(module, attr_name)


def prefect_json_object_encoder(obj: Any) -> Any:
    """
    `JSONEncoder.default` for encoding objects into JSON with extended type support.

    Raises a `TypeError` to fallback on other encoders on failure.
    """
    return {
        "__class__": to_qualified_name(obj.__class__),
        "data": pydantic_encoder(obj),
    }


def prefect_json_object_decoder(result: dict):
    """
    `JSONDecoder.object_hook` for decoding objects from JSON when previously encoded
    with `prefect_json_object_encoder`
    """
    if "__class__" in result:
        return pydantic.parse_obj_as(
            from_qualified_name(result["__class__"]), result["data"]
        )
    return result


class Serializer(Protocol):
    def dumps(self, obj: Any) -> bytes:
        ...

    def loads(self, blob: bytes) -> Any:
        ...


class PickleSerializer:
    def dumps(self, obj: Any) -> bytes:
        blob = pickle.dumps(obj)
        return base64.encodebytes(blob)

    def loads(self, blob: bytes) -> Any:
        return pickle.loads(base64.decodebytes(blob))


class JSONSerializer:
    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj, default=prefect_json_object_encoder).encode()

    def loads(self, blob: bytes) -> Any:
        return json.loads(blob.decode(), object_hook=prefect_json_object_decoder)


class MsgPackSerializer:
    def dumps(self, obj: Any) -> bytes:
        return cast(bytes, msgpack.dumps(obj, default=prefect_json_object_encoder))

    def loads(self, blob: bytes) -> Any:
        return msgpack.loads(blob, object_hook=prefect_json_object_decoder)


class CompressedSerializer:
    serializer: Serializer

    def dumps(self, obj: Any) -> bytes:
        blob = self.serializer.dumps(obj)
        return lzma.compress(blob)

    def loads(self, blob: bytes) -> Any:
        uncompressed = lzma.decompress(blob)
        return self.serializer.loads(uncompressed)


class CompressedPickleSerializer(CompressedSerializer):
    serializer: Serializer = PickleSerializer()


class CompressedJSONSerializer(CompressedSerializer):

    serializer: Serializer = JSONSerializer()


class CompressedMsgPackSerializer(CompressedSerializer):
    serializer: Serializer = MsgPackSerializer()
