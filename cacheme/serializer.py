import base64
import importlib
import json
import pickle
import zlib
from types import ModuleType
from typing import Any, Dict, cast

import msgpack
import pydantic
from pydantic.json import pydantic_encoder
from typing_extensions import Protocol


class Serializer(Protocol):
    def dumps(self, obj: Any) -> bytes:
        raise NotImplementedError()

    def loads(self, blob: bytes) -> Any:
        raise NotImplementedError()


def to_qualified_name(obj: Any) -> str:
    return obj.__module__ + "." + obj.__qualname__


__import_cache: Dict[str, Any] = {}


def from_qualified_name(name: str) -> Any:
    module = __import_cache.get(name, None)
    if module is not None:
        return module
    try:
        module = importlib.import_module(name)
        __import_cache[name] = module
        return module
    except ImportError:
        # If no subitem was included raise the import error
        if "." not in name:
            raise

    # Otherwise, we'll try to load it as an attribute of a module
    mod_name, attr_name = name.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    imported = getattr(module, attr_name)
    __import_cache[name] = imported
    return imported


def object_encoder(obj: Any) -> Any:
    return {
        "__class__": to_qualified_name(obj.__class__),
        "data": pydantic_encoder(obj),
    }


def object_decoder(result: dict):
    if "__class__" in result:
        return pydantic.parse_obj_as(
            from_qualified_name(result["__class__"]), result["data"]
        )
    return result


class PickleSerializer:
    def dumps(self, obj: Any) -> bytes:
        blob = pickle.dumps(obj)
        return base64.encodebytes(blob)

    def loads(self, blob: bytes) -> Any:
        return pickle.loads(base64.decodebytes(blob))


class JSONSerializer:
    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj, default=object_encoder).encode()

    def loads(self, blob: bytes) -> Any:
        return json.loads(blob.decode(), object_hook=object_decoder)


class MsgPackSerializer:
    def dumps(self, obj: Any) -> bytes:
        return cast(bytes, msgpack.dumps(obj, default=object_encoder))

    def loads(self, blob: bytes) -> Any:
        return msgpack.loads(blob, object_hook=object_decoder, strict_map_key=False)


class CompressedSerializer:
    serializer: Serializer

    def dumps(self, obj: Any) -> bytes:
        blob = self.serializer.dumps(obj)
        return zlib.compress(blob, level=3)

    def loads(self, blob: bytes) -> Any:
        uncompressed = zlib.decompress(blob)
        return self.serializer.loads(uncompressed)


class CompressedPickleSerializer(CompressedSerializer):
    serializer: Serializer = PickleSerializer()


class CompressedJSONSerializer(CompressedSerializer):

    serializer: Serializer = JSONSerializer()


class CompressedMsgPackSerializer(CompressedSerializer):
    serializer: Serializer = MsgPackSerializer()
