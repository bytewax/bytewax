"""Serialization for recovery and transport."""

import logging
import pickle
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import msgspec
from typing_extensions import override

logger = logging.getLogger(__name__)


class Serde(ABC):
    """A serialization format.

    This must support serializing arbitray Python objects and
    reconstituting them exactly. This means using things like
    `json.dumps` and `json.loads` directly will not work, as they do
    not support things like datetimes, integer keys, etc.

    Even if all of your dataflow's state is serializeable by a format,
    Bytewax generates Python objects to store internal data, and they
    must round-trip correctly or there will be errors.

    """

    @staticmethod
    @abstractmethod
    def ser(obj: Any) -> bytes:
        """Serialize the given object."""
        ...

    @staticmethod
    @abstractmethod
    def de(s: bytes) -> Any:
        """Deserialize the given object."""
        ...


class PickleSerde(Serde):
    """Serialize objects using `pickle`."""

    @override
    @staticmethod
    def ser(obj: Any) -> bytes:
        return pickle.dumps(obj)

    @override
    @staticmethod
    def de(s: bytes) -> Any:
        return pickle.loads(s)


class TestData(msgspec.Struct, kw_only=True, tag="testdata"):
    timestamp: datetime


def unpack(x: bytes) -> TestData:
    return msgspec.msgpack.decode(x, type=TestData)
