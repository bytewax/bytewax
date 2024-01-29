"""Serialization for recovery and transport."""

from abc import ABC, abstractmethod
from typing import Any, cast

import jsonpickle
from typing_extensions import override


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
    def ser(obj: Any) -> str:
        """Serialize the given object."""
        ...

    @staticmethod
    @abstractmethod
    def de(s: str) -> Any:
        """Deserialize the given object."""
        ...


class JsonPickleSerde(Serde):
    """Serialize objects using `jsonpickle`.

    See [`jsonpickle`](https://github.com/jsonpickle/jsonpickle) for
    more info.

    """

    @override
    @staticmethod
    def ser(obj: Any) -> str:
        # Enable `keys`, otherwise all __dict__ keys are coereced to
        # strings, which might not be true in general. `jsonpickle`
        # isn't at typed library, so we have to cast here.
        return cast(str, jsonpickle.encode(obj, keys=True))

    @override
    @staticmethod
    def de(s: str) -> Any:
        return jsonpickle.decode(s, keys=True)
