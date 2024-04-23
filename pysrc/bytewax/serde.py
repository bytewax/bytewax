"""Serialization for recovery and worker exchange.

This module provides a way to add custom ser/deserializers to Bytewax
that will be used internally to exchange data to other workers, in
the case of multiple processes, and to serialize data to be written
to the recovery store.

Using a custom serializer can result in better performance when
resuming a dataflow and when exchanging data between workers.

:::{warning}

Take care when implementing a custom Serde class to account for
things like schema evolution, and the handling of all data types
that are exchanged between workers which can include Bytewax
classes like {py:obj}`~bytewax.operators.window.WindowMetadata`.

Creating a custom Serde class can result in performance gains,
but should be considered an advanced optimization.

:::

By default, Python's {py:obj}`pickle` is used for serialization.
"""

import pickle
from abc import ABC, abstractmethod
from typing import Any

from typing_extensions import override

from bytewax._bytewax import (
    set_serde_obj,
)

__all__ = [
    "set_serde_obj",
    "Serde",
    "PickleSerde",
]


class Serde(ABC):
    """A serialization encoder/decoder.

    This must support serializing arbitray Python objects and
    reconstituting them exactly. This means using things like
    `json.dumps` and `json.loads` directly will not work, as they do
    not support things like datetimes, integer keys, etc.

    Even if all of your dataflow's state is serializeable by a format,
    Bytewax generates Python objects to store internal data, and they
    must round-trip correctly or there will be errors.

    """

    @abstractmethod
    def ser(self, obj: Any) -> bytes:
        """Serialize the given object."""
        ...

    @abstractmethod
    def de(self, s: bytes) -> Any:
        """Deserialize the given object."""
        ...


class PickleSerde(Serde):
    """Serialize objects using `pickle`."""

    @override
    def ser(self, obj: Any) -> bytes:
        return pickle.dumps(obj)

    @override
    def de(self, s: bytes) -> Any:
        return pickle.loads(s)
