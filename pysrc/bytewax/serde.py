"""
"""

from abc import ABC, abstractmethod
from typing import Any

import jsonpickle

__all__ = [
    "JsonPickleSerde",
    "Serde",
]


class Serde(ABC):
    """ """

    @staticmethod
    @abstractmethod
    def ser(obj: Any) -> str:
        """ """
        ...

    @staticmethod
    @abstractmethod
    def de(s: str) -> Any:
        """ """
        ...


class JsonPickleSerde(Serde):
    """Serialize objects using
    [`jsonpickle`](https://github.com/jsonpickle/jsonpickle)."""

    @staticmethod
    def ser(obj):
        return jsonpickle.encode(obj, keys=True)

    @staticmethod
    def de(s):
        return jsonpickle.decode(s, keys=True)
