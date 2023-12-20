from typing import Optional, TypeVar, Union

K = TypeVar("K")  # Key
V = TypeVar("V")  # Value
K2 = TypeVar("K2")  # Modified Key
V2 = TypeVar("V2")  # Modified Value
MaybeStrBytes = Optional[Union[str, bytes]]
