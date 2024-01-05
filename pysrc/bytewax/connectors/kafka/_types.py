from typing import Optional, TypeVar, Union

K = TypeVar("K")  # Key
K_co = TypeVar("K_co", covariant=True)
V = TypeVar("V")  # Value
V_co = TypeVar("V_co", covariant=True)
K2 = TypeVar("K2")  # Modified Key
V2 = TypeVar("V2")  # Modified Value
MaybeStrBytes = Optional[Union[str, bytes]]
