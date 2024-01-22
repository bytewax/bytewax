"""Helper functions for using operators."""

from typing import Callable, Dict, TypeVar

K = TypeVar("K")
V = TypeVar("V")


def map_dict_value(
    key: K, mapper: Callable[[V], V]
) -> Callable[[Dict[K, V]], Dict[K, V]]:
    """Build a function to map an item in a dict and return the dict.

    Use this to help build mapper functions for the
    {py:obj}`~bytewax.operators.map` operator that work on a specific
    value in a dict, but leave the other values untouched.

    ```python
    >>> import bytewax.operators as op
    >>> from bytewax.testing import run_main, TestingSource
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("lens_item_map_eg")
    >>> s = op.input(
    ...     "inp",
    ...     flow,
    ...     TestingSource(
    ...         [
    ...             {"name": "Rachel White", "email": "rachel@white.com"},
    ...             {"name": "John Smith", "email": "john@smith.com"},
    ...         ]
    ...     ),
    ... )
    >>> def normalize(name):
    ...     return name.upper()
    >>> s = op.map("normalize", s, map_dict_value("name", normalize))
    >>> _ = op.inspect("out", s)
    >>> run_main(flow)
    lens_item_map_eg.out: {'name': 'RACHEL WHITE', 'email': 'rachel@white.com'}
    lens_item_map_eg.out: {'name': 'JOHN SMITH', 'email': 'john@smith.com'}
    ```

    This type of "do an operation on a known nested structure" is
    called a **lens**. If you'd like to produce more complex lenses,
    see the [`lenses`](https://github.com/ingolemo/python-lenses)
    package. It handles many more nuances of this problem like mutable
    vs immutable data types, attributes vs keys, and mutating methods
    vs returning functions. You can use it to build mappers for
    Bytewax operators.


    :arg key: Dictionary key.

    :arg mapper: Function to run on the value for that key.

    :returns: A function which will perform that mapping operation
        when called.

    """

    def shim_mapper(obj: Dict[K, V]) -> Dict[K, V]:
        obj[key] = mapper(obj[key])
        return obj

    return shim_mapper
