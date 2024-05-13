"""Small generic utility functions."""

from typing import Callable, Iterable, List, Tuple, TypeVar

X = TypeVar("X")


def partition(
    it: Iterable[X], predicate: Callable[[X], bool]
) -> Tuple[List[X], List[X]]:
    """Split an iterable in two based on a predicate.

    :arg it: Input iterable.

    :arg predicate: Call this on each item.

    :returns: A list of items for which the predicate is true, and a
        list for which it is false.

    """
    # This has been measured to be faster than doing double filtering
    # list comprehensions or using `itertools.tee`.
    trues = []
    falses = []
    for x in it:
        if predicate(x):
            trues.append(x)
        else:
            falses.append(x)

    return (trues, falses)
