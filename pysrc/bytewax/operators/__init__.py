"""Built-in operators.

See the `bytewax` module docstring for the basics of building and
running dataflows.

See `bytewax.operators.window` for windowing operators.

# Reading Operator Documentation

Operators are used by calling the builder methods that are loaded onto
usually the `bytewax.dataflow.Stream` class. Because these methods are
dynamically loaded, their API docs do not actually live on that class,
though, they live where the operators are defined. For built-in
operators, that is this module.

Each operator appears as a `class` definition with the builder method
defined on that. So for example, the builder method you would use to
add a `map` step lives at the `"map.map"` method.

This builder method should be read just like any other method defined
on a class: the first argument is the `self` (and will be before the
`.` of the method call), and the rest of the arguments are provided in
the `()` of the call site.

The type annotation on the first argument is the class that the
builder method is loaded onto. This is usually `Stream`, but might be
`KeyedStream` or `Dataflow` or another class.

# Quick Logic Functions

Many of the operators take **logic functions** which help you
customize their behavior in a structured way. The most verbose way
would be to `def logic(...)` a function that does what you need to do,
but any callable value can be used as-is, though!

This means you can use the following existing callables to help you
make code more concise:

- [Built-in
  functions](https://docs.python.org/3/library/functions.html)

- [Constructors or
  `__init__`](https://docs.python.org/3/tutorial/classes.html#class-objects)

- [Methods](https://docs.python.org/3/glossary.html#term-method)

You can also use
[lambdas](https://docs.python.org/3/tutorial/controlflow.html#lambda-expressions)
to quickly define one-off anonymous functions for simple custom logic.

For example, all of the following dataflows are equivalent.

Using a defined function:

>>> import bytewax.operators as op
>>> from bytewax.dataflow import Dataflow
>>> from bytewax.testing import TestingSource, run_main
>>> flow = Dataflow("use_def")
>>> def split_sentence(sentence):
...     return sentence.split()
>>> s = op.input("inp", flow, TestingSource(["hello world"]))
>>> s = op.flat_map("split", s, split_sentence)
>>> op.inspect("out", s)
>>> run_main(flow)
use_def.out: 'hello'
use_def.out: 'world'

Or a lambda:

>>> flow = Dataflow("use_lambda")
>>> s = op.input("inp", flow, TestingSource(["hello world"]))
>>> s = op.flat_map("split", s, lambda s: s.split())
>>> op.inspect("out", s)
>>> run_main(flow)
use_lambda.out: 'hello'
use_lambda.out: 'world'

Or an unbound method:

>>> flow = Dataflow("use_method")
>>> s = op.input("inp", flow, TestingSource(["hello world"]))
>>> s = op.flat_map("split", s, str.split)
>>> op.inspect("out", s)
>>> run_main(flow)
use_method.out: 'hello'
use_method.out: 'world'

# Non-Built-In Operators

All operators in this module are automatically loaded when you run
import any submodule of `bytewax`.

To use operators defined in other packages, import their functions.

# Custom Operators

See the `bytewax.dataflow` module docstring for how to define your own
custom operators.

"""

import copy
import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    overload,
)

from bytewax.dataflow import (
    Dataflow,
    MultiStream,
    Stream,
    f_repr,
    operator,
)
from bytewax.inputs import Source
from bytewax.outputs import Sink

X = TypeVar("X")  # Item
Y = TypeVar("Y")  # Output Item
V = TypeVar("V")  # Value
W = TypeVar("W")  # Output Value
S = TypeVar("S")  # State
KeyedStream = Stream[Tuple[str, V]]


def _identity(x: X) -> X:
    return x


def _untyped_none() -> Any:
    return None


@dataclass(frozen=True)
class BranchOut(Generic[X]):
    """Streams returned from `branch` operator.

    You can tuple unpack this for convenience.

    """

    trues: Stream[X]
    falses: Stream[X]

    def __iter__(self):
        return iter((self.trues, self.falses))


@operator(_core=True)
def branch(
    step_id: str,
    up: Stream[X],
    predicate: Callable[[X], bool],
) -> BranchOut[X]:
    """Divide items into two streams with a predicate.

    >>> import bytewax.operators as op
    >>> from bytewax.testing import run_main, TestingSource
    >>> flow = Dataflow("branch_eg")
    >>> nums = op.input("nums", flow, TestingSource([1, 2, 3, 4, 5]))
    >>> evens, odds = op.branch("even_odd", nums, lambda x: x % 2 == 0)
    >>> op.inspect("evens", evens)
    >>> op.inspect("odds", odds)
    >>> run_main(flow)
    branch_eg.odds: 1
    branch_eg.evens: 2
    branch_eg.odds: 3
    branch_eg.evens: 4
    branch_eg.odds: 5

    Args:
        step_id: Unique ID.

        up: Stream to divide.

        predicate: Function to call on each upstream item. Items for
            which this returns `True` will be put into one branch
            `Stream`; `False` the other branc `Stream`.h

    Returns:
        A stream of items for which the predicate returns `True`, and
        a stream of items for which the predicate returns `False`.

    """
    return BranchOut(
        trues=Stream(f"{up._scope.parent_id}.trues", up._scope),
        falses=Stream(f"{up._scope.parent_id}.falses", up._scope),
    )


@operator(_core=True)
def flat_map(
    step_id: str,
    up: Stream[X],
    mapper: Callable[[X], Iterable[Y]],
) -> Stream[Y]:
    """Transform items one-to-many.

    This is like a combination of `map` and `flatten`.

    It is commonly used for:

    - Tokenizing

    - Flattening hierarchical objects

    - Breaking up aggregations for further processing

    >>> import bytewax.operators as op
    >>> from bytewax.testing import TestingSource, run_main
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("flat_map_eg")
    >>> inp = ["hello world"]
    >>> s = op.input("inp", flow, TestingSource(inp))
    >>> def split_into_words(sentence):
    ...     return sentence.split()
    >>> s = op.flat_map("split_words", s, split_into_words)
    >>> op.inspect("out", s)
    >>> run_main(flow)
    flat_map_eg.out: 'hello'
    flat_map_eg.out: 'world'

    Args:
        step_id: Unique ID.

        up: Stream.

        mapper: Called once on each upstream item. Returns the items
            to emit downstream.

    Returns:
        A stream of each item returned by the mapper.

    """
    return Stream(f"{up._scope.parent_id}.down", up._scope)


@operator(_core=True)
def input(  # noqa: A001
    step_id: str,
    flow: Dataflow,
    source: Source[X],
) -> Stream[X]:
    """Introduce items into a dataflow.

    See `bytewax.inputs` for more information on how input works. See
    `bytewax.connectors` for a buffet of our built-in connector types.

    Args:
        step_id: Unique ID.

        flow: The dataflow.

        source: Read items from.

    Returns:
        A stream of items from the source. See your specific source
        documentation for what kind of item that is.

        This stream might be keyed. See your specific
        `Source.stream_typ`.

    """
    return Stream(f"{flow._scope.parent_id}.down", flow._scope)


def _default_debug_inspector(step_id: str, item: Any, epoch: int, worker: int) -> None:
    print(f"{step_id} W{worker} @{epoch}: {item!r}", flush=True)


@operator(_core=True)
def inspect_debug(
    step_id: str,
    up: Stream[X],
    inspector: Callable[[str, X, int, int], None] = _default_debug_inspector,
) -> None:
    """Observe items, their worker, and their epoch for debugging.

    >>> import bytewax.operators as op
    >>> from bytewax.testing import TestingSource, run_main
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("inspect_debug_eg")
    >>> s = op.input("inp", flow, TestingSource(range(3)))
    >>> op.inspect_debug("help", s)
    >>> run_main(flow)
    inspect_debug_eg.help W0 @1: 0
    inspect_debug_eg.help W0 @1: 1
    inspect_debug_eg.help W0 @1: 2

    Args:
        step_id: Unique ID.

        up: Stream.

        inspector: Called with the step ID, each item in the stream,
            the epoch of that item, and the worker processing the
            item. Defaults to printing out all the arguments.

    """
    return None


@operator(_core=True)
def merge(step_id: str, *ups: Stream[X]) -> Stream[X]:
    """Combine multiple streams together.

    Args:
        step_id: Unique ID.

        *ups: Streams.

    Returns:
        A single stream of the same type as all the upstreams with
        items from all upstreams merged into it unmodified.

    """
    up_scopes = set(up._scope for up in ups)
    if len(up_scopes) < 1:
        msg = "`merge` operator requires at least one upstream"
        raise TypeError(msg)
    else:
        assert len(up_scopes) == 1  # @operator guarantees this.
        scope = next(iter(up_scopes))

    return Stream(f"{scope.parent_id}.down", scope)


@operator(_core=True)
def output(step_id: str, up: Stream[X], sink: Sink[X]) -> None:
    """Write items out of a dataflow.

    See `bytewax.outputs` for more information on how output works.
    See `bytewax.connectors` for a buffet of our built-in connector
    types.

    Args:
        step_id: Unique ID.

        up: Stream of items to write. See your specific sink
            documentation for the required type of those items.

        sink: Write items to.

    """
    return None


@operator(_core=True)
def redistribute(step_id: str, up: Stream[X]) -> Stream[X]:
    """Redistribute items randomly across all workers.

    Bytewax's execution model has workers executing all steps, but the
    state in each step is partitioned across workers by some key.
    Bytewax will only exchange an item between workers before stateful
    steps in order to ensure correctness, that they interact with the
    correct state for that key. Stateless operators (like `filter`)
    are run on all workers and do not result in exchanging items
    before or after they are run.

    This can result in certain ordering of operators to result in poor
    parallelization across an entire execution cluster. If the
    previous step (like a `reduce_window` or `input` with a
    `PartitionedInput`) concentrated items on a subset of workers in
    the cluster, but the next step is a CPU-intensive stateless step
    (like a `map`), it's possible that not all workers will contribute
    to processing the CPU-intesive step.

    This operation has a overhead, since it will need to serialize,
    send, and deserialize the items, so while it can significantly
    speed up the execution in some cases, it can also make it slower.

    A good use of this operator is to parallelize an IO bound step,
    like a network request, or a heavy, single-cpu workload, on a
    machine with multiple workers and multiple cpu cores that would
    remain unused otherwise.

    A bad use of this operator is if the operation you want to
    parallelize is already really fast as it is, as the overhead can
    overshadow the advantages of distributing the work. Another case
    where you could see regressions in performance is if the heavy CPU
    workload already spawns enough threads to use all the available
    cores. In this case multiple processes trying to compete for the
    cpu can end up being slower than doing the work serially. If the
    workers run on different machines though, it might again be a
    valuable use of the operator.

    Use this operator with caution, and measure whether you get an
    improvement out of it.

    Once the work has been spread to another worker, it will stay on
    those workers unless other operators explicitely move the item
    again (usually on output).

    Args:
        step_id: Unique ID.

        up: Stream.

    Returns:
        Stream unmodified.

    """
    return Stream(f"{up._scope.parent_id}.down", up._scope)


class UnaryLogic(ABC, Generic[V, W, S]):
    """Abstract class to define the behavior of a `unary` operator.

    The operator will call these methods in order: `on_item` once for
    any items queued, then `on_notify` if the notification time has
    passed, then `on_eof` if the upstream is EOF and no new items will
    be received this execution. If the logic is retained after all the
    above calls then `notify_at` will be called. `snapshot` is
    periodically called.

    """

    #: This logic should be retained after this call to `on_*`.
    #
    #: If you always return this, this state will never be deleted and
    #: if your key-space grows without bound, your memory usage will
    #: also grow without bound.
    RETAIN: bool = False

    #: This logic should be discarded immediately after this call to
    #: `on_*`.
    DISCARD: bool = True

    @abstractmethod
    def on_item(self, now: datetime, value: V) -> Tuple[List[W], bool]:
        """Called on each new upstream item.

        This will be called multiple times in a row if there are
        multiple items from upstream.

        Args:
            now: The current `datetime`.

            value: The value of the upstream `(key, value)`.

        Returns:
            A 2-tuple of: any values to emit downstream and wheither
            to discard this logic. Values will be wrapped in `(key,
            value)` automatically.

        """
        ...

    @abstractmethod
    def on_notify(self, sched: datetime) -> Tuple[List[W], bool]:
        """Called when the scheduled notification time has passed.

        Args:
            sched: The scheduled notification time.

        Returns:
            A 2-tuple of: any values to emit downstream and wheither
            to discard this logic. Values will be wrapped in `(key,
            value)` automatically.

        """
        ...

    @abstractmethod
    def on_eof(self) -> Tuple[List[W], bool]:
        """The upstream has no more items on this execution.

        This will only be called once per execution after `on_item` is
        done being called.

        Returns:
            A 2-tuple of: any values to emit downstream and wheither
            to discard this logic. Values will be wrapped in `(key,
            value)` automatically.

        """
        ...

    @abstractmethod
    def notify_at(self) -> Optional[datetime]:
        """Return the next notification time.

        This will be called once right after the logic is built, and
        if any of the `on_*` methods were called if the logic was
        retained by `is_complete`.

        This must always return the next notification time. The
        operator only stores a single next time, so if

        Returns:
            Scheduled time. If `None`, no `on_notify` callback will
            occur.

        """
        ...

    @abstractmethod
    def snapshot(self) -> S:
        """Return a immutable copy of the state for recovery.

        This will be called periodically by the runtime.

        The value returned here will be passed back to the `builder`
        function of `unary.unary` when resuming.

        The state must be `pickle`-able.

        **The state must be effectively immutable!** If any of the
        other functions in this class might be able to mutate the
        state, you must `copy.deepcopy` or something equivalent before
        returning it here.

        """
        ...


@operator(_core=True)
def unary(
    step_id: str,
    up: KeyedStream[V],
    builder: Callable[[datetime, Optional[S]], UnaryLogic[V, W, S]],
) -> KeyedStream[W]:
    """Advanced generic stateful operator.

    This is the lowest-level operator Bytewax provides and gives you
    full control over all aspects of the operator processing and
    lifecycle. Usualy you will want to use a higher-level operator
    than this.

    Subclass `UnaryLogic` to define its behavior. See documentation
    there.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        builder: Called whenver a new key is encountered with the
            current `datetime` and the resume state returned from
            `UnaryLogic.snapshot` for this key, if any. This should
            close over any non-state configuration and combine it with
            the resume state to return the prepared `UnaryLogic` for
            the new key.

    Returns:
        Keyed stream of all items returned from `UnaryLogic.on_item`,
        `UnaryLogic.on_notify`, and `UnaryLogic.on_eof`.

    """
    return Stream(f"{up._scope.parent_id}.down", up._scope)


@dataclass
class _BatchState(Generic[V]):
    acc: List[V] = field(default_factory=list)
    timeout_at: Optional[datetime] = None


@dataclass
class _BatchLogic(UnaryLogic[V, List[V], _BatchState[V]]):
    step_id: str
    timeout: timedelta
    batch_size: int
    state: _BatchState[V]

    def on_item(self, now: datetime, v: V) -> Tuple[List[List[V]], bool]:
        self.state.timeout_at = now + self.timeout

        self.state.acc.append(v)
        if len(self.state.acc) >= self.batch_size:
            # No need to deepcopy because we are discarding the state.
            return ([self.state.acc], UnaryLogic.DISCARD)

        return ([], UnaryLogic.RETAIN)

    def on_notify(self, sched: datetime) -> Tuple[List[List[V]], bool]:
        return ([self.state.acc], UnaryLogic.DISCARD)

    def on_eof(self) -> Tuple[List[List[V]], bool]:
        return ([self.state.acc], UnaryLogic.DISCARD)

    def notify_at(self) -> Optional[datetime]:
        return self.state.timeout_at

    def snapshot(self) -> _BatchState[V]:
        return copy.deepcopy(self.state)


@operator
def batch(
    step_id: str, up: KeyedStream[V], timeout: timedelta, batch_size: int
) -> KeyedStream[List[V]]:
    """Batch incoming items up to a size or a timeout.

    Args:
        step_id: Unique ID.

        up: Stream of individual items.

        timeout: Timeout before emitting the batch, even if max_size
            was not reached.

        batch_size: Maximum size of the batch.

    Returns:
        A stream of batches of upstream items gathered into a `list`.

    """

    def shim_builder(
        _now: datetime, resume_state: Optional[_BatchState[V]]
    ) -> _BatchLogic[V]:
        state = resume_state if resume_state is not None else _BatchState()
        return _BatchLogic(step_id, timeout, batch_size, state)

    return unary("unary", up, shim_builder)


@operator
def count_final(
    step_id: str, up: Stream[X], key: Callable[[X], str]
) -> KeyedStream[int]:
    """Count the number of occurrences of items in the entire stream.

    This will only return counts once the upstream is EOF. You'll need
    to use `count_window` on infinite data.

    Args:
        step_id: Unique ID.

        up: Stream of items to count.

        key: Function to convert each item into a string key. The
            counting machinery does not compare the items directly,
            instead it groups by this string key.

    Returns:
        A stream of `(key, count)` once the upstream is EOF.

    """
    down: KeyedStream[int] = map("init_count", up, lambda x: (key(x), 1))
    return reduce_final("sum", down, lambda s, x: s + x)


@operator
def flat_map_value(
    step_id: str,
    up: KeyedStream[V],
    mapper: Callable[[V], Iterable[W]],
) -> KeyedStream[W]:
    """Transform values one-to-many.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        mapper: Called once on each upstream value. Returns the values
            to emit downstream.

    Returns:
        A keyed stream of each value returned by the mapper.

    """

    def shim_mapper(k_v: Tuple[str, V]) -> Iterable[Tuple[str, W]]:
        try:
            k, v = k_v
        except TypeError as ex:
            msg = (
                f"step {step_id!r} requires `(key, value)` 2-tuple "
                "as upstream for routing; "
                f"got a {type(k_v)!r} instead"
            )
            raise TypeError(msg) from ex
        ws = mapper(v)
        return ((k, w) for w in ws)

    return flat_map("flat_map", up, shim_mapper)


@operator
def flatten(
    step_id: str,
    up: Stream[Iterable[X]],
) -> Stream[X]:
    """Move all sub-items up a level.

    Args:
        step_id: Unique ID.

        up: Stream of iterables.

    Returns:
        A stream of the items within each iterable in the upstream.

    """

    def shim_mapper(x: Iterable[X]) -> Iterable[X]:
        if not isinstance(x, Iterable):
            msg = (
                f"step {step_id!r} requires upstream to be iterables; "
                f"got a {type(x)!r} instead"
            )
            raise TypeError(msg)

        return x

    return flat_map("flat_map", up, shim_mapper)


@operator
def filter(  # noqa: A001
    step_id: str, up: Stream[X], predicate: Callable[[X], bool]
) -> Stream[X]:
    """Keep only some items.

    It is commonly used for:

    - Selecting relevant events

    - Removing empty events

    - Removing sentinels

    - Removing stop words

    >>> import bytewax.operators as op
    >>> from bytewax.testing import TestingSource, run_main
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("filter_eg")
    >>> s = op.input("inp", flow, TestingSource(range(4)))
    >>> def is_odd(item):
    ...     return item % 2 != 0
    >>> s = op.filter("filter_odd", s, is_odd)
    >>> op.inspect("out", s)
    >>> run_main(flow)
    filter_eg.out: 1
    filter_eg.out: 3

    Args:
        step_id: Unique ID.

        up: Stream.

        predicate: Called with each upstream item. Only items for
            which this returns true `True` will be emitted downstream.

    Returns:
        A stream with only the upstream items for which the predicate
        returns `True`.

    """

    def shim_mapper(x: X) -> List[X]:
        keep = predicate(x)
        if not isinstance(keep, bool):
            msg = (
                f"return value of `predicate` {f_repr(predicate)} "
                f"in step {step_id!r} must be a `bool`; "
                f"got a {type(keep)!r} instead"
            )
            raise TypeError(msg)
        if keep:
            return [x]

        return []

    return flat_map("flat_map", up, shim_mapper)


@operator
def filter_value(
    step_id: str, up: KeyedStream[V], predicate: Callable[[V], bool]
) -> KeyedStream[V]:
    """Selectively keep only some items from a keyed stream.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        predicate: Will be called with each upstream value. Only
            values for which this returns `True` will be emitted
            downstream.

    Returns:
        A keyed stream with only the upstream pairs for which the
        predicate returns `True`.

    """

    def shim_mapper(v: V) -> List[V]:
        keep = predicate(v)
        if not isinstance(keep, bool):
            msg = (
                f"return value of `predicate` {f_repr(predicate)} "
                f"in step {step_id!r} must be a `bool`; "
                f"got a {type(keep)!r} instead"
            )
            raise TypeError(msg)
        if keep:
            return [v]

        return []

    return flat_map_value("filter", up, shim_mapper)


@operator
def filter_map(
    step_id: str, up: Stream[X], mapper: Callable[[X], Optional[Y]]
) -> Stream[Y]:
    """A one-to-maybe-one transformation of items.

    This is like a combination of `map` and then `filter` with a
    predicate removing `None` values.

    >>> import bytewax.operators as op
    >>> from bytewax.testing import TestingSource, run_main
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("filter_map_eg")
    >>> s = op.input("inp", flow, TestingSource([
    ...     {"key": "a", "val": 1},
    ...     {"bad": "obj"},
    ... ]))
    >>> def validate(data):
    ...     if type(data) != dict or "key" not in data:
    ...         return None
    ...     else:
    ...         return data["key"], data
    >>> s = op.filter_map("validate", s, validate)
    >>> op.inspect("out", s)
    >>> run_main(flow)
    filter_map_eg.out: ('a', {'key': 'a', 'val': 1})

    Args:
        step_id: Unique ID.

        up: Stream.

        mapper: Called on each item. Each return value is emitted
            downstream, unless it is `None`.

    Returns:
        A stream of items returned from the mapper, unless it is
        `None`.

    """

    def shim_mapper(x: X) -> List[Y]:
        y = mapper(x)
        if y is not None:
            return [y]

        return []

    return flat_map("flat_map", up, shim_mapper)


@dataclass
class _FoldFinalLogic(UnaryLogic[V, S, S]):
    step_id: str
    folder: Callable[[S, V], S]
    state: S

    def on_item(self, _now: datetime, v: V) -> Tuple[List[S], bool]:
        self.state = self.folder(self.state, v)
        return ([], UnaryLogic.RETAIN)

    def on_notify(self, _s: datetime) -> Tuple[List[S], bool]:
        return ([], UnaryLogic.RETAIN)

    def on_eof(self) -> Tuple[List[S], bool]:
        # No need to deepcopy because we are discarding the state.
        return ([self.state], UnaryLogic.DISCARD)

    def notify_at(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> S:
        return copy.deepcopy(self.state)


@operator
def fold_final(
    step_id: str,
    up: KeyedStream[V],
    builder: Callable[[], S],
    folder: Callable[[S, V], S],
) -> KeyedStream[S]:
    """Build an empty accumulator, then combine values into it.

    It is like `reduce_final` but uses a function to build the initial
    value.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        builder: Called the first time a key appears and is expected
            to return the empty accumulator for that key.

        folder: Combines a new value into an existing accumulator and
            returns the updated accumulator. The accumulator is
            initially the empty accumulator.

    Returns:
        A keyed stream of the accumulators. _Only once the upstream is
        EOF._

    """

    def shim_builder(
        _now: datetime, resume_state: Optional[S]
    ) -> _FoldFinalLogic[V, S]:
        state = resume_state if resume_state is not None else builder()
        return _FoldFinalLogic(step_id, folder, state)

    return unary("unary", up, shim_builder)


def _default_inspector(step_id: str, item: Any) -> None:
    print(f"{step_id}: {item!r}", flush=True)


@operator
def inspect(
    step_id: str,
    up: Stream[X],
    inspector: Callable[[str, X], None] = _default_inspector,
) -> None:
    """Observe items for debugging.

    >>> import bytewax.operators as op
    >>> from bytewax.testing import run_main, TestingSource
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("my_flow")
    >>> s = op.input("inp", flow, TestingSource(range(3)))
    >>> op.inspect("help", s)
    >>> run_main(flow)
    my_flow.help: 0
    my_flow.help: 1
    my_flow.help: 2

    Args:
        step_id: Unique ID.

        up: Stream.

        inspector: Called with the step ID and each item in the
            stream. Defaults to printing the step ID and each item.

    """

    def shim_inspector(
        _fq_step_id: str, item: X, _epoch: int, _worker_idx: int
    ) -> None:
        inspector(step_id, item)

    return inspect_debug("inspect_debug", up, shim_inspector)


@dataclass
class _JoinState(Generic[V]):
    seen: Dict[str, List[V]]

    @classmethod
    def for_names(cls, names: List[str]) -> "_JoinState[V]":
        return cls({name: [] for name in names})

    def set_val(self, name: str, value: V) -> None:
        self.seen[name] = [value]

    def add_val(self, name: str, value: V) -> None:
        self.seen[name].append(value)

    def is_set(self, name: str) -> bool:
        return len(self.seen[name]) > 0

    def all_set(self) -> bool:
        return all(self.is_set(name) for name in self.seen.keys())

    def astuples(self, empty: Any = None) -> List[Tuple]:
        values = self.seen.values()
        return list(
            itertools.product(*(vals if len(vals) > 0 else [empty] for vals in values))
        )

    def asdicts(self) -> List[Dict[str, V]]:
        EMPTY = object()
        ts = self.astuples(empty=EMPTY)
        dicts = []
        for t in ts:
            dicts.append(
                dict((n, v) for n, v in zip(self.seen.keys(), t) if v is not EMPTY)
            )
        return dicts


@dataclass
class _JoinLogic(UnaryLogic[Tuple[str, V], _JoinState[V], _JoinState[V]]):
    step_id: str
    running: bool
    state: _JoinState[V]

    def on_item(
        self, _now: datetime, name_value: Tuple[str, V]
    ) -> Tuple[List[_JoinState[V]], bool]:
        name, value = name_value

        self.state.set_val(name, value)

        if self.running:
            return ([copy.deepcopy(self.state)], UnaryLogic.RETAIN)
        else:
            if self.state.all_set():
                # No need to deepcopy because we are discarding the state.
                return ([self.state], UnaryLogic.DISCARD)
            else:
                return ([], UnaryLogic.RETAIN)

    def on_notify(self, _s: datetime) -> Tuple[List[_JoinState[V]], bool]:
        return ([], UnaryLogic.RETAIN)

    def on_eof(self) -> Tuple[List[_JoinState[V]], bool]:
        return ([], UnaryLogic.RETAIN)

    def notify_at(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> _JoinState[V]:
        return copy.deepcopy(self.state)


@operator
def _join_name_merge(
    step_id: str,
    **named_ups: KeyedStream[V],
) -> KeyedStream[Tuple[str, V]]:
    with_names = [
        # Horrible mess, see
        # https://docs.astral.sh/ruff/rules/function-uses-loop-variable/
        map_value(f"name_{name}", up, partial(lambda name, v: (name, v), name))
        for name, up in named_ups.items()
    ]
    return merge("merge", *with_names)


@operator
def join(
    step_id: str,
    *sides: KeyedStream[V],
    running: bool = False,
) -> KeyedStream[Tuple]:
    """Gather together the value for a key on multiple streams.

    Args:
        step_id: Unique ID.

        *sides: Keyed streams.

        running: If `True`, emit the current set of values (if any)
            each time a new value arrives. The set of values will
            _never be discarded_ so might result in unbounded memory
            use. If `False`, only emit once there is a value on each
            stream, then discard the set. Defaults to `False`.

    Returns:
        Emits a tuple with the value from each stream in the order of
        the argument list. If `running` is `True`, some values might
        be `None`.

    """
    named_sides = dict((str(i), s) for i, s in enumerate(sides))
    names = list(named_sides.keys())

    def shim_builder(
        _now: datetime, resume_state: Optional[_JoinState[V]]
    ) -> _JoinLogic[V]:
        state = (
            resume_state if resume_state is not None else _JoinState.for_names(names)
        )
        return _JoinLogic(step_id, running, state)

    merged = _join_name_merge("add_names", **named_sides)
    joined = unary("join", merged, shim_builder)
    return flat_map_value("astuple", joined, _JoinState.astuples)


@operator
def join_named(
    step_id: str,
    running: bool = False,
    **sides: KeyedStream[V],
) -> KeyedStream[Dict[str, V]]:
    """Gather together the value for a key on multiple named streams.

    Args:
        step_id: Unique ID.

        **sides: Named keyed streams. The name of each stream will be
            used in the emitted `dict`s.

        running: If `True`, emit the current set of values (if any)
            each time a new value arrives. The set of values will
            _never be discarded_ so might result in unbounded memory
            use. If `False`, only emit once there is a value on each
            stream, then discard the set. Defaults to `False`.

    Returns:
        Emits a `dict` mapping the name to the value from each stream.
        If `running` is `True`, some names might be missing from the
        `dict`.

    """
    names = list(sides.keys())

    def shim_builder(
        _now: datetime, resume_state: Optional[_JoinState[V]]
    ) -> _JoinLogic[V]:
        state = (
            resume_state if resume_state is not None else _JoinState.for_names(names)
        )
        return _JoinLogic(step_id, running, state)

    merged = _join_name_merge("add_names", **sides)
    joined = unary("join", merged, shim_builder)
    return flat_map_value("asdict", joined, _JoinState.asdicts)


@operator
def key_on(step_id: str, up: Stream[X], key: Callable[[X], str]) -> KeyedStream[X]:
    """Add a key for each item.

    This allows you to use all the keyed operators that only are
    methods on `KeyedStream`.

    Args:
        step_id: Unique ID.

        up: Stream.

        key: Called on each item and should return the key for that
            item.

    Returns:
        A stream of 2-tuples of `(key, item)` AKA a keyed stream. The
        keys come from the return value of the `key` function;
        upstream items will automatically be attached as values.

    """

    def shim_mapper(x: X) -> Tuple[str, X]:
        k = key(x)
        if not isinstance(k, str):
            msg = (
                f"return value of `key` {f_repr(key)} "
                f"in step {step_id!r} must be a `str`; "
                f"got a {type(k)!r} instead"
            )
            raise TypeError(msg)
        return (k, x)

    return map("map", up, shim_mapper)


@operator
def key_split(
    step_id: str,
    up: Stream[X],
    key: Callable[[X], str],
    *values: Callable[[X], V],
) -> MultiStream[Tuple[str, V]]:
    """Split objects apart into a separate stream for each field.

    This allows you to use all the keyed operators that only are
    methods on `KeyedStream`.

    Args:
        step_id: Unique ID.

        up: Stream.

        key: Called on each item and should return the key for that
            item.

        *values: A "field getter" which

    Returns:
        A set of streams of 2-tuples of `(key, value)` AKA a keyed
        streams. The keys come from the return value of the `key`
        function; the return value of each `values` function will be
        the value.

    """
    keyed_up = key_on("key", up, key)
    streams = {
        str(i): map_value(f"value_{str(i)}", keyed_up, value)
        for i, value in enumerate(values)
    }
    return MultiStream(streams)


@operator
def map(  # noqa: A001
    step_id: str,
    up: Stream[X],
    mapper: Callable[[X], Y],
) -> Stream[Y]:
    """Transform items one-by-one.

    It is commonly used for:

    - Serialization and deserialization.

    - Selection of fields.

    >>> import bytewax.operators as op
    >>> from bytewax.testing import run_main, TestingSource
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("map_eg")
    >>> s = op.input("inp", flow, TestingSource(range(3)))
    >>> def add_one(item):
    ...     return item + 10
    >>> s = op.map("add_one", s, add_one)
    >>> op.inspect("out", s)
    >>> run_main(flow)
    map_eg.out: 10
    map_eg.out: 11
    map_eg.out: 12

    Args:
        step_id: Unique ID.

        up: Stream.

        mapper: Called on each item. Each return value is emitted
            downstream.

    Returns:
        A stream of items returned from the mapper.
    """

    def shim_mapper(x: X) -> List[Y]:
        y = mapper(x)
        return [y]

    return flat_map("flat_map", up, shim_mapper)


@operator
def map_value(
    step_id: str, up: KeyedStream[V], mapper: Callable[[V], W]
) -> KeyedStream[W]:
    """Transform values one-by-one.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        mapper: Called on each value. Each return value is emitted
            downstream.

    Returns:
        A keyed stream of values returned from the mapper. The key is
        unchanged.

    """

    def shim_mapper(v: V) -> List[W]:
        w = mapper(v)
        return [w]

    return flat_map_value("flat_map_value", up, shim_mapper)


@overload
def max_final(
    step_id: str,
    up: KeyedStream[V],
) -> KeyedStream[V]:
    ...


@overload
def max_final(
    step_id: str,
    up: KeyedStream[V],
    by: Callable[[V], Any],
) -> KeyedStream[V]:
    ...


@operator
def max_final(
    step_id: str,
    up: KeyedStream[V],
    by=_identity,
) -> KeyedStream:
    """Find the maximum value for each key.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        by: A function called on each value that is used to extract
            what to compare.

    Returns:
        A keyed stream of the max values. _Only once the upstream is
        EOF._

    """
    return reduce_final("reduce_final", up, partial(max, key=by))


@overload
def min_final(
    step_id: str,
    up: KeyedStream[V],
) -> KeyedStream[V]:
    ...


@overload
def min_final(
    step_id: str,
    up: KeyedStream[V],
    by: Callable[[V], Any],
) -> KeyedStream[V]:
    ...


@operator
def min_final(
    step_id: str,
    up: KeyedStream[V],
    by=_identity,
) -> KeyedStream:
    """Find the minumum value for each key.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        by: A function called on each value that is used to extract
            what to compare.

    Returns:
        A keyed stream of the min values. _Only once the upstream is
        EOF._

    """
    return reduce_final("reduce_final", up, partial(min, key=by))


@operator
def reduce_final(
    step_id: str,
    up: KeyedStream[V],
    reducer: Callable[[V, V], V],
) -> KeyedStream[V]:
    """Distill all values for a key down into a single value.

    It is like `fold_final` but the first value is the initial
    accumulator.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        reducer: Combines a new value into an old value and returns
            the combined value.

    Returns:
        A keyed stream of the accumulators. _Only once the upstream is
        EOF._

    """

    def shim_folder(s: V, v: V) -> V:
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return fold_final("fold_final", up, _untyped_none, shim_folder)


@dataclass
class _StatefulMapLogic(UnaryLogic[V, W, S]):
    step_id: str
    mapper: Callable[[S, V], Tuple[Optional[S], W]]
    state: S

    def on_item(self, _now: datetime, v: V) -> Tuple[List[W], bool]:
        res = self.mapper(self.state, v)
        try:
            s, w = res
        except TypeError as ex:
            msg = (
                f"return value of `mapper` {f_repr(self.mapper)} "
                f"in step {self.step_id!r} "
                "must be a 2-tuple of `(updated_state, emit_item)`; "
                f"got a {type(res)!r} instead"
            )
            raise TypeError(msg) from ex
        if s is None:
            # No need to update state as we're thowing everything
            # away.
            return ([w], UnaryLogic.DISCARD)
        else:
            self.state = s
            return ([w], UnaryLogic.RETAIN)

    def on_notify(self, _s: datetime) -> Tuple[List[W], bool]:
        return ([], UnaryLogic.RETAIN)

    def on_eof(self) -> Tuple[List[W], bool]:
        return ([], UnaryLogic.RETAIN)

    def notify_at(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> S:
        return copy.deepcopy(self.state)


@operator
def stateful_map(
    step_id: str,
    up: KeyedStream[V],
    builder: Callable[[], S],
    mapper: Callable[[S, V], Tuple[Optional[S], W]],
) -> KeyedStream[W]:
    """Transform values one-to-one, referencing a persistent state.

    It is commonly used for:

    - Anomaly detection

    - State machines

    >>> import bytewax.operators as op
    >>> from bytewax.testing import TestingSource, run_main
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("stateful_map_eg")
    >>> inp = [
    ...     "a",
    ...     "a",
    ...     "a",
    ...     "b",
    ...     "a",
    ... ]
    >>> s = op.input("inp", flow, TestingSource(inp))
    >>> s = op.key_on("self_as_key", s, lambda x: x)
    >>> def build_count():
    ...     return 0
    >>> def check(running_count, _item):
    ...     running_count += 1
    ...     return (running_count, running_count)
    >>> s = op.stateful_map("running_count", s, build_count, check)
    >>> op.inspect("out", s)
    >>> run_main(flow)
    stateful_map_eg.out: ('a', 1)
    stateful_map_eg.out: ('a', 2)
    stateful_map_eg.out: ('a', 3)
    stateful_map_eg.out: ('b', 1)
    stateful_map_eg.out: ('a', 4)

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        builder: Called whenever a new key is encountered and should
            return the "empty state" for this key.

        mapper: Called whenever a value is encountered from upstream
            with the last state, and then the upstream value. Should
            return a 2-tuple of `(updated_state, emit_value)`. If the
            updated state is `None`, discard it.

    Returns:
        A keyed stream.

    """

    def shim_builder(
        _now: datetime, resume_state: Optional[S]
    ) -> _StatefulMapLogic[V, W, S]:
        state = resume_state if resume_state is not None else builder()
        return _StatefulMapLogic(step_id, mapper, state)

    return unary("unary", up, shim_builder)
