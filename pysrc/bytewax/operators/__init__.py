"""Built-in operators.

See <project:#getting-started> for the basics of building and running
dataflows.

"""

import copy
import itertools
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
from itertools import chain
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from bytewax.dataflow import (
    Dataflow,
    Stream,
    f_repr,
    operator,
)
from bytewax.inputs import Source
from bytewax.outputs import DynamicSink, Sink, StatelessSinkPartition
from typing_extensions import Self, TypeAlias, TypeGuard, override

X = TypeVar("X")
"""Type of upstream items."""


Y = TypeVar("Y")
"""Type of modified downstream items."""


U = TypeVar("U")
"""Type of secondary upstream values."""


V = TypeVar("V")
"""Type of upstream values."""


W = TypeVar("W")
"""Type of modified downstream values."""


W_co = TypeVar("W_co", covariant=True)
"""Type of modified downstream values in a covariant container."""


S = TypeVar("S")
"""Type of state snapshots."""


DK = TypeVar("DK")
"""Type of {py:obj}`dict` keys."""


DV = TypeVar("DV")
"""Type of {py:obj}`dict` values."""


KeyedStream: TypeAlias = Stream[Tuple[str, V]]
"""A {py:obj}`~bytewax.dataflow.Stream` of `(key, value)` 2-tuples."""

_EMPTY: Tuple = tuple()


def _identity(x: X) -> X:
    return x


def _untyped_none() -> Any:
    return None


def _get_system_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(frozen=True)
class BranchOut(Generic[X, Y]):
    """Streams returned from the {py:obj}`branch` operator."""

    trues: Stream[X]
    falses: Stream[Y]


@overload
def branch(
    step_id: str,
    up: Stream[X],
    predicate: Callable[[X], TypeGuard[Y]],
) -> BranchOut[Y, X]: ...


@overload
def branch(
    step_id: str,
    up: Stream[X],
    predicate: Callable[[X], bool],
) -> BranchOut[X, X]: ...


@operator(_core=True)
def branch(
    step_id: str,
    up: Stream[X],
    predicate: Callable[[X], bool],
) -> BranchOut:
    """Divide items into two streams with a predicate.

    ```{testcode}
    import bytewax.operators as op
    from bytewax.dataflow import Dataflow
    from bytewax.testing import run_main, TestingSource

    flow = Dataflow("branch_eg")
    nums = op.input("nums", flow, TestingSource([1, 2, 3, 4, 5]))
    b_out = op.branch("even_odd", nums, lambda x: x % 2 == 0)
    evens = b_out.trues
    odds = b_out.falses
    _ = op.inspect("evens", evens)
    _ = op.inspect("odds", odds)
    ```

    ```{testcode}
    :hide:

    from bytewax.testing import run_main

    run_main(flow)
    ```

    ```{testoutput}
    branch_eg.odds: 1
    branch_eg.evens: 2
    branch_eg.odds: 3
    branch_eg.evens: 4
    branch_eg.odds: 5
    ```

    :arg step_id: Unique ID.

    :arg up: Stream to divide.

    :arg predicate: Function to call on each upstream item. Items for
        which this returns `True` will be put into one branch stream;
        `False` the other branch stream.

        If this function is a {py:obj}`typing.TypeGuard`, the
        downstreams will be properly typed.

    :returns: A stream of items for which the predicate returns
        `True`, and a stream of items for which the predicate returns
        `False`.

    """
    return BranchOut(
        trues=Stream(f"{up._scope.parent_id}.trues", up._scope),
        falses=Stream(f"{up._scope.parent_id}.falses", up._scope),
    )


@operator(_core=True)
def flat_map_batch(
    step_id: str,
    up: Stream[X],
    mapper: Callable[[List[X]], Iterable[Y]],
) -> Stream[Y]:
    """Transform an entire batch of items 1-to-many.

    The batch size received here depends on the exact behavior of the
    upstream input sources and operators. It should be used as a
    performance optimization when processing multiple items at once
    has much reduced overhead.

    See also the `batch_size` parameter on various input sources.

    See also the {py:obj}`collect` operator, which collects multiple
    items next to each other in a stream into a single list of them
    flowing through the stream.

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg mapper: Called once with each batch of items the runtime
        receives. Returns the items to emit downstream.

    :returns: A stream of each item returned by the mapper.

    """
    return Stream(f"{up._scope.parent_id}.down", up._scope)


@operator(_core=True)
def input(  # noqa: A001
    step_id: str,
    flow: Dataflow,
    source: Source[X],
) -> Stream[X]:
    """Introduce items into a dataflow.

    See {py:obj}`bytewax.inputs` for more information on how input
    works. See {py:obj}`bytewax.connectors` for a buffet of our
    built-in connector types.

    :arg step_id: Unique ID.

    :arg flow: The dataflow.

    :arg source: To read items from.

    :returns: A stream of items from the source. See your specific
        {py:obj}`~bytewax.inputs.Source` documentation for what kind
        of item that is.

        This stream might be keyed. See your specific
        {py:obj}`~bytewax.inputs.Source`.

    """
    return Stream(f"{flow._scope.parent_id}.down", flow._scope)


def _default_debug_inspector(step_id: str, item: Any, epoch: int, worker: int) -> None:
    print(f"{step_id} W{worker} @{epoch}: {item!r}", flush=True)


@operator(_core=True)
def inspect_debug(
    step_id: str,
    up: Stream[X],
    inspector: Callable[[str, X, int, int], None] = _default_debug_inspector,
) -> Stream[X]:
    """Observe items, their worker, and their epoch for debugging.

    ```{testcode}
    import bytewax.operators as op
    from bytewax.testing import TestingSource
    from bytewax.dataflow import Dataflow

    flow = Dataflow("inspect_debug_eg")
    s = op.input("inp", flow, TestingSource(range(3)))
    _ = op.inspect_debug("help", s)
    ```

    ```{testcode}
    :hide:

    from bytewax.testing import run_main

    run_main(flow)
    ```

    ```{testoutput}
    inspect_debug_eg.help W0 @1: 0
    inspect_debug_eg.help W0 @1: 1
    inspect_debug_eg.help W0 @1: 2
    ```

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg inspector: Called with the step ID, each item in the stream,
        the epoch of that item, and the worker processing the item.
        Defaults to printing out all the arguments.

    :returns: The upstream unmodified.

    """
    return Stream(f"{up._scope.parent_id}.down", up._scope)


@overload
def merge(
    step_id: str,
    up1: Stream[X],
    /,
) -> Stream[X]: ...


@overload
def merge(
    step_id: str,
    up1: Stream[X],
    up2: Stream[Y],
    /,
) -> Stream[Union[X, Y]]: ...


@overload
def merge(
    step_id: str,
    up1: Stream[X],
    up2: Stream[Y],
    up3: Stream[U],
    /,
) -> Stream[Union[X, Y, U]]: ...


@overload
def merge(
    step_id: str,
    up1: Stream[X],
    up2: Stream[Y],
    up3: Stream[U],
    up4: Stream[V],
    /,
) -> Stream[Union[X, Y, U, V]]: ...


@overload
def merge(
    step_id: str,
    *ups: Stream[X],
) -> Stream[X]: ...


@overload
def merge(
    step_id: str,
    *ups: Stream[Any],
) -> Stream[Any]: ...


@operator(_core=True)
def merge(
    step_id: str,
    *ups: Stream[Any],
) -> Stream[Any]:
    """Combine multiple streams together.

    :arg step_id: Unique ID.

    :arg *ups: Streams.

    :returns: A single stream of the same type as all the upstreams
        with items from all upstreams merged into it unmodified.

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

    See {py:obj}`bytewax.outputs` for more information on how output
    works. See {py:obj}`bytewax.connectors` for a buffet of our
    built-in connector types.

    :arg step_id: Unique ID.

    :arg up: Stream of items to write. See your specific
        {py:obj}`~bytewax.outputs.Sink` documentation for the required
        type of those items.

    :arg sink: Write items to.

    """
    return None


@operator(_core=True)
def redistribute(step_id: str, up: Stream[X]) -> Stream[X]:
    """Redistribute items randomly across all workers.

    Bytewax's execution model has workers executing all steps, but the
    state in each step is partitioned across workers by some key.
    Bytewax will only exchange an item between workers before stateful
    steps in order to ensure correctness, that they interact with the
    correct state for that key. Stateless operators (like
    {py:obj}`filter`) are run on all workers and do not result in
    exchanging items before or after they are run.

    This can result in certain ordering of operators to result in poor
    parallelization across an entire execution cluster. If the
    previous step (like a
    {py:obj}`bytewax.operators.windowing.reduce_window` or
    {py:obj}`input` with a
    {py:obj}`~bytewax.inputs.FixedPartitionedSource`) concentrated
    items on a subset of workers in the cluster, but the next step is
    a CPU-intensive stateless step (like a {py:obj}`map`), it's
    possible that not all workers will contribute to processing the
    CPU-intesive step.

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

    :arg step_id: Unique ID.

    :arg up: Stream.

    :returns: Stream unmodified.

    """
    return Stream(f"{up._scope.parent_id}.down", up._scope)


class StatefulBatchLogic(ABC, Generic[V, W, S]):
    """Abstract class to define a {py:obj}`stateful` operator.

    The operator will call these methods in order: {py:obj}`on_batch`
    once with all items queued, then {py:obj}`on_notify` if the
    notification time has passed, then {py:obj}`on_eof` if the
    upstream is EOF and no new items will be received this execution.
    If the logic is retained after all the above calls then
    {py:obj}`notify_at` will be called. {py:obj}`snapshot` is
    periodically called.

    """

    RETAIN: bool = False
    """This logic should be retained after this returns.

    If you always return this, this state will never be deleted and if
    your key-space grows without bound, your memory usage will also
    grow without bound.

    """

    DISCARD: bool = True
    """This logic should be discarded immediately after this returns."""

    @abstractmethod
    def on_batch(self, values: List[V]) -> Tuple[Iterable[W], bool]:
        """Called on each new upstream item.

        This will be called multiple times in a row if there are
        multiple items from upstream.

        :arg value: The value of the upstream `(key, value)`.

        :returns: A 2-tuple of: any values to emit downstream and
            wheither to discard this logic. Values will be wrapped in
            `(key, value)` automatically.

        """
        ...

    def on_notify(self) -> Tuple[Iterable[W], bool]:
        """Called when the scheduled notification time has passed.

        Defaults to emitting nothing and retaining the logic.

        :returns: A 2-tuple of: any values to emit downstream and
            wheither to discard this logic. Values will be wrapped in
            `(key, value)` automatically.

        """
        return (_EMPTY, StatefulBatchLogic.RETAIN)

    def on_eof(self) -> Tuple[Iterable[W], bool]:
        """The upstream has no more items on this execution.

        This will only be called once per awake after
        {py:obj}`on_batch` is called.

        Defaults to emitting nothing and retaining the logic.

        :returns: 2-tuple of: any values to emit downstream and
            wheither to discard this logic. Values will be wrapped in
            `(key, value)` automatically.

        """
        return (_EMPTY, StatefulBatchLogic.RETAIN)

    def notify_at(self) -> Optional[datetime]:
        """Return the next notification time.

        This will be called once right after the logic is built, and
        if any of the `on_*` methods were called if the logic was
        retained.

        This must always return the next notification time. The
        operator only stores a single next time, so if there are a
        series of times you would like to notify at, store all of them
        but only return the soonest.

        Defaults to returning `None`.

        :returns: Scheduled time. If `None`, no {py:obj}`on_notify`
            callback will occur.

        """
        return None

    @abstractmethod
    def snapshot(self) -> S:
        """Return a immutable copy of the state for recovery.

        This will be called periodically by the runtime.

        The value returned here will be passed back to the `builder`
        function of {py:obj}`stateful` when resuming.

        The state must be {py:obj}`pickle`-able.

        :::{danger}

        **The state must be effectively immutable!** If any of the
        other functions in this class might be able to mutate the
        state, you must {py:obj}`copy.deepcopy` or something
        equivalent before returning it here.

        :::

        :returns: The immutable state to be {py:obj}`pickle`d.

        """
        ...


@operator(_core=True)
def stateful_batch(
    step_id: str,
    up: KeyedStream[V],
    builder: Callable[[Optional[S]], StatefulBatchLogic[V, W, S]],
) -> KeyedStream[W]:
    """Advanced generic stateful operator.

    This is the lowest-level operator Bytewax provides and gives you
    full control over all aspects of the operator processing and
    lifecycle. Usualy you will want to use a higher-level operator
    than this.

    Subclass {py:obj}`StatefulBatchLogic` to define its behavior. See
    documentation there.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg builder: Called whenver a new key is encountered with the
        resume state returned from
        {py:obj}`StatefulBatchLogic.snapshot` for this key, if any.
        This should close over any non-state configuration and combine
        it with the resume state to return the prepared
        {py:obj}`StatefulBatchLogic` for the new key.

    :returns: Keyed stream of all items returned from
        {py:obj}`StatefulBatchLogic.on_batch`,
        {py:obj}`StatefulBatchLogic.on_notify`, and
        {py:obj}`StatefulBatchLogic.on_eof`.

    """
    return Stream(f"{up._scope.parent_id}.down", up._scope)


class StatefulLogic(ABC, Generic[V, W, S]):
    """Abstract class to define a {py:obj}`stateful` operator.

    The operator will call these methods in order: {py:obj}`on_item`
    once for any items queued, then {py:obj}`on_notify` if the
    notification time has passed, then {py:obj}`on_eof` if the
    upstream is EOF and no new items will be received this execution.
    If the logic is retained after all the above calls then
    {py:obj}`notify_at` will be called. {py:obj}`snapshot` is
    periodically called.

    """

    RETAIN: bool = False
    """This logic should be retained after this returns.

    If you always return this, this state will never be deleted and if
    your key-space grows without bound, your memory usage will also
    grow without bound.

    """

    DISCARD: bool = True
    """This logic should be discarded immediately after this returns."""

    @abstractmethod
    def on_item(self, value: V) -> Tuple[Iterable[W], bool]:
        """Called on each new upstream item.

        This will be called multiple times in a row if there are
        multiple items from upstream.

        :arg value: The value of the upstream `(key, value)`.

        :returns: A 2-tuple of: any values to emit downstream and
            wheither to discard this logic. Values will be wrapped in
            `(key, value)` automatically.

        """
        ...

    def on_notify(self) -> Tuple[Iterable[W], bool]:
        """Called when the scheduled notification time has passed.

        :returns: A 2-tuple of: any values to emit downstream and
            wheither to discard this logic. Values will be wrapped in
            `(key, value)` automatically.

        """
        return (_EMPTY, StatefulLogic.RETAIN)

    def on_eof(self) -> Tuple[Iterable[W], bool]:
        """The upstream has no more items on this execution.

        This will only be called once per execution after
        {py:obj}`on_item` is done being called.

        :returns: 2-tuple of: any values to emit downstream and
            wheither to discard this logic. Values will be wrapped in
            `(key, value)` automatically.

        """
        return (_EMPTY, StatefulLogic.RETAIN)

    def notify_at(self) -> Optional[datetime]:
        """Return the next notification time.

        This will be called once right after the logic is built, and
        if any of the `on_*` methods were called if the logic was
        retained.

        This must always return the next notification time. The
        operator only stores a single next time, so if

        :returns: Scheduled time. If `None`, no {py:obj}`on_notify`
            callback will occur.

        """
        return None

    @abstractmethod
    def snapshot(self) -> S:
        """Return a immutable copy of the state for recovery.

        This will be called periodically by the runtime.

        The value returned here will be passed back to the `builder`
        function of {py:obj}`stateful` when resuming.

        The state must be {py:obj}`pickle`-able.

        :::{danger}

        **The state must be effectively immutable!** If any of the
        other functions in this class might be able to mutate the
        state, you must {py:obj}`copy.deepcopy` or something
        equivalent before returning it here.

        :::

        :returns: The immutable state to be {py:obj}`pickle`d.

        """
        ...


@dataclass
class _StatefulLogic(StatefulBatchLogic[V, W, S]):
    logic: Optional[StatefulLogic[V, W, S]]
    builder: Callable[[Optional[S]], StatefulLogic[V, W, S]]

    @override
    def on_batch(self, values: List[V]) -> Tuple[Iterable[W], bool]:
        ws: List[W] = []
        for v in values:
            if self.logic is None:
                self.logic = self.builder(None)

            new_ws, discard = self.logic.on_item(v)

            ws.extend(new_ws)
            if discard:
                self.logic = None

        return (ws, self.logic is None)

    @override
    def on_notify(self) -> Tuple[Iterable[W], bool]:
        assert self.logic is not None
        return self.logic.on_notify()

    @override
    def on_eof(self) -> Tuple[Iterable[W], bool]:
        assert self.logic is not None
        return self.logic.on_eof()

    @override
    def notify_at(self) -> Optional[datetime]:
        assert self.logic is not None
        return self.logic.notify_at()

    @override
    def snapshot(self) -> S:
        assert self.logic is not None
        return self.logic.snapshot()


@operator
def stateful(
    step_id: str,
    up: KeyedStream[V],
    builder: Callable[[Optional[S]], StatefulLogic[V, W, S]],
) -> KeyedStream[W]:
    """Advanced generic stateful operator.

    This is a low-level operator Bytewax provides and gives you
    control over most aspects of the operator processing and
    lifecycle. Usualy you will want to use a higher-level operator
    than this. Also see {py:obj}`stateful_batch` for even more
    control.

    Subclass {py:obj}`StatefulLogic` to define its behavior. See
    documentation there.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg builder: Called whenver a new key is encountered with the
        resume state returned from {py:obj}`StatefulLogic.snapshot`
        for this key, if any. This should close over any non-state
        configuration and combine it with the resume state to return
        the prepared {py:obj}`StatefulLogic` for the new key.

    :returns: Keyed stream of all items returned from
        {py:obj}`StatefulLogic.on_item`,
        {py:obj}`StatefulLogic.on_notify`, and
        {py:obj}`StatefulLogic.on_eof`.

    """

    def shim_builder(resume_state: Optional[S]) -> _StatefulLogic[V, W, S]:
        inner_logic = builder(resume_state)
        return _StatefulLogic(inner_logic, builder)

    return stateful_batch("stateful_batch", up, shim_builder)


@dataclass
class _CollectState(Generic[V]):
    acc: List[V] = field(default_factory=list)
    timeout_at: Optional[datetime] = None


@dataclass
class _CollectLogic(StatefulLogic[V, List[V], _CollectState[V]]):
    step_id: str
    now_getter: Callable[[], datetime]
    timeout: timedelta
    max_size: int
    state: _CollectState[V]

    @override
    def on_item(self, value: V) -> Tuple[Iterable[List[V]], bool]:
        self.state.timeout_at = self.now_getter() + self.timeout

        self.state.acc.append(value)
        if len(self.state.acc) >= self.max_size:
            # No need to deepcopy because we are discarding the state.
            return ((self.state.acc,), StatefulLogic.DISCARD)

        return (_EMPTY, StatefulLogic.RETAIN)

    @override
    def on_notify(self) -> Tuple[Iterable[List[V]], bool]:
        return ((self.state.acc,), StatefulLogic.DISCARD)

    @override
    def on_eof(self) -> Tuple[Iterable[List[V]], bool]:
        return ((self.state.acc,), StatefulLogic.DISCARD)

    @override
    def notify_at(self) -> Optional[datetime]:
        return self.state.timeout_at

    @override
    def snapshot(self) -> _CollectState[V]:
        return copy.deepcopy(self.state)


@operator
def collect(
    step_id: str, up: KeyedStream[V], timeout: timedelta, max_size: int
) -> KeyedStream[List[V]]:
    """Collect items into a list up to a size or a timeout.

    See {py:obj}`bytewax.operators.windowing.collect_window` for more
    control over time.

    :arg step_id: Unique ID.

    :arg up: Stream of individual items.

    :arg timeout: Timeout before emitting the list, even if `max_size`
        was not reached.

    :arg max_size: Emit the list once it reaches this size, even if
        `timeout` was not reached.

    :returns: A stream of upstream items gathered into lists.

    """

    def shim_builder(resume_state: Optional[_CollectState[V]]) -> _CollectLogic[V]:
        now_getter = lambda: datetime.now(timezone.utc)
        state = resume_state if resume_state is not None else _CollectState()
        return _CollectLogic(step_id, now_getter, timeout, max_size, state)

    return stateful("stateful", up, shim_builder)


@operator
def count_final(
    step_id: str, up: Stream[X], key: Callable[[X], str]
) -> KeyedStream[int]:
    """Count the number of occurrences of items in the entire stream.

    This only works on finite data streams and only return counts once
    the upstream is EOF. You'll need to use
    {py:obj}`bytewax.operators.windowing.count_window` on infinite
    data.

    :arg step_id: Unique ID.

    :arg up: Stream of items to count.

    :arg key: Function to convert each item into a string key. The
        counting machinery does not compare the items directly,
        instead it groups by this string key.

    :returns: A stream of `(key, count)`. _Only once the upstream is
        EOF._

    """
    down: KeyedStream[int] = map("init_count", up, lambda x: (key(x), 1))
    return reduce_final("sum", down, lambda s, x: s + x)


@dataclass
class TTLCache(Generic[DK, DV]):
    """A simple TTL cache."""

    v_getter: Callable[[DK], DV]
    now_getter: Callable[[], datetime]
    ttl: timedelta
    _cache: Dict[DK, Tuple[datetime, DV]] = field(default_factory=dict)

    def get(self, k: DK) -> DV:
        """Get the cached value for a key.

        Will cache and return the updated value if TTL has expired.

        :arg k: Key.

        :returns: Value.

        """
        now = self.now_getter()
        try:
            ts, v = self._cache[k]
            if now - ts > self.ttl:
                raise KeyError()
        except KeyError:
            v = self.v_getter(k)
            self._cache[k] = (now, v)

        return v

    def remove(self, k: DK) -> None:
        """Remove the cached value for this key.

        :arg k: Key.

        """
        del self._cache[k]


@operator
def enrich_cached(
    step_id: str,
    up: Stream[X],
    getter: Callable[[DK], DV],
    mapper: Callable[[TTLCache[DK, DV], X], Y],
    ttl: timedelta = timedelta.max,
    _now_getter: Callable[[], datetime] = _get_system_utc,
) -> Stream[Y]:
    """Enrich / join items using a cached lookup.

    Use this if you'd like to join items in the dataflow with
    unsychronized pulled / polled data from an external service. This
    assumes that the joined data is static and will not emit updates.
    Since there is no integration with the recovery system, it's
    possible that results will change across resumes if the joined
    data source changes.

    The joined data is cached by a key you specify.

    ```{testcode}
    import bytewax.operators as op
    from bytewax.dataflow import Dataflow
    from bytewax.testing import TestingSource, run_main


    def query_icon_url_service(code):
        if code == "dog_ico":
            return "http://domain.invalid/static/dog_v1.png"
        elif code == "cat_ico":
            return "http://domain.invalid/static/cat_v2.png"
        elif code == "rabbit_ico":
            return "http://domain.invalid/static/rabbit_v1.png"


    flow = Dataflow("param_eg")
    inp = op.input(
        "inp",
        flow,
        TestingSource(
            [
                {"user_id": "1", "avatar_icon_code": "dog_ico"},
                {"user_id": "3", "avatar_icon_code": "rabbit_ico"},
                {"user_id": "2", "avatar_icon_code": "dog_ico"},
            ]
        ),
    )
    op.inspect("check_inp", inp)


    def icon_code_to_url(cache, msg):
        code = msg.pop("avatar_icon_code")
        msg["avatar_icon_url"] = cache.get(code)
        return msg


    with_urls = op.enrich_cached(
        "with_url",
        inp,
        query_icon_url_service,
        icon_code_to_url,
    )
    op.inspect("check_with_url", with_urls)
    ```

    If you have a join source which is push-based or need to emit
    updates when either side of the join changes, instead consider
    having that be a second {py:obj}`~bytewax.operators.input` to the
    dataflow and using a running {py:obj}`~bytewax.operators.join`.
    This reduces cache misses and startup overhead.

    Each worker will keep a local cache of values. There is no max
    size.

    You can also use a {py:obj}`~bytewax.operators.map` step in the
    same way to manage the cache yourself manually.

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg getter: On cache miss, get the new updated value for a key.

    :arg mapper: Called on each item with access to the cache. Each
        return value is emitted downstream.

    :arg ttl: Re-get values in the cache that are older than this.

    :returns: A stream of items returned by the mapper.

    """
    now = _now_getter()

    def batch_now_getter() -> datetime:
        nonlocal now
        return now

    cache = TTLCache(getter, batch_now_getter, ttl)

    def shim_mapper(xs: Iterable[X]) -> Iterable[Y]:
        nonlocal now

        now = _now_getter()
        for x in xs:
            yield mapper(cache, x)

    return flat_map_batch("flat_map_batch", up, shim_mapper)


@operator
def flat_map(
    step_id: str,
    up: Stream[X],
    mapper: Callable[[X], Iterable[Y]],
) -> Stream[Y]:
    """Transform items one-to-many.

    This is like a combination of {py:obj}`map` and {py:obj}`flatten`.

    It is commonly used for:

    - Tokenizing

    - Flattening hierarchical objects

    - Breaking up aggregations for further processing

    ```{testcode}
    import bytewax.operators as op
    from bytewax.testing import TestingSource
    from bytewax.dataflow import Dataflow

    flow = Dataflow("flat_map_eg")

    inp = ["hello world"]
    s = op.input("inp", flow, TestingSource(inp))

    def split_into_words(sentence):
        return sentence.split()

    s = op.flat_map("split_words", s, split_into_words)

    _ = op.inspect("out", s)
    ```

    ```{testcode}
    :hide:

    from bytewax.testing import run_main

    run_main(flow)
    ```

    ```{testoutput}
    flat_map_eg.out: 'hello'
    flat_map_eg.out: 'world'
    ```

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg mapper: Called once on each upstream item. Returns the items
        to emit downstream.

    :returns: A stream of each item returned by the mapper.

    """

    def shim_mapper(xs: List[X]) -> Iterable[Y]:
        return chain.from_iterable(mapper(x) for x in xs)

    return flat_map_batch("flat_map_batch", up, shim_mapper)


@operator
def flat_map_value(
    step_id: str,
    up: KeyedStream[V],
    mapper: Callable[[V], Iterable[W]],
) -> KeyedStream[W]:
    """Transform values one-to-many.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg mapper: Called once on each upstream value. Returns the
        values to emit downstream.

    :returns: A keyed stream of each value returned by the mapper.

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

    :arg step_id: Unique ID.

    :arg up: Stream of iterables.

    :returns: A stream of the items within each iterable in the
        upstream.

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

    ```{testcode}
    import bytewax.operators as op
    from bytewax.testing import TestingSource
    from bytewax.dataflow import Dataflow


    flow = Dataflow("filter_eg")
    s = op.input("inp", flow, TestingSource(range(4)))

    def is_odd(item):
        return item % 2 != 0

    s = op.filter("filter_odd", s, is_odd)
    _ = op.inspect("out", s)
    ```

    ```{testcode}
    :hide:

    from bytewax.testing import run_main

    run_main(flow)
    ```

    ```{testoutput}
    filter_eg.out: 1
    filter_eg.out: 3
    ```

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg predicate: Called with each upstream item. Only items for
        which this returns true `True` will be emitted downstream.

    :returns: A stream with only the upstream items for which the
        predicate returns `True`.

    """

    def shim_mapper(x: X) -> Iterable[X]:
        keep = predicate(x)
        if not isinstance(keep, bool):
            msg = (
                f"return value of `predicate` {f_repr(predicate)} "
                f"in step {step_id!r} must be a `bool`; "
                f"got a {type(keep)!r} instead"
            )
            raise TypeError(msg)
        if keep:
            return (x,)

        return _EMPTY

    return flat_map("flat_map", up, shim_mapper)


@operator
def filter_value(
    step_id: str, up: KeyedStream[V], predicate: Callable[[V], bool]
) -> KeyedStream[V]:
    """Selectively keep only some items from a keyed stream.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg predicate: Will be called with each upstream value. Only
        values for which this returns `True` will be emitted
        downstream.

    :returns: A keyed stream with only the upstream pairs for which
        the predicate returns `True`.

    """

    def shim_mapper(v: V) -> Iterable[V]:
        keep = predicate(v)
        if not isinstance(keep, bool):
            msg = (
                f"return value of `predicate` {f_repr(predicate)} "
                f"in step {step_id!r} must be a `bool`; "
                f"got a {type(keep)!r} instead"
            )
            raise TypeError(msg)
        if keep:
            return (v,)

        return _EMPTY

    return flat_map_value("filter", up, shim_mapper)


@operator
def filter_map(
    step_id: str, up: Stream[X], mapper: Callable[[X], Optional[Y]]
) -> Stream[Y]:
    """A one-to-maybe-one transformation of items.

    This is like a combination of `map` and then `filter` with a
    predicate removing `None` values.

    ```{testcode}
    import bytewax.operators as op
    from bytewax.testing import TestingSource
    from bytewax.dataflow import Dataflow

    flow = Dataflow("filter_map_eg")
    s = op.input(
        "inp",
        flow,
        TestingSource(
            [
                {"key": "a", "val": 1},
                {"bad": "obj"},
            ]
        ),
    )

    def validate(data):
        if type(data) != dict or "key" not in data:
            return None
        else:
            return data["key"], data

    s = op.filter_map("validate", s, validate)
    _ = op.inspect("out", s)
    ```

    ```{testcode}
    :hide:

    from bytewax.testing import run_main

    run_main(flow)
    ```

    ```{testoutput}
    filter_map_eg.out: ('a', {'key': 'a', 'val': 1})
    ```

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg mapper: Called on each item. Each return value is emitted
        downstream, unless it is `None`.

    :returns: A stream of items returned from `mapper`, unless it is
        `None`.

    """

    def shim_mapper(x: X) -> Iterable[Y]:
        y = mapper(x)
        if y is not None:
            return (y,)

        return _EMPTY

    return flat_map("flat_map", up, shim_mapper)


@operator
def filter_map_value(
    step_id: str,
    up: KeyedStream[V],
    mapper: Callable[[V], Optional[W]],
) -> KeyedStream[W]:
    """Transform values one-to-maybe-one.

    This is like a combination of {py:obj}`map_value` and then
    {py:obj}`filter_value` with a predicate removing `None` values.

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg mapper: Called on each value. Each return value is emitted
        downstream, unless it is `None`.

    :returns: A keyed stream of values returned from the mapper,
        unless the value is `None`. The key is unchanged.

    """

    def shim_mapper(v: V) -> Iterable[W]:
        w = mapper(v)
        if w is not None:
            return (w,)

        return _EMPTY

    return flat_map_value("flat_map_value", up, shim_mapper)


@dataclass
class _FoldFinalLogic(StatefulLogic[V, S, S]):
    step_id: str
    folder: Callable[[S, V], S]
    state: S

    @override
    def on_item(self, value: V) -> Tuple[Iterable[S], bool]:
        self.state = self.folder(self.state, value)
        return (_EMPTY, StatefulLogic.RETAIN)

    @override
    def on_eof(self) -> Tuple[Iterable[S], bool]:
        # No need to deepcopy because we are discarding the state.
        return ((self.state,), StatefulLogic.DISCARD)

    @override
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

    This only works on finite data streams and only returns a result
    once the upstream is EOF. You'll need to use
    {py:obj}`bytewax.operators.windowing.fold_window` or
    {py:obj}`bytewax.operators.stateful_flat_map` on infinite data.

    It is like {py:obj}`reduce_final` but uses a function to build the
    initial value.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg builder: Called the first time a key appears and is expected
        to return the empty accumulator for that key.

    :arg folder: Combines a new value into an existing accumulator and
        returns the updated accumulator. The accumulator is initially
        the empty accumulator.

    :returns: A keyed stream of the accumulators. _Only once the
        upstream is EOF._

    """

    def shim_builder(resume_state: Optional[S]) -> _FoldFinalLogic[V, S]:
        state = resume_state if resume_state is not None else builder()
        return _FoldFinalLogic(step_id, folder, state)

    return stateful("stateful", up, shim_builder)


def _default_inspector(step_id: str, item: Any) -> None:
    print(f"{step_id}: {item!r}", flush=True)


@operator
def inspect(
    step_id: str,
    up: Stream[X],
    inspector: Callable[[str, X], None] = _default_inspector,
) -> Stream[X]:
    """Observe items for debugging.

    ```{testcode}
    import bytewax.operators as op
    from bytewax.testing import TestingSource
    from bytewax.dataflow import Dataflow

    flow = Dataflow("my_flow")
    s = op.input("inp", flow, TestingSource(range(3)))
    _ = op.inspect("help", s)
    ```

    ```{testcode}
    :hide:

    from bytewax.testing import run_main

    run_main(flow)
    ```

    ```{testoutput}
    my_flow.help: 0
    my_flow.help: 1
    my_flow.help: 2
    ```

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg inspector: Called with the step ID and each item in the
        stream. Defaults to printing the step ID and each item.

    :returns: The upstream unmodified.

    """

    def shim_inspector(
        _fq_step_id: str, item: X, _epoch: int, _worker_idx: int
    ) -> None:
        inspector(step_id, item)

    return inspect_debug("inspect_debug", up, shim_inspector)


_LONE_NONE = [None]


@dataclass
class _JoinState:
    seen: List[List[Any]]

    @classmethod
    def for_side_count(cls, side_count: int) -> Self:
        return cls([[] for i in range(side_count)])

    def set_val(self, side: int, value: Any) -> None:
        self.seen[side] = [value]

    def add_val(self, side: int, value: Any) -> None:
        self.seen[side].append(value)

    def is_set(self, side: int) -> bool:
        return len(self.seen[side]) > 0

    def all_set(self) -> bool:
        return all(len(values) > 0 for values in self.seen)

    def astuples(self) -> List[Tuple]:
        return list(
            itertools.product(
                *(vals if len(vals) > 0 else _LONE_NONE for vals in self.seen)
            )
        )

    def clear(self) -> None:
        for values in self.seen:
            values.clear()

    def __iadd__(self, other: Self) -> Self:
        if len(self.seen) != len(other.seen):
            msg = "join states are not same cardinality"
            raise ValueError(msg)

        self.seen = [x + y for x, y in zip(self.seen, other.seen)]
        return self

    def __ior__(self, other: Self) -> Self:
        if len(self.seen) != len(other.seen):
            msg = "join states are not same cardinality"
            raise ValueError(msg)

        self.seen = [y if len(y) > 0 else x for x, y in zip(self.seen, other.seen)]
        return self


JoinInsertMode: TypeAlias = Literal["first", "last", "product"]
"""How to handle multiple values from a side during a join.

- *First*: Emit a row containing only the first value from a side, if
   any.

- *Last*: Emit a row containing only the last value from a side, if
   any.

- *Product*: Emit a row with every combination of values for all
   sides. This is similar to a SQL cross join.

"""


JoinEmitMode: TypeAlias = Literal["complete", "final", "running"]
"""When should a join emit rows downstream.

- *Complete*: Emit once a value has been seen from each side. Then
   discard the state.

- *Final*: Emit when the upstream is EOF or the window closes. Then
   discard the state.

   This mode only works on finite data streams and only returns a
   result once the upstream is EOF. You'll need to use a different
   mode on infinite data.

- *Running*: Emit every time a new value is seen on any side. Retain
   the state forever.

"""


@dataclass
class _JoinLogic(StatefulLogic[Tuple[int, Any], Tuple, _JoinState]):
    insert_mode: JoinInsertMode
    emit_mode: JoinEmitMode

    state: _JoinState

    @override
    def on_item(self, value: Tuple[int, Any]) -> Tuple[Iterable[Tuple], bool]:
        join_side, join_value = value
        if self.insert_mode == "first" and not self.state.is_set(join_side):
            self.state.set_val(join_side, join_value)
        elif self.insert_mode == "last":
            self.state.set_val(join_side, join_value)
        elif self.insert_mode == "product":
            self.state.add_val(join_side, join_value)

        if self.emit_mode == "complete" and self.state.all_set():
            return (self.state.astuples(), StatefulLogic.DISCARD)
        elif self.emit_mode == "running":
            return (self.state.astuples(), StatefulLogic.RETAIN)
        else:
            return (_EMPTY, StatefulLogic.RETAIN)

    @override
    def on_eof(self) -> Tuple[Iterable[Tuple], bool]:
        if self.emit_mode == "final":
            return (self.state.astuples(), StatefulLogic.DISCARD)
        else:
            return (_EMPTY, StatefulLogic.RETAIN)

    @override
    def snapshot(self) -> _JoinState:
        return copy.deepcopy(self.state)


@operator
def _join_label_merge(
    step_id: str,
    *ups: KeyedStream[Any],
) -> KeyedStream[Tuple[int, Any]]:
    with_labels = [
        # Horrible mess, see
        # https://docs.astral.sh/ruff/rules/function-uses-loop-variable/
        map_value(f"label_{i}", up, partial(lambda i, v: (i, v), i))
        for i, up in enumerate(ups)
    ]
    return merge("merge", *with_labels)


# https://stackoverflow.com/questions/73200382/using-typevartuple-with-inner-typevar
@overload
def join(
    step_id: str,
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: Literal["complete"] = ...,
) -> KeyedStream[Tuple[U, V]]: ...


@overload
def join(
    step_id: str,
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    side3: KeyedStream[W],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: Literal["complete"] = ...,
) -> KeyedStream[Tuple[U, V, W]]: ...


@overload
def join(
    step_id: str,
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    side3: KeyedStream[W],
    side4: KeyedStream[X],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: Literal["complete"] = ...,
) -> KeyedStream[Tuple[U, V, W, X]]: ...


@overload
def join(
    step_id: str,
    side1: KeyedStream[V],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode,
) -> KeyedStream[Tuple[V]]: ...


@overload
def join(
    step_id: str,
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode,
) -> KeyedStream[Tuple[Optional[U], Optional[V]]]: ...


@overload
def join(
    step_id: str,
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    side3: KeyedStream[W],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode,
) -> KeyedStream[Tuple[Optional[U], Optional[V], Optional[W]]]: ...


@overload
def join(
    step_id: str,
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    side3: KeyedStream[W],
    side4: KeyedStream[X],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode,
) -> KeyedStream[Tuple[Optional[U], Optional[V], Optional[W], Optional[X]]]: ...


@overload
def join(
    step_id: str,
    *sides: KeyedStream[V],
    insert_mode: JoinInsertMode = ...,
    emit_mode: Literal["complete"] = ...,
) -> KeyedStream[Tuple[V, ...]]: ...


@overload
def join(
    step_id: str,
    *sides: KeyedStream[V],
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode,
) -> KeyedStream[Tuple[Optional[V], ...]]: ...


@overload
def join(
    step_id: str,
    *sides: KeyedStream[Any],
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode,
) -> KeyedStream[Tuple]: ...


@operator
def join(
    step_id: str,
    *sides: KeyedStream[Any],
    insert_mode: JoinInsertMode = "last",
    emit_mode: JoinEmitMode = "complete",
) -> KeyedStream[Tuple]:
    """Gather together the value for a key on multiple streams.

    See <project:#xref-joins> for more information.

    :arg step_id: Unique ID.

    :arg *sides: Keyed streams.

    :arg insert_mode: Mode of this join. See
        {py:obj}`~bytewax.operators.JoinInsertMode` for more info.
        Defaults to `"last"`.

    :arg emit_mode: Mode of this join. See
        {py:obj}`~bytewax.operators.JoinEmitMode` for more info.
        Defaults to `"complete"`.

    :returns: Emits a tuple with the value from each stream in the
        order of the argument list. See
        {py:obj}`~bytewax.operators.JoinEmitMode` for when tuples are
        emitted.

    """
    if insert_mode not in typing.get_args(JoinInsertMode):
        msg = f"unknown join insert mode {insert_mode!r}"
        raise ValueError(msg)
    if emit_mode not in typing.get_args(JoinEmitMode):
        msg = f"unknown join emit mode {emit_mode!r}"
        raise ValueError(msg)

    side_count = len(sides)

    def shim_builder(
        resume_state: Optional[_JoinState],
    ) -> StatefulLogic[Tuple[int, Any], Tuple, _JoinState]:
        if resume_state is None:
            state = _JoinState.for_side_count(side_count)
            return _JoinLogic(insert_mode, emit_mode, state)
        else:
            return _JoinLogic(insert_mode, emit_mode, resume_state)

    merged = _join_label_merge("add_names", *sides)
    return stateful("join", merged, shim_builder)


@operator
def key_on(step_id: str, up: Stream[X], key: Callable[[X], str]) -> KeyedStream[X]:
    """Add a key for each item.

    This allows you to use all the keyed operators that require the
    upstream to be a {py:obj}`KeyedStream`.

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg key: Called on each item and should return the key for that
        item.

    :returns: A stream of 2-tuples of `(key, item)` AKA a keyed
        stream. The keys come from the return value of the `key`
        function; upstream items will automatically be attached as
        values.

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
def key_rm(step_id: str, up: KeyedStream[X]) -> Stream[X]:
    """Discard keys.

    {py:obj}`KeyedStream`s are 2-tuples of `(key, value)`. This will
    discard the key so you just have the values if you don't need the
    keys anymore.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :returns: A stream of just values.

    """

    def shim_mapper(k_v: Tuple[str, X]) -> X:
        k, v = k_v
        return v

    return map("map", up, shim_mapper)


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

    ```{testcode}
    import bytewax.operators as op
    from bytewax.testing import TestingSource
    from bytewax.dataflow import Dataflow

    flow = Dataflow("map_eg")
    s = op.input("inp", flow, TestingSource(range(3)))

    def add_one(item):
        return item + 10

    s = op.map("add_one", s, add_one)
    _ = op.inspect("out", s)
    ```

    ```{testcode}
    :hide:

    from bytewax.testing import run_main

    run_main(flow)
    ```

    ```{testoutput}
    map_eg.out: 10
    map_eg.out: 11
    map_eg.out: 12
    ```

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg mapper: Called on each item. Each return value is emitted
        downstream.

    :returns: A stream of items returned from the mapper.

    """

    def shim_mapper(xs: List[X]) -> Iterable[Y]:
        return (mapper(x) for x in xs)

    return flat_map_batch("flat_map_batch", up, shim_mapper)


@operator
def map_value(
    step_id: str, up: KeyedStream[V], mapper: Callable[[V], W]
) -> KeyedStream[W]:
    """Transform values one-by-one.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg mapper: Called on each value. Each return value is emitted
        downstream.

    :returns: A keyed stream of values returned from the mapper. The
        key is unchanged.

    """

    def shim_mapper(k_v: Tuple[str, V]) -> Tuple[str, W]:
        k, v = k_v
        w = mapper(v)
        return (k, w)

    return map("map", up, shim_mapper)


@overload
def max_final(
    step_id: str,
    up: KeyedStream[V],
) -> KeyedStream[V]: ...


@overload
def max_final(
    step_id: str,
    up: KeyedStream[V],
    by: Callable[[V], Any],
) -> KeyedStream[V]: ...


@operator
def max_final(
    step_id: str,
    up: KeyedStream[V],
    by=_identity,
) -> KeyedStream:
    """Find the maximum value for each key.

    This only works on finite data streams and only returns a result
    once the upstream is EOF. You'll need to use
    {py:obj}`bytewax.operators.windowing.max_window` on infinite data.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg by: A function called on each value that is used to extract
        what to compare.

    :returns: A keyed stream of the max values. _Only once the
        upstream is EOF._

    """
    return reduce_final("reduce_final", up, partial(max, key=by))


@overload
def min_final(
    step_id: str,
    up: KeyedStream[V],
) -> KeyedStream[V]: ...


@overload
def min_final(
    step_id: str,
    up: KeyedStream[V],
    by: Callable[[V], Any],
) -> KeyedStream[V]: ...


@operator
def min_final(
    step_id: str,
    up: KeyedStream[V],
    by=_identity,
) -> KeyedStream:
    """Find the minimum value for each key.

    This only works on finite data streams and only returns a result
    once the upstream is EOF. You'll need to use
    {py:obj}`bytewax.operators.windowing.min_window` on infinite data.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg by: A function called on each value that is used to extract
        what to compare.

    :returns: A keyed stream of the min values. _Only once the
        upstream is EOF._

    """
    return reduce_final("reduce_final", up, partial(min, key=by))


@dataclass
class _RaisePartition(StatelessSinkPartition[Any]):
    step_id: str

    @override
    def write_batch(self, items: List[Any]) -> None:
        for item in items:
            msg = f"`raises` step {self.step_id!r} got an item: {item!r}"
            raise RuntimeError(msg)


@dataclass
class _RaiseSink(DynamicSink[Any]):
    step_id: str

    @override
    def build(
        self, _step_id: str, worker_index: int, worker_count: int
    ) -> _RaisePartition:
        return _RaisePartition(self.step_id)


@operator
def raises(
    step_id: str,
    up: Stream[Any],
) -> None:
    """Raise an exception and crash the dataflow on any item.

    :arg step_id: Unique ID.

    :arg up: Any item on this stream will throw a
        {py:obj}`RuntimeError`.

    """
    return output("output", up, _RaiseSink(step_id))


@operator
def reduce_final(
    step_id: str,
    up: KeyedStream[V],
    reducer: Callable[[V, V], V],
) -> KeyedStream[V]:
    """Distill all values for a key down into a single value.

    This only works on finite data streams and only returns a result
    once the upstream is EOF. You'll need to use
    {py:obj}`bytewax.operators.windowing.reduce_window` or
    {py:obj}`bytewax.operators.stateful_flat_map` on infinite data.

    It is like {py:obj}`fold_final` but the first value is the initial
    accumulator.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg reducer: Combines a new value into an old value and returns
        the combined value.

    :returns: A keyed stream of the accumulators. _Only once the
        upstream is EOF._

    """

    def pre_reducer(mixed_batch: List[Tuple[str, V]]) -> Iterable[Tuple[str, V]]:
        states: Dict[str, V] = {}
        for k, v in mixed_batch:
            try:
                s = states[k]
            except KeyError:
                states[k] = v
            else:
                states[k] = reducer(s, v)
        return states.items()

    pre_up = flat_map_batch("pre_reduce", up, pre_reducer)

    def shim_folder(s: V, v: V) -> V:
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return fold_final("fold_final", pre_up, _untyped_none, shim_folder)


@dataclass
class _StatefulFlatMapLogic(StatefulLogic[V, W, S]):
    step_id: str
    mapper: Callable[[Optional[S], V], Tuple[Optional[S], Iterable[W]]]
    state: Optional[S]

    @override
    def on_item(self, value: V) -> Tuple[Iterable[W], bool]:
        res = self.mapper(self.state, value)
        try:
            s, ws = res
        except TypeError as ex:
            msg = (
                f"return value of `mapper` {f_repr(self.mapper)} "
                f"in step {self.step_id!r} "
                "must be a 2-tuple of `(updated_state, emit_values)`; "
                f"got a {type(res)!r} instead"
            )
            raise TypeError(msg) from ex
        if s is None:
            # No need to update state as we're thowing everything
            # away.
            return (ws, StatefulLogic.DISCARD)
        else:
            self.state = s
            return (ws, StatefulLogic.RETAIN)

    @override
    def snapshot(self) -> S:
        assert self.state is not None
        return copy.deepcopy(self.state)


@operator
def stateful_flat_map(
    step_id: str,
    up: KeyedStream[V],
    mapper: Callable[[Optional[S], V], Tuple[Optional[S], Iterable[W]]],
) -> KeyedStream[W]:
    """Transform values one-to-many, referencing a persistent state.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg mapper: Called whenever a value is encountered from upstream
        with the last state or `None`, and then the upstream value.
        Should return a 2-tuple of `(updated_state, emit_values)`. If
        the updated state is `None`, discard it.

    :returns: A keyed stream.

    """

    def shim_builder(resume_state: Optional[S]) -> _StatefulFlatMapLogic[V, W, S]:
        return _StatefulFlatMapLogic(step_id, mapper, resume_state)

    return stateful("stateful", up, shim_builder)


@operator
def stateful_map(
    step_id: str,
    up: KeyedStream[V],
    mapper: Callable[[Optional[S], V], Tuple[Optional[S], W]],
) -> KeyedStream[W]:
    """Transform values one-to-one, referencing a persistent state.

    It is commonly used for:

    - Anomaly detection

    - State machines

    ```{testcode}
    import bytewax.operators as op
    from bytewax.testing import TestingSource, run_main
    from bytewax.dataflow import Dataflow

    flow = Dataflow("stateful_map_eg")

    inp = [
        "a",
        "a",
        "a",
        "b",
        "a",
    ]
    s = op.input("inp", flow, TestingSource(inp))

    s = op.key_on("self_as_key", s, lambda x: x)

    def check(running_count, _item):
        if running_count is None:
            running_count = 0
        running_count += 1
        return (running_count, running_count)

    s = op.stateful_map("running_count", s, check)
    _ = op.inspect("out", s)
    ```

    ```{testcode}
    :hide:

    from bytewax.testing import run_main

    run_main(flow)
    ```

    ```{testoutput}
    stateful_map_eg.out: ('a', 1)
    stateful_map_eg.out: ('a', 2)
    stateful_map_eg.out: ('a', 3)
    stateful_map_eg.out: ('b', 1)
    stateful_map_eg.out: ('a', 4)
    ```

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg mapper: Called whenever a value is encountered from upstream
        with the last state or `None`, and then the upstream value.
        Should return a 2-tuple of `(updated_state, emit_value)`. If
        the updated state is `None`, discard it.

    :returns: A keyed stream.

    """

    def shim_mapper(state: Optional[S], v: V) -> Tuple[Optional[S], Iterable[W]]:
        res = mapper(state, v)
        try:
            s, w = res
        except TypeError as ex:
            msg = (
                f"return value of `mapper` {f_repr(mapper)} "
                f"in step {step_id!r} "
                "must be a 2-tuple of `(updated_state, emit_value)`; "
                f"got a {type(res)!r} instead"
            )
            raise TypeError(msg) from ex

        return (s, (w,))

    return stateful_flat_map("stateful_flat_map", up, shim_mapper)
