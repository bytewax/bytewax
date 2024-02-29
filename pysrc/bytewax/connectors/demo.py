"""Connectors for writing local-first demo dataflows."""
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional, Tuple, TypeVar

from typing_extensions import override

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

X = TypeVar("X")


@dataclass
class _RandomMetricState:
    awake_at: datetime
    count: int


@dataclass
class _RandomMetricPartition(
    StatefulSourcePartition[Tuple[str, float], _RandomMetricState]
):
    metric_name: str
    interval: timedelta
    count: int
    next_random: Callable[[], float]
    state: _RandomMetricState

    @override
    def next_batch(self) -> List[Tuple[str, float]]:
        self.state.awake_at += self.interval
        self.state.count += 1

        if self.state.count > self.count:
            raise StopIteration()

        value = self.next_random()
        return [(self.metric_name, value)]

    @override
    def next_awake(self) -> Optional[datetime]:
        return self.state.awake_at

    @override
    def snapshot(self) -> _RandomMetricState:
        return self.state


@dataclass
class RandomMetricSource(FixedPartitionedSource[Tuple[str, float], _RandomMetricState]):
    """Demo source that produces an infinite stream of random values.

    Emits downstream `(metric_name, val)` 2-tuples.

    If you want to emit multiple different metrics, create multiple
    {py:obj}`~bytewax.operators.input` steps and
    {py:obj}`~bytewax.operators.merge` them.

    """

    def __init__(
        self,
        metric_name: str,
        interval: timedelta = timedelta(seconds=0.7),
        count: int = sys.maxsize,
        next_random: Callable[[], float] = lambda: random.randrange(0, 10),
    ):
        """Init.

        :arg metric_name: To attach to each value.

        :arg interval: Emit a value on this cadence. Defaults to every
            0.7 seconds.

        :arg count: Number of values to generate. Defaults to
            infinite.

        :arg next_random: Callable used to generate the next random
            value. Defaults to a random {py:obj}`int` between 0 and
            10.

        """
        self._metric_name = metric_name
        self._interval = interval
        self._count = count
        self._next_random = next_random

    @override
    def list_parts(self) -> List[str]:
        return [self._metric_name]

    @override
    def build_part(
        self, _step_id: str, for_part: str, resume_state: Optional[_RandomMetricState]
    ):
        now = datetime.now(timezone.utc)
        state = resume_state if resume_state is not None else _RandomMetricState(now, 0)
        return _RandomMetricPartition(
            for_part, self._interval, self._count, self._next_random, state
        )
