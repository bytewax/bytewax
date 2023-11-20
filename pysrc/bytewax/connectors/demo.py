"""Connectors for writing local-first demo dataflows."""
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, List, Optional, Tuple, TypeVar

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

    def next_batch(self, sched: datetime) -> List[Tuple[str, float]]:
        self.state.awake_at = sched + self.interval
        self.state.count += 1

        if self.state.count > self.count:
            raise StopIteration()

        value = self.next_random()
        return [(self.metric_name, value)]

    def next_awake(self) -> Optional[datetime]:
        return self.state.awake_at

    def snapshot(self) -> _RandomMetricState:
        return self.state


@dataclass
class RandomMetricSource(FixedPartitionedSource[Tuple[str, float], _RandomMetricState]):
    """Demo source that produces an infinite stream of random values.

    Emits downstream `(metric_name, val)` 2-tuples.

    If you want to emit multiple different metrics, create multiple
    `bytewax.operators.input.input` steps and
    `bytewax.operators.merge.merge` them.

    Args:
        metric_name: To attach to each value.

        interval: Emit a value on this cadence. Defaults to every 0.7
            seconds.

        count: Number of values to generate. Defaults to infinite.

        next_random: Callable used to generate the next random value.
            Defaults to a random `int` between 0 and 10.

    """

    metric_name: str
    interval: timedelta = timedelta(seconds=0.7)
    count: int = sys.maxsize
    next_random: Callable[[], float] = lambda: random.randrange(0, 10)

    def list_parts(self) -> List[str]:
        """A single stream of values."""
        return [self.metric_name]

    def build_part(
        self, now: datetime, for_part: str, resume_state: Optional[_RandomMetricState]
    ):
        """See ABC docstring."""
        state = resume_state if resume_state is not None else _RandomMetricState(now, 0)
        return _RandomMetricPartition(
            for_part, self.interval, self.count, self.next_random, state
        )
