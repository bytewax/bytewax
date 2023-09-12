"""Periodic interval base class to build custom input sources."""
from abc import abstractmethod
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Any, Optional

from bytewax.inputs import PartitionedInput, StatefulSource

__all__ = [
    "SimplePollingInput",
]


class _SimplePollingSource(StatefulSource):
    def __init__(self, interval: timedelta, align_to: Optional[datetime], getter):
        now = datetime.now(timezone.utc)
        if align_to is not None:
            assert now > align_to, "align_to must be in the past"
            self._align_to = align_to
            # Next awake is the next datetime that is an integer
            # number of intervals from the align_to date.
            self._next_awake = align_to + (interval * ceil((now - align_to) / interval))
        else:
            self._align_to = now
            self._next_awake = now

        self._interval = interval
        self._getter = getter

    def next_batch(self):
        self._next_awake += self._interval
        return [self._getter()]

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        return None


class SimplePollingInput(PartitionedInput):
    """Calls a user defined function at a regular interval.

    Subclass this input source and write the `next_item` function
    that will be called at the defined interval.

    Notes:
    - The `interval` parameter is capped at a minimum of 10ms, but it can be as
      large as needed.
    - If you need fast input polling, consider writing a custom input source,
      where you can also batch items to avoid doing too much IO.
    """

    def __init__(self, interval: timedelta, align_to: Optional[datetime] = None):
        """Create a PeriodicInput.

        Args:
            interval: The interval between awakes. Must be >= 10ms.
            align_to: Align awake times to the given datetime.
        """
        if interval < timedelta(milliseconds=10):
            msg = "The interval for SimplePollingInput must be >= 10ms"
            raise ValueError(msg)
        self._interval = interval
        self._align_to = align_to

    def list_parts(self):
        """Only emit a single tick."""
        return ["singleton"]

    def build_part(self, for_part, _resume_state):  # noqa: D102
        assert for_part == "singleton"
        # Ignore resume state
        return _SimplePollingSource(self._interval, None, self.next_item)

    @abstractmethod
    def next_item(self) -> Any:
        """This function will be called at regular inerval."""
        ...
