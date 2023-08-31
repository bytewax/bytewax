"""Time related input source."""

from datetime import datetime, timedelta, timezone

from bytewax.inputs import PartitionedInput, StatefulSource


class _PeriodicSource(StatefulSource):
    def __init__(self, interval: timedelta):
        self._next_awake = datetime.now(timezone.utc)
        self._interval = interval

    def next_batch(self):
        # Emit the previous self._next_awake value
        item = self._next_awake
        self._next_awake += self._interval
        return [item]

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        return None


class PeriodicInput(PartitionedInput):
    """Emit datetimes at a regular interval.

    The first datetime emitted will be the moment the source is built. Then at
    each interval, that datetime is moved forward by `interval`. The emitted
    datetime will always be exactly previous+interval, but it could be emitted a
    bit later, though the latency should be below or around 1ms.

    Notes:
    - The `interval` parameter is capped at a minimum of 10ms, but it can be as
      large as needed.
    - If you need fast input polling, consider writing a custom input source,
      where you can also batch items to avoid doing too much IO.
    """

    def __init__(self, interval: timedelta):
        """Create a PeriodicInput.

        Args:
            interval (timedelta): The interval between awakes. Must be >= 10ms.
        """
        if interval < timedelta(milliseconds=10):
            msg = "The interval for PeriodicInput should be > 10ms"
            raise ValueError(msg)
        self._interval = interval

    def list_parts(self):
        """Only emit on a single worker."""
        return ["singleton"]

    def build_part(self, for_part, _resume_state):  # noqa: D102
        assert for_part == "singleton"
        # Ignore resume state
        return _PeriodicSource(self._interval)
