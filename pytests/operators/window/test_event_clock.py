from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from bytewax.operators.window import (
    UTC_MAX,
    UTC_MIN,
    _EventClockLogic,
)


@dataclass
class TimeTestSource:
    now: datetime

    def advance(self, td: timedelta) -> None:
        self.now += td

    def get(self) -> datetime:
        return self.now


def test_watermark_starts_at_beginning_of_time():
    source = TimeTestSource(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(source.get, lambda x: x, timedelta(seconds=5), None)
    assert logic.on_notify() == UTC_MIN


def test_watermark_is_item_timestamp_minus_wait():
    source = TimeTestSource(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(source.get, lambda x: x, timedelta(seconds=5), None)
    item_timestamp = datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc)
    _, found_watermark = logic.on_item(item_timestamp)
    assert found_watermark == datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc)


def test_watermark_forwards_by_system_time():
    source = TimeTestSource(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(source.get, lambda x: x, timedelta(seconds=5), None)
    item_timestamp = datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc)
    logic.on_item(item_timestamp)
    source.advance(timedelta(seconds=2))
    assert logic.on_notify() == datetime(2024, 1, 1, 0, 0, 4, tzinfo=timezone.utc)


def test_watermark_advances_with_later_item_timestamp():
    source = TimeTestSource(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(source.get, lambda x: x, timedelta(seconds=5), None)
    logic.on_item(datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc))
    _, found_watermark = logic.on_item(
        datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    )
    assert found_watermark == datetime(2024, 1, 1, 0, 0, 5, tzinfo=timezone.utc)


def test_watermark_does_not_reverse():
    source = TimeTestSource(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(source.get, lambda x: x, timedelta(seconds=5), None)
    logic.on_item(datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc))
    _, found_watermark = logic.on_item(
        datetime(2024, 1, 1, 0, 0, 3, tzinfo=timezone.utc)
    )
    assert found_watermark == datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc)


def test_watermark_is_end_of_time_on_eof():
    source = TimeTestSource(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(source.get, lambda x: x, timedelta(seconds=5), None)
    logic.on_eof()
    assert logic.on_eof() == UTC_MAX


def test_watermark_doesnt_overflow_after_eof():
    source = TimeTestSource(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(source.get, lambda x: x, timedelta(seconds=5), None)
    logic.on_eof()
    source.advance(timedelta(seconds=2))
    assert logic.on_eof() == UTC_MAX
