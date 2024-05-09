from datetime import datetime, timedelta, timezone

from bytewax.operators.windowing import (
    UTC_MAX,
    UTC_MIN,
    _EventClockLogic,
)
from bytewax.testing import TimeTestingGetter


def test_watermark_starts_at_beginning_of_time():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta(seconds=5),
    )
    assert logic.on_notify() == UTC_MIN


def test_watermark_is_item_timestamp_minus_wait():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta(seconds=5),
    )
    item_timestamp = datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc)
    logic.before_batch()
    _, found_watermark = logic.on_item(item_timestamp)
    assert found_watermark == datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc)


def test_watermark_forwards_by_system_time():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta(seconds=5),
    )
    item_timestamp = datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc)
    logic.before_batch()
    logic.on_item(item_timestamp)
    source.advance(timedelta(seconds=2))
    assert logic.on_notify() == datetime(2024, 1, 1, 0, 0, 4, tzinfo=timezone.utc)


def test_watermark_advances_in_batch():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta(seconds=5),
    )
    logic.before_batch()
    logic.on_item(datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc))
    _, found_watermark = logic.on_item(
        datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    )
    assert found_watermark == datetime(2024, 1, 1, 0, 0, 5, tzinfo=timezone.utc)


def test_watermark_does_not_reverse_in_batch():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta(seconds=5),
    )
    logic.before_batch()
    logic.on_item(datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc))
    _, found_watermark = logic.on_item(
        datetime(2024, 1, 1, 0, 0, 3, tzinfo=timezone.utc)
    )
    assert found_watermark == datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc)


def test_watermark_does_not_reverse_and_forwards_by_system_time_next_batch():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta(seconds=5),
    )
    logic.before_batch()
    logic.on_item(datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc))
    source.advance(timedelta(seconds=2))
    logic.before_batch()
    _, found_watermark = logic.on_item(
        datetime(2024, 1, 1, 0, 0, 3, tzinfo=timezone.utc)
    )
    assert found_watermark == datetime(2024, 1, 1, 0, 0, 4, tzinfo=timezone.utc)


def test_watermark_is_end_of_time_on_eof():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta(seconds=5),
    )
    logic.on_eof()
    assert logic.on_eof() == UTC_MAX


def test_watermark_doesnt_overflow_after_eof():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta(seconds=5),
    )
    logic.on_eof()
    source.advance(timedelta(seconds=2))
    assert logic.on_eof() == UTC_MAX


def test_allows_max_wait_for_system_duration_init():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta.max,
    )
    item_timestamp = datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc)
    logic.before_batch()
    _, found_watermark = logic.on_item(item_timestamp)
    assert found_watermark == UTC_MIN


def test_allows_max_wait_for_system_duration_update_does_not_regress():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _EventClockLogic(
        source.get,
        lambda x: x,
        lambda x: x,
        timedelta.max,
    )
    logic.before_batch()
    logic.on_item(datetime(2024, 1, 1, 0, 0, 7, tzinfo=timezone.utc))
    source.advance(timedelta(seconds=2))
    logic.before_batch()
    _, found_watermark = logic.on_item(
        datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    )
    assert found_watermark == UTC_MIN + timedelta(seconds=2)
