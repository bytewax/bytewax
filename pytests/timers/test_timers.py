from datetime import datetime, timezone

from bytewax.timers import MultiNotifier


def test_next_at() -> None:
    mn: MultiNotifier[str] = MultiNotifier()

    mn.notify_at(datetime(2024, 8, 20, 7, 0, 0, tzinfo=timezone.utc), "b")
    mn.notify_at(datetime(2024, 8, 20, 6, 0, 0, tzinfo=timezone.utc), "a")

    assert mn.next_at() == datetime(2024, 8, 20, 6, 0, 0, tzinfo=timezone.utc)


def test_next_at_none() -> None:
    mn: MultiNotifier[str] = MultiNotifier()

    assert mn.next_at() is None


def test_due() -> None:
    mn: MultiNotifier[str] = MultiNotifier()

    mn.notify_at(datetime(2024, 8, 20, 7, 0, 0, tzinfo=timezone.utc), "b")
    mn.notify_at(datetime(2024, 8, 20, 6, 0, 0, tzinfo=timezone.utc), "a")

    assert mn.due(datetime(2024, 8, 20, 6, 0, 0, tzinfo=timezone.utc)) == ["a"]


def test_due_removes() -> None:
    mn: MultiNotifier[str] = MultiNotifier()

    mn.notify_at(datetime(2024, 8, 20, 7, 0, 0, tzinfo=timezone.utc), "b")
    mn.notify_at(datetime(2024, 8, 20, 6, 0, 0, tzinfo=timezone.utc), "a")

    mn.due(datetime(2024, 8, 20, 6, 0, 0, tzinfo=timezone.utc))

    assert mn.next_at() == datetime(2024, 8, 20, 7, 0, 0, tzinfo=timezone.utc)
