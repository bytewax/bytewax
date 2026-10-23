import copy
import random
from datetime import datetime, timedelta, timezone

import pytest
from bytewax._utils import partition
from bytewax.operators.windowing import (
    UTC_MAX,
    EventClock,
    SlidingWindower,
    _FoldWindowLogic,
    _OrderedWindowQueue,
    _WindowLogic,
)

ORIGIN = datetime(2024, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("order", ["sorted", "reverse", "random", "ties"])
def test_queue_matches_stable_reference_through_partial_drains(order):
    rng = random.Random(379)
    model = []
    queue = _OrderedWindowQueue([])
    for batch_no in range(60):
        offsets = [batch_no * 100 + rng.randrange(1200) for _ in range(100)]
        if order == "sorted":
            offsets = list(range(batch_no * 100, (batch_no + 1) * 100))
        elif order == "reverse":
            offsets.sort(reverse=True)
        elif order == "ties":
            offsets = [x // 100 * 100 for x in offsets]
        # Dict payloads deliberately cannot be compared to break timestamp ties.
        batch = [({}, ORIGIN + timedelta(seconds=x)) for x in offsets]
        model.extend(batch)
        queue.extend(batch)
        watermark = ORIGIN + timedelta(seconds=batch_no * 100 - 200)
        due = sorted((e for e in model if e[1] <= watermark), key=lambda e: e[1])
        model = [e for e in model if e[1] > watermark]
        assert [id(v) for v, _ in queue.pop_due(watermark)] == [id(v) for v, _ in due]
        assert list(queue) == model
        assert queue.pop_due(watermark) == []
    assert queue.pop_due(UTC_MAX) == sorted(model, key=lambda e: e[1])
    assert not queue
    # Reusing an empty queue resets both the minimum and monotonicity state.
    entry = ({}, ORIGIN)
    queue.extend([entry])
    assert queue.pop_due(ORIGIN) == [entry]


def test_disorder_and_recovery_to_monotone_retained_entries():
    offsets = [100, 0, 2, 2, 5]
    entries = [(object(), ORIGIN + timedelta(seconds=x)) for x in offsets]
    queue = _OrderedWindowQueue(entries)
    assert queue.pop_due(ORIGIN) == [entries[1]]
    assert list(queue) == [entries[0], *entries[2:]]
    assert queue.pop_due(ORIGIN + timedelta(seconds=5)) == entries[2:]
    more = [(object(), ORIGIN + timedelta(seconds=x)) for x in [101, 102]]
    queue.extend(more)
    assert queue.pop_due(ORIGIN + timedelta(seconds=101)) == [entries[0], more[0]]
    assert queue.pop_due(UTC_MAX) == [more[1]]


def test_sparse_survivors_do_not_accumulate_empty_block_overhead():
    queue = _OrderedWindowQueue([])
    survivors = []
    for round_no in range(6):
        watermark = ORIGIN + timedelta(seconds=round_no)
        batch = []
        for block_no in range(30):
            survivor = (object(), ORIGIN + timedelta(days=1, seconds=block_no % 3))
            survivors.append(survivor)
            batch.append(survivor)
            batch.extend((object(), watermark) for _ in range(1023))
        queue.extend(batch)
        assert len(queue.pop_due(watermark)) == 30 * 1023
        assert list(queue) == survivors
        sizes = [len(block.entries) for block in queue._blocks]
        assert all(0 < n <= queue._BLOCK_SIZE for n in sizes)
        assert all(a + b > queue._BLOCK_SIZE for a, b in zip(sizes, sizes[1:]))
        assert len(sizes) <= 2 * len(survivors) // queue._BLOCK_SIZE + 1
    assert queue.pop_due(UTC_MAX) == sorted(survivors, key=lambda e: e[1])


class _ReferenceWindowLogic(_WindowLogic):
    """The pre-change queue algorithm, as an observable-behavior oracle."""

    def _flush_queue(self, watermark):
        if self.ordered:
            due, self.queue = partition(self.queue, lambda e: e[1] <= watermark)
            due.sort(key=lambda e: e[1])
        else:
            due, self.queue = self.queue, []
        yield from self._handle_inserts(due)
        yield from self._handle_merged()
        yield from self._handle_closed(watermark)


@pytest.mark.parametrize("ordered", [True, False])
@pytest.mark.parametrize("disordered", [True, False])
def test_window_events_notifications_snapshots_and_restore(ordered, disordered):
    rng = random.Random(57)
    clock = EventClock(lambda v: v[1], timedelta(seconds=4), now_getter=lambda: ORIGIN)
    windower = SlidingWindower(timedelta(seconds=5), timedelta(seconds=2), ORIGIN)

    def builder(state):
        return _FoldWindowLogic(
            lambda acc, v: [*acc, v], lambda a, b: a + b, [] if state is None else state
        )

    def build(kind, state=None):
        if state is None:
            return kind(clock.build(None), windower.build(None), builder, ordered)
        return kind(
            clock.build(state.clock_state),
            windower.build(state.windower_state),
            builder,
            ordered,
            {w: builder(s) for w, s in state.logic_states.items()},
            state.queue,
        )

    actual = build(_WindowLogic)
    expected = build(_ReferenceWindowLogic)
    old_snapshots = []
    for batch_no in range(14):
        # Retain more than the promotion threshold; mix ties, disorder and lateness.
        batch = [
            (
                i,
                ORIGIN
                + timedelta(
                    milliseconds=i + (rng.randrange(-6000, 1000) if disordered else 0)
                ),
            )
            for i in range(batch_no * 1200, (batch_no + 1) * 1200)
        ]
        assert actual.on_batch(batch) == expected.on_batch(batch)
        assert actual.notify_at() == expected.notify_at()
        assert actual.snapshot() == expected.snapshot()
        assert actual.on_notify() == expected.on_notify()
        if batch_no % 3 == 0:
            snap = actual.snapshot()
            old_snapshots.append((snap, copy.deepcopy(snap)))
            # Both directions use the same arrival-ordered list snapshot.
            actual = build(_WindowLogic, copy.deepcopy(expected.snapshot()))
            expected = build(_ReferenceWindowLogic, copy.deepcopy(snap))
    assert actual.on_eof() == expected.on_eof()
    assert actual.snapshot() == expected.snapshot()
    assert all(snap == saved for snap, saved in old_snapshots)


def test_notification_uses_first_arrival_even_if_another_entry_is_earlier():
    clock = EventClock(lambda v: v, timedelta(days=3), now_getter=lambda: ORIGIN)
    windower = SlidingWindower(timedelta(seconds=1), timedelta(seconds=1), ORIGIN)
    logic = _WindowLogic(
        clock.build(None),
        windower.build(None),
        lambda s: _FoldWindowLogic(lambda a, _: a + 1, lambda a, b: a + b, s or 0),
        True,
    )
    first = ORIGIN + timedelta(days=1)
    assert not logic.on_batch([first] + [ORIGIN] * 2048)[0]
    assert logic.notify_at() == logic.clock.to_system_utc(first)
    assert logic.snapshot().queue == [(first, first)] + [(ORIGIN, ORIGIN)] * 2048
