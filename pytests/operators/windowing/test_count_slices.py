import copy
import random
from datetime import datetime, timedelta, timezone

import pytest
from bytewax.operators.windowing import (
    EventClock,
    SlidingWindower,
    _CountWindowBuilder,
    _CountWindowOperatorLogic,
    _FoldWindowLogic,
    _WindowLogic,
)

ORIGIN = datetime(2024, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("length,offset", [(2, 1), (10, 3), (100, 1)])
def test_count_metadata_and_counts_across_density_changes(length, offset):
    conf = SlidingWindower(timedelta(seconds=length), timedelta(seconds=offset), ORIGIN)
    clock = EventClock(
        lambda value: value,
        timedelta(days=1),
        now_getter=lambda: ORIGIN,
        to_system_utc=lambda _: None,
    )
    actual = _CountWindowOperatorLogic(
        clock.build(None), conf.build(None), _CountWindowBuilder(), False
    )
    expected = _WindowLogic(
        clock.build(None),
        conf.build(None),
        lambda state: _FoldWindowLogic(
            lambda count, _: count + 1, lambda a, b: a + b, state or 0
        ),
        False,
    )
    sparse = [ORIGIN + timedelta(seconds=i) for i in range(-10, 100)]
    dense = [ORIGIN + timedelta(microseconds=i) for i in range(-100, 100)]
    # Repeat nonadjacent slices and cross boundaries on both sides of the
    # origin. Changing density inside a batch must not affect opening order,
    # metadata, accumulated counts or subsequent batches' existing windows.
    for batch in [sparse + dense, dense + sparse, sparse[::-1] + dense[::-1]]:
        assert actual.on_batch(batch) == expected.on_batch(batch)
        assert actual.snapshot() == expected.snapshot()
        assert actual.notify_at() == expected.notify_at()
    assert actual.on_eof() == expected.on_eof()
    assert actual.snapshot() == expected.snapshot()


@pytest.mark.parametrize("length,offset", [(10, 3), (10, 1), (100, 1), (7, 4)])
@pytest.mark.parametrize("batch_size", [1, 2, 200])
def test_count_slices_restore_match_scalar_fold(length, offset, batch_size):
    ordered = False
    rng = random.Random(555)
    conf = SlidingWindower(timedelta(seconds=length), timedelta(seconds=offset), ORIGIN)
    clock = EventClock(
        lambda x: x,
        timedelta(seconds=20),
        now_getter=lambda: ORIGIN,
        to_system_utc=lambda ts: None,
    )
    builder = _CountWindowBuilder()
    scalar_builder = lambda s: _FoldWindowLogic(
        lambda a, _: a + 1, lambda a, b: a + b, s or 0
    )
    kind = _CountWindowOperatorLogic
    actual = kind(clock.build(None), conf.build(None), builder, ordered)
    expected = _WindowLogic(
        clock.build(None), conf.build(None), scalar_builder, ordered
    )
    for batch_no in range(25):
        # Both sides of non-integral slice boundaries, negatives, ties, and late events.
        batch = [
            ORIGIN
            + timedelta(milliseconds=1000 * batch_no + rng.randrange(-25_000, 25_000))
            for _ in range(batch_size)
        ]
        assert actual.on_batch(batch) == expected.on_batch(batch)
        assert actual.notify_at() == expected.notify_at()
        snap = actual.snapshot()
        assert snap == expected.snapshot()
        if batch_no % 3 == 0:
            # Restore from the legacy scalar snapshot into the optimized implementation,
            # and vice versa. Persist ints, metadata and an arrival-order list.
            legacy = copy.deepcopy(expected.snapshot())
            actual = kind(
                clock.build(legacy.clock_state),
                conf.build(legacy.windower_state),
                builder,
                ordered,
                {w: builder(s) for w, s in legacy.logic_states.items()},
                legacy.queue,
            )
            expected = _WindowLogic(
                clock.build(snap.clock_state),
                conf.build(snap.windower_state),
                scalar_builder,
                ordered,
                {w: scalar_builder(s) for w, s in snap.logic_states.items()},
                snap.queue,
            )
    assert actual.on_eof() == expected.on_eof()
    assert actual.snapshot() == expected.snapshot()


def test_count_custom_sliding_subclass_preserves_each_open_callback():
    import bytewax.operators as op
    from bytewax.dataflow import Dataflow
    from bytewax.operators.windowing import count_window
    from bytewax.testing import TestingSink, TestingSource, run_main

    calls = []

    class CustomSliding(SlidingWindower):
        def build(self, state):
            logic = super().build(state)
            original = logic.open_for

            def open_for(timestamp):
                calls.append(timestamp)
                return original(timestamp)

            logic.open_for = open_for
            return logic

    data = [ORIGIN + timedelta(milliseconds=i) for i in range(500)]
    conf = CustomSliding(timedelta(seconds=10), timedelta(seconds=1), ORIGIN)
    clock = EventClock(
        lambda x: x,
        timedelta(0),
        now_getter=lambda: ORIGIN,
        to_system_utc=lambda ts: None,
    )
    flow = Dataflow("custom_count")
    up = op.input("input", flow, TestingSource(data, batch_size=500))
    output = count_window("count", up, clock, conf, lambda _: "k")
    result = []
    op.output("out", output.down, TestingSink(result))
    run_main(flow)
    assert calls == data
    assert sum(count for _, (_, count) in result) == len(data) * 10


@pytest.mark.parametrize("length,offset", [(10, 3), (100, 1), (2, 1), (10, 10)])
@pytest.mark.parametrize("wait", [0, 20])
def test_public_count_matches_scalar_fold_and_clock_callbacks(length, offset, wait):
    import bytewax.operators as op
    from bytewax.dataflow import Dataflow
    from bytewax.testing import TestingSink, TestingSource, run_main

    rng = random.Random(715)
    data = [
        (str(i % 3), ORIGIN + timedelta(milliseconds=rng.randrange(-15000, 40000)))
        for i in range(1000)
    ]

    def run(specialized):
        calls = []

        def timestamp(value):
            calls.append(value)
            return value[1]

        clock = EventClock(
            timestamp,
            timedelta(seconds=wait),
            now_getter=lambda: ORIGIN,
            to_system_utc=lambda _: None,
        )
        conf = SlidingWindower(
            timedelta(seconds=length), timedelta(seconds=offset), ORIGIN
        )
        flow = Dataflow("count_equivalence")
        up = op.input("input", flow, TestingSource(data, batch_size=100))
        if specialized:
            out = op.windowing.count_window("count", up, clock, conf, lambda x: x[0])
        else:
            keyed = op.key_on("key", up, lambda x: x[0])
            out = op.windowing.fold_window(
                "count",
                keyed,
                clock,
                conf,
                lambda: 0,
                lambda s, _: s + 1,
                lambda a, b: a + b,
                ordered=False,
            )
        streams = [[], [], []]
        for name, sink in zip(("down", "late", "meta"), streams):
            op.output(name, getattr(out, name), TestingSink(sink))
        run_main(flow)
        return streams, calls

    assert run(True) == run(False)
