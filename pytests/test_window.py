from datetime import datetime, timedelta

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingInputConfig
from bytewax.outputs import TestingOutputConfig
from bytewax.testing import TestingClock
from bytewax.window import TestingClockConfig, TumblingWindowConfig


def test_tumbling_window():
    start_at = datetime(2022, 1, 1)
    clock = TestingClock(start_at)

    flow = Dataflow()

    def gen():
        yield ("ALL", 1)
        clock.now += timedelta(seconds=4)
        yield ("ALL", 1)
        clock.now += timedelta(seconds=4)
        yield ("ALL", 1)
        # Window should close here.
        clock.now += timedelta(seconds=4)
        yield ("ALL", 1)
        clock.now += timedelta(seconds=4)

    flow.input("inp", TestingInputConfig(gen()))

    clock_config = TestingClockConfig(clock)
    window_config = TumblingWindowConfig(
        length=timedelta(seconds=10), start_at=start_at
    )

    def add(acc, x):
        return acc + x

    flow.reduce_window("sum", clock_config, window_config, add)

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted([("ALL", 3), ("ALL", 1)])


def test_fold_window():
    start_at = datetime(2022, 1, 1)
    clock = TestingClock(start_at)

    flow = Dataflow()

    def gen():
        yield {"user": "a", "type": "login"}  # +0 sec
        clock.now += timedelta(seconds=4)
        yield {"user": "a", "type": "post"}  # +4 sec
        clock.now += timedelta(seconds=4)
        yield {"user": "a", "type": "post"}  # +8 sec
        # First 10 sec window closes.
        clock.now += timedelta(seconds=4)
        yield {"user": "b", "type": "login"}  # +12 sec
        clock.now += timedelta(seconds=4)
        yield {"user": "a", "type": "post"}  # +16 sec
        clock.now += timedelta(seconds=4)
        # Second 10 sec window closes.
        yield {"user": "b", "type": "post"}  # +20 sec
        clock.now += timedelta(seconds=4)
        yield {"user": "b", "type": "post"}  # +24 sec
        clock.now += timedelta(seconds=4)

    flow.input("inp", TestingInputConfig(gen()))

    def key_off_user(event):
        return (event["user"], event["type"])

    flow.map(key_off_user)

    clock_config = TestingClockConfig(clock)
    window_config = TumblingWindowConfig(
        length=timedelta(seconds=10), start_at=start_at
    )

    def count(counts, typ):
        if typ not in counts:
            counts[typ] = 0
        counts[typ] += 1
        return counts

    flow.fold_window("sum", clock_config, window_config, dict, count)

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert out == [
        ("a", {"login": 1, "post": 2}),
        ("b", {"login": 1}),
        ("a", {"post": 1}),
        ("b", {"post": 2}),
    ]
