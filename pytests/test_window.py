from datetime import datetime, timedelta

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingBuilderInputConfig
from bytewax.outputs import TestingOutputConfig
from bytewax.testing import TestingClock
from bytewax.window import TestingClockConfig, TumblingWindowConfig


def test_tumbling_window():
    start_at = datetime(2022, 1, 1)
    clock = TestingClock(start_at)

    flow = Dataflow()

    def gen():
        clock.now = start_at  # +0 sec
        yield ("ALL", 1)
        clock.now += timedelta(seconds=4)  # +4 sec
        yield ("ALL", 1)
        clock.now += timedelta(seconds=4)  # +8 sec
        yield ("ALL", 1)
        clock.now += timedelta(seconds=4)  # + 12 sec
        # First 10 second window should close just before processing this item.
        yield ("ALL", 1)
        clock.now += timedelta(seconds=4)  # +16 sec

    flow.input("inp", TestingBuilderInputConfig(gen))

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
