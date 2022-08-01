from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingInputConfig
from bytewax.outputs import TestingOutputConfig
from bytewax.window import TestingClockConfig, TumblingWindowConfig


def test_tumbling_window():
    def gen():
        for e in range(4):
            yield ("ALL", 1)

    flow = Dataflow(TestingInputConfig(gen()))
    # This will result in times for events of +0, +4, +8, +12.
    clock_config = TestingClockConfig(item_incr=timedelta(seconds=4))
    # And since the window is +10, we should get a window with value
    # of 3 and then 1.
    window_config = TumblingWindowConfig(length=timedelta(seconds=10))

    def add(acc, x):
        return acc + x

    flow.reduce_window("sum", clock_config, window_config, add)
    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted([("ALL", 3), ("ALL", 1)])
