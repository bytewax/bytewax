from datetime import timedelta

from bytewax import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import Emit, ManualInputConfig
from bytewax.window import TestingClockConfig, TumblingWindowConfig


def test_tumbling_window():
    def ib(i, n, r):
        assert r == 0
        for e in range(4):
            yield Emit(("ALL", 1))

    out = []

    def ob(i, n):
        return out.append

    def add(acc, x):
        return acc + x

    # This will result in times for events of +0, +4, +8, +12.
    clock_config = TestingClockConfig(item_incr=timedelta(seconds=4))
    # And since the window is +10, we should get a window with value
    # of 3 and then 1.
    window_config = TumblingWindowConfig(length=timedelta(seconds=10))

    flow = Dataflow()
    flow.reduce_window("sum", clock_config, window_config, add)
    flow.capture()

    run_main(flow, ManualInputConfig(ib), ob)

    assert sorted(out) == sorted(
        [
            (0, ("ALL", 3)),
            (0, ("ALL", 1)),
        ]
    )
