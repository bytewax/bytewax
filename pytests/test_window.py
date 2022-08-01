from datetime import timedelta
from time import sleep

from bytewax import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import AdvanceTo, Emit, ManualInputConfig
from bytewax.window import SystemClockConfig, TumblingWindowConfig


def test_system_clock_tumbling_window():
    def ib(i, n, r):
        assert r == 0
        for e in range(4):
            yield Emit(("ALL", 1))
            # Remember currently the input handle buffers unless the
            # epoch is advanced and windowing happens _inside_ the
            # reduce_window operator. So we have to kick the epoch to
            # get things to work.
            yield AdvanceTo(e + 1)
            sleep(0.4)

    out = []

    def ob(i, n):
        return out.append

    def add(acc, x):
        return acc + x

    clock_config = SystemClockConfig()
    window_config = TumblingWindowConfig(length=timedelta(seconds=1))

    flow = Dataflow()
    flow.reduce_window("sum", clock_config, window_config, add)
    flow.capture()

    run_main(flow, ManualInputConfig(ib), ob)

    assert sorted(out) == sorted(
        [
            (3, ("ALL", 3)),
            (4, ("ALL", 1)),
        ]
    )
