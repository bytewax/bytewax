from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.window import TumblingWindowConfig, EventClockConfig
from bytewax.outputs import TestingOutputConfig
from bytewax.testing import TestingClock


def test_event_time_processing():
    """
    Test used to validate the EventClockConfig workings.
    """
    start_at = datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    clock = TestingClock(start_at)
    late = timedelta(seconds=10)
    window_length = timedelta(seconds=5)

    def s(val):
        return timedelta(seconds=val)

    def input_builder(worker_index, worker_count, state):
        clock.now = start_at + timedelta(seconds=2)
        # This should be processed in the first window
        yield None, {"type": "temp", "time": start_at, "value": 1}
        # This too should be processed in the first window
        clock.now = start_at + timedelta(seconds=6)
        yield None, {"type": "temp", "time": start_at + s(2), "value": 2}
        # This should be dropped, because its event_time is before
        # the latest event time, and it arrives `late` seconds after
        # the closing of the window + the delay of the latest received item.
        clock.now = start_at + timedelta(seconds=19.1)
        yield None, {"type": "temp", "time": start_at + s(1), "value": 200}
        # This should be processed in the second window
        clock.now += timedelta(seconds=1)
        yield None, {"type": "temp", "time": start_at + s(7), "value": 200}
        # This should be processed in the third window
        clock.now += timedelta(seconds=1)
        yield None, {"type": "temp", "time": start_at + s(12), "value": 17}

    def extract_sensor_type(event):
        return event["type"], event

    def acc_values(acc, event):
        acc.append(event["value"])
        return acc

    cc = EventClockConfig(
        lambda event: event["time"],
        wait_for_system_duration=late,
        system_clock=clock
    )
    wc = TumblingWindowConfig(start_at=start_at, length=window_length)

    flow = Dataflow()
    flow.input("input", ManualInputConfig(input_builder))
    flow.map(extract_sensor_type)
    flow.fold_window("running_average", cc, wc, list, acc_values)
    flow.map(lambda x: {f"{x[0]}_avg": sum(x[1]) / len(x[1])})
    out = []
    flow.capture(TestingOutputConfig(out))
    run_main(flow)

    expected = [
        {"temp_avg": 1.5},
        {"temp_avg": 200},
        {"temp_avg": 17},
    ]
    assert len(out) == 3
    assert expected[0] in out
    assert expected[1] in out
    assert expected[2] in out
