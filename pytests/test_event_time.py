from datetime import datetime, timedelta

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.window import TumblingWindowConfig, EventClockConfig, TestingClockConfig
from bytewax.outputs import TestingOutputConfig


def test_event_time_processing():
    """
    Test used to validate the EventClockConfig workings.
    """
    start_at = datetime.now()
    late = timedelta(seconds=10)
    window_length = timedelta(seconds=5)

    # Utility functions to generate "temp" events
    def temp(val, seconds):
        time = start_at + timedelta(seconds=seconds)
        return None, {"type": "temp", "time": time, "value": val}

    # Utility functions to generate wait events (used for advancing the clock)
    def wait():
        return None, {"type": "temp", "time": start_at}

    def input_builder(worker_index, worker_count, state):
        # Each yield advances the clock by 1 second
        # This should be processed in the first window
        yield temp(1, 1)
        # This too should be processed in the first window
        yield temp(2, 2)
        # Wait 10 seconds
        for _ in range(10):
            yield wait()
        # This should be dropped, because its received is after start_at + late
        yield temp(200, 1)
        # This should be processed in the second window
        yield temp(200, 7)
        # This should be processed in the third window
        yield temp(17, 12)

    def extract_sensor_type(event):
        return event["type"], event

    def acc_values(acc, event):
        if "value" in event:
            acc.append(event["value"])
        return acc

    cc = EventClockConfig(
        lambda event: event["time"],
        late_after_system_duration=late,
        system_clock_config=TestingClockConfig(start_at=start_at, item_incr=timedelta(seconds=1))
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
