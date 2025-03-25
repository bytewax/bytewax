from typing import Optional, Tuple

import rerun as rr
from bytewax import operators as op
from bytewax.bytewax_rerun import RerunMessage, RerunSink
from bytewax.connectors.demo import RandomMetricSource
from bytewax.dataflow import Dataflow

flow = Dataflow("rerun-example-flow")

# Let's simulate input from 3 devices and merge them into a single stream
inp_1 = op.input("test-input-1", flow, RandomMetricSource("test-metric-1"))
inp_2 = op.input("test-input-2", flow, RandomMetricSource("test-metric-2"))
inp_3 = op.input("test-input-3", flow, RandomMetricSource("test-metric-3"))
metrics = op.merge("metrics", inp_1, inp_2, inp_3)
keyed_metrics = op.key_on("keyed-elements", metrics, lambda x: x[0])


def make_message(
    seconds_since_start: Optional[float], item: Tuple[str, float]
) -> Tuple[Optional[float], RerunMessage]:
    # Here we just pretend events come in every 0.2 seconds.
    # In a real world scenario, you would take the event's time from
    # the data, take the first one as a starting point, and always use
    # the number of seconds passed (in event time) since the first one
    # as `seconds_since_start`
    if seconds_since_start is None:
        seconds_since_start = 0
    else:
        seconds_since_start += 0.2

        # Time doesn't need to always go forward, rerun accepts items
        # in "the past". Try adding a random value that can be negative
        # instead of the fixed +0.2 we do above to see how it behaves.
        # from random import random
        # seconds_since_start += random() * 2 - 0.5

    name, value = item
    message = RerunMessage(
        entity_path=f"metrics/{name}",
        entity=rr.Scalar(value),
        timeline="metrics",
        time=seconds_since_start,
    )

    return (seconds_since_start, message)


keyed_times = op.stateful_map("get_time", keyed_metrics, make_message)
times = op.key_rm("remove-key", keyed_times)

# The default operating_mode is `spawn`, which spawns a rerun viewer instance
# if one is not running already, connects to the running one otherwise.
op.output("rerun-time-sink", times, RerunSink("app", "recording"))

# Other operating modes:

# `serve` operating mode: open a browser with the visualizer
# receiving data via websocket.
# op.output(
#     "rerun-time-sink",
#     times,
#     RerunSink("app", "recording", operating_mode="serve"),
# )

# `save` operating_mode: save data into a separate file for each worker.
# from pathlib import Path

# op.output(
#     "rerun-time-sink",
#     times,
#     RerunSink("app", "recording", operating_mode="save", save_dir=Path("./")),
# )

# `connect` operating_mode. Connects to an already running viewer instance.
# op.output(
#     "rerun-time-sink",
#     times,
#     RerunSink("app", "recording", operating_mode="connect", address="127.0.0.1:9876"),
# )
