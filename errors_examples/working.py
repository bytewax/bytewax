import json
import operator
from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.window import SystemClockConfig, TumblingWindow
from common import WikiStreamInput


def initial_count(data_dict):
    return data_dict["server_name"], 1


def keep_max(max_count, new_count):
    new_max = max(max_count, new_count)
    return new_max, new_max


flow = Dataflow()
flow.input("inp", WikiStreamInput())
# "event_json"
flow.map(json.loads)
# {"server_name": "server.name", ...}
flow.map(initial_count)
# ("server.name", 1)
flow.reduce_window(
    "sum",
    SystemClockConfig(),
    TumblingWindow(length=timedelta(seconds=2)),
    operator.add,
)
# ("server.name", sum_per_window)
flow.stateful_map(
    "keep_max",
    lambda: 0,
    keep_max,
)
# ("server.name", max_per_window)
flow.output("out", StdOutput())


if __name__ == "__main__":
    # from bytewax.execution import run_main
    # run_main(flow, epoch_interval=timedelta(seconds=0))
    from bytewax.execution import spawn_cluster
    spawn_cluster(flow, epoch_interval=timedelta(seconds=0))
