import json
import operator
from datetime import timedelta

# pip install sseclient-py urllib3
import sseclient
import urllib3

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.recovery import SqliteRecoveryConfig
from bytewax.window import SystemClockConfig, TumblingWindowConfig


def input_builder(worker_index, worker_count, resume_state):
    # Multiple SSE connections will duplicate the streams, so only
    # have the first worker generate input.
    if worker_index == 0:
        pool = urllib3.PoolManager()
        resp = pool.request(
            "GET",
            "https://stream.wikimedia.org/v2/stream/recentchange/",
            preload_content=False,
            headers={"Accept": "text/event-stream"},
        )
        client = sseclient.SSEClient(resp)

        # Since there is no way to replay missed SSE data, we're going
        # to drop missed data. That's fine as long as we know to
        # interpret the results with that in mind.
        for event in client.events():
            yield (None, event.data)


def initial_count(data_dict):
    return data_dict["server_name"], 1


def keep_max(max_count, new_count):
    new_max = max(max_count, new_count)
    return new_max, new_max


flow = Dataflow()
flow.input("inp", ManualInputConfig(input_builder))
# "event_json"
flow.map(json.loads)
# {"server_name": "server.name", ...}
flow.map(initial_count)
# ("server.name", 1)
flow.reduce_window(
    "sum",
    SystemClockConfig(),
    TumblingWindowConfig(length=timedelta(seconds=2)),
    operator.add,
)
# ("server.name", sum_per_window)
flow.stateful_map(
    "keep_max",
    lambda: 0,
    keep_max,
)
# ("server.name", max_per_window)
flow.capture(StdOutputConfig())


if __name__ == "__main__":
    spawn_cluster(
        flow,
        recovery_config=SqliteRecoveryConfig("."),
        **parse.cluster_args(),
    )
