import json
import operator
from datetime import timedelta

# pip install sseclient-py urllib3
import sseclient
import urllib3

from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicInput, StatelessSource, EmptySource
from bytewax.connectors.stdio import StdOutput
from bytewax.window import SystemClockConfig, SessionWindow


class WikiSource(StatelessSource):
    def __init__(self):
        pool = urllib3.PoolManager()
        resp = pool.request(
            "GET",
            "https://stream.wikimedia.org/v2/stream/recentchange/",
            preload_content=False,
            headers={"Accept": "text/event-stream"},
        )
        self.client = sseclient.SSEClient(resp)
        self.events = self.client.events()

    def next(self):
        return next(self.events).data

    def close(self):
        self.client.close()


class WikiStreamInput(DynamicInput):
    def build(self, worker_index, worker_count):
        # We can't properly partition the input,
        # so we only get data in the first worker.
        if worker_index == 0:
            return WikiSource()
        else:
            return EmptySource()


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
    SessionWindow(gap=timedelta(seconds=2)),
    operator.add,
)
# ("server.name", sum_per_window)
flow.stateful_map("keep_max", lambda: 0, keep_max)
# ("server.name", max_per_window)
flow.output("out", StdOutput())


if __name__ == "__main__":
    from bytewax.execution import run_main

    run_main()
