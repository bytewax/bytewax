import json
import operator
from datetime import timedelta

# pip install sseclient-py urllib3
import sseclient
import urllib3

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import DynamicInput, StatelessSource
from bytewax.window import SystemClockConfig, TumblingWindow


class WikiSource(StatelessSource):
    def __init__(self, client, events, worker_index):
        self.worker_index = worker_index
        if worker_index == 0:
            self.client = client
            self.events = events

    def next(self):
        if self.worker_index == 0:
            return next(self.events).data

    def close(self):
        if self.worker_index == 0:
            self.client.close()


class WikiStreamInput(DynamicInput):
    def __init__(self):
        self.pool = urllib3.PoolManager()
        self.resp = self.pool.request(
             "GET",
             "https://stream.wikimedia.org/v2/stream/recentchange/",
             preload_content=False,
             headers={"Accept": "text/event-stream"},
         )
        self.client = sseclient.SSEClient(self.resp)
        self.events = self.client.events()

    def build(self, worker_index, worker_count):
        return WikiSource(self.client, self.events, worker_index)


def initial_count(data_dict):
    return data_dict["server_name"], 1


def keep_max(max_count, new_count):
    new_max = max(max_count, new_count)
    return new_max, new_max


flow = Dataflow()
flow.input("inp", WikiStreamInput())
flow.map(json.loads)
flow.map(initial_count)
flow.reduce_window(
    "sum",
    SystemClockConfig(),
    TumblingWindow(length=timedelta(seconds=2)),
    operator.add,
)
flow.stateful_map(
    "keep_max",
    lambda: 0,
    keep_max,
)
# XXX: Error here
# flow.output("out", StdOutput())


if __name__ == "__main__":
    run_main(flow)
