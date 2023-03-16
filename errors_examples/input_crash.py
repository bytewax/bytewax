import json
import operator
from datetime import timedelta

# pip install sseclient-py urllib3
import sseclient
import urllib3

from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicInput, StatelessSource
from bytewax.connectors.stdio import StdOutput
from bytewax.window import SystemClockConfig, TumblingWindow


class WikiSource(StatelessSource):
    def __init__(self, client, events):
        self.client = client
        self.events = events

    def next(self):
        raise Exception("BOOM")
        return next(self.events).data

    def close(self):
        self.client.close()


class WikiStreamInput(DynamicInput):
    def build(self, worker_index, worker_count):
        self.pool = urllib3.PoolManager()
        self.resp = self.pool.request(
             "GET",
             "https://stream.wikimedia.org/v2/stream/recentchange/",
             preload_content=False,
             headers={"Accept": "text/event-stream"},
         )
        self.client = sseclient.SSEClient(self.resp)
        self.events = self.client.events()
        return WikiSource(self.client, self.events)


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
flow.output("out", StdOutput())


if __name__ == "__main__":
    from bytewax.execution import run_main
    run_main(flow, epoch_interval=timedelta(seconds=0))
    # from bytewax.execution import spawn_cluster
    # spawn_cluster(flow, epoch_interval=timedelta(seconds=0))
