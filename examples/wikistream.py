import json
import operator
from datetime import timedelta

# pip install sseclient-py urllib3
import sseclient
import urllib3

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import PartInput
from bytewax.outputs import StdOutputConfig
from bytewax.window import SystemClockConfig, TumblingWindow


class WikiStreamInput(PartInput):
    def list_parts(self):
        # Wikimedia's SSE stream has no way to request disjoint data,
        # so we have only one partition.
        return ["single-stream"]

    def build_part(self, for_key, resume_state):
        # Since there is no way to rewind to SSE data we missed while
        # resuming a dataflow, we're going to ignore `resume_state`
        # and drop missed data. That's fine as long as we know to
        # interpret the results with that in mind.
        assert for_key == "single-stream"
        assert resume_state is None

        pool = urllib3.PoolManager()
        resp = pool.request(
            "GET",
            "https://stream.wikimedia.org/v2/stream/recentchange/",
            preload_content=False,
            headers={"Accept": "text/event-stream"},
        )
        client = sseclient.SSEClient(resp)

        for event in client.events():
            yield None, event.data


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
flow.capture(StdOutputConfig())


if __name__ == "__main__":
    run_main(flow)
