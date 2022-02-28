import collections
import json
import operator
from datetime import timedelta

import sseclient
import urllib3
from bytewax import Dataflow, inputs, parse, run_cluster


def open_stream():
    pool = urllib3.PoolManager()
    resp = pool.request(
        "GET",
        "https://stream.wikimedia.org/v2/stream/recentchange/",
        preload_content=False,
        headers={"Accept": "text/event-stream"},
    )
    client = sseclient.SSEClient(resp)

    for event in client.events():
        yield event.data


def initial_count(data_dict):
    return data_dict["server_name"], 1


flow = Dataflow()
# "event_json"
flow.map(json.loads)
# {"server_name": "server.name", ...}
flow.map(initial_count)
# ("server.name", 1)
flow.reduce_epoch(operator.add)
# ("server.name", count)
flow.capture()


if __name__ == "__main__":
    for epoch, item in run_cluster(
        flow,
        inputs.tumbling_epoch(open_stream(), timedelta(seconds=2)),
        **parse.cluster_args()
    ):
        print(epoch, item)
