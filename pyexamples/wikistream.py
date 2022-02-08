import collections
import json
import operator
import time

import bytewax

import sseclient
import urllib3
from bytewax import inp


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


ec = bytewax.Executor()
flow = ec.Dataflow(inp.tumbling_epoch(2.0, open_stream()))
# "event_json"
flow.map(json.loads)
# {"server_name": "server.name", ...}
flow.map(group_by_server)
# ("server.name", 1)
flow.reduce_epoch(operator.add)
# ("server.name", count)
flow.inspect_epoch(print)


if __name__ == "__main__":
    ec.build_and_run()
