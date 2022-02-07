import collections
import json
import time

import sseclient

import bytewax
import urllib3
from bytewax import inp


def gen_input():
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


def server_name(data_dict):
    return data_dict["server_name"]


def count_edits(acc, server_names):
    for server_name in server_names:
        acc[server_name] += 1
    return acc


ec = bytewax.Executor()
flow = ec.Dataflow(inp.tumbling_epoch(2.0, gen_input()))
flow.map(json.loads)
flow.map(server_name)
flow.exchange(hash)
flow.accumulate(lambda: collections.defaultdict(int), count_edits)
flow.inspect_epoch(print)


if __name__ == "__main__":
    ec.build_and_run()
