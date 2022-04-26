import collections
import json
import operator
from datetime import timedelta

import redis
import sseclient
import urllib3
from bytewax import Dataflow, inputs, parse, spawn_cluster


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


def input_builder(worker_index, worker_count):
    try:
        epoch_start = int(r.get("epoch_index")) + 1
    except IndexError:
        epoch_start = 0
    if worker_index == 0:
        return inputs.tumbling_epoch(
            open_stream(), timedelta(seconds=2), epoch_start=epoch_start
        )
    else:
        return []


def output_builder(worker_index, worker_count):
    def write_to_redis(data):
        epoch, (server_name, counts) = data
        r.zadd(str(epoch), {server_name: counts})
        r.set("epoch_index", epoch)

    return write_to_redis


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
pool = redis.ConnectionPool(host="localhost", port=6379, db=0)
r = redis.Redis(connection_pool=pool)
flow.capture()


if __name__ == "__main__":
    spawn_cluster(flow, input_builder, output_builder, **parse.cluster_args())
