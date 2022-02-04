import json
import sseclient
import urllib3
import collections
import time

import bytewax


def tick_every(interval_sec):
    epoch = 0
    last_bump_sec = time.time() - interval_sec
    while True:
        yield epoch
        now_sec = time.time()
        frac_intervals = (now_sec - last_bump_sec) / interval_sec
        if frac_intervals >= 1.0:
            epoch += int(frac_intervals)
            last_bump_sec = now_sec


def gen_input():
    pool = urllib3.PoolManager()
    resp = pool.request(
        "GET",
        "https://stream.wikimedia.org/v2/stream/recentchange/",
        preload_content=False,
        headers={"Accept": "text/event-stream"},
    )
    client = sseclient.SSEClient(resp)

    for epoch, event in zip(tick_every(2), client.events()):
        yield epoch, event.data


def server_name(data_dict):
    return data_dict["server_name"]


def count_edits(acc, server_names):
    for server_name in server_names:
        acc[server_name] += 1
    return acc


ec = bytewax.Executor()
flow = ec.Dataflow(gen_input())
flow.map(json.loads)
flow.map(server_name)
flow.exchange(hash)
flow.accumulate(lambda: collections.defaultdict(int), count_edits)
flow.inspect_epoch(print)


if __name__ == "__main__":
    ec.build_and_run()
