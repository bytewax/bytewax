import json
import time
from collections import defaultdict

import bytewax
from bytewax import inp
from utils import twitter


def decode(x):
    # add try/except to handle nulls
    try:
        return json.loads(x)
    except ValueError:
        return {"matching_rules": []}


# get all of the mentioned hashtags
def coin_name(data_dict):
    return [x["tag"] for x in data_dict["matching_rules"]]


# count the mentions
def count_edits(acc, tags):
    for tag in tags:
        acc[tag] += 1
    return acc


ec = bytewax.Executor()
df = ec.Dataflow(inp.tumbling_epoch(5.0, twitter.get_stream()))
df.map(decode)
df.flat_map(coin_name)
df.exchange(hash)
df.accumulate(lambda: defaultdict(int), count_edits)
df.inspect(print)


if __name__ == "__main__":
    ec.build_and_run()
