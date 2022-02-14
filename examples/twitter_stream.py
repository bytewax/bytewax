import json
import operator
import time
from collections import defaultdict

from bytewax import Dataflow, inp, parse, run_cluster

from utils import twitter


def decode(x):
    try:
        return [json.loads(x)]
    except ValueError:
        return []  # Ignore errors. This will skip because in flat_map.


def coin_name(data_dict):
    # Get all of the mentioned hashtags.
    return [x["tag"] for x in data_dict["matching_rules"]]


def initial_count(coin):
    return coin, 1


flow = Dataflow()
# "event_json"
flow.flat_map(decode)
# {event_dict}
flow.flat_map(coin_name)
# "coin"
flow.map(initial_count)
# ("coin", 1)
flow.reduce_epoch(operator.add)
# ("coin", count)
flow.inspect(print)


if __name__ == "__main__":
    run_cluster(
        flow, inp.tumbling_epoch(2.0, twitter.get_stream()), **parse.cluster_args()
    )
