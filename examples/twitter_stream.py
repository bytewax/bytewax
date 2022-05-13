import json
import operator
from collections import defaultdict
from datetime import timedelta

from utils import twitter

from bytewax import Dataflow, inputs, parse, run_cluster


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
flow.capture()


if __name__ == "__main__":
    for epoch, item in run_cluster(
        flow,
        inputs.tumbling_epoch(twitter.get_stream(), timedelta(seconds=2)),
        **parse.cluster_args()
    ):
        print(epoch, item)
