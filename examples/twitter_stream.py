import json
import operator
from collections import defaultdict
from datetime import timedelta

from utils import twitter

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.window import TumblingWindowConfig, SystemClockConfig


def input_builder(worker_index, worker_count, resume_state):
    state = None  # ignore recovery
    for tweet in twitter.get_stream():
        yield state, tweet


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
flow.input("input", ManualInputConfig(input_builder))
# "event_json"
flow.flat_map(decode)
# {event_dict}
flow.flat_map(coin_name)
# "coin"
flow.map(initial_count)
# ("coin", 1)
flow.reduce_window(
    "sum", SystemClockConfig(), TumblingWindowConfig(length=timedelta(seconds=2)), operator.add
)
# ("coin", count)
flow.capture(StdOutputConfig())

if __name__ == "__main__":
    run_main(flow)

