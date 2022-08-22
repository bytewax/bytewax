import re
import operator
from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.window import TumblingWindowConfig, SystemClockConfig


def input_builder(worker_index, worker_count, resume_state):
    state = None  # ignore recovery
    for line in open("examples/sample_data/wordcount.txt"):
        yield state, line


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1

def output_builder(worker_index, worker_count):
    return print


cc = SystemClockConfig()
wc = TumblingWindowConfig(length=timedelta(seconds=5))

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
# Full line WITH uppercase
flow.map(lower)
# full line lowercased
flow.flat_map(tokenize)
# word
flow.map(initial_count)
# ("word, 1")
flow.reduce_window(
    "sum", SystemClockConfig(), TumblingWindowConfig(length=timedelta(seconds=5)), operator.add
)
# ("word", count)
flow.capture(StdOutputConfig())

if __name__ == "__main__":
    run_main(flow)
