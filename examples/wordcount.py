import operator
import re
from datetime import timedelta

from bytewax.connectors.files import FileInput
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.outputs import StdOutputConfig
from bytewax.window import SystemClockConfig, TumblingWindowConfig


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


cc = SystemClockConfig()
wc = TumblingWindowConfig(length=timedelta(seconds=5))

flow = Dataflow()
flow.input("inp", FileInput("examples/sample_data/wordcount.txt"))
# Full line WITH uppercase
flow.map(lower)
# full line lowercased
flow.flat_map(tokenize)
# word
flow.map(initial_count)
# ("word, 1")
flow.reduce_window(
    "sum",
    SystemClockConfig(),
    TumblingWindowConfig(length=timedelta(seconds=5)),
    operator.add,
)
# ("word", count)
flow.capture(StdOutputConfig())

if __name__ == "__main__":
    run_main(flow)
