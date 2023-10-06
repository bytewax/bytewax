import operator
import re
from datetime import datetime, timedelta, timezone

from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.window import SystemClockConfig, TumblingWindow


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


cc = SystemClockConfig()
wc = TumblingWindow(
    length=timedelta(seconds=5), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)

flow = Dataflow()
flow.input("inp", FileSource("examples/sample_data/wordcount.txt"))
# Full line WITH uppercase
flow.map("lower", lower)
# full line lowercased
flow.flat_map("tokenize", tokenize)
# word
flow.map("initial_count", initial_count)
# ("word, 1")
flow.reduce_window(
    "sum",
    SystemClockConfig(),
    TumblingWindow(
        length=timedelta(seconds=5),
        align_to=datetime(2023, 1, 1, tzinfo=timezone.utc),
    ),
    operator.add,
)
# ("word", count)
flow.output("out", StdOutSink())
