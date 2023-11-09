import re

from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    if word == "arrows":
        msg = "BOOM"
        raise RuntimeError(msg)
    return word, 1


def count_builder():
    return 0


def add(running_count, new_count):
    running_count += new_count
    return running_count, running_count


flow = Dataflow("recovery")
(
    flow.input("inp", FileSource("examples/sample_data/wordcount.txt"))
    # "Here, we have FULL sentences."
    .map("lower", lower)
    # "here, we have lowercase sentences."
    .flat_map("tokenize", tokenize)
    # "words"
    .map("initial_count", initial_count).key_assert("assert keyed")
    # ("word", 1)
    .stateful_map("running_count", count_builder, add)
    # ("word", running_count)
    .output("out", StdOutSink())
)
