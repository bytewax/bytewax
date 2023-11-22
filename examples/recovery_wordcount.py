import re

from bytewax import operators as op
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
stream = op.input("inp", flow, FileSource("examples/sample_data/wordcount.txt"))
# "Here, we have FULL sentences."
stream = op.map("lower", stream, lower)
# "here, we have lowercase sentences."
stream = op.flat_map("tokenize", stream, tokenize)
# "words"
stream = op.map("initial_count", stream, initial_count)
# ("word", 1)
stream = op.stateful_map("running_count", stream, count_builder, add)
# ("word", running_count)
op.output("out", stream, StdOutSink())
