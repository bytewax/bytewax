import re

from bytewax.connectors.files import FileInput
from bytewax.connectors.stdio import StdOutput
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


flow = Dataflow()
flow.input("inp", FileInput("examples/sample_data/wordcount.txt"))
# "Here, we have FULL sentences."
flow.map(lower)
# "here, we have lowercase sentences."
flow.flat_map(tokenize)
# "words"
flow.map(initial_count)
# ("word", 1)
flow.stateful_map("running_count", count_builder, add)
# ("word", running_count)
flow.output("out", StdOutput())
