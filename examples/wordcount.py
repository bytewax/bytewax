import re

from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


flow = Dataflow("wordcount")
stream = flow.input("inp", FileSource("examples/sample_data/wordcount.txt"))
# Full line WITH uppercase
stream = stream.map("lower", lower)
# full line lowercased
stream = stream.flat_map("tokenize", tokenize)
# "word"
stream = stream.count_final("count", lambda word: word)
# ("word", count)
stream.output("out", StdOutSink())
