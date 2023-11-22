import re

import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


flow = Dataflow("wordcount")
stream = op.input("inp", flow, FileSource("examples/sample_data/wordcount.txt"))
# Full line WITH uppercase
stream = op.map("lower", stream, lower)
# full line lowercased
stream = op.flat_map("tokenize", stream, tokenize)
# "word"
count_stream = op.count_final("count", stream, lambda word: word)
# ("word", count)
op.output("out", count_stream, StdOutSink())
