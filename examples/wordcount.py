import re

from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


flow = Dataflow("wordcount")
lines = flow.input("inp", FileSource("examples/sample_data/wordcount.txt"))
# Full line WITH uppercase
lower_lines = lines.map("lower", lower)
# full line lowercased
lower_words = lower_lines.flat_map("tokenize", tokenize)
# "word"
word_counts = lower_words.count_final(
    "count",
    lambda word: word,
)
# ("word", count)
word_counts.output("out", StdOutSink())
