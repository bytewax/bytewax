import re

import bytewax
from bytewax import inp


def file_input():
    for line in open("examples/sample_data/wordcount.txt"):
        yield 1, line


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


def add(count1, count2):
    return count1 + count2


ec = bytewax.Executor()
flow = ec.Dataflow(file_input())
# "Here, we have FULL sentences."
flow.map(lower)
# "here, we have lowercase sentences."
flow.flat_map(tokenize)
# "words"
flow.map(initial_count)
# ("word", 1)
flow.reduce_epoch(add)
# ("word", count)
flow.inspect_epoch(print)


if __name__ == "__main__":
    ec.build_and_run()
