import re

from bytewax import Dataflow, parse, run_cluster


def file_input():
    return open("examples/sample_data/wordcount.txt")

def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


def add(count1, count2):
    return count1 + count2


flow = Dataflow()
# "Here, we have FULL sentences."
flow.map(lower)
# "here, we have lowercase sentences."
flow.flat_map(tokenize)
# "words"
flow.map(initial_count)
# ("word", 1)
flow.reduce_epoch(add)
# ("word", count)
flow.capture()


if __name__ == "__main__":
    for epoch, item in run_cluster(flow, file_input(), **parse.cluster_args()):
        print(epoch, item)
