import collections
import operator
import re

from bytewax import Executor
from bytewax import inp
from bytewax import workers


def tokenize(x):
    return re.findall(r'[^\s!,.?":;0-9]+', x)


def initial_count(word):
    return word, 1


ec = Executor()
flow = ec.Dataflow(inp.single_batch(open("benches/benchmarks/collected-works.txt")))
# "Here we have full sentences"
flow.flat_map(tokenize)
# "Words"
flow.map(str.lower)
# "word"
flow.filter(lambda x: x != "and")
# "word_no_and"
flow.map(initial_count)
# ("word", 1)
flow.reduce_epoch(operator.add)
# ("word", count)
flow.inspect(print)


if __name__ == "__main__":
    workers.start_local_workers(ec, 4)
