import re
import operator

from bytewax import inputs, run, Dataflow


def tokenize(x):
    x = x.lower()
    return re.findall(r'[^\s!,.?":;0-9]+', x)


def initial_count(word):
    return word, 1


flow = Dataflow()
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_epoch(operator.add)

if __name__ == "__main__":
    run(flow, inputs.single_batch(open("benches/benchmarks/collected-works.txt")))
