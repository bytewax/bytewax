import itertools
import operator
from collections import Counter
from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import TestingOutputConfig
from bytewax.window import SystemClockConfig, TumblingWindowConfig


def input_builder(worker_index, worker_count, state):
    def file_input():
        for line in open("examples/sample_data/apriori.txt"):
            yield None, line

    return file_input()


def lower(line):
    return line.lower()


def tokenize(line):
    return [word.strip() for word in line.split(",")]


def initial_count(word):
    return word, 1


def is_most_common(item):
    return any((i in most_popular_products) for i in tokenize(item))


def build_pairs(line):
    tokens = tokenize(line)

    for left, right in itertools.combinations(tokens, 2):
        # popular pairs need to contain a popular product
        if left in most_popular_products or right in most_popular_products:
            pair = sorted((left, right))  # order elements for count
            yield "-".join(pair)


product_counter: Counter = Counter()
most_popular_products: dict[str, int] = {}

cc = SystemClockConfig()
wc = TumblingWindowConfig(length=timedelta(seconds=5))

# first pass - count products
out = []
flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_window("reduce", cc, wc, operator.add)
flow.capture(TestingOutputConfig(out))

# second pass - count pairs
out2 = []
flow2 = Dataflow()
flow2.input("input", ManualInputConfig(input_builder))
flow2.map(lower)
flow2.filter(is_most_common)  # count basket only with popular products
flow2.flat_map(build_pairs)
flow2.map(initial_count)
flow2.reduce_window("reduce", cc, wc, operator.add)
flow2.capture(TestingOutputConfig(out2))


if __name__ == "__main__":
    run_main(flow)
    for item in out:
        pair, count = item
        product_counter[pair] = count

    print("most popular products:")
    print(f"{'count':>5} name")
    # select top 3 popular products
    for product, count in product_counter.most_common(3):
        print(f"{count:>5} {product}")
        most_popular_products[product] = count

    most_popular_pairs: Counter = Counter()
    run_main(flow2)
    for item in out2:
        pair, count = item
        most_popular_pairs[pair] = count

    print("most popular pairs:")
    print(f"{'count':>5} pair")
    for pair, counter in most_popular_pairs.most_common(10):
        print(f"{counter:>5} {pair}")
