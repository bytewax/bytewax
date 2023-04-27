import itertools
import operator

from collections import Counter
from datetime import timedelta, timezone, datetime

from bytewax.connectors.files import FileInput
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingOutput
from bytewax.window import SystemClockConfig, TumblingWindow


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
wc = TumblingWindow(
    length=timedelta(seconds=5), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)

# first pass - count products
out = []
flow = Dataflow()
flow.input("inp", FileInput("examples/sample_data/apriori.txt"))
flow.map(lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_window("reduce", cc, wc, operator.add)
flow.output("out", TestingOutput(out))

# second pass - count pairs
out2 = []
flow2 = Dataflow()
flow2.input("input", FileInput("examples/sample_data/apriori.txt"))
flow2.map(lower)
flow2.filter(is_most_common)  # count basket only with popular products
flow2.flat_map(build_pairs)
flow2.map(initial_count)
flow2.reduce_window("reduce", cc, wc, operator.add)
flow2.output("out", TestingOutput(out2))


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
