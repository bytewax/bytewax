import itertools
import operator
from collections import Counter

from bytewax import Dataflow, parse, run_cluster


def file_input():
    for line in open("examples/sample_data/apriori.txt"):
        yield 1, line


def lower(line):
    return line.lower()


def tokenize(line):
    return [l.strip() for l in line.split(",")]


def initial_count(word):
    return word, 1


def is_most_common(item):
    return any((i in most_popular_products) for i in tokenize(item))


def build_pairs(line):
    tokens = tokenize(line)

    for left, right in itertools.combinations(tokens, 2):

        # popular pairs need to contain a popular product
        if left not in (most_popular_products) and right not in most_popular_products:
            continue

        pair = tuple(sorted((left, right)))  # order tuple for count
        yield pair


product_counter: Counter = Counter()
most_popular_products: dict[str, int] = {}

# first pass - count products
flow = Dataflow()
flow.map(lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_epoch(operator.add)
flow.capture()

# second pass - count pairs
flow2 = Dataflow()
flow2.map(lower)
flow2.filter(is_most_common)  # count basket only with popular products
flow2.flat_map(build_pairs)
flow2.map(initial_count)
flow2.reduce_epoch(operator.add)
flow2.capture()


if __name__ == "__main__":
    for _, item in run_cluster(flow, file_input(), **parse.cluster_args()):
        pair, count = item
        product_counter[pair] = count

    print("most popular products:")
    print(f"{'count':>5} name")
    # select top 3 popular products
    for product, count in product_counter.most_common(3):
        print(f"{count:>5} {product}")
        most_popular_products[product] = count

    most_popular_pairs: Counter = Counter()
    for _, item in run_cluster(flow2, file_input(), **parse.cluster_args()):
        pair, count = item
        most_popular_pairs[pair] = count

    print("most popular pairs:")
    print(f"{'count':>5} pair")
    for pair, counter in most_popular_pairs.most_common(10):
        print(f"{counter:>5} {pair}")
