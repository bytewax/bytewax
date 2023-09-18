import itertools

from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow("apriori")
inp = flow.input("inp", FileSource("examples/sample_data/apriori.txt"))


def tokenize(line):
    return [word.strip() for word in line.split(",")]


baskets = inp.map("tokenize", tokenize)

items = baskets.flatten("flatten").count_final("count_items", lambda item: item)


def pair_key(pair):
    a, b = pair
    return f"{a},{b}"


pairs = (
    baskets.flat_map("pairs", lambda basket: itertools.combinations(basket, 2))
    .map("normalize", sorted)
    .count_final("count_pairs", pair_key)
)

items.merge("merge", pairs).output("out", StdOutSink())
