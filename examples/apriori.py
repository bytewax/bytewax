import itertools
from typing import List

import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow("apriori")
inp = op.input("inp", flow, FileSource("examples/sample_data/apriori.txt"))


def tokenize(line: str) -> List[str]:
    return [word.strip() for word in line.split(",")]


baskets = op.map("tokenize", inp, tokenize)
items = op.flatten("flatten", baskets)
item_count = op.count_final("count_items", items, lambda item: item)


def pair_key(pair):
    a, b = pair
    return f"{a},{b}"


pairs = op.flat_map("pairs", baskets, lambda basket: itertools.combinations(basket, 2))
pairs = op.map("normalize", pairs, sorted)
pair_count = op.count_final("count_pairs", pairs, pair_key)

op.output("out_items", item_count, StdOutSink())
op.output("out_pairs", pair_count, StdOutSink())
