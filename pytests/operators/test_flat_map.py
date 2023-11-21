from typing import Iterable, List, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_flat_map():
    inp = ["split this"]
    out = []

    def split_into_words(sentence: str) -> List[str]:
        return sentence.split()

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.flat_map("split_into_words", s, split_into_words)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == ["split", "this"]


def test_flat_map_iterable():
    inp = [("a", 2), ("b", 4)]
    out = []

    def repeat(val_count: Tuple[str, int]) -> Iterable[str]:
        val, count = val_count
        for _ in range(count):
            yield val

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.flat_map("repeat", s, repeat)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == ["a", "a", "b", "b", "b", "b"]
