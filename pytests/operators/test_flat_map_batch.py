from typing import Iterable, List

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_flat_map_batch():
    inp = ["split this", "and this"]
    out = []

    def split_into_words(sentences: List[str]) -> Iterable[str]:
        for sentence in sentences:
            yield from sentence.split()

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp, batch_size=10))
    s = op.flat_map_batch("split_into_words", s, split_into_words)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == ["split", "this", "and", "this"]
