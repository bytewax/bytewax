import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_flat_map():
    inp = ["split this"]
    out = []

    def split_into_words(sentence):
        return sentence.split()

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.flat_map("split_into_words", s, split_into_words)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == ["split", "this"]
