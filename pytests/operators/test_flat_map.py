from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_flat_map():
    inp = ["split this"]
    out = []

    def split_into_words(sentence):
        return sentence.split()

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.flat_map("split_into_words", split_into_words)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == ["split", "this"]
