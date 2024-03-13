from typing import Iterable, List

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import mark


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


def build_flat_map_batch_dataflow(out: List) -> Dataflow:
    flow = Dataflow("flat_map_batch")
    inp = TestingSource(range(100_000), 10)
    s = op.input("inp", flow, inp)
    batch_out = op.flat_map_batch("flat_map", s, lambda xs: (x for x in xs))
    op.output("out", batch_out, TestingSink(out))
    return flow


def run_flat_map_batch_dataflow(entry_point, flow, out, expected):
    entry_point(flow)
    assert out == expected
    out.clear()


@mark.parametrize("entry_point_name", ["run_main", "cluster_main_one_worker"])
def test_flat_map_batch_benchmark(benchmark, entry_point):
    out = []
    flow = build_flat_map_batch_dataflow(out)
    expected = list(range(100_000))
    benchmark(lambda: run_flat_map_batch_dataflow(entry_point, flow, out, expected))
