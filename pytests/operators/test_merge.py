from typing import List

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main


def test_merge():
    inp_odds = [1, 3, 5]
    inp_evens = [2, 4, 6]
    inp_huge = [100, 200, 300]
    out = []

    flow = Dataflow("test_df")
    odds = op.input("inp_odds", flow, TestingSource(inp_odds))
    evens = op.input("inp_evens", flow, TestingSource(inp_evens))
    huge = op.input("inp_huge", flow, TestingSource(inp_huge))
    s = op.merge("merge", odds, evens, huge)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 100, 3, 4, 200, 5, 6, 300]


def build_fan_out_merge_dataflow(out: List[int]) -> Dataflow:
    flow = Dataflow("fan_out_merge")
    inp = TestingSource(range(100_000), 1000)
    s = op.input("inp", flow, inp)
    s = op.redistribute("redistribute", s)
    # Timely clones every item of a stream read by more than one
    # operator, so this exercises cloning on all workers at once.
    zeros = op.map("times_3", s, lambda x: x * 3)
    ones = op.map("times_3_plus_1", s, lambda x: x * 3 + 1)
    twos = op.map("times_3_plus_2", s, lambda x: x * 3 + 2)
    merged = op.merge("merge", zeros, ones, twos)
    op.output("out", merged, TestingSink(out))
    return flow


def run_fan_out_merge_dataflow(entry_point, flow: Dataflow, out: List[int]) -> None:
    entry_point(flow)
    # Workers interleave their output, so the order is not stable.
    assert sorted(out) == list(range(300_000))
    out.clear()


def test_merge_fan_out(entry_point):
    out = []
    flow = build_fan_out_merge_dataflow(out)
    run_fan_out_merge_dataflow(entry_point, flow, out)


def _cluster_main_4thread(flow: Dataflow) -> None:
    cluster_main(flow, [], 0, worker_count_per_proc=4)


def test_merge_fan_out_4thread_benchmark(benchmark):
    out = []
    flow = build_fan_out_merge_dataflow(out)
    benchmark(lambda: run_fan_out_merge_dataflow(_cluster_main_4thread, flow, out))
