from datetime import timedelta
from typing import Tuple

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

# from bytewax.tracing import setup_tracing

# setup_tracing(log_level="TRACE")


def double(x: int) -> int:
    return x * 2


def halve(x: int) -> int:
    return x // 2


def minus_one(x: int) -> int:
    return x - 1


def stringy(x: int) -> Tuple[str, str]:
    return "all", f"<dance>{x}</dance>"


flow = Dataflow("basic")

inp = op.input("inp", flow, TestingSource(range(10)))
branch = op.branch("e_o", inp, lambda x: x % 2 == 0)
evens = op.map("halve", branch.trues, halve)
odds = op.map("double", branch.falses, double)
combo = op.merge("merge", evens, odds)
combo = op.map("minus_one", combo, minus_one)
string_output = op.map("stringy", combo, stringy)
out = op.collect("collect", string_output, timedelta(seconds=10), 3)
op.output("out", out, StdOutSink())
