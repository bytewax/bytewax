import re

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main
from pytest import raises


def test_flow_requires_output():
    inp = range(3)

    flow = Dataflow("test_df")
    op.input("inp", flow, TestingSource(inp))

    expect = "at least one output"
    with raises(ValueError, match=re.escape(expect)):
        run_main(flow)
