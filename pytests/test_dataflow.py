import re
from typing import List

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream, operator
from bytewax.testing import TestingSource
from pytest import raises


@operator
def bad_op_with_nested_stream(
    step_id: str, up: Stream, not_allowed: List[Stream]
) -> Stream:
    return op.merge("merge", up, *not_allowed)


def test_raises_on_nested_stream():
    flow = Dataflow("test_df")
    inp1 = op.input("inp1", flow, TestingSource([]))
    inp2 = op.input("inp2", flow, TestingSource([]))

    expect = "inconsistent stream scoping"
    with raises(ValueError, match=re.escape(expect)):
        bad_op_with_nested_stream("bad", inp1, [inp2])
