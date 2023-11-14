import re
from typing import List

from bytewax.dataflow import Dataflow, Stream, load_op, operator
from bytewax.testing import TestingSource
from pytest import raises


@operator
def bad_op_with_nested_stream(
    up: Stream, step_id: str, not_allowed: List[Stream]
) -> Stream:
    return up.merge("merge", *not_allowed)


def test_raises_on_nested_stream():
    load_op(bad_op_with_nested_stream)

    flow = Dataflow("test_df")
    inp1 = flow.input("inp1", TestingSource([]))
    inp2 = flow.input("inp2", TestingSource([]))

    expect = "inconsistent stream scoping"
    with raises(ValueError, match=re.escape(expect)):
        inp1.bad_op_with_nested_stream("bad", [inp2])
