import functools

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream, operator
from bytewax.testing import TestingSource


def add_one(x: int) -> int:
    return x + 1


v1 = lambda step_id, up: op.map(step_id, up, add_one)


def v2(step_id, up):
    return op.map(step_id, up, add_one)


v3 = functools.partial(op.map, mapper=add_one)


@operator
def v4(step_id: str, up: Stream[int]) -> Stream[int]:
    return op.map("inner", up, add_one)


flow = Dataflow("basic")
inp = op.input("inp", flow, TestingSource(range(10)))
x0 = inp.then(op.map, "v0", add_one)
x2 = inp.then(v1, "v1")
x2 = inp.then(v2, "v2")
x3 = inp.then(v3, "v3")
x4 = inp.then(v4, "v4")
