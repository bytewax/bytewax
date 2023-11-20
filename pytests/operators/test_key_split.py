from dataclasses import dataclass

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


@dataclass(frozen=True)
class Obj:
    obj_id: int
    name: str
    age: int
    is_cat: bool


def test_key_split():
    inp = [Obj(1234, "Burrito", 4, True), Obj(5678, "David", 36, False)]
    out_names = []
    out_ages = []
    out_is_cats = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    names, ages, is_cats = op.key_split(
        "key",
        s,
        lambda x: str(x.obj_id),
        lambda x: x.name,
        lambda x: x.age,
        lambda x: x.is_cat,
    )
    op.output("out_a", names, TestingSink(out_names))
    op.output("out_b", ages, TestingSink(out_ages))
    op.output("out_c", is_cats, TestingSink(out_is_cats))

    run_main(flow)

    assert out_names == [("1234", "Burrito"), ("5678", "David")]
    assert out_ages == [("1234", 4), ("5678", 36)]
    assert out_is_cats == [("1234", True), ("5678", False)]
