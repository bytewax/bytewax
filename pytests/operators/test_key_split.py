from dataclasses import dataclass

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
    s = flow.input("inp", TestingSource(inp))
    names, ages, is_cats = s.key_split(
        "key",
        lambda x: str(x.obj_id),
        lambda x: x.name,
        lambda x: x.age,
        lambda x: x.is_cat,
    )
    names.output("out_a", TestingSink(out_names))
    ages.output("out_b", TestingSink(out_ages))
    is_cats.output("out_c", TestingSink(out_is_cats))

    run_main(flow)

    assert out_names == [("1234", "Burrito"), ("5678", "David")]
    assert out_ages == [("1234", 4), ("5678", 36)]
    assert out_is_cats == [("1234", True), ("5678", False)]
