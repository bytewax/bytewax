from bytewax import Dataflow, run_sync
from bytewax.inp import fully_ordered, single_batch


def test_single_batch():
    def add_one(item):
        return item + 1

    batch = single_batch([0, 1, 2])

    flow = Dataflow()
    flow.map(add_one)
    flow.capture()

    out = run_sync(flow, batch)

    assert sorted(out) == sorted(
        [
            (0, 1),
            (0, 2),
            (0, 3),
        ]
    )


def test_fully_ordered():
    def add_one(item):
        return item + 1

    ordered = fully_ordered([0, 1, 2])

    flow = Dataflow()
    flow.map(add_one)
    flow.capture()

    out = run_sync(flow, ordered)

    assert sorted(out) == sorted(
        [
            (0, 1),
            (1, 2),
            (2, 3),
        ]
    )
