import bytewax

from bytewax.inp import single_batch, fully_ordered


def test_single_batch():
    out = []

    def add_one(item):
        return item + 1

    batch = single_batch([0, 1, 2])

    ec = bytewax.Executor()
    flow = ec.Dataflow(batch)
    flow.map(add_one)
    flow.capture(out.append)

    ec.build_and_run()

    assert sorted(out) == sorted(
        [
            (0, 1),
            (0, 2),
            (0, 3),
        ]
    )


def test_fully_ordered():
    out = []

    def add_one(item):
        return item + 1

    ordered = fully_ordered([0, 1, 2])

    ec = bytewax.Executor()
    flow = ec.Dataflow(ordered)
    flow.map(add_one)
    flow.capture(out.append)

    ec.build_and_run()

    assert sorted(out) == sorted(
        [
            (0, 1),
            (1, 2),
            (2, 3),
        ]
    )
