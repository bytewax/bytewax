import bytewax
from pytest import raises


def test_threads_exception_in_all_workers():
    out = []

    def boom(item):
        raise RuntimeError()

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (0, 0),
            (0, 1),
            (0, 2),
        ]
    )
    flow.map(boom)
    flow.capture(out.append)

    with raises(RuntimeError):
        ec.build_and_run(threads=2)

    assert out == []


def test_threads_exception_in_one_worker_shuts_down_all():
    out = []

    def boom(item):
        if item == 0:
            raise RuntimeError()
        else:
            return item

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (0, 0),
            (0, 1),
            (0, 2),
        ]
    )
    flow.map(boom)
    flow.capture(out.append)

    with raises(RuntimeError):
        ec.build_and_run(threads=2)

    assert len(out) < 3
