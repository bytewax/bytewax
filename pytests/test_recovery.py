from datetime import timedelta

from pytest import raises

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.testing import TestingInput, TestingOutput

ZERO_TD = timedelta(seconds=0)


def build_keep_max_dataflow(inp, explode_on):
    """Builds a dataflow that keeps track of the largest value seen for
    each key, but also allows you to reset the max with a value of
    `None`. Input is `(key, value, should_explode)`. Will throw
    exception if `should_explode` is truthy and `armed` is set.

    """
    flow = Dataflow()

    flow.input("inp", TestingInput(inp))

    def trigger(item):
        key, value, should_explode = item
        if should_explode == explode_on:
            raise RuntimeError("BOOM")
        return key, value

    flow.map(trigger)

    def keep_max(previous_max, new_item):
        if previous_max is None:
            new_max = new_item
        else:
            if new_item is not None:
                new_max = max(previous_max, new_item)
            else:
                new_max = None
        return new_max, new_max

    flow.stateful_map("keep_max", lambda: None, keep_max)

    return flow


def test_recover_with_latest_state(recovery_config):
    # Epoch is incremented after each item.
    inp = [
        # Epoch 0
        ("a", 4, False),
        # Epoch 1
        ("b", 4, False),
        # Epoch 2
        # Will fail here on first execution.
        ("a", 1, "BOOM1"),
        # Epoch 3
        ("b", 9, False),
        # Epoch 4
        # Will fail here on second execution.
        ("a", 9, "BOOM2"),
        # Epoch 3
        ("b", 1, False),
    ]

    out = []
    flow = build_keep_max_dataflow(inp, "BOOM1")
    flow.output("out", TestingOutput(out))

    # First execution.
    with raises(RuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert out == [
        ("a", 4),
        ("b", 4),
    ]

    # Disable first bomb.
    out = []
    flow = build_keep_max_dataflow(inp, "BOOM2")
    flow.output("out", TestingOutput(out))

    # Second execution.
    with raises(RuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # Restarts from failed epoch.
    assert out == [
        ("a", 4),
        ("b", 9),
    ]

    # Disable second bomb.
    out = []
    flow = build_keep_max_dataflow(inp, None)
    flow.output("out", TestingOutput(out))

    # Recover.
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # Restarts from failed epoch.
    assert out == [
        ("a", 9),
        ("b", 9),
    ]


def test_recover_doesnt_gc_last_write(recovery_config):
    # Epoch is incremented after each item.
    inp = [
        # Epoch 0
        # "a" is old enough to be GCd by time failure happens, but
        # shouldn't be because the key hasn't been seen again.
        ("a", 4, False),
        # Epoch 1
        ("b", 4, False),
        # Epoch 2
        ("b", 4, False),
        # Epoch 3
        ("b", 4, False),
        # Epoch 4
        ("b", 4, False),
        # Epoch 5
        # Will fail here on first execution.
        ("b", 5, "BOOM1"),
        # Epoch 6
        ("a", 1, False),
    ]

    out = []
    flow = build_keep_max_dataflow(inp, "BOOM1")
    flow.output("out", TestingOutput(out))

    # First execution.
    with raises(RuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert out == [
        ("a", 4),
        ("b", 4),
        ("b", 4),
        ("b", 4),
        ("b", 4),
    ]

    # Disable bomb.
    out = []
    flow = build_keep_max_dataflow(inp, None)
    flow.output("out", TestingOutput(out))

    # Recover.
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # Restarts from failed epoch.
    assert out == [
        ("b", 5),
        # Remembered "a": 4
        ("a", 4),
    ]


def test_recover_respects_delete(recovery_config):
    # Epoch is incremented after each item.
    inp = [
        # Epoch 0
        ("a", 4, False),
        # Epoch 1
        ("b", 4, False),
        # Epoch 2
        # Delete state for key.
        ("a", None, False),
        # Epoch 3
        ("b", 2, False),
        # Epoch 4
        # Will fail here on first execution.
        ("b", 5, "BOOM1"),
        # Epoch 5
        # Should be max for "a" on resume.
        ("a", 2, False),
    ]

    out = []
    flow = build_keep_max_dataflow(inp, "BOOM1")
    flow.output("out", TestingOutput(out))

    # First execution.
    with raises(RuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert out == [
        ("a", 4),
        ("b", 4),
        ("a", None),
        ("b", 4),
    ]

    # Disable bomb.
    out = []
    flow = build_keep_max_dataflow(inp, None)
    flow.output("out", TestingOutput(out))

    # Recover.
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # Restarts from failed epoch.
    assert out == [
        ("b", 5),
        # Notice not 4.
        ("a", 2),
    ]


def test_continuation(entry_point, inp, out, recovery_config):
    # Since we're modifying the input, use the fixture so it works
    # across processes. Currently, `inp = []`.
    inp.extend(
        [
            ("a", 4, False),
            ("b", 4, False),
        ]
    )
    flow = build_keep_max_dataflow(inp, None)
    flow.output("out", TestingOutput(out))

    entry_point(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert sorted(out) == [
        ("a", 4),
        ("b", 4),
    ]

    # Add new input. Don't clear because `TestingInputConfig` needs
    # the initial items so the resume epoch skips to here.
    inp.extend(
        [
            ("a", 1, False),
            ("b", 5, False),
        ]
    )
    # Unfortunately `ListProxy`, which we'd use in the cluster entry
    # point, does not have `clear`.
    del out[:]

    # Continue.
    entry_point(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # Incorporates new input.
    assert sorted(out) == [
        ("a", 4),
        ("b", 5),
    ]

    # Add more new input. Don't clear because `TestingInputConfig` needs
    # the initial items so the resume epoch skips to here.
    inp.extend(
        [
            ("a", 8, False),
            ("b", 1, False),
        ]
    )
    # Unfortunately `ListProxy`, which we'd use in the cluster entry
    # point, does not have `clear`.
    del out[:]

    # Continue again.
    entry_point(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # Incorporates new input.
    assert sorted(out) == [
        ("a", 8),
        ("b", 5),
    ]
