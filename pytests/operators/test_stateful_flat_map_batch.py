"""Tests for stateful_flat_map_batch operator (Fix #543)."""

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulLogic, _StatefulFlatMapBatchLogic
from bytewax.testing import TestingSink, TestingSource, run_main


class TestStatefulFlatMapBatchLogic:
    """Unit tests for _StatefulFlatMapBatchLogic."""

    def test_basic_state_updates(self):
        """State updates incrementally with each yielded pair."""

        def mapper(state, value):
            s = state or 0
            for _i in range(value):
                s += 1
                yield (s, s)

        logic = _StatefulFlatMapBatchLogic("test_step", mapper, None)
        items, discard = logic.on_item(3)

        assert list(items) == [1, 2, 3]
        assert discard == StatefulLogic.RETAIN
        assert logic.state == 3

    def test_discard_on_none_state(self):
        """State is discarded when mapper yields None as final state."""

        def mapper(state, value):
            yield (None, value)

        logic = _StatefulFlatMapBatchLogic("test_step", mapper, "initial")
        items, discard = logic.on_item(42)

        assert list(items) == [42]
        assert discard == StatefulLogic.DISCARD

    def test_empty_iterable(self):
        """Empty mapper output preserves existing state."""

        def mapper(state, value):
            return iter([])

        logic = _StatefulFlatMapBatchLogic("test_step", mapper, "keep_me")
        items, discard = logic.on_item(1)

        assert list(items) == []
        assert discard == StatefulLogic.RETAIN
        assert logic.state == "keep_me"

    def test_snapshot(self):
        """Snapshot returns deep copy of current state."""

        def mapper(state, value):
            s = (state or 0) + value
            yield (s, s)

        logic = _StatefulFlatMapBatchLogic("test_step", mapper, None)
        logic.on_item(5)
        snap = logic.snapshot()

        assert snap == 5
        # Verify deep copy (mutating snap doesn't affect logic)
        logic.on_item(3)
        assert snap == 5
        assert logic.state == 8

    def test_generator_mapper(self):
        """Mapper can be a generator function."""

        def gen_mapper(state, value):
            s = state or []
            for char in value:
                s = s + [char]
                yield (s[:], char)

        logic = _StatefulFlatMapBatchLogic("test_step", gen_mapper, None)
        items, _ = logic.on_item("abc")

        assert list(items) == ["a", "b", "c"]
        assert logic.state == ["a", "b", "c"]


class TestStatefulFlatMapBatchOperator:
    """Integration tests using run_main."""

    def test_running_sum(self):
        """Mapper yields running sum and each input value."""
        inp = [1, 2, 3]
        out = []

        def running_sum(state, value):
            s = state or 0
            s += value
            yield (s, value)

        flow = Dataflow("test_df")
        s = op.input("inp", flow, TestingSource(inp))
        s = op.key_on("key", s, lambda _x: "ALL")
        s = op.stateful_flat_map_batch("sum", s, running_sum)
        op.output("out", s, TestingSink(out))

        run_main(flow)
        assert out == [("ALL", 1), ("ALL", 2), ("ALL", 3)]

    def test_expand_with_state(self):
        """Each input value expands to multiple outputs with state tracking."""
        inp = [2, 3]
        out = []

        def expand(state, value):
            counter = state or 0
            for _i in range(value):
                counter += 1
                yield (counter, f"item-{counter}")

        flow = Dataflow("test_df")
        s = op.input("inp", flow, TestingSource(inp))
        s = op.key_on("key", s, lambda _x: "k")
        s = op.stateful_flat_map_batch("expand", s, expand)
        op.output("out", s, TestingSink(out))

        run_main(flow)
        assert out == [
            ("k", "item-1"),
            ("k", "item-2"),
            ("k", "item-3"),
            ("k", "item-4"),
            ("k", "item-5"),
        ]

    def test_discard_state(self):
        """Yielding None state discards state for that key."""
        inp = [1, 2, 3]
        out = []

        def discard_on_three(state, value):
            s = (state or 0) + value
            if s >= 6:
                yield (None, f"done-{s}")
            else:
                yield (s, f"acc-{s}")

        flow = Dataflow("test_df")
        s = op.input("inp", flow, TestingSource(inp))
        s = op.key_on("key", s, lambda _x: "k")
        s = op.stateful_flat_map_batch("acc", s, discard_on_three)
        op.output("out", s, TestingSink(out))

        run_main(flow)
        assert out == [("k", "acc-1"), ("k", "acc-3"), ("k", "done-6")]

    def test_multiple_keys(self):
        """Different keys maintain independent state."""
        inp = [("a", 1), ("b", 10), ("a", 2), ("b", 20)]
        out = []

        def accumulate(state, value):
            s = (state or 0) + value
            yield (s, s)

        flow = Dataflow("test_df")
        s = op.input("inp", flow, TestingSource(inp))
        s = op.stateful_flat_map_batch("acc", s, accumulate)
        op.output("out", s, TestingSink(out))

        run_main(flow)
        assert sorted(out) == sorted([("a", 1), ("b", 10), ("a", 3), ("b", 30)])
