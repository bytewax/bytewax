from datetime import timedelta
from typing import List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.stateful_timeout import stateful_flat_map_timeout
from bytewax.testing import TestingSink, TestingSource, run_main


def test_stateful_map_timeout() -> None:
    flow = Dataflow("test_df")

    inp = [2, 5, 8, 1, 3]
    nums = op.input("inp", flow, TestingSource(inp))
    keyed_nums = op.key_on("key", nums, lambda _x: "ALL")

    def keep_smaller(last: Optional[int], new: int) -> Tuple[Optional[int], List[int]]:
        if last is None:
            return (new, [new])
        elif new > last:
            return (new, [new])
        else:
            return (new, [last])

    smaller_nums = stateful_flat_map_timeout(
        "keep_smaller",
        keyed_nums,
        keep_smaller,
        timeout=timedelta.max,
    )
    cleaned = op.key_rm("key_rm", smaller_nums)

    out: List[int] = []
    op.output("out", cleaned, TestingSink(out))

    run_main(flow)
    assert out == [2, 5, 8, 8, 3]
