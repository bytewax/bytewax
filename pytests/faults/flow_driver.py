"""Deterministic recovery workload for the block-fault gates.

Run as a subprocess, so that a panic or a stalled store only ends this
process:

    python flow_driver.py STORE_DIR OUT_FILE N [--gate G GATE_DIR] [--marks DM]

Items `0..N` are summed per key (`item % KEYS`) by `stateful_map`, and
every emitted `(key, item, running_total)` is appended to OUT_FILE
with an fsync, outside the store's device. A running total depends on
the whole history of its key, so a wrong restored state shows up in
every later line of that key.

With `--gate`, the source stops before emitting item G: it creates
GATE_DIR/reached and waits for GATE_DIR/release. The source is only
asked for a batch once the previous epoch's recovery data is written,
so at the gate the store is durable up to item G exactly. Tests use
the gate to kill the process at a known resume point or to arm a
fault before the next commit.

With `--marks DM`, the workload also writes marks into the dm-log-writes
device DM, so a power-cut replay can be placed exactly in the log:
`b<item>` when the source is asked for the batch starting at `item`
(the store is durable up to `item` by then), and `o<item>` once that
item's output line is on disk.

"""

import argparse
import os
import subprocess
import time
from datetime import timedelta
from typing import List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.recovery import RecoveryConfig
from bytewax.testing import run_main

KEYS = 4
BATCH = 5
# Large enough that every pickled total has the same 4-byte integer
# encoding, so a test can locate and flip its bytes in the store.
TOTAL_BASE = 1_000_000_000


def expected_total(item: int) -> int:
    """Running total of `item`'s key after `item` is added."""
    return TOTAL_BASE + sum(range(item % KEYS, item + 1, KEYS))


def _mark(dm: Optional[str], label: str) -> None:
    """Write a mark into the dm-log-writes device, when recording."""
    if dm is not None:
        subprocess.run(
            ["dmsetup", "message", dm, "0", "mark", label],
            check=True,
            capture_output=True,
        )


class _Partition(StatefulSourcePartition[int, int]):
    def __init__(
        self,
        n: int,
        gate: Optional[int],
        gate_dir: Optional[str],
        resume: Optional[int],
        marks: Optional[str],
    ) -> None:
        self._next = 0 if resume is None else resume
        self._n = n
        self._gate = gate
        self._gate_dir = gate_dir
        self._marks = marks

    def next_batch(self) -> List[int]:
        _mark(self._marks, f"b{self._next}")
        if self._next >= self._n:
            raise StopIteration()
        if self._gate is not None and self._next >= self._gate:
            assert self._gate_dir is not None
            open(os.path.join(self._gate_dir, "reached"), "w").close()
            release = os.path.join(self._gate_dir, "release")
            while not os.path.exists(release):
                time.sleep(0.01)
            self._gate = None
        end = min(self._next + BATCH, self._n)
        batch = list(range(self._next, end))
        self._next = end
        return batch

    def snapshot(self) -> int:
        return self._next


class _Source(FixedPartitionedSource[int, int]):
    def __init__(
        self,
        n: int,
        gate: Optional[int],
        gate_dir: Optional[str],
        marks: Optional[str],
    ) -> None:
        self._n = n
        self._gate = gate
        self._gate_dir = gate_dir
        self._marks = marks

    def list_parts(self) -> List[str]:
        return ["p0"]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _Partition:
        return _Partition(
            self._n, self._gate, self._gate_dir, resume_state, self._marks
        )


class _FilePartition(StatelessSinkPartition[Tuple[str, Tuple[int, int]]]):
    def __init__(self, path: str, marks: Optional[str]) -> None:
        self._f = open(path, "a")
        self._marks = marks

    def write_batch(self, items: List[Tuple[str, Tuple[int, int]]]) -> None:
        for key, (item, total) in items:
            self._f.write(f"{key},{item},{total}\n")
        self._f.flush()
        os.fsync(self._f.fileno())
        for _key, (item, _total) in items:
            _mark(self._marks, f"o{item}")

    def close(self) -> None:
        self._f.close()


class _FileSink(DynamicSink[Tuple[str, Tuple[int, int]]]):
    def __init__(self, path: str, marks: Optional[str]) -> None:
        self._path = path
        self._marks = marks

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _FilePartition:
        return _FilePartition(self._path, self._marks)


def _add(total: Optional[int], item: int) -> Tuple[int, Tuple[int, int]]:
    total = (TOTAL_BASE if total is None else total) + item
    return total, (item, total)


def main() -> None:
    """Run the workload described by the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument("store")
    parser.add_argument("out")
    parser.add_argument("n", type=int)
    parser.add_argument("--gate", nargs=2, metavar=("ITEM", "DIR"))
    parser.add_argument("--marks", metavar="DM")
    args = parser.parse_args()

    gate = int(args.gate[0]) if args.gate else None
    gate_dir = args.gate[1] if args.gate else None

    flow = Dataflow("blockfault")
    nums = op.input("inp", flow, _Source(args.n, gate, gate_dir, args.marks))
    keyed = op.key_on("key", nums, lambda x: str(x % KEYS))
    totals = op.stateful_map("sum", keyed, _add)
    op.output("out", totals, _FileSink(args.out, args.marks))
    run_main(
        flow, epoch_interval=timedelta(0), recovery_config=RecoveryConfig(args.store)
    )


if __name__ == "__main__":
    main()
