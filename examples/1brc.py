import os
from datetime import datetime
from typing import Tuple

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from typing_extensions import TypeAlias


class FilePartition(StatelessSourcePartition):
    def __init__(self, path: str, start_offset: int, end_offset: int, batch_bytes: int):
        self._f = open(path, "rb")
        self._f.seek(start_offset)
        self._end_offset = end_offset
        self._batch_bytes = batch_bytes

    def next_batch(self, sched: datetime):
        at = self._f.tell()
        if at >= self._end_offset:
            raise StopIteration()
        hint = min(self._batch_bytes, self._end_offset - at)
        return self._f.readlines(hint)


class CoopFileSource(DynamicSource):
    def __init__(self, path: str, batch_bytes: int):
        self._path = path
        self._batch_bytes = batch_bytes

    def build(
        self, now: datetime, worker_index: int, worker_count: int
    ) -> FilePartition:
        file_size = os.path.getsize(self._path)
        chunk_size = file_size // worker_count
        start_offset = worker_index * chunk_size
        end_offset = (worker_index + 1) * chunk_size
        with open(self._path, "rb") as f:
            if start_offset > 0:
                f.seek(start_offset)
                f.readline()
                start_offset = f.tell()
            if end_offset < file_size:
                f.seek(end_offset)
                f.readline()
                end_offset = f.tell()
        print(
            f"Worker {worker_index}/{worker_count} reading from "
            f"{start_offset} to {end_offset}"
        )
        return FilePartition(self._path, start_offset, end_offset, self._batch_bytes)


flow = Dataflow("1brc")
rows = op.input("inp", flow, CoopFileSource(os.environ["BRC_FILE"], 2**16))


State: TypeAlias = Tuple[float, float, float, int]


def key_init(line: bytes) -> Tuple[str, State]:
    key, x = line.split(b";")
    # `float(b"1.0\n")` parses correctly so we don't have to strip or
    # decode.
    x = float(x)
    return (key.decode(), (x, x, x, 1))


keyed = op.map("key", rows, key_init)


def reducer(x: State, y: State) -> State:
    x_min, x_max, x_sum, x_count = x
    y_min, y_max, y_sum, y_count = y
    return (min(x_min, y_min), max(x_max, y_max), x_sum + y_sum, x_count + y_count)


def mapper(key_state: Tuple[str, State]) -> str:
    key, state = key_state
    min_x, max_x, sum_x, count = state
    mean = sum_x / count
    return f"{key}={min_x:.1f}/{max_x:.1f}/{mean:.1f}"


stats = op.reduce_final("reduce", keyed, reducer).then(op.map, "fmt", mapper)
op.output("out", stats, StdOutSink())
