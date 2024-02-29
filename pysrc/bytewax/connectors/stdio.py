"""Connectors to console IO."""
import sys
from typing import Any, List

from typing_extensions import override

from bytewax.outputs import DynamicSink, StatelessSinkPartition


class _PrintSinkPartition(StatelessSinkPartition[Any]):
    @override
    def write_batch(self, items: List[Any]) -> None:
        for item in items:
            line = str(item)
            sys.stdout.write(line)
            sys.stdout.write("\n")
        sys.stdout.flush()


class StdOutSink(DynamicSink[Any]):
    """Write each output item to stdout on that worker.

    Items consumed from the dataflow must look like a string. Use a
    proceeding map step to do custom formatting.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    """

    @override
    def build(
        self, _step_id: str, _worker_index: int, _worker_count: int
    ) -> _PrintSinkPartition:
        return _PrintSinkPartition()
