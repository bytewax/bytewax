"""Connectors to console IO."""
import sys
from typing import Any, List

from bytewax.outputs import DynamicSink, StatelessSinkPartition

__all__ = [
    "StdOutSink",
]


class _PrintSinkPartition(StatelessSinkPartition[Any]):
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

    def build(self, worker_index: int, worker_count: int) -> _PrintSinkPartition:
        """See ABC docstring."""
        return _PrintSinkPartition()
