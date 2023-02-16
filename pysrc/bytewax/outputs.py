"""Low-level output interfaces.

If you want pre-built connectors for various external systems, see
`bytewax.connectors`. That is also a rich source of examples.

Subclass the types here to implement input for your own custom sink.

"""

from abc import abstractmethod
from typing import Any, Callable, Iterable, Optional

StatefulSink = Callable[[Any], Any]
"""Output sink that maintains state of its position.

Called once for each `(key, value)` at this point in the dataflow.

See `PartOutput.assign_part` for how the key is mapped to partition.

Args:

    value: Value in the dataflow.

Returns:

    State which will be returned to you via the `resume_state`
    parameter of the output builder (if any).

"""


# TODO: Add ABC superclass. It messes up pickling. We should get rid
# of pickling...
class PartOutput:
    """An output with a fixed number of independent partitions.

    Will maintain the state of each sink and re-build using it during
    resume. If the sink supports seeking and overwriting, this output
    can support exactly-once processing.

    """

    @abstractmethod
    def list_parts(self) -> Iterable[str]:
        """List all partitions by a string key.

        This must consistently return the same keys when called by all
        workers in all executions.

        Keys must be unique within this list.

        Returns:

            Partition keys.

        """
        ...

    @abstractmethod
    def assign_part(self, item_key: str) -> str:
        """Define how incoming `(key, value)` pairs should be routed
        to partitions.

        Args:

            item_key: Key that is about to be written.

        Returns:

            Partition key the value for this key should be written
            to. Must be one of the partition keys returned by
            `list_parts`.

        """
        ...

    @abstractmethod
    def build_part(
        self,
        for_part: str,
        resume_state: Optional[Any],
    ) -> Optional[StatefulSink]:
        """Build an output partition, resuming writing at the position
        encoded in the resume state.

        Will be called once within each cluster for each partition
        key.

        Be careful of "off by one" errors in resume state. This should
        return a source that resumes from _the next item_, not the
        same item that the state was paired with.

        Return `None` if for some reason this partition is no longer
        valid and can be skipped coherently. Raise an exception if
        not.

        Do not pre-build state about a partition in the
        constructor. All state must be derived from `resume_state` for
        recovery to work properly.

        Args:

            for_part: Which partition to build.

            resume_state: State data containing where in the output
                stream this partition should be begin writing during
                this execution.

        Returns:

            The built partition, or `None`.

        """
        ...

    def __json__(self):
        """This is used by the Bytewax platform internally and should
        not be overridden.

        """
        return {
            "type": type(self).__name__,
        }


StatelessSink = Callable[[Any], None]
"""Output sink that is stateless.

Called once for each item at this point in the dataflow.

Args:

    item: Item in the dataflow.

"""


# TODO: Add ABC superclass. It messes up pickling. We should get rid
# of pickling...
class DynamicOutput:
    """An output that supports writing from any number of workers
    concurrently.

    Does not support storing any resume state. Thus these kind of
    outputs only naively can support at-least-once processing.

    """

    @abstractmethod
    def build(self) -> StatelessSink:
        """Build an output sink for a worker.

        Will be called once on each worker.

        Returns:

            Output sink.

        """
        ...

    def __json__(self):
        """This is used by the Bytewax platform internally and should
        not be overridden.

        """
        return {
            "type": type(self).__name__,
        }
