"""Low-level input interfaces.

If you want pre-built connectors for various external systems, see
`bytewax.connectors`. That is also a rich source of examples.

Subclass the types here to implement input for your own custom source.

"""

from abc import abstractmethod
from typing import Any, Iterable, Optional, Tuple

PartIter = Iterable[Optional[Tuple[Any, Any]]]
"""A single partition.

It must yield a two-tuple of `(state, item)` where the state will be
returned to you via the `resume_state` parameter of
`CustomPartInput.build_part`.

If this is a generator, it must do a kind of cooperative
multi-tasking, never blocking but yielding a bare `None` if there is
no new input.

Yields:

    Either a new item and state, or `None`.

"""


# TODO: Add ABC superclass. It messes up pickling. We should get rid
# of pickling...
class CustomPartInput:
    """An input source with a fixed number of independent partitions."""

    @abstractmethod
    def list_parts(self) -> Iterable[str]:
        """List all partitions for this input by a string key.

        This must consistently return the same keys when called by any
        worker in a cluster.

        Keys must be unique within this list.

        Returns:

            Partition keys.

        """
        ...

    @abstractmethod
    def build_part(
        self,
        for_part: str,
        resume_state: Optional[Any],
    ) -> Optional[PartIter]:
        """Build an input source for a given partition, resuming from
        the position encoded in the resume state.

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

            resume_state: State data containing where in the input
                stream this partition should be begin on this invocation.

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
