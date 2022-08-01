"""Dataflow output sinks.

Bytewax provides pre-packaged output configuration options for common
sinks you might want to write dataflow output to.

Use
---

Create an `OutputConfig` subclass for the sink you want to write
to. Then pass that config object to the
`bytewax.dataflow.Dataflow.capture` operator.

"""

from .bytewax import (  # noqa: F401
    ManualEpochOutputConfig,
    ManualOutputConfig,
    OutputConfig,
    StdOutputConfig,
)


def TestingEpochOutputConfig(ls):
    """Append each output `(epoch, item)` to a list. You only want to use
    this for unit testing.

    Because the list is in-memory, you will need to carefuly
    coordinate use or assertions on this list when using multiple
    workers.

    Args:

        ls: Append each `(epoch, item)` to this list.

    Returns:

        Config object. Pass this to the
        `bytewax.dataflow.Dataflow.capture` operator.

    """
    return ManualEpochOutputConfig(lambda wi, wn: ls.append)


def TestingOutputConfig(ls):
    """Append each output item to a list. You only want to use this for
    unit testing.

    Because the list is in-memory, you will need to carefuly
    coordinate use or assertions on this list when using multiple
    workers.

    Args:

        ls: Append each `(epoch, item)` to this list.

    Returns:

        Config object. Pass this to the
        `bytewax.dataflow.Dataflow.capture` operator.

    """
    return ManualOutputConfig(lambda wi, wn: ls.append)
