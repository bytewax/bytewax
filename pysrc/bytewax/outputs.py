"""Dataflow output sinks.

Bytewax provides pre-packaged output configuration options for common
sinks you might want to write dataflow output to.

Use
---

Create an `OutputConfig` subclass for the sink you want to write to.
Then pass that config object to the `bytewax.dataflow.Dataflow.capture`
operator.
"""

from .bytewax import (  # noqa: F401
    KafkaOutputConfig,
    ManualEpochOutputConfig,
    ManualOutputConfig,
    OutputConfig,
    StdOutputConfig,
)


class TestingEpochOutputConfig(ManualEpochOutputConfig):
    """
    Append each output `(epoch, item)` to a list.
    You only want to use this for unit testing.

    Because the list is in-memory, you will need to carefuly
    coordinate use or assertions on this list when using multiple
    workers.

    Args:

        ls: Append each `(epoch, item)` to this list.

    Returns:

        Config object.
        Pass this to the `bytewax.dataflow.Dataflow.capture` operator.

    """

    # This is needed to avoid pytest trying to load this class as
    # a test since its name starts with "Test"
    __test__ = False

    def __new__(cls, ls):
        """
        In classes defined by PyO3 we can only use __new__, not __init__
        """
        return super().__new__(cls, lambda wi, wn: ls.append)


class TestingOutputConfig(ManualOutputConfig):
    """
    Append each output item to a list.
    You only want to use this for unit testing.

    Because the list is in-memory, you will need to carefuly
    coordinate use or assertions on this list when using multiple
    workers.

    Args:

        ls: Append each `item` to this list.

    Returns:

        Config object.
        Pass this to the `bytewax.dataflow.Dataflow.capture` operator.

    """

    # This is needed to avoid pytest trying to load this class as
    # a test since its name starts with "Test"
    __test__ = False

    def __new__(cls, ls):
        """
        In classes defined by PyO3 we can only use __new__, not __init__
        """
        return super().__new__(cls, lambda wi, wn: ls.append)
