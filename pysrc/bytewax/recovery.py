"""Recovering from failures.

Bytewax allows you to **recover** a stateful dataflow; it will let you
resume processing and output due to a failure without re-processing
all initial data to re-calculate all internal state.

It does this by storing state and progress information for a single
dataflow instance in a **recovery store** backed by a durable state
storage system of your choosing, e.g. SQLite or Kafka. See the
subclasses of `RecoveryConfig` in this module for the supported
datastores, the specifics of how each is utilized, and tradeoffs.

Preparation
-----------

1. Create a **recovery config** for describing how to connect to the
recovery store of your choosing.

2. Pass that recovery config as the `recovery_config` argument to the
entry point running your dataflow (e.g. `bytewax.cluster_main()`).

Execution
---------

Make sure your worker processes have access to the recovery store.

Then, run your dataflow! It will start backing up recovery data
automatically.

Recovering
----------

If the dataflow fails, first you must fix whatever underlying fault
caused the issue. That might mean deploying new code which fixes a bug
or resolving an issue with a connected system.

Once that is done, re-run the dataflow using the _same recovery
config_ and thus re-connect to the _same recovery store_. Bytewax will
automatically read the progress of the previous dataflow execution and
determine the most recent point that processing can resume at. Output
should resume from that point.

If you want to fully restart a dataflow and ignore previous state,
delete the data in the recovery store using whatever operational tools
you have for that storage type.

Caveats
-------

Recovery data for multiple dataflows _must not_ be mixed together. See
the docs for each `RecoveryConfig` subclass for what this means
depending on the recovery store. E.g. when using a
`KafkaRecoveryConfig`, each dataflow must have a distinct topic
prefix.

See comments on input configuration types in `bytewax.inputs` for any
limitations each might have regarding recovery.

It is possible that your output systems will see duplicate data around
the resume point; design your systems to support at-least-once
processing.

Currently it is not possible to recover a dataflow with a different
number of workers than when it failed.

The epoch is the time unit of recovery: dataflows will only resume on
epoch boundaries, with the **resume epoch** being where the dataflow
witll resume. Bytewax defaults to a new epoch every 10 seconds. See
`bytewax.execution.EpochConfig` for more info on epochs.

"""

from .bytewax import (  # noqa: F401
    KafkaRecoveryConfig,
    RecoveryConfig,
    SqliteRecoveryConfig,
)

__all__ = [
    "KafkaRecoveryConfig",
    "RecoveryConfig",
    "SqliteRecoveryConfig",
]
