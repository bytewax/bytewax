"""Bytewax's state recovery machinery.

Bytewax allows you to **recover** a stateful dataflow; it will let you
resume processing and output due to a failure without re-processing
all initial data to re-calculate all internal state.

It does this by storing state and progress information for a single
dataflow instance in a **recovery store** backed by a durable state
storage system of your choosing, e.g. Sqlite or Kafka. See the
subclasses of `RecoveryConfig` in this module for the supported
datastores, the specifics of how each is utilized, and tradeoffs.

Preparation
-----------

1. Create a **recovery config** for describing how to connect to the
recovery store of your choosing.

2. Pass that recovery config as the `recovery_config` argument to the
entry point running your dataflow (e.g. `bytewax.cluster_main()`).

3. Run your dataflow! It will start backing up recovery data
automatically.

Caveats
-------

Recovery data for multiple dataflows _must not_ be mixed together. See
the docs for each `RecoveryConfig` subclass for what this means
depending on the recovery store. E.g. when using a
`KafkaRecoveryConfig`, each dataflow must have a distinct topic
prefix.

The epoch is the unit of recovery: dataflows will only resume on epoch
boundaries.

See comments on input configuration types in `bytewax.inputs` for any
limitations each might have regarding recovery.

It is possible that your output systems will see duplicate data during
the resume epoch; design your systems to support at-least-once
processing.

Currently it is not possible to recover a dataflow with a different
number of workers than when it failed.

On Failure
----------

If the dataflow fails, first you must fix whatever underlying fault
caused the issue. That might mean deploying new code which fixes a bug
or resolving an issue with a connected system.

Once that is done, re-run the dataflow using the _same recovery
config_ and thus re-connect to the _same recovery store_. Bytewax will
automatically read the progress of the previous dataflow execution and
determine the most recent epoch that processing can resume at, called
the **resume epoch**. Output should resume from the beginning of the
resume epoch.

If you want to fully restart a dataflow and ignore previous state,
delete the data in the recovery store using whatever operational tools
you have for that storage type.

"""
from .bytewax import KafkaRecoveryConfig, RecoveryConfig, SqliteRecoveryConfig  # noqa
