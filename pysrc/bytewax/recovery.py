"""Bytewax's state recovery machinery.

Preparation
-----------

This allows you to **recover** a stateful dataflow; it will let you
resume processing and output due to a failure without re-processing
all initial data to re-calculate all internal state.

0. Pick a **recovery store** in which internal state and progress will
be continuously backed up. Different storage systems will have
different performance and reconfiguration trade-offs.

1. Create a **recovery config** for connecting to your recovery
store. See the class types in this module for the datstores supported.

2. Pass that created config as the `recovery_config` argument to the
entry point running your dataflow (e.g. `bytewax.cluster_main()`).

3. Run your dataflow! It will start backing up state data
automatically.

The epoch is the unit of recovery. It is your responsibility to design
your input handlers in such a way that it jumps to the point in the
input that corresponds to the `resume_epoch` argument; if it can't
(because the input is ephemeral) you can still recover the dataflow,
but the lost input is unable to be replayed so the output will be
different.

It is possible that your output systems will see duplicate data right
around the resume epoch; design your systems to support at-least-once
processing.

Recovery
--------

If the dataflow fails, first you must fix whatever underlying fault
caused the issue. That might mean deploying new code which fixes a bug
or resolving an issue with a connected system.

Once that is done, re-run the dataflow using the _same recovery
config_. Bytewax will automatically recover state from the recovery
store and tell your input handlers to fast forward to the resume
epoch. Output should resume.

If you want to fully _restart_ a dataflow and ignore previous state,
you either have to create a recovery config that points at a new
datastore or delete the recovery data in the datastore.

"""
from .bytewax import KafkaRecoveryConfig, RecoveryConfig, SqliteRecoveryConfig  # noqa
