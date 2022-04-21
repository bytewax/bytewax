"""Bytewax's state recovery machinery.

This allows you to **recover** a stateful dataflow; it will let you
resume processing and output due to system outage or error without
re-processing all initial data to re-calculate all internal state.

0. Pick a **recovery store** in which internal state will be
   continously backed up. Different storage systems will have
   different performance and reconfiguration tradeoffs.

1. Create a **recovery config** for connecting to your recovery store.

2. Pass that created config as the `recovery_config` argument to the
   entry point running your dataflow (e.g. `bytewax.run_cluster()`).

3. Run your dataflow from the beginning, or resuming input from the
   last finalized epoch.

The epoch is the unit of recovery. It is your responsibility to design
your input builders or iterators in such a way that it can be resumed
from a given epoch; if it can't (because the input is ephemeral) then
this feature won't allow complete recovery. It is possible that your
output systems will see duplicate data right around the resumed epoch;
design your systems to be idempotent or support at-least-once
processing.

Bytewax will automatically save and recover state from the recovery
store during the execution of stateful operators. You know an operator
is stateful when it takes a `step_id` argument.

"""
from .bytewax import RecoveryConfig, SqliteRecoveryConfig
