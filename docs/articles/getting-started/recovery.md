One of the powers of Bytewax is the ability to develop dataflows that
accumulate state as they process data. By default, this state is just
stored in memory and will be lost if the dataflow is restarted or
there is a fault. Bytewax provides some tools and techniques to allow
you to **recover** your state and resume the dataflow after a fault.

## Prerequisites

Not all dataflows are possible to be recovered. There are a few
requirements:

1. _Replayable Input_ - Your data source needs to support re-playing
   input from a specific point in the past we call the **resume
   state** to resume making progress from where the failure happened.

2. _At-least-once Output_ - Bytewax only provides at-least-once
   guarantees when sending data into a downstream system when
   performing a recovery. You should design your architecture to
   support this via some sort of idempotency.

3. _Metadata around input order for manual input_ - The `resume_state`
   is the minimum unit of recovery for inputs using a `ManualInputConfig`.
   You will need to design your input builder to be able to resume from
   a location of failure. If you're using `KafkaInputConfig`, we handle
   this for you.

## Enabling Recovery

Your dataflow needs a few features to enable recovery on Bytewax:

1. You must design your [input
   builder](/getting-started/ins_and_outs) to be able to resume
   and replay all data from the `resume_state` onwards. Do not
   ignore this argument!

2. Create a **recovery config** for your workers to know how to
   connect to the **recovery store**, the database or file system that
   will store recovery data. See our API docs for `bytewax.recovery`
   on our supported options.

3. Pass the recovery config as the `recovery_config` argument to
   whatever execution [entry point](/getting-started/execution)
   (e.g. `cluster_main()`) you are using.

4. Run the dataflow! Bytewax will automatically snapshots of state and
   the resume epoch to the recovery store.

## Performing Recovery

If a dataflow has failed, follow this high-level game plan to recover
and have it resume processing from where it failed:

1. Terminate the dataflow processes.

2. Restart your dataflow using the same recovery config. Bytewax will
   read the input state from the recovery store and resume output
   from the latest possible location in the stream.

## Examples

Here are some concrete examples of using the recovery machinery.

- [Cart Join Example](/examples/cart-join)
