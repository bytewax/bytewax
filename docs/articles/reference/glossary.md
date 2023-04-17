## Cluster

A coordinated collection of worker processes that are collectively
executing dataflow logic. It is possible that the cluster's processes
are distributed, or could be a single process with a single thread on
a single machine, but the concept of "cluster" still applies.

## End of File / EOF

The state of an operator's input or a probe meaning that it will see
no more input in this execution. This manifests as an "empty
frontier".

## Epochs

An incrementing integer which represents the ordering of data flowing
through the dataflow cluster. Timely calls this a timestamp.

Epochs induce a sense of order, causation, and synchronization. They
must:

- Have an order
- End regularly
- Have the same semantics on every worker

You can't arbitrarily increment epochs individually in each
worker. For example, incriminating the epoch every 5 messages on each
worker would result in stalls: any given worker might have a pause in
the data on the fourth message and thus delay every other worker from
making progress.

## Epoch, End of an

Stateful operators apply input items to their internal state in epoch
order. When an operator has applied all items from a closed epoch, you
can say that that epoch has ended within that operator.

## Epoch, Closed

A closed epoch is one that that is before the frontier, thus will not
see any new input.

## Epoch, Frontier

The epoch which is equal to the current frontier in an operator. This
operator will continue to see new input items within this epoch, but
will never see items from before this epoch.

## Epoch, Resume

The resume epoch is the epoch you'd like to resume a dataflow from the
start of. This is usually the dataflow frontier of the last cluster
execution, but could be set earlier if recovery data has not been
garbage collected.

## Execution

A single run of a dataflow cluster from initial input or resume until
failure or completion.

Whenever you recover a dataflow, you start a different execution.

Executions are never concurrent. The entire previous cluster must be
stopped before a new execution can begin.

## Execution ID

An incrementing integer that uniquely identifies an execution.

Larger execution IDs mean that that execution happened "after" another
with a smaller ID.

## Frontier

Oldest or smallest epoch a probe or input could still see in the
future.

All epochs before or smaller than the frontier are closed and won't be
seen in the input again.

It is possible for there to be an "empty frontier" or no smallest
epoch. This happens when the input is EOF and thus will see no more
items.

/Note:/ Only probes and inputs formally have frontiers. Operators
formally do not but we can talk about what the frontier of an input
attached to the output of an operator would be.

## Frontier, Cluster

The combined frontier of all worker frontiers. This represents the
oldest epoch that is still in progress in the execution cluster. It is
usually the epoch you'd want to resume from in the next execution if a
failure occurred.

## Frontier, Worker

The combined frontier of all output and backup operators on a single
worker. This represents the oldest epoch that is still in progress on
that worker.

## Item

A single unit of data flowing through a dataflow.

Sometimes certain operators require that items have a specific shape,
e.g. `(key, value)` 2-tuple.

## Notificator

A Timely object which keeps track of what epochs you have seen input
items during, and capabilities to emit items in those epochs. Then
lets you run processing logic and emit items downstream in epoch order
once the epochs are closed.

Note that the logic will be executed _only happens once the epoch is
closed!_ That means that you will continue to queue all input until
that happens. You probably want instead an Eager Notificator.

## Notificator, Eager

A modified version of a Timely notificator that runs processing logic
"eagerly" or when the new items come in during the current frontier,
and end logic whenever an epoch closes.

In general, this is what you'll want to use for most operators rather
than a plain notificator because you want to emit output ASAP, but you
still need to order the input by epoch so you can ensure recovery
snapshots happen at the right time.

Be very, very careful writing any operators that do not use
notificators and can emit items without a correlated input: you will
have to make sure you correctly manage handling a flash of the "0
epoch" during resume, running logic eagerly and in-order and labeling
output with the correct epoch, and placing output in the correct epoch
on input EOF.

## Operator

A generic kind of operation that can be performed on items. E.g. map,
reduce, collect window. These generic operations must be fleshed out
with domain-specific logic into a step in order to do anything.

## Operator Logic

User-provided functions that flesh out an operator into a step. They
have to follow very specific signatures in order to play into the API
of the operator.

## Operator, Stateful

An operator which has internal state that is modified via successive
items.

All stateful operators take a step ID in order to label recovery data.

## Process(, Worker)

A single OS process that is collectively executing dataflow logic via
some number of contained worker threads. Processes are grouped into a
cluster and might not necessarily be on the same machine.

## Snapshot(, Recovery)

A blob of serialized bytes that is a freeze frame snapshot of operator
state at the end of an epoch.

## Step

A specific instance of an operator and logic in a dataflow. A dataflow
is made up of a graph of steps.

E.g. you might have one step that uses the map operator to increment a
number by one, and a second step which uses the map operator to
decrement a number by five. Both use the same operator, and thus shape
of transformation, but are distinct steps.

## Step ID

A string key that uniquely identifies a step for recovery so data is
routed correctly.

## Step, Stateful

A step which is an instance of a stateful operator.

## Timely (Dataflow)

[Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow)
is the [Rust](https://www.rust-lang.org/)-based dataflow runtime
Bytewax is built on top of.

## Timestamp

Timely's word for what Bytewax calls an epoch.

## Worker (Thread)

A single OS thread executing dataflow logic. Worker threads are
grouped into processes on a single machine.
