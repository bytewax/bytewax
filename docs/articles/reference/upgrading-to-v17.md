## Upgrading to v0.17

Bytewax v0.17 introduces major changes to the way that recovery works
in order to support rescaling. In v0.17, the number of workers in a
cluster can now be changed by stopping the dataflow execution and specifying a different number of workers on resume.

In addition to rescaling, we've changed the Bytewax inputs and outputs
API to support batching which has yielded significant performance
improvements.

In this article, we'll cover some of the updates we've made to our API
to make upgrading your Dataflows to v0.17 easier.

## Recovery changes

In v0.17, recovery has been changed to support rescaling the number of
workers in a dataflow. The number of recovery partitions is no longer
tied to the number of workers in a cluster.

SQLite recovery DBs created with versions of Bytewax prior to v0.17
are not compatible with this release.

### Creating recovery partitions

Creating recovery stores has been moved to a separate step from
running your dataflow.

Recovery partitions must be pre-initialized before running the
dataflow initially. This is done by executing this module:

``` bash
$ python -m bytewax.recovery db_dir/ 4
```


This will create a set of partitions:

``` bash
$ ls db_dir/
part-0.sqlite3
part-1.sqlite3
part-2.sqlite3
part-3.sqlite3
```

Once the recovery partition files have been created, they must be
placed in locations that are accessible to the workers. The cluster
has a whole must have access to all partitions, but any given worker
need not have access to any partition in particular (or any at
all). It is ok if a given partition is accesible by multiple workers;
only one worker will use it.

If you are not running in a cluster environment but on a single
machine, placing all the partitions in a single local filesystem
directory is fine.

Although the partition init script will not create these, partitions
after execution may consist of multiple files:

```
$ ls db_dir/
part-0.sqlite3
part-0.sqlite3-shm
part-0.sqlite3-wal
part-1.sqlite3
part-2.sqlite3
part-3.sqlite3
part-3.sqlite3-shm
part-3.sqlite3-wal
```
You must remember to move the files with the prefix `part-*.` all together.

### Choosing the number of recovery partitions

An important consideration when creating the initial number of
recovery partitions; When rescaling the number of workers, the number
of initial recovery partitions is currently fixed. If you are scaling
up the number of workers in your cluster to increase the total
throughput of your dataflow, the work of writing recovery data to the
recovery partitions will still be limited to the initial number of
recovery partitions. If you will likely scale up your dataflow to
accomodate increased demand, we recommend that that you consider
creating more recovery partitions than you will initially need. Having
multiple recovery partitions handled by a single worker is fine.

## Epoch interval -> Snapshot interval

We've renamed the cli option of `epoch-interval` to
`snapshot-interval`. The snapshot interval is the system time duration
(in seconds) to snapshot state for recovery.

Recovering a dataflow can only happen on the boundaries of the most
recently completed snapshot across all workers, but be aware that
making the `snapshot-interval` more frequent increases the amount of
recovery work and may impact performance.

## Backup interval, and backing up recovery partitions.

We've also introduced an additional parameter to running a dataflow:
`backup-interval`.

When running a Dataflow with recovery enabled, it is recommended to
back up your recovery partitions on a regular basis for disaster
recovery.

The `backup-interval` is a system time duration in seconds to keep
extra state snapshots around within each recovery partition. It
defaults to one day.

During recovery, each of the recovery partitions is read to determine
what the most recently completed snapshot was. In order for recovery
to happen, that snapshot must be available in each of the recovery
partitions. If one worker did not complete writing it's snapshot,
Bytewax must use an earlier snapshot that exists in all recovery
partitions.

The `backup-interval` parameter can be understood as the length of
time to wait before "garbage collecting" older snapshots that are no
longer needed. In order to accomodate recovery scenarios where a
backup of these recovery partitions did not complete for some workers,
we delay "garbage collecting" those older snapshots for this period.

This parameter defaults to one day. We recommend that you tune this
parameter to be well in excess of the interval you back up recovery
partitions to account for transient failures.

## Input and Output changes

In v0.17, we have restructured input and output to support batching
for increased throughput.

If you have created custom input connectors, you'll need to update
them to use the new API.

### Input changes

The `list_parts` method has been updated to return a `List[str]` instead of
a `Set[str]`, and now should only reflect the available input partitions
that a given worker has access to. You no longer need to return the
complete set of partitions for all workers.

The `next` method of `StatefulSource` and `StatelessSource` has been
changed to `next_batch` and should to return a `List` of elements each
time it is called. If there are no elements to return, this method
should return the empty list.

### Next awake

Bytewax makes use of a sort of cooperative multitasking system
in order to schedule work. Calling functions that block for
an indefinite period of time, like `time.sleep`, blocks the
progress of *all* work in a Dataflow.

Many input sources are best utilized in a "polling" fashion, and
should not be called frequently. In order to support this pattern,
v0.17 adds the `next_awake` method to the `StatefulSource` and
`StatelessSource` classes.

The default behavior (when `next_awake` returns `None`) is to activate
the input immediately if any item was returned in `next`. If no items
were returned in `next`, we will wait for 1ms before reactivating the
input source. The short cooldown helps avoid high cpu load if there's
no work to do.

`next_awake` is called before `self.next`, and if the
datetime returned is in the future, `self.next` won't be
called until that datetime has passed.

Always use this method to wait for input rather than
using a `time.sleep` in your connector.

See the `periodic_input.py` example in the examples directory for an
implementation that uses this functionality.

## Async input adapter

We've included an Async input adapter to help people bridge Async
input sources with our new batched input system.

The new `AsyncBatcher` class which lets you specify a maximum timeout
period to wait, or a batch size to output when it is met when the
input source is an async iterator. This lets you more easily play
nicely with cooperative multitasking but still use Python libraries
with async APIs.

## Using Kafka for recovery is now removed

v0.17 removes the deprecated `KafkaRecoveryConfig` as a recovery store.

## Support for additional platforms

Bytewax is now available for linux/aarch64 and linux/armv7.
