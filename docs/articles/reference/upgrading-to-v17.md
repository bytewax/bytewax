## Upgrading to v0.17

Bytewax v0.17 introduces major changes to the way that recovery works
in order to support rescaling. In v0.17, the number of workers in a
cluster can now be changed by stopping the dataflow execution and specifying a different number of workers on resume.

In addition to rescaling, we've changed the Bytewax inputs and outputs
API to support batching which has yielded significant performance
improvements.

In this article, we'll go over the updates we've made to our API and
explain the changes you'll need to make to your dataflows to upgrade
to v0.17.

## Recovery changes

In v0.17, recovery has been changed to support rescaling the number of
workers in a dataflow. You now pre-create a fixed number of SQLite recovery DB files before running the dataflow.

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
of recovery partitions is currently fixed.

If you are scaling up the number of workers in your cluster to
increase the total throughput of your dataflow, the work of writing
recovery data to the recovery partitions will still be limited to the
initial number of recovery partitions. If you will likely scale up
your dataflow to accomodate increased demand, we recommend that that
you consider creating more recovery partitions than you will initially
need. Having multiple recovery partitions handled by a single worker
is fine.

## Epoch interval -> Snapshot interval

We've renamed the cli option of `epoch-interval` to
`snapshot-interval` to better describe its affect on dataflow execution. The snapshot interval is the system time duration
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

The `backup-interval` parameter is the length of time to wait before
"garbage collecting" older snapshots. This enables a dataflow to
successfully resume when backups of recovery partitions happen at
different times, which will be true in most distributed deployments.

This value should be set in excess of the interval you can guarantee
that all recovery partitions will be backed up to account for
transient failures. It defaults to one day.

If you attempt to resume from a set of recovery partitions for which
the oldest and youngest backups are more than the backup interval
apart, the resumed dataflow could have corrupted state.

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

Input sources now have an optional `next_awake` method which you can
use to schedule when the next `next_batch` call should occur. You can
use this to "sleep" the input operator for a fixed amount of time
while you are waiting for more input.

The default behavior uses a simple heuristic to prevent a spin loop
when there is no input. Always use `next_awake` rather than using a
`time.sleep` in an input source.

See the `periodic_input.py` example in the examples directory for an
implementation that uses this functionality.

## Async input adapter

We've included a new `bytewax.inputs.batcher_async` to help you use
async Python libraries in Bytewax input sources. It lets you wrap an
async iterator and specify a maximum time and size to collect items
into a batch.

## Using Kafka for recovery is now removed

v0.17 removes the deprecated `KafkaRecoveryConfig` as a recovery store
to support the ability to rescale the number of workers.

## Support for additional platforms

Bytewax is now available for linux/aarch64 and linux/armv7.
