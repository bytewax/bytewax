"""Failure recovery.

Bytewax allows you to **recover** a stateful dataflow; it will let you
resume processing and output due to a failure _without_ re-processing
all initial data to re-calculate all internal state. It does this by
periodically snapshotting all internal state and having a way to
resume from a recent snapshot.

See `python -m bytewax.recovery --help` for an overview of
initializing recovery partitions.

Overview
--------

Bytewax implements recovery by periodically snapshoting state and
progress information for a single dataflow instance in a partitioned
set of **recovery partitions**, [SQLite](https://sqlite.org/)
databases in the **recovery directory**. Recovery data for multiple
dataflows _must not_ be mixed together.

When you run your dataflow it will start backing up recovery data
automatically. Each run of a dataflow cluster is called an
**execution**.

If the dataflow fails, first you must fix whatever underlying fault
caused the issue. That might mean deploying new code which fixes a
bug, re-creating destroyed VMs, or resolving an issue with a connected
system.

Once that is done, re-run the dataflow using the _same recovery
directory_. Bytewax will automatically read the progress of the
previous dataflow execution and determine the most recent coordinated
snapshot to resume processing from.

Because snapshotting only happens periodically, it is possible that
your output systems will see duplicate data around resume with some
input and output connectors. See documentation for each connector for
how it is designed and what kinds of guarantees it can enable. In
general, design your systems to be idempotent or support at-least-once
processing.

Setup
-----

Recovery partitions must be pre-initialized before running the
dataflow initially. This is done by executing this module:

```
$ python -m bytewax.recovery db_dir/ 4
```

The second parameter (e.g. `4`) is the number of recovery partitions
to create. This number is fixed and cannot be changed later without
manual SQLite interventions. In general, we recommend picking a number
of partitions equal to the maximum number of workers you think you
will ever rescale to.

This will create a set of partitions:

```
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

You must move the files with the prefix `part-*.` all together.

In general, you'll want to your environment to try and evenly
distribute partitions among your workers.

If you are not running in a cluster environment but on a single
machine, placing all the partitions in a single local filesystem
directory is fine.

Execution
---------

To enable recovery when you execute a dataflow, pass the `-r` flag to
`bytewax.run` and specify the recovery directory.

```
$ python -m bytewax.run ... -r db_dir/
```

See the module docstring for `bytewax.run` for more information.

Resume
------

If a dataflow aborts, abruptly shuts down, or gracefully exits due to
EOF, you can resume the dataflow via running it again pointing at the
same recovery directory. Bytewax will automatically find the most
recent consistent snapshot to resume from.

This requires that the workers in the next execution collectively have
access to all of the recovery partitions.

This next execution does not need to have the same number of workers
as the previous one; you are allowed to rescale the cluster and state
will find its way to the proper workers.

If you want to fully restart a dataflow at the beginning of input and
ignore all previous state, delete partitions in the recovery
directory.

Continuation
------------

Another use of the recovery system is to allow a dataflow to be
**continued** in a followup execution with new data. For example, you
might be processing a large log file, run a dataflow on it, and
calculate some metrics. You could then append to that log file, resume
the dataflow and it would be processed without needing to re-read and
re-process the initial part of the log file.

Bytewax snapshots the dataflow at the end of all input to support this
use case. You'll need to read the specific documentation for each
connector to see what kind of resume semantics it has.

Snapshotting
------------

The **snapshot interval** is the system time interval at which an
execution cluster synchronizes and snapshots its progress and
state. You can adjust this duration via the `-s` parameter to
`bytewax.run`. It defaults to every 10 seconds.

The dataflow can only resume on snapshot interval boundaries.

In general, the longer this duration is, the less overhead there will
be while the dataflow is executing, but the further back the dataflow
might have to resume from in case of failure.

Backup and Disaster Recovery
----------------------------

Usually in a production environment, you'll want to durably back up
your recovery partitions from the machines that are executing the
dataflow in case they are destroyed. This can be done through a
variety of mechanisms, like a [cron job that runs a backup
command](https://litestream.io/alternatives/cron/) or
[litestream](https://litestream.io/how-it-works/).

The recovery system also tries to be efficient and does not want
recovery data to grow without bound. It will **garbage collect**
snapshot data that is no longer necessary to support resumeing from an
epoch far in the past.

Bytewax can only resume a dataflow when it is able to find a
consistent snapshot which exists across all recovery partitions. If
you need to re-constitute a cluster from these backups, there is then
the problem that unless the backups are all from an identical point in
time, there might not be an overlapping snapshot among them.

The **backup interval** is the system time interval for which snapshot
data should be retained longer than when it would be otherwise garbage
collected. This generally should be set slightly longer than your
backup latency. This gives a larger window for independent backup
processes for each partition to complete and still enable a succesful
recovery.

"""

import argparse
from pathlib import Path

from .bytewax import RecoveryConfig, init_db_dir  # noqa: F401

__all__ = [
    "RecoveryConfig",
    "init_db_dir",
]


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.recovery",
        description="Create and init a set of empty recovery partitions.",
        epilog="""See the `bytewax.recovery` module docstring for more
        info.""",
    )
    parser.add_argument(
        "db_dir",
        type=Path,
        help="Local directory to create partitions in",
    )
    parser.add_argument(
        "part_count",
        type=int,
        help="Number of partitions to create",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    init_db_dir(args.db_dir, args.part_count)
