Bytewax allows you to **recover** a stateful dataflow; it will let you
resume processing and output due to a failure _without_ re-processing
all initial data to re-calculate all internal state. It does this by
periodically snapshotting all internal state and having a way to
resume from a recent snapshot.

Here, we'll give a quick tutorial on how to enable the most basic form
of recovery. For more advanced settings and details, see the [module
docstring for `bytewax.recovery`](/apidocs/bytewax.recovery).

## Create Recovery Partitions

Recovery partitions must be pre-initialized before running the
dataflow initially. This is done by executing this module:

```
$ python -m bytewax.recovery db_dir/ 4
```

This will create a set of partitions:

```
$ ls db_dir/
part-0.sqlite3
part-1.sqlite3
part-2.sqlite3
part-3.sqlite3
```

## Executing with Recovery

To enable recovery when you execute a dataflow, pass the `-r` flag to
`bytewax.run` and specify the recovery directory.

```
$ python -m bytewax.run ... -r db_dir/
```

As the dataflow executes, it now will automatically back up state
snapshot and progress data.

If you terminate the process after 10 seconds, Bytewax will have saved
snapshots of all operator state.

## Resuming

If a dataflow aborts, abruptly shuts down, or gracefully exits due to
EOF, you can resume the dataflow via running it again pointing at the
same recovery directory. Bytewax will automatically find the most
recent consistent snapshot to resume from.

Re-execute the above command and you should see the dataflow resume
from the last snapshot.

## Caveats

Because snapshotting only happens periodically, it is possible that
your output systems will see duplicate data around resume with some
input and output connectors. See documentation for each connector for
how it is designed and what kinds of guarantees it can enable. In
general, design your systems to be idempotent or support at-least-once
processing.
