import os
from pathlib import Path

from bytewax import parse
from bytewax.connectors.files import DirInput
from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main, TestingEpochConfig
from bytewax.outputs import ManualOutputConfig
from bytewax.recovery import SqliteRecoveryConfig

input_dir = Path("./examples/sample_data/cluster")
output_dir = Path("./cluster_out/")
recovery_dir = Path("./cluster_recovery/")

flow = Dataflow()

flow.input("inp", DirInput(input_dir))

flow.map(str.upper)

# Give the dataflow a way to fail mid-way.
def trigger(item):
    if os.environ.get("FAIL") and item.endswith("3"):
        raise RuntimeError("BOOM")


flow.inspect(trigger)


def output_builder(worker_index, worker_count):
    output_dir.mkdir(exist_ok=True)
    # Open a file that just this worker will write to.
    write_to = open(output_dir / f"worker{worker_index}.out", "a")
    # Build a function that can be called for each captured output.
    def write(item):
        write_to.write(f"{item}\n")

    # Return it so Bytewax will run it whenever an item is seen by a
    # capture operator.
    return write


flow.capture(ManualOutputConfig(output_builder))


if __name__ == "__main__":
    # Run these two commands in separate terminals:

    # $ python ./examples/manual_cluster.py -p0 -a localhost:2101 -a localhost:2102
    # $ python ./examples/manual_cluster.py -p1 -a localhost:2101 -a localhost:2102

    # They'll collectively read the files in
    # ./examples/sample_data/cluster/*.txt which have lines like
    # `one1`.

    # They will then both finish and you'll see
    # ./cluster_out/worker0.out and ./cluster_out/worker1.out with the
    # data that each process in the cluster wrote with the lines
    # uppercased.

    # Recovery data will be written to ./cluster_recovery/ and you'll
    # see worker0.sqlite3 and worker1.sqlite3 files.
    recovery_dir.mkdir(exist_ok=True)
    recovery_config = SqliteRecoveryConfig(recovery_dir)

    # If you blow away the data from that successful run:

    # $ rm -rf ./cluster_out/ ./cluster_recovery/

    # Then we can try out recovery if a worker crashes:

    # $ FAIL=true python ./examples/manual_cluster.py -p0 -a localhost:2101 -a localhost:2102
    # $ FAIL=true python ./examples/manual_cluster.py -p1 -a localhost:2101 -a localhost:2102

    # If you look at ./cluster_out/worker0.out you'll see partial
    # data.

    # Then if you re-run the dataflow "fixing the bug" and disabling
    # the error:

    # $ python ./examples/manual_cluster.py -p0 -a localhost:2101 -a localhost:2102
    # $ python ./examples/manual_cluster.py -p1 -a localhost:2101 -a localhost:2102

    # You'll see the correct output in ./cluster_out/ again. Note that
    # there might be duplicate data near the failure point. Bytewax
    # does not in-general guarantee exactly-once processing.

    # You could imagine reading from / writing to separate Kafka
    # partitions, S3 blobs, etc.

    # When using `cluster_main()` you have to coordinate ensuring each
    # process knows the address of all other processes in the cluster
    # and their unique process ID.
    cluster_main(
        flow,
        recovery_config=recovery_config,
        # Because this dataflow finishes so fast (it's only reading a
        # handful of lines), and recovery by-default only snapshots
        # every 10 system time seconds, for this demo, we tell Bytewax
        # to snapshot after each item. In general, you should not use
        # this.
        epoch_config=TestingEpochConfig(),
        **parse.proc_args(),
    )
