import os
from pathlib import Path

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.inputs import distribute, ManualInputConfig
from bytewax.outputs import ManualOutputConfig
from bytewax.recovery import SqliteRecoveryConfig

input_dir = Path("./examples/sample_data/cluster/")
output_dir = Path("./cluster_out/")
recovery_dir = Path("./cluster_recovery/")

# to see more on recovery with this example, see "examples/manual_cluster.py"
recovery_dir.mkdir(exist_ok=True)
recovery_config = SqliteRecoveryConfig(recovery_dir)


def input_builder(worker_index, worker_count, resume_state):
    print(f"Worker {worker_index} resuming with state: {resume_state}")
    # Fill in a default resume state if we have None. The resume state
    # will be a dict from path to line number to start reading at.
    state = resume_state or {}
    # List all the input partitions in the reading directory.
    all_partitions = input_dir.glob("*.txt")
    # Then have this worker only read every `n` files so each worker
    # will read a disjoint set.
    this_worker_partitions = distribute(all_partitions, worker_index, worker_count)
    # Open all the ones that this worker should read.
    for path in this_worker_partitions:
        with open(path) as f:
            for i, line in enumerate(f):
                # If we're resuming, skip ahead to the line for this
                # file in the state.
                if i < state.get(path, 0):
                    continue
                # Since the file has just read the current line as
                # part of the for loop, note that on resume we should
                # start reading from the next line.
                state[path] = i + 1
                # Now send them into the dataflow on this worker.
                yield state, line.strip()
                print(f"Worker {worker_index} input state: {state}")


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


flow = Dataflow()
flow.input("inp", ManualInputConfig(input_builder))
flow.map(str.upper)
flow.capture(ManualOutputConfig(output_builder))



if __name__ == "__main__":
    # We are going to use Waxctl, you can download it from https://bytewax.io/downloads
    # Run these commands in your terminal to run a cluster of two containers:

    # $ tar -C ./ -cvf cluster.tar examples
    # $ waxctl dataflow deploy ./cluster.tar --name k8s-cluster --python-file-name examples/k8s_cluster.py -p2

    # Each worker will read the files in
    # ./examples/sample_data/cluster/*.txt which have lines like
    # `one1`.

    # They will then both finish and you'll see ./cluster_out/0.out
    # and ./cluster_out/1.out with the data that each process in the
    # cluster wrote with the lines uppercased.

    # To see that files in each container you can run these commands:

    # kubectl exec -it k8s-cluster-0 -cprocess -- cat /var/bytewax/cluster_out/0.out
    # kubectl exec -it k8s-cluster-1 -cprocess -- cat /var/bytewax/cluster_out/1.out

    # You could imagine reading from / writing to separate Kafka
    # partitions, S3 blobs, etc.

    # When using `cluster_main()` you have to coordinate ensuring each
    # process knows the address of all other processes in the cluster
    # and their unique process ID. You can address that easily by deploying your
    # dataflow program using Waxctl or installing the Bytewax Helm Chart
    cluster_main(flow, recovery_config=recovery_config, **parse.proc_env())
