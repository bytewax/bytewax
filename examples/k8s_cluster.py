import os
from pathlib import Path

from bytewax import parse
from bytewax.connectors.files import DirInput
from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.outputs import ManualOutputConfig
from bytewax.recovery import SqliteRecoveryConfig

input_dir = Path("./examples/sample_data/cluster/")
output_dir = Path("./cluster_out/")
recovery_dir = Path("./cluster_recovery/")

# to see more on recovery with this example, see "examples/manual_cluster.py"
recovery_dir.mkdir(exist_ok=True)
recovery_config = SqliteRecoveryConfig(recovery_dir)


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
flow.input("inp", DirInput(input_dir))
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
