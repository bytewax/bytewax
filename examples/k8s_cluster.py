from itertools import chain
from pathlib import Path

from bytewax import cluster_main, Dataflow, ManualInputConfig, parse

read_dir = Path("./examples/sample_data/cluster/")
write_dir = Path("./cluster_out/")


def input_builder(worker_index, worker_count, resume_epoch):
    # List all the input partitions in the reading directory.
    all_partitions = read_dir.glob("*.txt")
    # Then have this worker only read every `n` files so each worker
    # will read a disjoint set.
    this_worker_partitions = [
        path
        for i, path in enumerate(all_partitions)
        if i % worker_count == worker_index
    ]
    # Open all the ones that this worker should read.
    files = [open(path) for path in this_worker_partitions]
    # Now send them into the dataflow on this worker.
    for line in chain(*files):
        yield 0, line.strip()


def output_builder(worker_index, worker_count):
    write_dir.mkdir(exist_ok=True)
    # Open a file that just this worker will write to.
    write_to = open(write_dir / f"{worker_index}.out", "w")
    # Build a function that can be called for each captured output.
    def write(epoch_item):
        epoch, item = epoch_item
        write_to.write(f"{epoch} {item}\n")

    # Return it so Bytewax will run it whenever an item is seen by a
    # capture operator.
    return write


flow = Dataflow()
flow.map(str.upper)
flow.capture()


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
    cluster_main(
        flow, ManualInputConfig(input_builder), output_builder, **parse.proc_env()
    )
