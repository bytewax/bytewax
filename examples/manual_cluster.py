from itertools import chain
from pathlib import Path

from bytewax import Dataflow, main_proc, parse


read_dir = Path("./examples/sample_data/cluster/")
write_dir = Path("./cluster_out/")


def input_builder(worker_index, worker_count):
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
    # Run these two commands in separate terminals:

    # $ python ./examples/manual_cluster.py -p0 -a localhost:2101 -a localhost:2102
    # $ python ./examples/manual_cluster.py -p1 -a localhost:2101 -a localhost:2102

    # They'll collectively read the files in
    # ./examples/sample_data/cluster/*.txt which have lines like
    # `one1`.

    # They will then both finish and you'll see ./cluster_out/0.out
    # and ./cluster_out/1.out with the data that each process in the
    # cluster wrote with the lines uppercased.

    # You could imagine reading from / writing to separate Kafaka
    # partitions, S3 blobs, etc.

    # When using `main_proc()` you have to coordinate ensuring each
    # process knows the address of all other processes in the cluster
    # and their unique process ID.
    main_proc(flow, input_builder, output_builder, **parse.proc_args())
