import time

import bytewax
from bytewax import Dataflow, parse, run_cluster


def slow(x):
    print("start slow")
    # time.sleep(5)  # Works in parallel.
    # bytewax.sleep_release_gil(5)  # Works in parallel.
    bytewax.sleep_keep_gil(5)  # Does not.
    print("stop slow")
    return x


def busy(x):
    print("start busy")
    y = 0
    for i in range(50000000):
        y += 1
    print("stop busy")
    return x


def output(x):
    return x.replace("in", "out")


flow = Dataflow()
flow.inspect(print)
# flow.map(slow)
flow.map(busy)
flow.map(output)
flow.inspect(print)


if __name__ == "__main__":
    run_cluster(
        flow, enumerate(["in1", "in2", "in3", "in4", "in5"]), **parse.cluster_args()
    )
