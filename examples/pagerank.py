import collections
import operator

from bytewax import Dataflow, parse, run_cluster

FIRST_ITERATION = 0


def read_edges(filename):
    with open(filename) as lines:
        for line in lines:
            line = line.strip()
            if line:
                parent, child = tuple(x.strip() for x in line.split(","))
                yield FIRST_ITERATION, (parent, {child})


INITIAL_WEIGHT = 1.0


def with_initial_weight(parent_children):
    parent, children = parent_children
    return parent, INITIAL_WEIGHT, children


def parent_contribs(parent_weight_children):
    parent, weight, children = parent_weight_children
    contrib_from_parent = weight / len(children)
    for child in children:
        yield child, contrib_from_parent


def sum_to_weight(node_sum):
    node, sum_contrib = node_sum
    updated_weight = 0.15 + 0.85 * sum_contrib
    return node, updated_weight


flow = Dataflow()
# (parent, {child}) per edge
flow.reduce_epoch(operator.or_)
# (parent, children) per parent

# TODO: Some sort of state capture here. This will be tricky because
# we don't have a way of building state per-worker generically
# yet. Timely uses Rust closures, but we're outside that context here.

flow.map(with_initial_weight)
# (parent, weight, children) per parent
flow.flat_map(parent_contribs)
# (child, contrib) per child * parent

# This is a network-bound performance optimization to pre-aggregate
# contribution sums in the worker before sending them to other
# workers. See
# https://github.com/frankmcsherry/blog/blob/master/posts/2015-07-08.md#implementation-2-worker-level-aggregation
flow.reduce_epoch_local(operator.add)

# (node, sum_contrib) per node per worker
flow.reduce_epoch(operator.add)
# (node, sum_contrib) per node
flow.map(sum_to_weight)
# (node, updated_weight) per node

# TODO: Figure out worker-persistent state? Then we don't need to
# re-join with the graph to get connectivity. Also figure out how to
# iterate.

flow.capture()


if __name__ == "__main__":
    for epoch, item in run_cluster(
        flow, read_edges("examples/sample_data/graph.txt"), **parse.cluster_args()
    ):
        print(epoch, item)
