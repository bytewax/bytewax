import collections
import operator

from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.window import TumblingWindowConfig, SystemClockConfig

def read_edges(worker_index, worker_count, resume_state):
    state = resume_state or None #ignoring recovery here
    with open("examples/sample_data/graph.txt") as lines:
        for line in lines:
            line = line.strip()
            if line:
                parent, child = tuple(x.strip() for x in line.split(","))
                yield state, (parent, {child})


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
flow.input("input", ManualInputConfig(read_edges))
# (parent, {child}) per edge
flow.reduce_window(
    "sum", SystemClockConfig(), TumblingWindowConfig(length=timedelta(seconds=5)), operator.or_
)
# (parent, children) per parent
flow.map(with_initial_weight)
# (parent, weight, children) per parent
flow.flat_map(parent_contribs)
# (node, sum_contrib) per node per worker
flow.reduce_window(
    "sum", SystemClockConfig(), TumblingWindowConfig(length=timedelta(seconds=5)), operator.add
)
# (node, sum_contrib) per node
flow.map(sum_to_weight)
# (node, updated_weight) per node
flow.capture(StdOutputConfig())


if __name__ == "__main__":
    run_main(flow)
