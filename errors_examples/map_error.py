"""
This dataflow crashes for an Exception in the mapper function
of the `map` operator.
We cause the crash by raising an exception in one of the maps.
"""

from datetime import timedelta
from collections import defaultdict

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.inputs import StatelessSource, DynamicInput
from bytewax.window import TumblingWindow, SystemClockConfig, SessionWindow


class NumberSource(StatelessSource):
    def __init__(self, max, worker_index):
        if worker_index == 0:
            self.iterator = iter(range(max))

    def next(self):
        if self.iterator is not None:
            return next(self.iterator)

    def close(self):
        pass


class NumberInput(DynamicInput):
    def __init__(self, max):
        self.max = max

    def build(self, worker_index, worker_count):
        return NumberSource(self.max, worker_index)


def filter_odd(x):
    return x % 2 == 0


def filter_double(x):
    if x == 0:
        return None
    else:
        return x * 2


def expand(x):
    return range(x)


def minus_one(x):
    # XXX: Error here
    raise TypeError("A really nice type error")
    # Should be:
    # return "ALL", [x - 1]


def folder(acc, x):
    acc[x[0]] += 1
    return acc


def reducer(count, event_count):
    return count + event_count


flow = Dataflow()
flow.input("inp", NumberInput(10))
# Stateless operators
flow.filter(filter_odd)
flow.filter_map(filter_double)
flow.flat_map(expand)
flow.inspect(lambda x: print(f"Inspecting {x}"))
flow.inspect_epoch(lambda epoch, x: print(f"(epoch {epoch}) Inspecting {x}"))
flow.map(minus_one)
# Stateful operators
flow.reduce("reduce", lambda acc, x: [*acc, x], lambda acc: True)
cc = SystemClockConfig()
wc = TumblingWindow(length=timedelta(seconds=1))
flow.fold_window("fold_window", cc, wc, lambda: defaultdict(lambda: 0), folder)
wc = SessionWindow(gap=timedelta(seconds=1))
flow.reduce_window("reduce_window", cc, wc, reducer)
flow.stateful_map("stateful_map", lambda: 0, lambda acc, x: (acc, x))
flow.map(lambda x: dict(x[1]))
flow.output("out", StdOutput())


if __name__ == "__main__":
    from bytewax.execution import run_main
    run_main(flow)
    # from bytewax.execution import spawn_cluster
    # spawn_cluster(flow)
