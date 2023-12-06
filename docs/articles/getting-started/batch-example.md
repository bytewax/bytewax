# Example batch operator

Showing the use of batch and simple window

## Stateful Operators and Keyed Streams

Bytewax has **stateful operators** that allow you to persist state
between items flowing in the dataflow. Some examples are
`bytewax.operators.window.fold_window` and
`bytewax.operators.stateful_map`. The Bytewax runtime partitions state
across multiple workers by a **state key**. This means that for a
given key, all items are moved onto the primary worker which has the
state for the key before processing the step. This means the state
overall is balanced over all workers, allowing rescaling, but the
state for a single key is localized.

This means that all stateful operators only work with a **keyed
stream** upstream with items that are shaped like 2-tuples of `(key,
value)`. Whenever we refer to a **value** in reference to stateful
operators, it means specifically the second value of these tuples. The
operators `bytewax.operators.key_on` and
`bytewax.operators.key_assert` help you make streams of this
`bytewax.dataflow.KeyedStream` shape. Downstreams of the stateful
operators will also be keyed and the relevant key will automatically
be applied.

For example, let's write a small dataflow that calculates a running
sum of points in a basketball game. First, let's define a

>>> flow = Dataflow("running_sum")
>>> points_a = op.input("points_a", flow, TestingSource([2, 2, 3, 2, 3, 3]))
>>> points_b = op.input("points_b", flow, TestingSource([2, 3, 2, 2, 2, 2]))
>>> points_a = op.key_on("key_a", points_a, lambda _x: "A")
>>> points_b = op.key_on("key_b", points_b, lambda _x: "B")
>>> points = op.merge("merge", points_a, points_b)
>>> def running_sum(old_sum, just_scored_points):
...     new_sum = old_sum + just_scored_points
...     return (new_sum, new_sum)
>>> running_sum = op.stateful_map("running_sum", points, lambda: 0, running_sum)
>>> op.output("out", running_sum, StdOutSink())
>>> bytewax.testing.run_main(flow)
('A', 2)
('B', 2)
('A', 4)
('B', 5)
('A', 7)
('B', 7)
('A', 9)
('B', 9)
('A', 12)
('B', 11)
('A', 15)
('B', 13)
