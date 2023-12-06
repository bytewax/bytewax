## Debugging

Tricks for solving problems in your dataflow.

### Inspecting

Bytewax provides the `bytewax.operators.inspect` operator to help you
visualize what is going on inside your dataflow. It prints out the
repr of all items that pass through it when you run the dataflow. You
can attach it to any stream you'd like to see.

TODO Example?

### Step Names and Step IDs

Operator methods take a string step name as their first argument which
should represent the semantic purpose of that computational step.

Bytewax operators are defined in terms of other operators
hierarchically in nested **substeps**. In order to disambiguate
information about substeps under a single top-level step, each substep
is assigned a unique **step ID**, which is a dotted path of all the
step names down the heirarchy to that individual operator.

This means exceptions, logs, traces, and metrics might show step IDs
like `branching_math.do_double.flat_map`. Don't fret! Read them from
left to right: the first component will be the flow ID, then the
top-level step name, then the path to the specific inner substep the
information applies to.

Just because an exception references a step ID of a substep of one of
your steps, does not mean the bug is in the inner step. It's possible
some logic function or value supplied is not conforming to the
contract the operator set. You'll need to read the exception and
documentation of the operator carefully.
