(xref-join-eg)=
# Join Example

This section will introduce some of the basic concepts of merging and
joining streams of data.

## Multiple input sources

Bytewax dataflows can receive input from multiple sources. In the
following example, we create two
{py:obj}`~bytewax.testing.TestingSource` sources and add them to our
{py:obj}`~bytewax.dataflow.Dataflow` as input.

```{testcode}
from bytewax import operators as op

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("join")

src_1 = [
    {"user_id": "123", "name": "Bumble"},
]
inp1 = op.input("inp1", flow, TestingSource(src_1))

src_2 = [
    {"user_id": "123", "email": "bee@bytewax.com"},
    {"user_id": "456", "email": "hive@bytewax.com"},
]
inp2 = op.input("inp2", flow, TestingSource(src_2))
```

In order for our dataflow to process input from either of these
sources, we'll need to create a {py:obj}`~bytewax.dataflow.Stream`
that combines input from both of them, we can use the
{py:obj}`~bytewax.operators.merge` operator to do so:

```{testcode}
merged_stream = op.merge("merge", inp1, inp2)
```

Now that we have our merged stream, we can write it to standard out:

```{testcode}
op.inspect("debug", merged_stream)
```

```console
$ python -m bytewax.run merge_example
```

```{testcode}
:hide:

from bytewax.testing import run_main

run_main(flow)
```

```{testoutput}
join.debug: {'user_id': '123', 'name': 'Bumble'}
join.debug: {'user_id': '123', 'email': 'bee@bytewax.com'}
join.debug: {'user_id': '456', 'email': 'hive@bytewax.com'}
```

The dataflow will stop once all input sources are completely
exhausted. Even though the input sources have different numbers of
items, we see all of them.

## Joining streams

To create a streaming join of data from both of our input sources,
we'll need to first choose a key that we want to join our streams on.
In our example data, we'll use the `user_id` field.

```{testcode}
from bytewax import operators as op

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("join")

src_1 = [
    {"user_id": "123", "name": "Bumble"},
]
inp1 = op.input("inp1", flow, TestingSource(src_1))
keyed_inp_1 = op.key_on("key_stream_1", inp1, lambda x: x["user_id"])
src_2 = [
    {"user_id": "123", "email": "bee@bytewax.com"},
    {"user_id": "456", "email": "hive@bytewax.com"},
]
inp2 = op.input("inp2", flow, TestingSource(src_2))
keyed_inp_2 = op.key_on("key_stream_2", inp2, lambda x: x["user_id"])
```

Now that we have our two keyed streams of data, we can join them
together with the {py:obj}`~bytewax.operators.join` operator.

When creating a dataflow, you can use the
{py:obj}`~bytewax.operators.inspect` operator to view the data in a
stream. The {py:obj}`~bytewax.operators.inspect` operator can be used
multiple times and counts as an output (recall that every dataflow
requires an output).

```{testcode}
merged_stream = op.join("join", keyed_inp_1, keyed_inp_2)
op.inspect("debug", merged_stream)
```

Running this example, we should see the following output for our
stream, which includes the `step_id` for our `inspect` operator.

```console
$ python -m bytewax.run join_example
```

```{testcode}
:hide:

run_main(flow)
```

```{testoutput}
join.debug: ('123', ({'user_id': '123', 'name': 'Bumble'}, {'user_id': '123', 'email': 'bee@bytewax.com'}))
```

Notice that we don't see any output for `user_id` 456. Since we didn't
receive any input for that key from `inp1`, we won't see any output
for that user until we do.

For more details about the behavior of the join operator, see the
<project:#xref-joins> section of the documentation.
