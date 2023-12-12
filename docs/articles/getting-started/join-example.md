This section will introduce some of the basic concepts of merging and
joining streams of data.

## Multiple input sources

Bytewax dataflows can receive input from multiple sources. In the following
example, we create two `TestingSources` and add them to our `Dataflow` as input.

```python
from bytewax import operators as op

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("join")

inp1 = op.input("inp1", flow, TestingSource([{"user_id": "123", "name": "Bumble"}]))
inp2 = op.input(
    "inp2",
    flow,
    TestingSource(
        [
            {"user_id": "123", "email": "bee@bytewax.com"},
            {"user_id": "456", "email": "hive@bytewax.com"},
        ]
    ),
)
```

In order for our dataflow to process input from either of these sources, we'll need
to create a `Stream` that combines input from both of them, we can use the `op.merge`
operator to do so:

```python
merged_stream = op.merge("merge", inp1, inp2)
```

Now that we have our merged stream, we can write it to standard out:

```python
op.output("out", merged_stream, StdOutSink())
```

```shell
>  python -m bytewax.run merge_example
{'user_id': "123", 'name': 'Bumble'}
{'user_id': "123", 'email': 'bee@bytewax.com'}
{'user_id': "234", 'email': 'hive@bytewax.com'}
```

Note that even when the first input source of data is out of input, we still
see the output from the second source.

## Joining streams

To create a streaming join of data from both of our input sources, we'll need to
first choose a key that we want to join our streams on. In our example data,
we'll use the `user_id` field.

```python
keyed_inp_1 = op.key_on("key_stream_1", inp1, lambda x: x["user_id"])
keyed_inp_2 = op.key_on("key_stream_2", inp2, lambda x: x["user_id"])
```

Now that we have our two keyed streams of data, we can join them together with
the `op.join` operator:


```python
merged_stream = op.join("join", keyed_inp_1, keyed_inp_2)
```

Running the updated example, we should see the following output:


```shell
> python -m bytewax.run join_example
('123', ({'user_id': '123', 'name': 'Bumble'}, {'user_id': '123', 'email': 'bee@bytewax.com'}))
```

For more information see the [Joins](/docs/articles/concepts/joins.md) section of the documentation.
