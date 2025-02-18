"""A Bytewax connector for [Rerun](https://rerun.io).

This modules offers a single sink: {py:class}`sinks.RerunSink`.

This makes it easy to log messages to Rerun from Bytewax, properly handling multiple
workers.

You can instantiate as many sinks as you need, and you have to pass a stream of
{py:class}`sinks.RerunMessage`s to them:

```python
op.output("rerun-time-sink", messages_stream, RerunSink("app_id", "recording_id"))
```

{py:class}`sinks.RerunMessage` is a helper class that defines an entity that can be
logged into Rerun with all the properties needed:

```python
message = RerunMessage(
    entity_path=f"metrics/{name}",
    entity=rr.Scalar(value),
    timeline="metrics",
    time=seconds_since_start,
)
```

The sink supports all Rerun's operating modes: `spawn`, `connect`, `save` and `serve`.
You can use the sink to record metrics to a file for each worker, and later use the
Rerun viewer to replay all the recordings togheter.

The sink also offers a {py:obj}`sinks.RerunSink.rerun_log` decorator.

If you decorate any of your functions with this, Bytewax will log the moment the
function was called, and how long it took to run the function into a separate timeline
in `Rerun`.

The metrics are divided by worker, so you can see when each one is activated and for how
long. You can optionally log the arguments used in each function, so you can see your
items flowing through the dataflow.

To use it, instance the sink first:

```python
sink = RerunSink(...)
```

And then decorate your function:

```python
@sink.rerun_log()
def my_calculation(item: int) -> int:
    # Do something with the data
    return item
```
"""

from .sinks import RerunMessage, RerunSink

__all__ = [
    "RerunSink",
    "RerunMessage",
]
