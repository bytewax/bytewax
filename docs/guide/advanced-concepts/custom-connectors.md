(xref-custom-connectors)=
# Custom IO Connectors

More documentation here is forthcoming.

For now look at {py:obj}`bytewax.inputs.FixedPartitionedSource` and
{py:obj}`bytewax.inputs.DynamicSource` for inputs and
{py:obj}`bytewax.outputs.FixedPartitionedSink` and
{py:obj}`bytewax.outputs.DynamicSink` for outputs for the abstract
base classes you need to implement.

Also check out some of our built-in custom connectors in
{py:obj}`bytewax.connectors` and looking at the source for how they
are implemented.

(xref-async)=
## Async Client Libraries

The lowest-level hooks that Bytewax's source and sink APIs provide are
currently all _synchronous_. E.g.
{py:obj}`bytewax.inputs.StatefulSourcePartition.next_batch` is a `def
next_batch` and not an `async def next_batch`. This means that you
cannot directly call async functions provided by a client library.

Here we'll provide some common patterns for bridging the gap between
async client libraries and Bytewax's source and sink APIs. It's
possible the client library you pick will use an API design that is
not compatible with these patterns, but the ideas within can be reused
to fit with other designs.

### Async Sources

If your source's client library only provides async functions, we
recommend creating a helper async generator that wraps up all async
operations, and using {py:obj}`bytewax.inputs.batch_async`.

In the following example, we'll use a hypothetical
[WebSockets](https://en.wikipedia.org/wiki/WebSocket) library that
gives us the following `WSClient` class with async methods:

```python
from typing_extensions import Self


class WSClient:
    @classmethod
    async def connect(cls, to_url: str) -> Self:
        """Open a new connection."""
        ...

    async def recv(self) -> bytes:
        """Get the next message, waiting until one is availible."""
        ...
```

Given this API, first lets encapsulate all of the async calls into a
single {external+python:std:term}`asynchronous generator` so we only
have a single async entry point which we need to shim.

```python
from typing import AsyncIterator


async def _ws_helper(to_url: str) -> AsyncIterator[bytes]:
    client = await WSClient.connect(to_url)

    while True:
        msg = await client.recv()
        yield msg
```

This sets up a connection, then polls it for an unending stream of new
data.

We now need to have some way to make this async generator play well
with the cooperative multitasking source API: Each worker goes through
all of the operators, polls them for work to be done. If a
badly-behaved operator blocks forever, then the rest of the operators
in the dataflow are never executed! Thus, e.g.
{py:obj}`~bytewax.inputs.StatefulSourcePartition.next_batch` must
never block for long periods of time. Sources are especially easy to
accidentally make badly-behaved logic which never yields because they
aren't responding to upstream items: they generate items from an
external system and you need to introduce some manual way of saying
"that's enough work, take a break".

{py:obj}`bytewax.inputs.batch_async` is a helper function which turns
an async iterator into a normal non-async iterator of batches. It
gives you a way to run an async iterator for a short period of time,
collect the items yielded, then pause the iterator so you can come
back to it later. This lets you play well with the cooperative
multitasking requirement of sources.

To use it, you pick two parameters:

* `timeout` - How long to block and poll for new items before pausing.

* `batch_size` - The maximum number of items to accumulate before
  pausing.

Now that we have our tools, we'll build an input source that
represents the WebSocket stream as a single partition. First we have
to call our `_ws_helper` to instantiate the async generator that will
yield the incoming messages. But then we wrap that in
{py:obj}`bytewax.inputs.batch_async` to get a normal sync iterator of
batches. Here we specify that whenever we poll `self._batcher` for
more items, it'll wait up to 500 milliseconds or gather up to 100
items before returning that batch, whichever comes first.
{py:obj}`next` is used to advance that sync iterator.

```python
from typing import List

from bytewax.inputs import batch_async, StatefulSourcePartition


class EgWebSocketSourcePartition(StatefulSourcePartition[bytes, None]):
    def __init__(self):
        agen = _ws_helper("http://test.invalid/socket")
        self._batcher = batch_async(agen, timedelta(seconds=0.5), 100)

    def next_batch(self) -> List[bytes]:
        return next(self._batcher)

    def snapshot(self) -> None:
        return None
```

Then finally we wrap that up in the boilerplate to make a single
partitioned source.

```python
from bytewax.inputs import FixedPartitionedSource


class EgWebSocketSource(FixedPartitionedSource[bytes, None]):
    def list_parts(self) -> List[str]:
        return ["singleton"]

    def build_part(
        self, step_id: str, for_key: str, _resume_state: None
    ) -> EgWebSocketSourcePartition:
        return EgWebSocketSourcePartition()
```

This `WebSocketSource` can then be used with any
{py:obj}`bytewax.operators.input` like usual.

### Async Sinks

Writing sinks which use an async client library requires a slightly
different pattern because a sink must successfully write out all
provided items before
{py:obj}`bytewax.outputs.StatefulSinkPartition.write_batch` returns;
it can't delay writing items and maintain delivery guarantees.
Thankfully the Python standard library gives you the
{py:obj}`asyncio.run` method, which will block and run an async
function to completion.

This fits well when you are given a client library with the following
shape:

```python
class WSClient:
    async def send(self, msg: bytes) -> None:
        """Send the following message."""
        ...
```

You could then use {py:obj}`asyncio.run` to call the `async def send`
method synchronously for each item.

```python
import asyncio

from bytewax.outputs import StatefulSinkPartition


class EgWebSocketSinkPartition(StatefulSinkPartition[bytes, None]):
    def __init__(self):
        self._client = WSClient()

    def write_batch(self, batch: List[bytes]) -> None:
        for item in batch:
            asyncio.run(self._client.send(item))

    def snapshot(self) -> None:
        return None
```
