import asyncio
import json
import operator
import sys
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import Any, List

# pip install aiohttp-sse-client
from aiohttp_sse_client.client import EventSource

from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.inputs import PartitionedInput, StatefulSource
from bytewax.tracing import setup_tracing
from bytewax.window import SystemClockConfig, TumblingWindow

setup_tracing(log_level="TRACE")


async def _sse_agen(url):
    async with EventSource(url) as source:
        async for event in source:
            yield event.data


class AsyncBatcher:
    def __init__(self, aiter: AsyncIterator[Any], loop=None):
        self._aiter = aiter

        self._loop = loop if loop is not None else asyncio.new_event_loop()
        self._task = None

    def next_batch(self, timeout: timedelta, max_len: int = sys.maxsize) -> List[Any]:
        async def anext_batch():
            batch = []
            for _ in range(max_len):
                if self._task is None:
                    self._task = self._loop.create_task(self._aiter.__anext__())

                try:
                    # Prevent the `wait_for` cancellation from
                    # stopping the `__anext__` task; usually all
                    # sub-tasks are cancelled too. It'll be re-used in
                    # the next batch.
                    next_item = await asyncio.shield(self._task)
                except asyncio.CancelledError:
                    # Timeout was hit and thus return the batch
                    # immediately.
                    break
                except StopAsyncIteration:
                    if len(batch) > 0:
                        # Return a half-finished batch if we run out
                        # of source items.
                        break
                    else:
                        # We can't raise `StopIteration` directly here
                        # because it's part of the coro protocol and
                        # would mess with this async function.
                        raise

                batch.append(next_item)
                self._task = None
            return batch

        try:
            # `wait_for` will raise `CancelledError` at the internal
            # await point if the timeout is hit.
            batch = self._loop.run_until_complete(
                asyncio.wait_for(anext_batch(), timeout.total_seconds())
            )
            return batch
        except StopAsyncIteration:
            # Suppress automatic exception chaining.
            raise StopIteration() from None


class WikiSource(StatefulSource):
    def __init__(self):
        agen = _sse_agen("https://stream.wikimedia.org/v2/stream/recentchange")
        self._batcher = AsyncBatcher(agen)

    def next_batch(self):
        return self._batcher.next_batch(timedelta(seconds=0.25))

    def snapshot(self):
        return None


class WikiStreamInput(PartitionedInput):
    def list_parts(self):
        return ["single-part"]

    def build_part(self, for_key, resume_state):
        assert for_key == "single-part"
        assert resume_state is None
        return WikiSource()


def initial_count(data_dict):
    return data_dict["server_name"], 1


def keep_max(max_count, new_count):
    new_max = max(max_count, new_count)
    # print(f"Just got {new_count}, old max was {max_count}, new max is {new_max}")
    return new_max, new_max


flow = Dataflow()
flow.input("inp", WikiStreamInput())
# "event_json"
flow.map(json.loads)
# {"server_name": "server.name", ...}
flow.map(initial_count)
# ("server.name", 1)
flow.reduce_window(
    "sum",
    SystemClockConfig(),
    TumblingWindow(
        length=timedelta(seconds=2), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
    ),
    operator.add,
)
# ("server.name", sum_per_window)
flow.stateful_map("keep_max", lambda: 0, keep_max)
# ("server.name", max_per_window)
flow.output("out", StdOutput())
