import json
import operator
from datetime import datetime, timedelta, timezone

# pip install aiohttp-sse-client
from aiohttp_sse_client.client import EventSource

from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.inputs import AsyncBatcher, PartitionedInput, StatefulSource
from bytewax.window import SystemClockConfig, TumblingWindow


async def _sse_agen(url):
    async with EventSource(url) as source:
        async for event in source:
            yield event.data


class WikiSource(StatefulSource):
    def __init__(self):
        agen = _sse_agen("https://stream.wikimedia.org/v2/stream/recentchange")
        self._batcher = AsyncBatcher(agen)

    def next(self):
        # Gather up to 0.25 sec of items.
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
