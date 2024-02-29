import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import bytewax.operators as op
import bytewax.operators.window as win

# pip install aiohttp-sse-client
from aiohttp_sse_client.client import EventSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async
from bytewax.operators.window import SystemClockConfig, TumblingWindow


async def _sse_agen(url):
    async with EventSource(url) as source:
        async for event in source:
            yield event.data


class WikiPartition(StatefulSourcePartition[str, None]):
    def __init__(self):
        agen = _sse_agen("https://stream.wikimedia.org/v2/stream/recentchange")
        # Gather up to 0.25 sec of or 1000 items.
        self._batcher = batch_async(agen, timedelta(seconds=0.25), 1000)

    def next_batch(self) -> List[str]:
        return next(self._batcher)

    def snapshot(self) -> None:
        return None


class WikiSource(FixedPartitionedSource[str, None]):
    def list_parts(self):
        return ["single-part"]

    def build_part(self, _step_id, _for_key, _resume_state):
        return WikiPartition()


flow = Dataflow("wikistream")
inp = op.input("inp", flow, WikiSource())
inp = op.map("load_json", inp, json.loads)
# { "server_name": ..., ... }


def get_server_name(data_dict):
    return data_dict["server_name"]


server_counts = win.count_window(
    "count",
    inp,
    SystemClockConfig(),
    TumblingWindow(
        length=timedelta(seconds=2), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
    ),
    get_server_name,
)
# ("server.name", count_per_window)


def keep_max(max_count: Optional[int], new_count: int) -> Tuple[Optional[int], int]:
    if max_count is None:
        new_max = new_count
    else:
        new_max = max(max_count, new_count)
    # print(f"Just got {new_count}, old max was {max_count}, new max is {new_max}")
    return (new_max, new_max)


max_count_per_window = op.stateful_map("keep_max", server_counts, keep_max)
# ("server.name", max_per_window)


def format_nice(name_max):
    server_name, max_per_window = name_max
    return f"{server_name}, {max_per_window}"


out = op.map("format", max_count_per_window, format_nice)
op.output("out", out, StdOutSink())
