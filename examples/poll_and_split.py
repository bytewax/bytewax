import logging
from datetime import timedelta
from typing import Optional

import requests
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import SimplePollingSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HNSource(SimplePollingSource):
    def next_item(self):
        return (
            "GLOBAL_ID",
            requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json(),
        )


def download_metadata(hn_id) -> Optional[dict]:
    # Given an hacker news id returned from the api, fetch metadata
    # Try 3 times, waiting more and more, or give up
    data = requests.get(
        f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
    ).json()

    if data is None:
        logger.warning(f"Couldn't fetch item {hn_id}, skipping")
    return data


flow = Dataflow("hn_scraper")
max_id = op.input("in", flow, HNSource(timedelta(seconds=1)))


def mapper(old_max_id, new_max_id):
    if old_max_id is None:
        # Get the last 10 items on the first run.
        old_max_id = new_max_id - 10
    return (new_max_id, range(old_max_id, new_max_id))


ids = op.stateful_map("range", max_id, lambda: None, mapper)
ids = op.flat_map("strip_key_flatten", ids, lambda key_ids: key_ids[1])
ids = op.redistribute("redist", ids)

# If you run this dataflow with multiple workers, downloads in the
# next `map` will be parallelized thanks to .redistribute()
items = op.filter_map("meta_download", ids, download_metadata)
split_stream = op.branch("split_comments", items, lambda item: item["type"]=="story")
stories = split_stream.trues
comments = split_stream.falses
op.output("stories-out", stories, StdOutSink())
op.output("comments-out", comments, StdOutSink())
