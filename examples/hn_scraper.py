import logging
import time
from datetime import timedelta

import requests
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


def download_metadata(hn_id):
    # Given an hacker news id returned from the api, fetch metadata
    req = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json")
    if not req.json():
        logger.warning(f"error getting payload from item {hn_id} trying again")
        time.sleep(0.5)
        return download_metadata(hn_id)
    return req.json()


def recurse_tree(metadata):
    try:
        parent_id = metadata["parent"]
        parent_metadata = download_metadata(parent_id)
        return recurse_tree(parent_metadata)
    except KeyError:
        return (metadata["id"], {**metadata, "key_id": metadata["id"]})


flow = Dataflow("hn_scraper")
max_id = flow.input("in", HNSource(timedelta(seconds=15)))


def mapper(old_max_id, new_max_id):
    if old_max_id is None:
        # Get the last 10 items on the first run.
        old_max_id = new_max_id - 10
    return (new_max_id, range(old_max_id, new_max_id))


ids = (
    max_id.stateful_flat_map("range", mapper)
    .map("strip_key", lambda key_max: key_max[1])
    .redistribute("redist")
)
# If you run this dataflow with multiple workers, downloads in the
# next `map` will be parallelized thanks to .redistribute()

items = ids.map("meta_download", download_metadata)
stories = items.filter("just_stories", lambda meta: meta["type"] == "story")
stories.output("out", StdOutSink())
