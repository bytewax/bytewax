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
    # Try 3 times, waiting more and more, or give up
    for i in range(1, 4):
        req = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json")
        if not req.json():
            logger.warning(f"error getting payload from item {hn_id} trying again")
            time.sleep(1**i)
        else:
            return req.json()
    logger.warning(f"Couldn't fetch page {hn_id}, skipping")
    return None


flow = Dataflow("hn_scraper")
max_id = flow.input("in", HNSource(timedelta(seconds=1)))


def mapper(old_max_id, new_max_id):
    if old_max_id is None:
        # Get the last 10 items on the first run.
        old_max_id = new_max_id - 10
    return (new_max_id, range(old_max_id, new_max_id))


ids = (
    max_id.key_assert("keyed on id")
    .stateful_map("range", lambda: None, mapper)
    .flat_map("strip key, flatten", lambda key_ids: key_ids[1])
    .redistribute("redist")
)
# If you run this dataflow with multiple workers, downloads in the
# next `map` will be parallelized thanks to .redistribute()
def worker_inspector(message):
    def inspector(_id, item, _epoch, worker):
        print(f"Worker {worker} ==> {message(item)}")

    return inspector


ids.inspect_debug(
    "inspect ids",
    worker_inspector(lambda x: f"Downloading {x}..."),
)
items = ids.filter_map("meta_download", download_metadata)
items.inspect_debug(
    "download finished",
    worker_inspector(lambda x: f"Downloaded {x['id']}"),
)
stories = items.filter("just_stories", lambda meta: meta["type"] == "story")
stories.output("out", StdOutSink())
