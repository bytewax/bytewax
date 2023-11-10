import logging
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
    data = requests.get(
        f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
    ).json()

    if data is None:
        logger.warning(f"Couldn't fetch item {hn_id}, skipping")
    return data


flow = Dataflow("hn_scraper")
max_id = flow.input("in", HNSource(timedelta(seconds=1)))


def mapper(old_max_id, new_max_id):
    if old_max_id is None:
        # Get the last 10 items on the first run.
        old_max_id = new_max_id - 10
    return (new_max_id, range(old_max_id, new_max_id))


ids = max_id.key_assert("keyed_on_id").stateful_map("range", lambda: None, mapper)
ids = ids.flat_map("strip_key_flatten", lambda key_ids: key_ids[1])
ids = ids.redistribute("redist")

# If you run this dataflow with multiple workers, downloads in the
# next `map` will be parallelized thanks to .redistribute()
items = ids.filter_map("meta_download", download_metadata)
stories = items.filter("just_stories", lambda meta: meta["type"] == "story")
stories.output("out", StdOutSink())
