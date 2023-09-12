"""Fetch the home page of hackernews every hour and extract all the links from there."""
from datetime import timedelta

import requests
from bytewax.connectors.periodic import SimplePollingInput
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow


class HNInput(SimplePollingInput):
    def next_item(self):
        # Extract the first 10 item ids from newstories api.
        # You can then use the id to fetch metadata about
        # an hackernews item
        return requests.get(
            "https://hacker-news.firebaseio.com/v0/newstories.json"
        ).json()[:10]


def extract_json(response):
    return response.json()


def download_details(hn_id):
    return requests.get(f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json")


flow = Dataflow()
flow.input("in", HNInput(timedelta(hours=1)))
flow.flat_map(lambda x: x)
# flow.inspect(print)
# If you run this dataflow with multiple workers, downloads in
# the next `map` will be parallelized thanks to .redistribute()
flow.redistribute()
flow.map(download_details)
flow.map(extract_json)
# flow.inspect(print)
flow.map(lambda x: {**x, "content": requests.get(x["url"]).content})
# We could do something useful, but we just print the title instead
flow.map(lambda x: f"Downloaded: {x['title']}")
flow.output("out", StdOutput())
