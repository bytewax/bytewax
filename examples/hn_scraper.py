"""Fetch the home page of hackernews every hour and extract all the links from there."""
from datetime import timedelta

import requests
from bs4 import BeautifulSoup
from bytewax.connectors.periodic import SimplePollingInput
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow


class HNInput(SimplePollingInput):
    def next_item(self):
        return requests.get("https://news.ycombinator.com")


def recurse_hn(html: str):
    webpages = []
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("tr[class='athing']")
    for lineItem in items:
        ranking = lineItem.select_one("span[class='rank']").text
        title = lineItem.find("span", {"class": "titleline"}).text.strip()

        link = lineItem.find("span", {"class": "titleline"}).find("a").get("href")
        if "item?id=" in link:
            link = f"https://news.ycombinator.com/{link}"

        webpages.append(
            {
                "title": title,
                "link": link,
                "ranking": ranking,
            }
        )
    return webpages


flow = Dataflow()
flow.input("in", HNInput(timedelta(hours=1)))
flow.map(lambda res: res.content)
flow.map(recurse_hn)
flow.flat_map(lambda x: x)
# If you run this dataflow with multiple workers, downloads in
# the next `map` will be parallelized thanks to .redistribute()
flow.redistribute()
flow.map(lambda x: {**x, "content": requests.get(x["link"]).content})
flow.map(lambda x: f"DOWNLOADED {x['title']}")
flow.output("out", StdOutput())
