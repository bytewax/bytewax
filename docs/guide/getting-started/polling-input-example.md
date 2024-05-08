# Polling Input Example

In this getting started guide, we'll explore how to use Bytewax to
create a simple dataflow that utilizes polling. Polling, in the
context of data processing, refers to the regular and repeated
querying of a data source to check for updates or new data. This is
particularly useful when dealing with APIs or databases that update
frequently but don't have a change stream that can be consumed.

In our example, we will build a dataflow to fetch and process data
from the Hacker News API. We will poll the publicly available hacker
news API for the most recent items available and then process them as
a stream.

## Setting Up the Environment

First, import the necessary modules. We'll use {py:obj}`logging` for
basic logging, {py:obj}`datetime.timedelta` for time manipulation,
[`requests`](https://requests.readthedocs.io/en/latest/) for HTTP
requests, and various components from the Bytewax framework.

```{testcode}
import logging
import requests

from typing import Optional
from datetime import timedelta
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import SimplePollingSource
```

## Creating a Custom Polling Source

{py:obj}`~bytewax.inputs.SimplePollingSource` is one of the base
classes available in Bytewax that we can use to build custom
connectors. Below, we define a class `HNSource` that inherits from
{py:obj}`~bytewax.inputs.SimplePollingSource`. This class will handle
fetching the latest item ID from the Hacker News API.

```{testcode}
class HNSource(SimplePollingSource):
    def next_item(self):
        return (
            "GLOBAL_ID",
            requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json(),
        )
```

## Building the Dataflow

### Initializing and Defining Inputs

Initialize the dataflow and set up the pipeline.

```{testcode}
flow = Dataflow("hn_scraper")
max_id = op.input("in", flow, HNSource(timedelta(seconds=15)))
```

### Determining Most Recent IDs

We can use the {py:obj}`~bytewax.operators.stateful_map` operator to
keep track of the last Id and determine the most recent IDs based on
the received max item ID. {py:obj}`~bytewax.operators.stateful_map` is
like {py:obj}`~bytewax.operators.map` in that it emits downstream the
modified record, but it differs from {py:obj}`~bytewax.operators.map`
in that you can maintain some idea of the current state. All stateful
operators partition state by a key, so notice the previous step is
emitting `"GLOBAL_ID"` in the key position.

```{testcode}
def mapper(old_max_id, new_max_id):
    if old_max_id is None:
        old_max_id = new_max_id - 10
    return (new_max_id, range(old_max_id, new_max_id))


ids = op.stateful_map("range", max_id, mapper)
```

### Flat Map and Redistribute

Our list of IDs can be split into single records with the
{py:obj}`~bytewax.operators.flat_map` operator and the resulting data
can be {py:obj}`~bytewax.operators.redistribute`d amongst the workers
defined when running the dataflow. This is particularly helpful for
instances such as this where we might have a bottleneck in the
downstream http request to fetch additional data.

```{testcode}
ids = op.flat_map("strip_key_flatten", ids, lambda key_ids: key_ids[1])
ids = op.redistribute("redist", ids)
```

### Fetching Item Metadata

Now, let's define a function to download the metadata for each Hacker
News item, given its ID. If there is no associated metadata for
whatever reason, we will filter these records out with the
{py:obj}`~bytewax.operators.filter_map` operator. This operator is
like a combination of {py:obj}`~bytewax.operators.filter` and
{py:obj}`~bytewax.operators.map` in that you can process the data like
a map operator and if the processing fails or results in None, those
items will not be emitted.

```{testcode}
def download_metadata(hn_id) -> Optional[dict]:
    data = requests.get(
        f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
    ).json()

    if data is None:
        logging.warning(f"Couldn't fetch item {hn_id}, skipping")
    return data


items = op.filter_map("meta_download", ids, download_metadata)
```

### Splitting the Stream

We have different types of items retrieved from the API and we are
intrested in dealing with them separately so we can leverage the
{py:obj}`~bytewax.operators.branch` operator to split out the comments
from the stories. We can then print the streams out separately.
Building on this, you would most likely publish the separate streams
to separate Kafka topics or tables in a database.

```{testcode}
split_stream = op.branch("split_comments", items, lambda item: item["type"] == "story")
stories = split_stream.trues
comments = split_stream.falses
op.output("stories-out", stories, StdOutSink())
op.output("comments-out", comments, StdOutSink())
```

## Running the Dataflow

To execute this dataflow, simply run the script just like a regular
Python file with a few added arguments to scale the number of workers.
Bytewax will handle the polling, data fetching, and processing in
real-time. You should see the latest Hacker News stories printed to
the standard output.

Here we can run with five worker threads in one process.

```console
$ python -m bytewax.run periodic_hacker_news.py -w 5
```

% We can't run these doctests because it'd take 15 seconds to poll.

_A quick aside on scaling. You can scale things across threads and
processes with Bytewax. There are limitations to the thread approach
due to the global interpreter lock (GIL) that Python uses. In most
instances, processes is the more suitable method of parallelization._

## Conclusion

This example demonstrates the power and simplicity of Bytewax for
building real-time data processing applications with polling.
