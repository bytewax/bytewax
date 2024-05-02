# Building Sessions from Search Logs
<a id="top"></a>
<p align='center'>Here is an introductory example of using Bytewax to turn an incoming stream of event logs from a hypothetical search engine into metrics over search sessions. In this example, we're going to focus on the dataflow itself and aggregating state.</p>

<table style="width:100%; border: none;">
  <tr>
    <th style="text-align:center; padding: 8px;">Skill level</th>
    <th style="text-align:center; padding: 8px;">Time to complete</th>
    <th style="text-align:center; padding: 8px;">Level</th>
  </tr>
  <tr>
    <td style="text-align:center; padding: 8px;">Basic, no prior knowledge requirements</td>
    <td style="text-align:center; padding: 8px;">Approx. 25 min</td>
    <td style="text-align:center; padding: 8px; color:#30D5C8">Beginner</td>
  </tr>
</table>


## Prerequisites

**Python modules**
- bytewax==0.19.*

## Your Takeaway

This guide will teach you how to use Bytewax to detect and calculate the Click-Through Rate (CTR) on a custom session window on streaming data using a window and then calculate metrics downstream.



## Table of Contents

- [Resources](#xref-resources)
- [Introduction and problem statement](#xref-introduction)
- [Strategy](#xref-strategy)
- [Assumptions](#xref-assumptions)
- [Imports and Setup](#xref-imports)
- [Data Model](#xref-data-model)
- [Defining user events, adding events, and calculating CTR](#xref-define-user-events)
- [Creating our Dataflow](#xref-create-dataflow)
- [Execution](#xref-execution-search-logs)
- [Summary](#xref-summary)

(xref-resources)=
## Resources

<p style="text-align: center;">
  <a href="https://github.com/bytewax/search-session" rel="noopener noreferrer" target="_blank" class="btn btn--github btn--lg">
    GitHub Repo
    <svg data-v-e8d572f6="" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" aria-hidden="true" role="img" class="inline-block -mr-1 ml-2" width="1.5rem" height="1.5rem" viewBox="0 0 24 24">
      <path fill="currentColor" d="M12 2A10 10 0 0 0 2 12c0 4.42 2.87 8.17 6.84 9.5c.5.08.66-.23.66-.5v-1.69c-2.77.6-3.36-1.34-3.36-1.34c-.46-1.16-1.11-1.47-1.11-1.47c-.91-.62.07-.6.07-.6c1 .07 1.53 1.03 1.53 1.03c.87 1.52 2.34 1.07 2.91.83c.09-.65.35-1.09.63-1.34c-2.22-.25-4.55-1.11-4.55-4.92c0-1.11.38-2 1.03-2.71c-.1-.25-.45-1.29.1-2.64c0 0 .84-.27 2.75 1.02c.79-.22 1.65-.33 2.5-.33s1.71.11 2.5.33c1.91-1.29 2.75-1.02 2.75-1.02c.55 1.35.2 2.39.1 2.64c.65.71 1.03 1.6 1.03 2.71c0 3.82-2.34 4.66-4.57 4.91c.36.31.69.92.69 1.85V21c0 .27.16.59.67.5C19.14 20.16 22 16.42 22 12A10 10 0 0 0 12 2"></path>
    </svg>
  </a>
</p>

(xref-introduction)=
## Introduction and problem statement

One of the most critical metrics in evaluating the effectiveness of online platforms, particularly search engines, is the Click-Through Rate (CTR). The CTR is a measure of how frequently users engage with search results or advertisements, making it an indispensable metric for digital marketers, web developers, and data analysts.

This relevance of CTR extends to any enterprise aiming to understand user behavior, refine content relevancy, and ultimately, increase the profitability of online activities. As such, efficiently calculating and analyzing CTR is not only essential for enhancing user experience but also for driving strategic business decisions. The challenge, however, lies in accurately aggregating and processing streaming data to generate timely and actionable insights.

Our focus on developing a dataflow using Bytewax—an open-source Python framework for streaming data processing—addresses this challenge head-on. Bytewax allows for the real-time processing of large volumes of event data, which is particularly beneficial for organizations dealing with continuous streams of user interactions. This tutorial is specifically relevant for:

- Digital Marketers: Who need to analyze user interaction to optimize ad placements and content strategy effectively.
- Data Analysts and Scientists: Who require robust tools to process and interpret user data to derive insights that drive business intelligence.
- Web Developers: Focused on improving site architecture and user interface to enhance user engagement and satisfaction.
- Product Managers: Who oversee digital platforms and are responsible for increasing user engagement and retention through data-driven methodologies.

<a href="#top" class="jump-top">Jump to Top</a>


(xref-strategy)=
## Strategy

In this tutorial, we will demonstrate how to build a dataflow using Bytewax to process streaming data from a hypothetical search engine. The dataflow will be designed to calculate the Click-Through Rate (CTR) for each search session, providing a comprehensive overview of user engagement with search results. The key steps involved in this process include:

1. Defining a data model/schema for incoming events.
2. Generating input data to simulate user interactions.
3. Implementing logic functions to calculate CTR for each search session.
4. Creating a dataflow that incorporates windowing to process the incoming event stream.
5. Executing the dataflow to generate actionable insights.

(xref-assumptions)=
## Assumptions

- Searches are per-user, so we need to divvy up events by user.
- Searches don't span user sessions, so we should calculate user sessions first.
- Sessions without a search shouldn't contribute.
- Calculate one metric: **click through rate** (or **CTR**), if a user clicked on any result in a search.

<a href="#top" class="jump-top">Jump to Top</a>

(xref-imports)=
## Imports and Setup

Before we begin, let's import the necessary modules and set up the environment for building the dataflow.

Complete installation - we recommend using a virtual environment to manage your Python dependencies. You can install Bytewax using pip:

```bash
pip install bytewax==0.19.0
```

Now, let's import the required modules and set up the environment for building the dataflow.

```python
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List

from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.window import SessionWindow, EventClockConfig
from bytewax.operators import window as wop
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
```

(xref-data-model)=
## Data Model


In this example, we will define a data model for the incoming events, generate input data to simulate user interactions, and implement logic functions to calculate the Click-Through Rate (CTR) for each search session. We will then create a dataflow to process the incoming event stream and execute it to generate actionable insights.


Let's start by defining a data model/schema for our incoming events. We'll make model classes for all the relevant events we'd want to monitor.

```python
@dataclass
class AppOpen:
    """Represents an app opening event with user ID and timestamp."""

    user: int
    time: datetime


@dataclass
class Search:
    """Represents a search event with user ID, query, and timestamp."""

    user: int
    query: str
    time: datetime


@dataclass
class Results:
    """Represents a search results event with user ID, list of items, and timestamp."""

    user: int
    items: List[str]
    time: datetime


@dataclass
class ClickResult:
    """Represents a click result event with user ID, clicked item, and timestamp."""

    user: int
    item: str
    time: datetime
```

In a production system, these might come from external schema or be auto generated.

Once the data model is defined, we can move on to generating input data to simulate user interactions. This will allow us to test our dataflow and logic functions before deploying them in a live environment. Let's create 2 users and simulate their click activity as follows:

```python
# Simulated events to emit into our Dataflow
align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
client_events = [
    Search(user=1, query="dogs", time=align_to + timedelta(seconds=5)),
    Results(
        user=1, items=["fido", "rover", "buddy"], time=align_to + timedelta(seconds=6)
    ),
    ClickResult(user=1, item="rover", time=align_to + timedelta(seconds=7)),
    Search(user=2, query="cats", time=align_to + timedelta(seconds=5)),
    Results(
        user=2,
        items=["fluffy", "burrito", "kathy"],
        time=align_to + timedelta(seconds=6),
    ),
    ClickResult(user=2, item="fluffy", time=align_to + timedelta(seconds=7)),
    ClickResult(user=2, item="kathy", time=align_to + timedelta(seconds=8)),
]
```

The client events will constitute the data input for our dataflow, simulating user interactions with the search engine. The events will include user IDs, search queries, search results, and click activity. This data will be used to calculate the Click-Through Rate (CTR) for each search session.

<a href="#top" class="jump-top">Jump to Top</a>


(xref-define-user-events)=
## Defining user events, adding events and calculating CTR

We will define three helper functions: `user_event`,  and `calculate_ctr` to process the incoming events and calculate the CTR for each search session.

1. The `user_event` function will extract the user ID from the incoming event and use it as the key for grouping the events by user.

```python
def user_event(event):
    """Extract user ID as a key and pass the event itself."""
    return str(event.user), event
```

2. The `calculate_ctr` function will calculate the Click-Through Rate (CTR) for each search session based on the click activity in the session.

```python
def calc_ctr(user__search_session):
    """Calculate the click-through rate (CTR) for each user session."""
    user, (window_metadata, search_session) = user__search_session
    searches = [event for event in search_session if isinstance(event, Search)]
    clicks = [event for event in search_session if isinstance(event, ClickResult)]
    print(f"User {user}: {len(searches)} searches, {len(clicks)} clicks")

    return (user, len(clicks) / len(searches) if searches else 0)
```

<a href="#top" class="jump-top">Jump to Top</a>

(xref-create-dataflow)=
## Creating our Dataflow

A dataflow is the unit of work in Bytewax. Dataflows are data-parallel directed acyclic graphs that are made up of processing steps.

```python
# Create the Dataflow
flow = Dataflow("search_ctr")
```

### Generating Input Data

Bytewax has a `TestingSource` class that takes an enumerable list of events that it will emit, one at a time into our dataflow. `TestingSource` will be initialized with the list of events we created earlier in the variable `client_events`.

```python
# Add input source
inp = op.input("inp", flow, TestingSource(client_events))
```

### Mapping user events

All of Bytewax's operators are in the {py:obj}`bytewax.operators` module, which we've imported here by a shorter name, `op`. We are using the {py:obj}`~bytewax.operators.map` operator - it takes each event from the input and applies the `user_event` function. This function is transforming each event into a format suitable for grouping by user (key-value pairs where the key is the user ID).

```python
# Map user events function
user_event_map = op.map("user_event", inp, user_event)
```

### The role of windowed data in analysis for CTR

We will now turn our attention to windowing the data. In a dataflow pipeline, the role of collecting windowed data, particularly after mapping user events, is crucial for segmenting the continuous stream of events into manageable, discrete chunks based on time or event characteristics. This step enables the aggregation and analysis of events within specific time frames or sessions, which is essential for understanding patterns, behaviors, and trends over time.

After user events are mapped, typically transforming each event into a tuple of (user_id, event_data), the next step is to group these events into windows. In this example, we will use a `SessionWindow` to group events by user sessions. We will also use an `EventClockConfig` to manage the timing and order of events as they are processed through the dataflow.

```python
# Collect windowed data
# Configuration for the Dataflow
event_time_config = EventClockConfig(
    dt_getter=lambda e: e.time, wait_for_system_duration=timedelta(seconds=1)
)
# Configuration for the windowing operator
clock_config = SessionWindow(gap=timedelta(seconds=10))
```

* The `EventClockConfig` is responsible for managing the timing and order of events as they are processed through the dataflow. It's crucial for ensuring that events are handled accurately in real-time or near-real-time streaming applications.

* The `SessionWindow` specifies how to group these timestamped events into sessions. A session window collects all events that occur within a specified gap of each other, allowing for dynamic window sizes based on the flow of incoming data

These configurations ensure that your dataflow can handle streaming data effectively, capturing user behavior in sessions and calculating relevant metrics like CTR in a way that is timely and reflective of actual user interactions. This setup is ideal for scenarios where user engagement metrics over time are critical, such as in digital marketing analysis, website optimization, or interactive application monitoring.

Once the events are grouped into windows, further processing can be performed on these grouped events, such as calculating metrics like CTR within each session. This step often involves applying additional functions to the windowed data to extract insights, such as counting clicks and searches to compute the CTR.

We can do this as follows:

```python
window = wop.collect_window(
    "windowed_data", user_event_map, clock=event_time_config, windower=clock_config
)
# Calculate search CTR.
calc = op.map("calc_ctr", window, calc_ctr)
```
In here, we are setting up data windowing as a step in the dataflow after the user events were created. We can then calculate the CTR using our function on the windowed data.

### Returning results

Finally, we can add an output step to our dataflow to return the results of the CTR calculation. This step will emit the CTR for each search session, providing a comprehensive overview of user engagement with search results.

```python
# Output the results
op.output("out", calc, StdOutSink())
```
<a href="#top" class="jump-top">Jump to Top</a>


(xref-execution-search-logs)=
## Execution

Now we're done with defining the dataflow. Let's run it!

```console
> python -m bytewax.run dataflow:flow
>> User 1: 1 searches, 1 clicks
>> User 2: 1 searches, 2 clicks
>>('1', 1.0)
>>('2', 2.0)
```

(xref-summary)=
## Summary

That’s it, now you have an understanding of how you can build custom session windows, how you can define data classes to be used in Bytewax, and how to calculate the click-through rate on a stream of logs.

<a href="#top" class="jump-top">Jump to Top</a>

## We want to hear from you!

If you have any trouble with the process or have ideas about how to improve this document, come talk to us in the [#troubleshooting Slack channel!](https://join.slack.com/t/bytewaxcommunity/shared)
