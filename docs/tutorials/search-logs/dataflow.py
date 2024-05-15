"""Setup a dataflow for processing search log events using Bytewax.

This module creates and configures a Dataflow to compute the Click-Through Rate (CTR)
based on user interactions from simulated event logs. Each user's actions, such as
searches, viewing results, and clicks, are tracked and processed to calculate
metrics that could inform improvements in search functionality and user interface
design.
"""

# start-imports
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators import windowing as win
from bytewax.operators.windowing import EventClock, SessionWindower
from bytewax.testing import TestingSource

# end-imports


# start-dataclasses
@dataclass
class AppOpen:
    """Represents an app opening event.

    This class encapsulates the data for an app
    opening event,including the user ID and the
    timestamp when the app was opened.
    """

    user: int
    time: datetime


@dataclass
class Search:
    """Represents a search event.

    This class encapsulates the data for an app
    search event,including the user ID and the
    timestamp when the app was opened.
    """

    user: int
    query: str
    time: datetime


@dataclass
class Results:
    """Represents a search results event.

    This class encapsulates the data for an app
    result event, including the user ID and the
    timestamp when the app was opened.
    """

    user: int
    items: List[str]
    time: datetime


@dataclass
class ClickResult:
    """Represents a click result event.

    This class encapsulates the data for an app
    click event,including the user ID and the
    timestamp when the app was opened.
    """

    user: int
    item: str
    time: datetime


# end-dataclasses

# start-simulated-events
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
# end-simulated-events


# start-user-event
def user_event(event):
    """Extract user ID as a key and pass the event itself."""
    return str(event.user), event


# end-user-event


# start-calc-ctr
def calc_ctr(window_out):
    """Calculate the click-through rate (CTR)."""
    _, search_session = window_out
    user, events = search_session

    searches = [event for event in events if isinstance(event, Search)]
    clicks = [event for event in events if isinstance(event, ClickResult)]

    print(f"User {user}: {len(searches)} searches, {len(clicks)} clicks")

    return (user, len(clicks) / len(searches) if searches else 0)


# end-calc-ctr

# start-dataflow
# Create and configure the Dataflow
flow = Dataflow("search_ctr")

# Feed input data
inp = op.input("inp", flow, TestingSource(client_events))

# Map the user event function to the input
user_event_map = op.map("user_event", inp, user_event)

# Configure the event clock and session windower
event_time_config: EventClock = EventClock(
    ts_getter=lambda e: e.time, wait_for_system_duration=timedelta(seconds=1)
)
clock_config = SessionWindower(gap=timedelta(seconds=10))

# Collect the windowed data
window = win.collect_window(
    "windowed_data", user_event_map, clock=event_time_config, windower=clock_config
)

# Calculate the click-through rate using the
# calc_ctr function and the windowed data
calc = op.map("calc_ctr", window.down, calc_ctr)

# Output the results to the standard output
op.output("out", calc, StdOutSink())
# end-dataflow
