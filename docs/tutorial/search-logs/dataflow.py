"""Setup a dataflow for processing search log events using Bytewax.

This module creates and configures a Dataflow to compute the Click-Through Rate (CTR)
based on user interactions from simulated event logs. Each user's actions, such as
searches, viewing results, and clicks, are tracked and processed to calculate
metrics that could inform improvements in search functionality and user interface
design.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators import window as wop
from bytewax.operators.window import EventClock, SessionWindower
from bytewax.testing import TestingSource


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


def user_event(event):
    """Extract user ID as a key and pass the event itself."""
    return str(event.user), event


def calc_ctr(user__search_session):
    """Calculate the click-through rate (CTR) for each user session."""
    user, (window_metadata, search_session) = user__search_session
    searches = [event for event in search_session if isinstance(event, Search)]
    clicks = [event for event in search_session if isinstance(event, ClickResult)]
    print(f"User {user}: {len(searches)} searches, {len(clicks)} clicks")

    return (user, len(clicks) / len(searches) if searches else 0)


# Create and configure the Dataflow
flow = Dataflow("search_ctr")
inp = op.input("inp", flow, TestingSource(client_events))
user_event_map = op.map("user_event", inp, user_event)
event_time_config = EventClockConfig(
    dt_getter=lambda e: e.time, wait_for_system_duration=timedelta(seconds=1)
)
clock_config = SessionWindow(gap=timedelta(seconds=10))
window = wop.collect_window(
    "windowed_data", user_event_map, clock=event_time_config, windower=clock_config
)
calc = op.map("calc_ctr", window, calc_ctr)
op.output("out", calc, StdOutSink())
