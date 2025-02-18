import operator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators import windowing as win
from bytewax.windowing import EventClock, SessionWindower
from bytewax.testing import TestingSource


@dataclass
class Event:
    user: int
    dt: datetime


@dataclass
class AppOpen(Event): ...


@dataclass
class Search(Event):
    query: str


@dataclass
class Results(Event):
    items: List[str]


@dataclass
class ClickResult(Event):
    item: str


@dataclass
class AppClose(Event): ...


@dataclass
class Timeout(Event): ...


start = datetime(2023, 1, 1, tzinfo=timezone.utc)


def after(seconds):
    return start + timedelta(seconds=seconds)


IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS = [
    AppOpen(user=1, dt=start),
    Search(user=1, query="dogs", dt=after(1)),
    Results(user=1, items=["fido", "rover", "buddy"], dt=after(2)),
    ClickResult(user=1, item="rover", dt=after(3)),
    Search(user=1, query="cats", dt=after(4)),
    Results(user=1, items=["fluffy", "burrito", "kathy"], dt=after(5)),
    ClickResult(user=1, item="fluffy", dt=after(6)),
    AppOpen(user=2, dt=after(7)),
    ClickResult(user=1, item="kathy", dt=after(8)),
    Search(user=2, query="fruit", dt=after(9)),
    AppClose(user=1, dt=after(10)),
    AppClose(user=2, dt=after(11)),
]


def is_search(event):
    return type(event).__name__ == "Search"


def remove_key(user_event):
    user, event = user_event
    return event


def has_search(session):
    return any(is_search(event) for event in session)


# From a list of events in a user session, split by Search() and
# return a list of search sessions.
def split_into_searches(wm__user_session):
    user_session = wm__user_session[1]
    search_session = []
    for event in user_session:
        if is_search(event):
            yield search_session
            search_session = []
        search_session.append(event)
    yield search_session


def calc_ctr(search_session):
    if any(type(event).__name__ == "ClickResult" for event in search_session):
        return 1.0
    else:
        return 0.0


flow = Dataflow("search session")
stream = op.input("inp", flow, TestingSource(IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS))
# event
initial_session_stream = op.map("initial_session", stream, lambda e: [e])
keyed_stream = op.key_on("add_key", initial_session_stream, lambda e: str(e[0].user))
# (user, [event])
window = SessionWindower(gap=timedelta(seconds=5))
session_stream = win.reduce_window(
    "sessionizer",
    keyed_stream,
    EventClock(lambda x: x[-1].dt, timedelta(seconds=10)),
    window,
    operator.add,
)
# (user, [event, ...])
event_stream = op.map("remove_key", session_stream.down, remove_key)
# [event, ...]
# Take a user session and split it up into a search session, one per
# search.
event_stream = op.flat_map(
    "split_into_searches", event_stream, lambda x: list(split_into_searches(x))
)
event_stream = op.filter("filter_search", event_stream, has_search)
# Calculate search CTR per search.
ctr_stream = op.map("calc_ctr", event_stream, calc_ctr)
op.output("out", ctr_stream, StdOutSink())
