import operator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators.window import EventClockConfig, SessionWindow
from bytewax.testing import TestingSource


@dataclass
class Event:
    dt: datetime


@dataclass
class AppOpen(Event):
    user: int


@dataclass
class Search(Event):
    user: int
    query: str


@dataclass
class Results(Event):
    user: int
    items: List[str]


@dataclass
class ClickResult(Event):
    user: int
    item: str


@dataclass
class AppClose(Event):
    user: int


@dataclass
class Timeout(Event):
    user: int


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
stream = flow.input("inp", TestingSource(IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS))
# event
stream = stream.map("initial_session", lambda e: [e])
stream = stream.key_on("add_key", lambda e: str(e[0].user))
# (user, [event])
clock = EventClockConfig(lambda x: x[-1].dt, timedelta(seconds=10))
window = SessionWindow(gap=timedelta(seconds=5))
stream = stream.reduce_window("sessionizer", clock, window, operator.add)
# (user, [event, ...])
stream = stream.map("remove_key", remove_key)
# [event, ...]
# Take a user session and split it up into a search session, one per
# search.
stream = stream.flat_map("split_into_searches", lambda x: list(split_into_searches(x)))
stream = stream.filter("filter_search", has_search)
# Calculate search CTR per search.
stream = stream.map("calc_ctr", calc_ctr)
stream.output("out", StdOutSink())
