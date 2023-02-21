import operator
from dataclasses import dataclass
from typing import List

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.outputs import StdOutputConfig
from bytewax.testing import TestingInput


@dataclass
class AppOpen:
    user: int


@dataclass
class Search:
    user: int
    query: str


@dataclass
class Results:
    user: int
    items: List[str]


@dataclass
class ClickResult:
    user: int
    item: str


@dataclass
class AppClose:
    user: int


@dataclass
class Timeout:
    user: int


IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS = [
    AppOpen(user=1),
    Search(user=1, query="dogs"),
    # Eliding named args...
    Results(1, ["fido", "rover", "buddy"]),
    ClickResult(1, "rover"),
    Search(1, "cats"),
    Results(1, ["fluffy", "burrito", "kathy"]),
    ClickResult(1, "fluffy"),
    AppOpen(2),
    ClickResult(1, "kathy"),
    Search(2, "fruit"),
    AppClose(1),
    AppClose(2),
]


def initial_session(event):
    return str(event.user), [event]


def session_has_closed(session):
    # isinstance does not work on objects sent through pickling, which
    # Bytewax does when there are multiple workers.
    return type(session[-1]).__name__ == "AppClose"


def is_search(event):
    return type(event).__name__ == "Search"


def remove_key(user_event):
    user, event = user_event
    return event


def has_search(session):
    return any(is_search(event) for event in session)


# From a list of events in a user session, split by Search() and
# return a list of search sessions.
def split_into_searches(user_session):
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


flow = Dataflow()
flow.input("inp", TestingInput(IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS))
# event
flow.map(initial_session)
# (user, [event])
# TODO: reduce_window with clock so we can get the mean CTR per minute.
flow.reduce("sessionizer", operator.add, session_has_closed)
# (user, [event, ...])
flow.map(remove_key)
# [event, ...]
# Take a user session and split it up into a search session, one per
# search.
flow.flat_map(split_into_searches)
flow.filter(has_search)
# Calculate search CTR per search.
flow.map(calc_ctr)
flow.capture(StdOutputConfig())


if __name__ == "__main__":
    run_main(flow)
