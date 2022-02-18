import operator
from dataclasses import dataclass
from typing import List

from bytewax import Dataflow, parse, run_cluster


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
    # epoch, event
    (0, AppOpen(user=1)),
    (1, Search(user=1, query="dogs")),
    # Eliding named args...
    (2, Results(1, ["fido", "rover", "buddy"])),
    (3, ClickResult(1, "rover")),
    (4, Search(1, "cats")),
    (5, Results(1, ["fluffy", "burrito", "kathy"])),
    (6, ClickResult(1, "fluffy")),
    (7, AppOpen(2)),
    (8, ClickResult(1, "kathy")),
    (9, Search(2, "fruit")),
    (10, AppClose(1)),
    (11, AppClose(2)),
]


def group_by_user(event):
    return event.user, [event]


def session_has_closed(session):
    # isinstance does not work on objects sent through pickling, which
    # Bytewax does when there are multiple workers.
    return any(type(event).__name__ == "AppClose" for event in session)


def is_search(event):
    return type(event).__name__ == "Search"


def remove_key(user_event):
    user, event = user_event
    return event


def has_search(session):
    return any(is_search(event) for event in session)


# From a list of events in a user session, split by Search() and return a list of search sessions.
def split_into_searches(user_session):
    search_session = []
    for event in user_session:
        if is_search(event):
            if len(search_session) > 0:
                yield search_session
            search_session = []
        search_session.append(event)


def calc_ctr(search_session):
    if any(type(event).__name__ == "ClickResult" for event in search_session):
        return 1.0
    else:
        return 0.0


flow = Dataflow()
# event
flow.map(group_by_user)
# (user, [event])
flow.reduce(operator.add, session_has_closed)
# (user, [event, ...])
flow.map(remove_key)
# [event, ...]
flow.filter(has_search)
# Take a user session and split it up into a search session, one per
# search.
flow.flat_map(split_into_searches)
# Calculate search CTR per search.
flow.map(calc_ctr)
# TODO: Mix in a "clock" stream so we can get the mean CTR per minute.
flow.inspect_epoch(print)


if __name__ == "__main__":
    run_cluster(flow, IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS, **parse.cluster_args())
