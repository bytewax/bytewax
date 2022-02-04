from dataclasses import dataclass
from typing import List

import bytewax


@dataclass
class AppOpen:
    user_id: int


@dataclass
class Search:
    user_id: int
    query: str


@dataclass
class Results:
    user_id: int
    items: List[str]


@dataclass
class ClickResult:
    user_id: int
    item: str


@dataclass
class AppClose:
    user_id: int


@dataclass
class Timeout:
    user_id: int


IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS = [
    # epoch, event
    (0, AppOpen(user_id=1)),
    (1, Search(user_id=1, query="dogs")),
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


def inp():
    return IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS


def key_on_user_id(event):
    return (event.user_id, event)


# Input data must look like (k, v).
# Takes in (key, value, state).
# Returns (new_state, output_iterable).
def sessionizer(user_session, user_id, event):
    # Events are supplied in epoch order, so this will be an ordered list.
    user_session.append(event)
    if not isinstance(event, AppClose):  # Session still going.
        # Return updated session state for next call.
        # Don't emit any events yet cuz session isn't done.
        return user_session, []
    else:  # Session: OVER.
        # Return a 2-tuple:
        # 1. Updated session state; None if we're done.
        # 2. Events to send downstream, with current / latest epoch. Send the whole session.
        return None, [user_session]


def is_search(event):
    return isinstance(event, Search)


def has_search(session):
    return any(is_search(event) for event in session)


# From a list of events in a user session, split by Search() and return a list of search sessions.
def split_into_searches(user_session):
    search_sessions = []
    search_session = []
    for event in user_session:
        if is_search(event):
            search_sessions.append(search_session)
            search_session = []
        search_session.append(event)
    search_sessions.append(search_session)
    search_sessions = filter(has_search, search_sessions)
    return search_sessions


def calc_ctr(search_session):
    if any(isinstance(event, ClickResult) for event in search_session):
        return 1.0
    else:
        return 0.0


ec = bytewax.Executor()
# inp() returns (epoch, event) stream where epoch is just event order.
flow = ec.Dataflow(inp())
# state_machine() aggregates across epochs by k in (k, v) pairs, so
# pull out user as key.
flow.map(key_on_user_id)
# Wait until we see AppClose for a user and then send all events in
# session downstream with that epoch. This means that now we only have
# one epoch per session.
flow.state_machine(list, sessionizer)
# Take a user session and split it up into a search session, one per
# search.
flow.flat_map(split_into_searches)
# Calculate search CTR per search.
flow.map(calc_ctr)
# TODO: Mix in a "clock" stream so we can get the mean CTR per minute.
flow.inspect_epoch(print)


if __name__ == "__main__":
    ec.build_and_run()
