from datetime import datetime, timedelta, timezone
from typing import List

import bytewax.operators as op
from bytewax.operators import TTLCache
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main, TimeTestingGetter


def test_cache_get_init() -> None:
    source = TimeTestingGetter(datetime(2024, 5, 10, 10, 0, 0, tzinfo=timezone.utc))
    lookup = {
        "a": 1,
        "b": 2,
    }

    cache = TTLCache(lookup.get, source.get, ttl=timedelta(minutes=1))

    assert cache.get("a") == 1


def test_cache_get_cached() -> None:
    source = TimeTestingGetter(datetime(2024, 5, 10, 10, 0, 0, tzinfo=timezone.utc))
    lookup = {
        "a": 1,
        "b": 2,
    }

    cache = TTLCache(lookup.pop, source.get, ttl=timedelta(minutes=1))

    assert cache.get("a") == 1
    assert cache.get("a") == 1


def test_cache_get_expire() -> None:
    source = TimeTestingGetter(datetime(2024, 5, 10, 10, 0, 0, tzinfo=timezone.utc))
    lookup = {
        "a": 1,
        "b": 2,
    }

    cache = TTLCache(lookup.get, source.get, ttl=timedelta(minutes=1))

    assert cache.get("a") == 1

    source.advance(timedelta(minutes=2))
    lookup["a"] = 3

    assert cache.get("a") == 3


def test_enrich_cached() -> None:
    inp = ["a", "b", "a"]
    out: List[int] = []

    lookup = {
        "a": 1,
        "b": 2,
    }

    def mapper(item: str, cache: TTLCache) -> int:
        return cache.get(item)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.enrich_cached("enrich", s, lookup.get, mapper)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 1]
