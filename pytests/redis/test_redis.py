import os
from threading import Thread
from typing import Dict, List

import redis
from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.redis import RedisKVSink, RedisStreamSink, RedisStreamSource
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import mark

pytestmark = mark.skipif(
    "TEST_REDIS_HOST" not in os.environ,
    reason="Set `TEST_REDIS_HOST` env var to run",
)
REDIS_HOST = os.environ.get("TEST_REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("TEST_REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("TEST_REDIS_DB", "0"))
REDIS_STREAM = os.environ.get("TEST_REDIS_STREAM", "test-stream")


def test_redis_stream_roundtrip() -> None:
    # First, cleanup the stream
    redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_conn.delete(REDIS_STREAM)

    # Create the dataflow that will put data into the redis stream
    flow = Dataflow("redis-stream-producer")
    stream_data: List[Dict[str, int]] = [
        {"field-1": 1},
        {"field-1": 2},
        {"field-2": 1},
        {"field-2": 2},
    ]
    stream_inp = op.input("test-input", flow, TestingSource(stream_data))
    sink = RedisStreamSink(REDIS_STREAM, REDIS_HOST, REDIS_PORT, REDIS_DB)
    op.output("redis-stream-out", stream_inp, sink)  # type: ignore
    run_main(flow)

    # Now run a different dataflow that reads from the same stream
    # and check that there's the data we just sent there.

    # Since the RedisStreamSource doesn't have a way to reach EOF,
    # we run the dataflow in a separate thread, so that we can check
    # the output while it is running
    def run_flow(out: list) -> None:
        flow = Dataflow("redis-consumer")
        source = RedisStreamSource([REDIS_STREAM], REDIS_HOST, REDIS_PORT, REDIS_DB)
        inp = op.input("from_redis", flow, source)
        inp = op.key_rm("remove_key", inp)
        op.output("received", inp, TestingSink(out))
        run_main(flow)

    out: List[Dict[bytes, bytes]] = []
    expected = [
        {b"field-1": b"1"},
        {b"field-1": b"2"},
        {b"field-2": b"1"},
        {b"field-2": b"2"},
    ]

    thread = Thread(target=run_flow, args=(out,))
    # We make the thread a daemon so that it also stops when this function returns
    thread.daemon = True
    # Start the thread and check the `out` variable for 5 times, waiting
    # more at every check.
    # If after 5 checks the output is not correct, fail the test
    thread.start()
    for i in range(5):
        thread.join(i)
        if out == expected:
            break

    assert out == expected


def test_kv_sink() -> None:
    data = [
        ("key-1", 1),
        ("key-1", 2),
        ("key-2", 1),
        ("key-1", 3),
        ("key-2", 5),
    ]

    # Cleanup the data first
    redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_conn.delete("key-1")
    redis_conn.delete("key-2")

    # Run the dataflow that will put the key-value pairs into redis
    flow = Dataflow("redis-kv-producer")
    inp = op.input("test-input", flow, TestingSource(data))
    op.output("redis-kv-out", inp, RedisKVSink(REDIS_HOST, REDIS_PORT, REDIS_DB))
    run_main(flow)

    # Now check that the data is there
    assert redis_conn.get("key-1") == b"3"
    assert redis_conn.get("key-2") == b"5"
