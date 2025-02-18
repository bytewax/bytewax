import os
from threading import Thread

import redis
from pytest import mark

from bytewax import operators as op
from bytewax.bytewax_redis import RedisKVSink, RedisStreamSink, RedisStreamSource
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


@mark.integration
def test_redis_stream_roundtrip() -> None:
    # Get redis config values.
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", 6379))
    redis_db = int(os.environ.get("REDIS_DB", 0))
    redis_stream = os.environ.get("REDIS_STREAM", "test-stream")

    # First, cleanup the stream
    redis_conn = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    redis_conn.delete(redis_stream)

    # Create the dataflow that will put data into the redis stream
    flow = Dataflow("redis-stream-producer")
    stream_data: list[dict] = [
        {"field-1": 1},
        {"field-1": 2},
        {"field-2": 1},
        {"field-2": 2},
    ]
    stream_inp = op.input("test-input", flow, TestingSource(stream_data))
    sink = RedisStreamSink(redis_stream, redis_host, redis_port, redis_db)
    op.output("redis-stream-out", stream_inp, sink)
    run_main(flow)

    # Now run a different dataflow that reads from the same stream
    # and check that there's the data we just sent there.

    # Since the RedisStreamSource doesn't have a way to reach EOF,
    # we run the dataflow in a separate thread, so that we can check
    # the output while it is running
    def run_flow(out: list) -> None:
        flow = Dataflow("redis-consumer")
        source = RedisStreamSource([redis_stream], redis_host, redis_port, redis_db)
        inp = op.input("from_redis", flow, source)
        inp = op.key_rm("remove_key", inp)
        op.output("received", inp, TestingSink(out))
        run_main(flow)

    out: list[dict] = []
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


@mark.integration
def test_kv_sink() -> None:
    # Get redis config values.
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", 6379))
    redis_db = int(os.environ.get("REDIS_DB", 0))

    data = [
        ("key-1", 1),
        ("key-1", 2),
        ("key-2", 1),
        ("key-1", 3),
        ("key-2", 5),
    ]

    # Cleanup the data first
    redis_conn = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    redis_conn.delete("key-1")
    redis_conn.delete("key-2")

    # Run the dataflow that will put the key-value pairs into redis
    flow = Dataflow("redis-kv-producer")
    inp = op.input("test-input", flow, TestingSource(data))
    op.output("redis-kv-out", inp, RedisKVSink(redis_host, redis_port, redis_db))
    run_main(flow)

    # Now check that the data is there
    assert redis_conn.get("key-1") == b"3"
    assert redis_conn.get("key-2") == b"5"


# Test here to avoid exit code 5 from Pytest runs
def test_placeholder() -> None:
    pass
