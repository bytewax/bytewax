"""Example usage of redis connector's source and sinks.

There are 3 separate dataflows:
- `stream_producer_flow` writes some data to a Redis stream.
- `consumer_flow` reads data from the same Redis stream the previous dataflow wrote to.
- `kv_producer_flow` writes some keys to a Redis instance.

Run the three dataflows separately:
```
$ python -m bytewax.run examples.redis_connector:stream_producer_flow
$ python -m bytewax.run examples.redis_connector:consumer_flow
$ python -m bytewax.run examples.redis_connector:kv_producer_flow
```

"""

import os

from bytewax import operators as op
from bytewax.bytewax_redis import RedisKVSink, RedisStreamSink, RedisStreamSource
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_STREAM_NAME = os.environ.get("REDIS_STREAM_NAME", "example-stream")

# This dataflow writes to a stream in redis.
stream_producer_flow = Dataflow("redis-stream-producer")
stream_data: list[dict] = [
    {"field-1": 1},
    {"field-1": 2},
    {"field-1": 3},
    {"field-1": 4},
    {"field-2": 1},
    {"field-2": 2},
]
stream_inp = op.input("test-input", stream_producer_flow, TestingSource(stream_data))

op.inspect("redis-stream-writing", stream_inp)
op.output(
    "redis-stream-out",
    stream_inp,
    RedisStreamSink(REDIS_STREAM_NAME, REDIS_HOST, REDIS_PORT, REDIS_DB),
)


# This dataflow writes key/value pairs to redis.
kv_producer_flow = Dataflow("redis-kv-producer")
kv_data = [
    ("key-1", 1),
    ("key-1", 2),
    ("key-2", 1),
    ("key-1", 3),
    ("key-2", 5),
]
kv_inp = op.input("test-input", kv_producer_flow, TestingSource(kv_data))

op.inspect("redis-kv-writing", kv_inp)
op.output("redis-kv-out", kv_inp, RedisKVSink(REDIS_HOST, REDIS_PORT, REDIS_DB))

# The third dataflow consumes data from the stream and prints it.
consumer_flow = Dataflow("redis-consumer")
consumer_inp = op.input(
    "from_redis",
    consumer_flow,
    RedisStreamSource([REDIS_STREAM_NAME], REDIS_HOST, REDIS_PORT, REDIS_DB),
)
op.inspect("received", consumer_inp)
