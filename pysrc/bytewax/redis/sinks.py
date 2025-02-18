"""Sinks for Redis."""

from typing import Any

import redis
from redis.typing import EncodableT, FieldT

from bytewax.outputs import DynamicSink, StatelessSinkPartition


class _RedisStreamSinkPartition(StatelessSinkPartition[dict[FieldT, EncodableT]]):
    def __init__(self, stream_name: str, host: str, port: int, db: int):
        """Initialize a Redis stream sink partition.

        :param stream_name: The name of the Redis stream to write to.
        :param host: Redis server hostname.
        :param port: Redis server port.
        :param db: Redis database index.
        """
        self.stream_name = stream_name
        self.redis_conn = redis.StrictRedis(host=host, port=port, db=db)

    def write_batch(self, items: list[dict[FieldT, EncodableT]]) -> None:
        """Write a batch of items to the Redis stream.

        This method uses the Redis `XADD` command to add items to the Redis stream.
        Each item in the batch is a dictionary that is written as a message
        to the stream.

        :param items: A list of dictionaries, where each dictionary contains key-value
                      pairs to be written as fields to the Redis stream.
        """
        for item in items:
            self.redis_conn.xadd(self.stream_name, item)

    def close(self) -> None:
        """Close the Redis connection (optional).

        This closes the Redis connection when the partition is finished.
        This is optional and may not always be called.
        """
        self.redis_conn.close()


class RedisStreamSink(DynamicSink[dict[FieldT, EncodableT]]):
    """Redis stream sink.

    This sink takes a stream of dictionaries containing key-value pairs
    that will be sent to a redis stream defined by the `stream_name` argument.
    """

    def __init__(self, stream_name: str, host: str, port: int, db: int):
        """Initialize the Redis stream sink.

        :param stream_name: The name of the Redis stream to write to.
        :param host: Redis server hostname.
        :param port: Redis server port.
        :param db: Redis database index.
        """
        self.stream_name = stream_name
        self.redis_host = host
        self.redis_port = port
        self.redis_db = db

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _RedisStreamSinkPartition:
        """Build a stateless sink partition for each worker.

        This method creates a `_RedisStreamSinkPartition` for each worker in the
        dataflow.
        Each worker will independently write messages to the Redis stream.

        :param step_id: The ID of the current dataflow step.
        :param worker_index: The index of the current worker.
        :param worker_count: The total number of workers in the dataflow.
        :return: A `_RedisStreamSinkPartition` object for writing data to the
                 Redis stream.
        """
        return _RedisStreamSinkPartition(
            stream_name=self.stream_name,
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
        )


class _RedisKVSinkPartition(StatelessSinkPartition[tuple[Any, Any]]):
    def __init__(
        self, redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0
    ):
        # Establish connection to Redis
        self.redis_conn = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    def write_batch(self, items: list[tuple[Any, Any]]) -> None:
        """Write a batch of key-value pairs to Redis using pipelines.

        This writes a batch of key-value pairs to Redis.
        It uses Redis pipelines to send multiple commands in
        a single round-trip for efficiency.

        :param items: A list of tuples where each tuple contains a key
                      and its corresponding value.
        """
        pipe = self.redis_conn.pipeline()
        for key, value in items:
            pipe.set(key, value)
        pipe.execute()

    def close(self) -> None:
        """Close the Redis connection."""
        self.redis_conn.close()


class RedisKVSink(DynamicSink[tuple[Any, Any]]):
    """Redis key-value sink.

    This sink take a stream of (key, value) 2-tuples, and writes every key
    to the specified Redis instance.
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        """Initialize the Redis key-value dynamic sink.

        :param host: Redis server hostname.
        :param port: Redis server port.
        :param db: Redis database index.
        """
        # Store Redis connection info for workers
        self.redis_host = host
        self.redis_port = port
        self.redis_db = db

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _RedisKVSinkPartition:
        """Build a stateless sink partition for each worker.

        This creates a `_RedisKVSinkPartition` for each worker in the dataflow.
        Each worker will independently write key-value pairs to Redis.

        :param step_id: The ID of the current dataflow step.
        :param worker_index: The index of the current worker.
        :param worker_count: The total number of workers in the dataflow.
        :return: A `_RedisKVSinkPartition` object for writing key-value pairs to Redis.
        """
        return _RedisKVSinkPartition(self.redis_host, self.redis_port, self.redis_db)
