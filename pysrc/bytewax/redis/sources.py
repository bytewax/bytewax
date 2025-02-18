"""Sources for Redis."""

from typing import Iterable, Optional

import redis

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition


class _RedisStreamPartition(StatefulSourcePartition):
    def __init__(
        self,
        host: str,
        port: int,
        db: int,
        stream_name: str,
        batch_size: int,
        resume_id: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.stream_name = stream_name
        self.batch_size = batch_size
        self.resume_id = resume_id or "0-0"
        self.redis_conn = redis.Redis(host=self.host, port=self.port, db=self.db)

    def next_batch(self) -> Iterable:
        """Fetch the next batch of messages from the Redis stream.

        This reads a batch of messages from the Redis stream, starting
        from the current `resume_id`. It blocks for a specified duration
        if no messages are immediately available.

        :return: An iterable of messages from the Redis stream.
        """
        messages = self.redis_conn.xread(
            streams={self.stream_name: self.resume_id},
            count=self.batch_size,
            # Never block while reading messages
            block=0,
        )
        if messages != []:
            # The structure of the object is quite nested.
            # and the type is Any, hope the format is stable enough.
            messages = messages[0][-1]
            self.resume_id = messages[-1][0]
            return messages
        else:
            return []

    def snapshot(self) -> str:
        """Snapshot the current position in the stream.

        This returns the ID of the last processed message in the Redis stream,
        which can be used to resume processing from this point in case of restart.

        :return: The ID of the last processed message as a string.
        """
        return self.resume_id

    def close(self) -> None:
        """Cleanup resources when the partition is closed.

        :return: None
        """
        self.redis_conn.close()


class RedisStreamSource(FixedPartitionedSource):
    """Read from a set of Redis Streams.

    At-least-once possible if recovery enabled.
    """

    def __init__(
        self,
        stream_names: list[str],
        host: str,
        port: int,
        db: int,
        batch_size: int = 100,
    ):
        """Initialize the RedisStreamSource.

        :param host: Redis server hostname.
        :param port: Redis server port.
        :param db: Redis database index.
        :param stream_name: Name of the Redis stream to read from.
        :param batch_size: Number of messages to read in each batch.
        """
        self.host = host
        self.port = port
        self.db = db
        assert len(stream_names) > 0, "At least one stream name must be specified"
        self.stream_names = stream_names
        self.batch_size = batch_size

    def list_parts(self) -> list[str]:
        """List all available partitions for the Redis stream source.

        :return: A list of available partitions, in this case, each stream
                 in `self.stream_names` makes a partition.
        """
        return self.stream_names

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[str]
    ) -> _RedisStreamPartition:
        """Build a partition for reading from the Redis stream.

        Constructs and returns a partition for reading data from the Redis stream.
        It accepts a `resume_state` parameter, which is the message ID to resume
        reading from.

        :param step_id: The ID of the current dataflow step.
        :param for_part: The partition being built, always "singleton".
        :param resume_state: The ID of the last processed message to resume from.
        :return: A `_RedisStreamPartition` object for reading from the Redis stream.
        """
        return _RedisStreamPartition(
            self.host, self.port, self.db, for_part, self.batch_size, resume_state
        )
