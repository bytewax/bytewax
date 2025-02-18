"""Redis connector for Bytewax.

This connectors offers 2 sinks and 1 source:

- `RedisKVSink`: A dynamic sink that writes key-value pairs to a Redis instance.
- `RedisStreamSink`: A dynamic sink that writes key-value dicts to the specifid Redis stream.
- `RedisStreamSource`: A fixed partitioned source that reads data from a list of Redis streams.
"""

from .sinks import RedisKVSink, RedisStreamSink
from .sources import RedisStreamSource

__all__ = ["RedisKVSink", "RedisStreamSink", "RedisStreamSource"]
