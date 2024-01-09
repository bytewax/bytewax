"""Connectors for [Kafka](https://kafka.apache.org).

Importing this module requires the
[`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python)
package to be installed.

The input source returns a stream of `KafkaMessage`.
See the docstring for its use.

You can use `KafkaSource` and `KafkaSink` directly:

>>> from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
>>> from bytewax import operators as op
>>> from bytewax.dataflow import Dataflow
>>>
>>> brokers = ["localhost:19092"]
>>> flow = Dataflow("example")
>>> kinp = op.input("kafka-in", flow, KafkaSource(brokers, ["in-topic"]))
>>> processed = op.map("map", kinp, lambda x: KafkaSinkMessage(x.key, x.value))
>>> op.output("kafka-out", processed, KafkaSink(brokers, "out-topic"))

Or the custom operators:

>>> from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage
>>> from bytewax import operators as op
>>> from bytewax.dataflow import Dataflow
>>>
>>> brokers = ["localhost:19092"]
>>> flow = Dataflow("example")
>>> kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["in-topic"])
>>> errs = op.inspect("errors", kinp.errs).then(op.raises, "crash-on-err")
>>> processed = op.map("map", kinp.oks, lambda x: KafkaSinkMessage(x.key, x.value))
>>> kop.output("kafka-out", processed, brokers=brokers, topic="out-topic")

"""
from . import operators, registry, serde
from .message import KafkaSinkMessage, KafkaSourceMessage
from .sink import KafkaSink
from .source import KafkaSource

__all__ = [
    "KafkaSource",
    "KafkaSink",
    "KafkaSinkMessage",
    "KafkaSourceMessage",
    "operators",
    "registry",
    "serde",
]
