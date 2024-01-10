Bytewax ships with connectors for [Kafka](https://www.confluent.io/) and
Kafka compatible systems, like [Redpanda](https://redpanda.com/).

In this section, we'll discuss these connectors in detail, as well as
provide some important operational information about using them as sources
and sinks.

## Connecting to Kafka

Bytewax provides two basic ways to connect to Kafka.

You can use [`KafkaSource`](/apidocs/bytewax.connectors/kafka/index#bytewax.connectors.kafka.KafkaSource)
and [`KafkaSink`](/apidocs/bytewax.connectors/kafka/index#bytewax.connectors.kafka.KafkaSink) directly:

```python
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax import operators as op
from bytewax.dataflow import Dataflow

brokers = ["localhost:19092"]
flow = Dataflow("example")
kinp = op.input("kafka-in", flow, KafkaSource(brokers, ["in-topic"]))
processed = op.map("map", kinp, lambda x: KafkaSinkMessage(x.key, x.value))
op.output("kafka-out", processed, KafkaSink(brokers, "out-topic"))
```

Or use the Kafka input [operator](/apidocs/bytewax.connectors/kafka/operators#bytewax.connectors.kafka.operators.input):

```python
from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage
from bytewax import operators as op
from bytewax.dataflow import Dataflow

brokers = ["localhost:19092"]
flow = Dataflow("example")
kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["in-topic"])
processed = op.map("map", kinp.oks, lambda x: KafkaSinkMessage(x.key, x.value))
kop.output("kafka-out", processed, brokers=brokers, topic="out-topic")
```

Typical use cases should prefer the Kafka input operator, while the `KafkaSource` connector
can be used for other customizations.

## Error handling

The [`KafkaSource`](/apidocs/bytewax.connectors/kafka/index#bytewax.connectors.kafka.KafkaSource) input
source will raise an exception whenever an error is encountered when consuming from Kafka.

The Kafka input [operator](/apidocs/bytewax.connectors/kafka/operators#bytewax.connectors.kafka.operators.input)
returns a dataclass containing two output streams. The `.oks` field is a stream of
[`KafkaMessage`](/apidocs/bytewax.connectors/kafka/message#bytewax.connectors.kafka.message.KafkaSourceMessage)
that were successfully processed. The `.errs` field is a stream of [`KafkaError`](/apidocs/bytewax.connectors/kafka/error#bytewax.connectors.kafka.message.KafkaError)
messages where an error was encountered. Items that encountered an error have their `.err` field set with more
details about the error.

```python
flow = Dataflow("example")
kinp = kop.input("kafka-in-2", flow, brokers=brokers, topics=["in-topic"])
# Print out errors that are encountered, and then raise an exception
op.inspect("inspect_err", kinp.errs).then(op.raises, "raise_errors")
```

Note that if no processing is attached to the `.errs` stream of messages, they will be silently
dropped, and processing will continue.

Alternatively, error messages in the `.errs` stream can be published to a "dead letter queue" where they
can be inspected and reprocessed later, while allowing the dataflow to continue processing data.

## Batch sizes

By default, Bytewax will consume a batch of up to 1000 messages at a time from Kafka. The default
setting is often sufficient, but some dataflows may benefit from a smaller batch size to decrease
overall latency.

If your dataflow would benefit from lower latency, you can set the `batch_size` parameter
to a lower value. The `batch_size` parameter configures the maximum number of messages
that will be fetched at a time from each worker.

## Message Types

Messages received from `KafkaSource` are of type [`KafkaSourceMessage`](
/apidocs/bytewax.connectors/kafka/message#bytewax.connectors.kafka.message.KafkaSourceMessage).
This dataclass includes basic Kafka fields like `.key` and `.value`, as well as
extra information fields like `.headers`.

Messages that are published to a `KafkaSink` must be of type
[`KafkaSinkMessage`](/apidocs/bytewax.connectors/kafka/message#bytewax.connectors.kafka.message.KafkaSinkMessage).

You can create a `KafkaSinkMessage` with the data you want:
```python
msg = KafkaSinkMessage(key=None, value="some_value")
```
And you can optionally set `topic`, `headers`, `partition` and `timestamp`.

The `output` operator also accepts messages of type [`KafkaSourceMessage`](/apidocs/bytewax.connectors/kafka/message#bytewax.connectors.kafka.message.KafkaSourceMessage).
 They are automatically converted to `KafkaSinkMessage` keeping only `.key` and `.value` from `KafkaSourceMessage`.

## Dynamic topic writes

Setting the `topic` field of a `KafkaSinkMessage` will cause that message to be written to
that topic.

Additionally, the `KafkaSink` class can be constructed without specifying a topic:

```python
op.output("kafka-dynamic-out", processed, KafkaSink(brokers, topic=None))
```

Writes to this output will be written to the topic that is specified when creating a `KafkaSinkMessage`.

```python
KafkaSinkMessage(msg.key, msg.value, topic="out-topic-1")
```

Note that not setting a topic for a `KafkaSinkMessage` when `KafkaSink` is not configured with
a default topic will result in a runtime error.

## Kafka and Recovery

Typical deployments of Kafka utilize [consumer groups](
https://developer.confluent.io/learn/apache-kafka-on-the-go/consumer-groups/)
in order to manage partition assignment and the storing of Kafka offsets.

Bytewax does **not** use consumer groups to store offsets or asssign Kafka topic partitions to consumers.
In order to correctly support [recovery](/docs/concepts/recovery), Bytewax must manage and store the
consumer offsets in Bytewax recovery partitions.

When recovery is not enabled, Bytewax will start consuming from each partition using the earliest
available offset. This setting can be changed when creating a new `KafkaSource` with the
`starting_offset` parameter, which accepts the types defined in the
[`confluent_kafka`](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#offset)
library.

If you are not using recovery and would prefer to track offsets on the broker side, you can
pass additional options to the Kafka input sources to create a consumer group:

```python
from confluent_kafka import OFFSET_STORED

add_config = {"group.id": "consumer_group", "enable.auto.commit": "true"}
BROKERS = ["localhost:19092"]
IN_TOPICS = ["in_topic"]

flow = Dataflow("kafka_in_out")
kinp = kop.input(
    "inp",
    flow,
    starting_offset=OFFSET_STORED,
    add_config=add_config,
    brokers=BROKERS,
    topics=IN_TOPICS,
)
```

The code above creates a Kafka consumer that uses a `group_id` of `consumer_group` that periodically
commits it's consumed offsets according to the `auto.commit.interval.ms` parameter. By default,
this interval is set to 5000ms.

It is important to note that this interval is not coordinated with any processing steps within Bytewax.
As a result, some messages may not be processed if the dataflow crashes.

## Partitions

Partitions are a fundamental concept for Kafka and Redpanda, and are the unit of parallelism for
producing and consuming messages.

When multiple workers are started, Bytewax will assign individual Kafka topic partitions
to the available number of workers. If there are fewer partitions than workers, some workers will not
be assigned a partition to read from. If there are more partitions than workers, some workers
will handle more than one partition.

If the number of partitions changes, Dataflows will need to be restarted in order to rebalance
new partition assignments to workers.

## Schema registry

Bytewax supports integrating with the [Redpanda Schema Registry](https://docs.redpanda.com/current/manage/schema-reg/)
as well as the [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

The following is an example of integrating with the Redpanda Schema Registry:

```python doctest:SKIP
import bytewax.operators as op

from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSinkMessage, KafkaSourceMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.kafka.registry import RedpandaSchemaRegistry, SchemaRef

BROKERS = ["localhost:19092"]
IN_TOPICS = ["in_topic"]
REDPANDA_REGISTRY_URL = "http://localhost:8080/schema-registry"

registry = RedpandaSchemaRegistry(REDPANDA_REGISTRY_URL)

flow = Dataflow("schema_registry")
kinp = kop.input("kafka-in", flow, brokers=BROKERS, topics=IN_TOPICS)
op.inspect("inspect-kafka-errors", kinp.errs).then(op.raises, "kafka-error")
key_de = registry.deserializer(SchemaRef("sensor-key"))
val_de = registry.deserializer(SchemaRef("sensor-value"))
msgs = kop.deserialize("de", kinp.oks, key_deserializer=key_de, val_deserializer=val_de)
op.inspect("inspect-deser", msgs.errs).then(op.raises, "deser-error")
```

The [`deserialize`](/apidocs/bytewax.connectors/kafka/operators#bytewax.connectors.kafka.operators.deserialize)
operator accepts a `SchemaDeserializer` for both the key and value of the Kafka message.

If the deserialization step encounters an error, a separate stream of `.errs` is returned that
can be used for error handling.

When integrating with the Confluent schema registry, new schema versions will attempt to be fetched
when a message with a new schema id is encountered. When using the Redpanda schema registry, dataflows
will need to be restarted in order to fetch new versions of a schema.

## Implementing custom ser/de classes

Bytewax includes support for creating your own schema registry implementation, or
custom (de)serializers.

As a trivial example, we can implement a class that uses the [orjson](https://github.com/ijl/orjson)
library to deserialize a JSON payload from bytes.

```python doctest:SKIP
import orjson

from typing import Dict, Any

from bytewax import operators as op
from bytewax.connectors.kafka.serde import SchemaDeserializer

from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage, KafkaSink
from bytewax.dataflow import Dataflow

BROKERS = ["localhost:19092"]
IN_TOPICS = ["in_topic"]


class KeyDeserializer(SchemaDeserializer[bytes, str]):
    def de(self, obj: bytes) -> str:
        return str(obj)


class JSONDeserializer(SchemaDeserializer[bytes, Dict]):
    def de(self, obj: bytes) -> Dict[Any, Any]:
        return orjson.loads(obj)


brokers = ["localhost:19092"]
val_de = JSONDeserializer()
key_de = KeyDeserializer()

flow = Dataflow("example")
kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["in-topic"])
json_stream = kop.deserialize(
    "load_json", kinp.oks, key_deserializer=key_de, val_deserializer=val_de
)
op.inspect("inspect", json_stream.oks)
```
