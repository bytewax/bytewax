# Kafka

Bytewax ships with connectors for [Kafka](https://www.confluent.io/)
and Kafka compatible systems, like [Redpanda](https://redpanda.com/)
in the {py:obj}`bytewax.connectors.kafka` module.

In this section, we'll discuss these connectors in detail, as well as
provide some important operational information about using them as
sources and sinks.

## Connecting to Kafka

Bytewax provides two basic ways to connect to Kafka.

You can use {py:obj}`~bytewax.connectors.kafka.KafkaSource` and
{py:obj}`~bytewax.connectors.kafka.KafkaSink` directly:

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

Or use the {py:obj}`bytewax.connectors.kafka.operators.input`
operator:

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

Typical use cases should prefer the Kafka operators, while the
{py:obj}`~bytewax.connectors.kafka.KafkaSource` connector can
be used for other customizations.

## Error handling

By default, {py:obj}`~bytewax.connectors.kafka.KafkaSource` will raise
an exception whenever an error is encountered when consuming from
Kafka. This behavior can be configured with the `raise_on_errors`
parameter, which will yield
{py:obj}`~bytewax.connectors.kafka.KafkaError` items. Those
errors can be handled downstream individually.

The {py:obj}`bytewax.connectors.kafka.operators.input` operator
returns a dataclass containing two output streams. The `.oks` field is
a stream of
{py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage` that
were successfully processed. The `.errs` field is a stream of
{py:obj}`~bytewax.connectors.kafka.KafkaError` messages where an
error was encountered. Items that encountered an error have their
`.err` field set with more details about the error.

```python
flow = Dataflow("example")
kinp = kop.input("kafka-in-2", flow, brokers=brokers, topics=["in-topic"])
# Print out errors that are encountered, and then raise an exception
op.inspect("inspect_err", kinp.errs).then(op.raises, "raise_errors")
```

Note that if no processing is attached to the `.errs` stream of
messages, they will be silently dropped, and processing will continue.

Alternatively, error messages in the `.errs` stream can be published
to a "dead letter queue", a separate Kafka topic where they can be
inspected and reprocessed later, while allowing the dataflow to
continue processing data.

## Batch sizes

By default, Bytewax will consume a batch of up to 1000 messages at a
time from Kafka. The default setting is often sufficient, but some
dataflows may benefit from a smaller batch size to decrease overall
latency.

If your dataflow would benefit from lower latency, you can set the
`batch_size` parameter to a lower value. The `batch_size` parameter
configures the maximum number of messages that will be fetched at a
time from each worker.

## Message Types

Messages received from
{py:obj}`~bytewax.connectors.kafka.KafkaSource` are emitted
into the dataflow as items of type
{py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage`. This
dataclass includes basic Kafka fields like `.key` and `.value`, as
well as extra information fields like `.headers`.

Messages that are published to a
{py:obj}`~bytewax.connectors.kafka.KafkaSink` must be of type
{py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage`

You can create a
{py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage` with the
data you want:

```python
msg = KafkaSinkMessage(key=None, value="some_value")
```
And you can optionally set `topic`, `headers`, `partition` and `timestamp`.

The `output` operator also accepts messages of type
 {py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage`. They
 are automatically converted to
 {py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage` keeping
 only `.key` and `.value` from
 {py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage`.

## Dynamic topic writes

Setting the `topic` field of a
{py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage` will
cause that message to be written to that topic.

Additionally, the {py:obj}`~bytewax.connectors.kafka.KafkaSink`
class can be constructed without specifying a topic:

```python
op.output("kafka-dynamic-out", processed, KafkaSink(brokers, topic=None))
```

Writes to this output will be written to the topic that is specified
when creating a
{py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage`.

```python
KafkaSinkMessage(msg.key, msg.value, topic="out-topic-1")
```

Note that not setting a topic for a
{py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage` when
`KafkaSink` is not configured with a default topic will result in a
runtime error.

## Kafka and Recovery

Typical deployments of Kafka utilize [consumer groups](
https://developer.confluent.io/learn/apache-kafka-on-the-go/consumer-groups/)
in order to manage partition assignment and the storing of Kafka
offsets.

Bytewax does **not** use consumer groups to store offsets or asssign
Kafka topic partitions to consumers. In order to correctly support
[recovery](/articles/concepts/recovery), Bytewax must manage and store
the consumer offsets in Bytewax recovery partitions.

When recovery is not enabled, Bytewax will start consuming from each
partition using the earliest available offset. This setting can be
changed when creating a new `KafkaSource` with the `starting_offset`
parameter, which accepts the types defined in the
[`confluent_kafka`](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#offset)
library.

If you are not using recovery and would prefer to track offsets on the
broker side, you can pass additional options to the Kafka input
sources to create a consumer group:

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

The code above creates a Kafka consumer that uses a `group_id` of
`consumer_group` that periodically commits it's consumed offsets
according to the `auto.commit.interval.ms` parameter. By default, this
interval is set to 5000ms.

It is important to note that this interval is not coordinated with any
processing steps within Bytewax. As a result, some messages may not be
processed if the dataflow crashes.

## Partitions

Partitions are a fundamental concept for Kafka and Redpanda, and are
the unit of parallelism for producing and consuming messages.

When multiple workers are started, Bytewax will assign individual
Kafka topic partitions to the available number of workers. If there
are fewer partitions than workers, some workers will not be assigned a
partition to read from. If there are more partitions than workers,
some workers will handle more than one partition.

If the number of partitions changes, Dataflows will need to be
restarted in order to rebalance new partition assignments to workers.

## Serialization and deserialization
Bytewax supports (de)serialization of messages using serializers that conforms to
`confluent_kafka.serialization.{Deserializer, Serializer}` interface.

The `kafka` connector offers some custom operators to help with that.
If you are working with confluent's python libraries, you can use confluent's
schema registry client and (de)serializers directly:

```python doctest:SKIP
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import operators as kop
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

client = SchemaRegistryClient(...)
# We don't need to specify the schema, as the client handles that on its own
# if you are serializing messages with confluent's library, that uses
# a custom wire format that includes schema_id in each message.
key_de = AvroDeserializer(client)
val_de = AvroDeserializer(client)

# Initialize the flow, read from kafka, and deserialize messages
flow = Dataflow("schema_registry")
kinp = kop.input("kafka-in", flow, ...)
msgs = kop.deserialize("de", kinp.oks, key_deserializer=key_de, val_deserializer=val_de)
```

This works with `Redpanda`'s schema registry too.

If you are serializing messages with other libraries that do not use confluent's wire format,
you'll need to use a different deserializer. The connector offers (de)serializers for plain avro
format:

```python doctest:SKIP
client = SchemaRegistryClient(...)

# Here we do need to specify the schema we want to use, as the schema_id
# is not included in plain avro messages. We can use the client to retrieve
# the schema and pass it to the deserializers:
key_schema = client.get_latest_version("sensor_key").schema
key_de = PlainAvroDeserializer(schema=key_schema.schema_str)
val_schema = client.get_latest_version("sensor_value").schema
val_de = PlainAvroDeserializer(schema=val_schema.schema_str)

# Same as before...
flow = Dataflow("schema_registry")
kinp = kop.input("kafka-in", flow, ...)
msgs = kop.deserialize("de", kinp.oks, key_deserializer=key_de, val_deserializer=val_de)
```

## Implementing custom ser/de classes

Bytewax includes support for creating your own schema registry
implementation, or custom (de)serializers.

As a trivial example, we can implement a class that uses the
[orjson](https://github.com/ijl/orjson) library to deserialize a JSON
payload from bytes.

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
