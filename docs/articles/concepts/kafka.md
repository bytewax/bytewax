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
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax import operators as op
from bytewax.dataflow import Dataflow

brokers = ["localhost:19092"]
flow = Dataflow("example")
kinp = op.input("kafka-in", flow, KafkaSource(brokers, ["in-topic"]))
processed = op.map("map", kinp, lambda x: (x.key, x.value))
op.output("kafka-out", processed, KafkaSink(brokers, "out-topic"))
```

Or use the Kafka input [operator](/apidocs/bytewax.connectors/kafka/operators#bytewax.connectors.kafka.operators.input):

```python
from bytewax.connectors.kafka import operators as kop
from bytewax import operators as op
from bytewax.dataflow import Dataflow

brokers = ["localhost:19092"]
flow = Dataflow("example")
kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["in-topic"])
processed = op.map("map", kinp.oks, lambda x: (x.key, x.value))
kop.output("kafka-out", processed, brokers=brokers, topic="out-topic")
```

## Batch sizes

By default, Bytewax will consume a single message at a time from Kafka. The default
setting is often sufficient for lower latency, but negatively affects throughput.

If your dataflow would benefit from higher throughput, you can set the `batch_size` parameter
to a higher value (eg: 1000). The `batch_size` parameter configures the maximum number of messages
that will be fetched at a time from each worker.

## Message Types

Messages received from `KafkaSource` are of type [`KafkaSourceMessage`](
/apidocs/bytewax.connectors/kafka/message#bytewax.connectors.kafka.message.KafkaSourceMessage).
This dataclass includes basic Kafka fields like `.key` and `.value`, as well as
extra information fields like `.headers`.

Messages that are published to a `KafkaSink` must be of type [`KafkaSinkMessage`](
/apidocs/bytewax.connectors/kafka/message#bytewax.connectors.kafka.message.KafkaSinkMessage).
With the `.key` and `.value` fields set.

## Kafka and Recovery

Typical deployments of Kafka utilize [consumer groups](https://developer.confluent.io/courses/architecture/consumer-group-protocol/)
in order to manage partition assignment and the storing of Kafka offsets.

Bytewax does **not** use consumer groups to store offsets or asssign partitions to consumers. In order
to correctly support [recovery](/docs/concepts/recovery), Bytewax must manage and store the
consumer offsets in recovery partitions.

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

## Partitions

Partitions are a fundamental concept for Kafka and Redpanda, and are the unit of parallelism for
producing and consuming messages.

When multiple workers are started, Bytewax will internally assign individual partitions to the
available number of workers. If there are fewer partitions than workers, some workers will not
be assigned a partition to read from.

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
OUT_TOPIC = "out_topic"
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

## Error handling

Kafka input sources in Bytewax return a dataclass that contains two output streams.
The `.oks` field contains a stream of [`KafkaMessage`](/apidocs/bytewax.connectors/kafka/message#bytewax.connectors.kafka.message.KafkaSourceMessage)
that were successfully processed. The `.errs` field contains a stream of [`KafkaError`](/apidocs/bytewax.connectors/kafka/error#bytewax.connectors.kafka.message.KafkaError)
where an error was encountered. Items that encountered an error have their `.err` field set with more details about the error.

It is important to note that if no processing is attached to the `.errs` stream of messages, they will be silently
dropped, and processing will continue.

In some cases, you will want your dataflow to crash and stop consuming messages so that the error can be inspected
and fixed. In other cases, you may want error messages to be published to a "dead letter queue" where they can be
inspected and reprocessed later, while allowing the main dataflow to continue processing new data.
