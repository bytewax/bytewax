Bytewax ships with connectors for [Kafka](https://www.confluent.io/) and
Kafka compatible systems, like [Redpanda](https://redpanda.com/).

## Connecting to Kafka

Bytewax provides two basic ways to connect to Kafka.

You can use [`KafkaSource`](/apidocs/bytewax.connectors/kafka#bytewax.connectors.kafka.KafkaSource)
and [`KafkaSink`](/apidocs/bytewax.connectors/kafka#bytewax.connectors.kafka.KafkaSink) directly:

```python
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax import operators as op
from bytewax.dataflow import Dataflow

brokers = ["localhost:1909"]
flow = Dataflow("example")
kinp = op.input("kafka-in", flow, KafkaSource(["localhost:1909"], ["in-topic"]))
processed = op.map("map", kinp, lambda x: (x.key, x.value))
op.output("kafka-out", processed, KafkaSink(brokers, "out-topic"))
```

Or use the custom Kafka operator:

```python
from bytewax.connectors.kafka import operators as kop
from bytewax import operators as op
from bytewax.dataflow import Dataflow

brokers = ["localhost:1909"]
flow = Dataflow("example")
kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["in-topic"])
op.inspect("errors", kinp.errs).then(op.raises, "crash-on-err")
processed = op.map("map", kinp.oks, lambda x: (x.key, x.value))
kop.output("kafka-out", processed, brokers=brokers, topic="out-topic")
```

## Error handling

In the previous section, you may have noted that we added a chain of operators
for error handling using the [`raises`](/apidocs/bytewax.operators/index#bytewax.operators.raises)
operator.

Kafka sources in Bytewax return a dataclass that contains two output streams of
[`KafkaMessage`s](/apidocs/bytewax.connectors/kafka/message#bytewax.connectors.kafka.message.KafkaSourceMessage).

The `.oks` stream contains messages that were successfully processed. The `.err` stream
contains messages where an error was encountered. Items that encountered an error
have their `.error` field set with details about the error.

## Kafka Offsets and Recovery

Typical deployments of Kafka utilize [consumer groups](https://developer.confluent.io/courses/architecture/consumer-group-protocol/)
in order to manage partition assignment and the storing of Kafka offsets.

Bytewax by default does **not** use consumer groups to store partition offsets for consumers. In order
to support [recovery](/docs/concepts/recovery), Bytewax must manage and store the partition offsets
in recovery partitions.

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

When multiple workers are started, Bytewax will assign individual partitions to the available number of workers.
If the number of partitions changes, Dataflows will need to be restarted in order to assign new partitions
to workers.

If there are fewer partitions than workers, some workers will not be assigned a partition to read from.
In cases where items are not exchanged between workers, workers without an assigned input partition
will not process any data.

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
