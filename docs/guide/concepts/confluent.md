# Connecting Bytewax to Confluent Cloud Using the Bytewax Kafka Connector

Bytewax provides connectors for Kafka and Kafka-compatible systems, such as Confluent Cloud. This guide will walk you through the steps to connect Bytewax to a Confluent Cloud-hosted Kafka topic, allowing you to read from and write to Kafka.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Step 1: Obtain Confluent Cloud Credentials](#step-1-obtain-confluent-cloud-credentials)
  - [Getting the Bootstrap Server URL](#getting-the-bootstrap-server-url)
  - [Creating Kafka API Keys](#creating-kafka-api-keys)
  - [Using the Bytewax Client.id](#using-the-bytewax-client-id)
  - [Setting Up Schema Registry (Optional)](#setting-up-schema-registry-optional)
- [Step 2: Install Required Dependencies](#step-2-install-required-dependencies)
- [Step 3: Configure Bytewax Dataflow](#step-3-configure-bytewax-dataflow)
  - [Basic Connection](#basic-connection)
  - [Error Handling](#error-handling)
  - [Serialization and Deserialization](#serialization-and-deserialization)
    - [Using Confluent Schema Registry](#using-confluent-schema-registry)
    - [Custom Serialization Format](#custom-serialization-format)
  - [Managing Partitions](#managing-partitions)
  - [Recovery and Offsets](#recovery-and-offsets)
- [Conclusion](#conclusion)

## Prerequisites

- **Confluent Cloud Account**: Access to a Kafka cluster in Confluent Cloud. For more information on setting one up, you can see the Confluent instructions [here](https://docs.confluent.io/cloud/current/get-started/index.html#quick-start-for-ccloud).
- **Python Environment**: Python 3.9 or higher installed.
- **Bytewax & Confluent Kafka**: Install using `pip install "bytewax[kafka]"`.
- **Kafka Topics Created**: If you don't already have that, you can do that [here](https://docs.confluent.io/cloud/current/get-started/index.html#step-2-create-a-ak-topic).

## Step 1: Obtain Confluent Cloud Credentials

### Getting the Bootstrap Server URL

1. **Log In**: Access your [Confluent Cloud console](https://confluent.cloud/).
2. **Navigate to Cluster**: Select the Kafka cluster you want to connect to.
3. **Cluster Settings**: Click on **Cluster Settings**.
4. **Copy URL**: Under **Bootstrap server**, copy the server URL (e.g., `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`).

![Cluster Settings](https://hackmd.io/_uploads/BJ5KiQsCA.png)


### Creating Kafka API Keys

1. **API Keys**: In your Kafka cluster, click on **API Keys**.
2. **Create Key**: Click **Create API Key**.
3. **Select Account**: Choose **Service Account** for a specific service.
4. **Define ACLs**: Define fine grained access as required.
5. **Copy Credentials**: Note down the **API Key** and **API Secret**.

![Screenshot 2024-10-02 at 1.24.45 PM](https://hackmd.io/_uploads/BkN43XsR0.png)


### Using the Bytewax client.id

Confluent provides Bytewax with a unique client.id to understand which clients are interacting with a cluster. This can be set in the configration.
**Bytewax's client ID**: `cwc|26e5460b-797d-43fe-809a-67c8c80ab871`

This should be included in the configuration parameters that you pass through in a dictionary when using the Kafka Source and Sink. You can find some more info on all of the confluent-kafka configuration parameters including `client.id` in [Confluent's documentation](https://docs.confluent.io/platform/7.7/installation/configuration/consumer-configs.html#client-id).

### Setting Up Schema Registry (Optional)

If you plan to use Confluent Schema Registry:

1. **Navigate to Schema Registry**: In the Confluent Cloud console, click the schema registry at the bottom of the page. Or more recently you can find this in the environments page on the right hand side.
     ![Screenshot 2024-10-07 at 3.02.00 PM](https://hackmd.io/_uploads/B1gh90ZyJl.png)
2. **Copy URL**: Copy the **Schema Registry URL** (e.g., `https://psrc-xxxxx.us-east-1.aws.confluent.cloud`).
3. **Create API Key**: Click on **Add key** under **Credentials**.
![Screenshot 2024-10-07 at 3.02.16 PM](https://hackmd.io/_uploads/Hyzgj0W1Jg.png)
4. **Copy Credentials**: Note the **Schema Registry API Key** and **API Secret**.

## Step 2: Install Required Dependencies

Install Bytewax and the Confluent Kafka library:

```bash
pip install "bytewax[kafka]"
```

## Step 3: Configure Bytewax Dataflow

### Basic Connection

Here's how to set up a Bytewax dataflow to read from and write to Kafka topics in Confluent Cloud:


```python
from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage
from bytewax import operators as op
from bytewax.dataflow import Dataflow

# Replace these with your Confluent Cloud settings
BROKERS = ["<Your Bootstrap Server URL>"]
INPUT_TOPICS = ["<Your Input Topic>"]
OUTPUT_TOPIC = "<Your Output Topic>"

# Kafka authentication configuration
KAFKA_CONFIG = {
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "<Your Kafka API Key>",
    "sasl.password": "<Your Kafka API Secret>",
    "client.id": "cwc|26e5460b-797d-43fe-809a-67c8c80ab871"
}

flow = Dataflow()

# Kafka input operator
kinp = kop.input(
    "kafka_in",
    flow,
    brokers=BROKERS,
    topics=INPUT_TOPICS,
    add_config=KAFKA_CONFIG,
)

# Process messages (pass-through in this example)
processed = op.map(
    "process_messages",
    kinp.oks,
    lambda msg: KafkaSinkMessage(key=msg.key, value=msg.value),
)

# Kafka output operator
kop.output(
    "kafka_out",
    processed,
    brokers=BROKERS,
    topic=OUTPUT_TOPIC,
    add_config=KAFKA_CONFIG,
)
```

#### Explanation

- **BROKERS**: List containing your Kafka cluster's bootstrap server URL.
- **INPUT_TOPICS**: List of Kafka topics to read from.
- **OUTPUT_TOPIC**: Kafka topic to write to.
- **KAFKA_CONFIG**: Dictionary with Kafka authentication settings required by Confluent Cloud.

**Note**: Confluent Cloud requires SASL/SSL authentication. Ensure that `security.protocol`, `sasl.mechanisms`, `sasl.username`, and `sasl.password` are correctly set in `KAFKA_CONFIG`.

### Error Handling

The `kop.input` operator returns a dataclass with two streams:

- **`oks`**: Stream of successfully processed `KafkaSourceMessage` items.
- **`errs`**: Stream of `KafkaError` items encountered during processing.

Handle errors by attaching processing to the `errs` stream:

```python
# Handle errors (e.g., log them)
op.inspect("inspect_errors", kinp.errs)
```

If you are using the Kafka input operator, and do not process the `errs` stream, errors will be silently dropped. If you are using the `KafkaSource` connector, the dataflow will fail on an error and raise the error in it's default configuration.

### Serialization and Deserialization

#### Using Confluent Schema Registry

To work with Avro or JSON schemas stored in Confluent Schema Registry:

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Schema Registry configuration
SCHEMA_REGISTRY_CONF = {
    'url': '<Your Schema Registry URL>',
    'basic.auth.user.info': '<Schema Registry API Key>:<Schema Registry API Secret>',
}
schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONF)

# Deserializers
# The deserializer schema is automatically fetched from the schema registry
key_deserializer = AvroDeserializer(schema_registry_client)
value_deserializer = AvroDeserializer(schema_registry_client)

# Deserialize messages
deserialized_stream = kop.deserialize(
    "deserialize",
    kinp.oks,
    key_deserializer=key_deserializer,
    val_deserializer=value_deserializer,
)

# Process messages
processed = op.map(
    "process_messages",
    deserialized_stream.oks,
    lambda msg: KafkaSinkMessage(key=msg.key, value=msg.value),
)

# The serializers require a specific schema. Get it from the client.
# assumes the schema is created under the id 100004 and 100005
key_schema = client.get_schema(100004)
val_schema = client.get_schema(100005)

# Serializers
key_serializer = AvroSerializer(key_schema, schema_registry_client)
value_serializer = AvroSerializer(val_schema, schema_registry_client)

# Serialize messages
serialized_stream = kop.serialize(
    "serialize",
    processed,
    key_serializer=key_serializer,
    val_serializer=value_serializer,
)
```

#### Custom Serialization Format

To use a custom serialization format, such as JSON with `orjson`:

```python
import orjson
from confluent_kafka.serialization import StringDeserializer, StringSerializer

class OrJSONDeserializer:
    def __call__(self, value, ctx):
        if value is None:
            return None
        return orjson.loads(value)

class OrJSONSerializer:
    def __call__(self, obj, ctx):
        if obj is None:
            return None
        return orjson.dumps(obj)

# Deserializers
key_deserializer = StringDeserializer('utf_8')
value_deserializer = OrJSONDeserializer()

# Serializers
key_serializer = StringSerializer('utf_8')
value_serializer = OrJSONSerializer()

# Deserialize messages
deserialized_stream = kop.deserialize(
    "deserialize_json",
    kinp.oks,
    key_deserializer=key_deserializer,
    val_deserializer=value_deserializer,
)

# Process messages
processed = op.map(
    "process_json_messages",
    deserialized_stream.oks,
    lambda msg: KafkaSinkMessage(key=msg.key, value=msg.value),
)

# Serialize messages
serialized_stream = kop.serialize(
    "serialize_json",
    processed,
    key_serializer=key_serializer,
    val_serializer=value_serializer,
)
```

### Managing Partitions

Bytewax assigns Kafka topic partitions to workers:

- **Number of Partitions**: Ensure your Kafka topics have a suitable number of partitions to match your desired parallelism.
- **Rebalancing**: If the number of partitions changes, you need to restart the Bytewax dataflow to rebalance partition assignments.

### Recovery and Offsets

Bytewax manages Kafka offsets internally for recovery:

- **Recovery Enabled**: Offsets are stored in Bytewax's recovery partitions. For more details, see the [Bytewax recovery documentation](https://docs.bytewax.io/stable/guide/concepts/recovery.html).
- **Recovery Disabled**: Configure the starting offset manually.

Set the starting offset when creating the Kafka input operator:

```python
from confluent_kafka import OFFSET_BEGINNING

kinp = kop.input(
    "kafka_in",
    flow,
    brokers=BROKERS,
    topics=INPUT_TOPICS,
    starting_offset=OFFSET_BEGINNING,
    add_config=KAFKA_CONFIG,
)
```

**Available Starting Offsets**:

- **`OFFSET_BEGINNING`**: Start from the earliest offset.
- **`OFFSET_END`**: Start from the latest offset.
- **`OFFSET_STORED`**: Start from the last committed offset in Kafka (requires consumer group configuration).

**Note**: Bytewax does not use Kafka consumer groups by default. If you prefer to use consumer groups, configure them in `KAFKA_CONFIG`, but this is not recommended when using Bytewax's recovery mechanisms.

## Conclusion

By following this guide, you can connect Bytewax to Confluent Cloud using the Bytewax Kafka Connector. Ensure that you:

- Correctly configure your Kafka and Schema Registry credentials.
- Handle serialization and deserialization according to your data format.
- Manage partitions and recovery settings to suit your application's needs.

For more detailed information, refer to the [Bytewax documentation on Kafka connectors](https://docs.bytewax.io/).
