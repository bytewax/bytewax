[![Actions Status](https://github.com/bytewax/bytewax/workflows/CI/badge.svg)](https://github.com/bytewax/bytewax/actions)
[![PyPI](https://img.shields.io/pypi/v/bytewax.svg?style=flat-square)](https://pypi.org/project/bytewax/)
[![Bytewax User Guide](https://img.shields.io/badge/user-guide-brightgreen?style=flat-square)](https://bytewax.io/docs)


<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://user-images.githubusercontent.com/6073079/195393689-7334098b-a8cd-4aaa-8791-e4556c25713e.png" width="350">
  <source media="(prefers-color-scheme: light)" srcset="https://user-images.githubusercontent.com/6073079/194626697-425ade3d-3d72-4b4c-928e-47bad174a376.png" width="350">
  <img alt="Bytewax">
</picture>

## Python Stateful Stream Processing Framework

Bytewax is a Python framework that simplifies event and stream processing. Because Bytewax couples the stream and event processing capabilities of Flink, Spark, and Kafka Streams with the friendly and familiar interface of Python, you can re-use the Python libraries you already know and love. Connect data sources, run stateful transformations and write to various different downstream systems with built-in connectors or existing Python libraries.

<img width="1303" alt="Screen Shot 2022-10-07 at 2 22 49 PM" src="https://user-images.githubusercontent.com/6073079/194624582-c16df8d6-d501-46b2-bdbc-78dbed67902e.png">


### How it all works

Bytewax is a Python framework and Rust distributed processing engine that uses a dataflow computational model to provide parallelizable stream processing and event processing capabilities similar to Flink, Spark, and Kafka Streams. You can use Bytewax for a variety of workloads from moving data à la Kafka Connect style all the way to advanced online machine learning workloads. Bytewax is not limited to streaming applications but excels anywhere that data can be distributed at the input and output.

Bytewax has an accompanying command line interface, [waxctl](https://bytewax.io/docs/deployment/waxctl/), which supports the deployment of dataflows on cloud servers or kuberentes. You can download it [here](https://bytewax.io/downloads/).

_____________

### Getting Started with Bytewax

```sh
pip install bytewax
```

[_Install waxctl_](https://bytewax.io/downloads/)

#### Dataflow, Input and Operators

A Bytewax dataflow is Python code that will represent an input, a series of processing steps, and an output. The inputs could range from a Kafka stream to a WebSocket and the outputs could vary from a data lake to a key-value store.

```python
from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow

# Bytewax has input and output helpers for common input and output data sources
# but you can also create your own with the [Sink and Source API](https://github.com/bytewax/bytewax/blob/main/docs/articles/advanced-concepts/custom-io-connectors.md).
```

At a high-level, the dataflow compute model is one in which a program execution is conceptualized as data flowing through a series of operator-based steps. Operators like `map` and `filter` are the processing primitives of Bytewax. Each of them gives you a “shape” of data transformation, and you give them regular Python functions to customize them to a specific task you need. See the documentation for a list of the [available operators](https://bytewax.io/apidocs/bytewax.operators/index)

```python
BROKERS = ["localhost:19092"]
IN_TOPICS = ["in_topic"]
OUT_TOPIC = "out_topic"
ERR_TOPIC = "errors"


def deserialize(key_bytes__payload_bytes):
    _, payload_bytes = key_bytes__payload_bytes
    event_data = json.loads(payload_bytes) if payload_bytes else None
    return event_data["user_id"], event_data


def anonymize_email(user_id__event_data):
    user_id, event_data = user_id__event_data
    event_data["email"] = "@".join(["******", event_data["email"].split("@")[-1]])
    return user_id, event_data


def remove_bytewax(user_id__event_data):
    user_id, event_data = user_id__event_data
    return "bytewax" not in event_data["email"]

flow = Dataflow("kafka_in_out")
stream = kop.input("inp", flow, brokers=BROKERS, topics=IN_TOPICS)
# we can inspect the stream coming from the kafka topic
op.inspect("inspect-errors", stream.errs)
op.inspect("inspect-oks", stream.oks, brokers=BROKERS, topic=ERR_TOPIC)
# and output errors to be handled as needed
kop.output("out_errs", stream.errs)
stream = op.map("deserialize", stream.oks, deserialize)
stream = op.map("anon", stream, anonymize_email)
stream = op.filter("filter_employees", stream, remove_bytewax)
# and finally output the cleaned data to a new topic
kop.output("out1", stream, brokers=BROKERS, topic=OUT_TOPIC)
```

#### Windowing, Reducing and Aggregating

Bytewax is a stateful stream processing framework, which means that some operations remember information across multiple events.  Windows and aggregations are also stateful, and can be reconstructed in the event of failure. Bytewax can be configured with different [state recovery mechanisms](https://bytewax.io/apidocs/bytewax.recovery) to durably persist state in order to recover from failure.

There are multiple stateful operators available like `reduce`, `stateful_map` and `fold_window`. The complete list can be found in the [API documentation for all operators](https://www.bytewax.io/apidocs/bytewax.operators/index). Below we use the `fold_window` operator with a tumbling window based on system time to gather events and calculate the number of times events have occurred on a per-user basis.

```python
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.operators import window as window_op
from bytewax.operators.window import EventClockConfig, TumblingWindow


# This is the accumulator function, and outputs a list of 2-tuples,
# containing the event's "value" and it's "time" (used later to print info)
def acc_values(acc, event):
    acc.append((event["value"], event["time"]))
    return acc


# This function instructs the event clock on how to retrieve the
# event's datetime from the input.
# Note that the datetime MUST be UTC. If the datetime is using a different
# representation, we would have to convert it here.
def get_event_time(event):
    return datetime.fromisoformat(event["time"])


# Configure the `fold_window` operator to use the event time.
cc = EventClockConfig(get_event_time, wait_for_system_duration=timedelta(seconds=10))

# And a 5 seconds tumbling window
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
wc = TumblingWindow(align_to=align_to, length=timedelta(seconds=5))

running_avg_stream = window_op.fold_window(
    "running_average", keyed_stream, cc, wc, list, acc_values
)
```

#### Merges and Joins

Merging or Joining multiple input streams is a common task for stream processing, Bytewax enables different types of joins to facilitate different patters.

##### Merging Streams

Merging streams is like concatenating, there is no logic and the resulting stream will potentially include heterogenous records.

```python
from bytewax import operators as op

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("merge")

src_1 = [
    {"user_id": "123", "name": "Bumble"},
]
inp1 = op.input("inp1", flow, TestingSource(src_1))

src_2 = [
    {"user_id": "123", "email": "bee@bytewax.com"},
    {"user_id": "456", "email": "hive@bytewax.com"},
]
inp2 = op.input("inp2", flow, TestingSource(src_2))
merged_stream = op.merge("merge", inp1, inp2)
op.inspect("debug", merged_stream)
```

##### Joining Streams

Joining streams is different than merging because it uses logic to join the records in the streams together. The joins in Bytewax can be running or not. A regular join in streaming is more closely related to an inner join in SQL in that the dataflow will emit data downstream from a join when all of the sides of the join have matched on the key.

```python
from bytewax import operators as op

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("join")

src_1 = [
    {"user_id": "123", "name": "Bumble"},
]
inp1 = op.input("inp1", flow, TestingSource(src_1))
keyed_inp_1 = op.key_on("key_stream_1", inp1, lambda x: x["user_id"])
src_2 = [
    {"user_id": "123", "email": "bee@bytewax.com"},
    {"user_id": "456", "email": "hive@bytewax.com"},
]
inp2 = op.input("inp2", flow, TestingSource(src_2))
keyed_inp_2 = op.key_on("key_stream_2", inp2, lambda x: x["user_id"])

merged_stream = op.join("join", keyed_inp_1, keyed_inp_2)
op.inspect("debug", merged_stream)
```

#### Output

Output in Bytewax is described as a sink and these are grouped into [connectors](https://www.bytewax.io/apidocs/bytewax.connectors/index). There are a number of basic connectors in the bytewax repo to help you during development. In addition to the built-in connectors, it is possible to use the input and output API to build a custom sink and source. There is also a hub for connectors built by the community, partners and Bytewax. Below is an example of a custom connector for Postgres using the psycopg2 library.

```python
import json

import psycopg2

from bytewax import operators as op
from bytewax.outputs import PartitionedOutput, StatefulSink


class PsqlSink(StatefulSink):
    def __init__(self):
        self.conn = psycopg2.connect("dbname=website user=bytewax")
        self.conn.set_session(autocommit=True)
        self.cur = self.conn.cursor()

    def write(self, user_id__user_data):
        user_id, user_data = user_id__user_data
        query_string = """
            INSERT INTO events (user_id, data)
            VALUES (%s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET data = %s;
        """
        self.cur.execute(
            query_string, (user_id, json.dumps(user_data), json.dumps(user_data))
        )

    def snapshot(self):
        pass

    def close(self):
        self.conn.close()


class PsqlOutput(PartitionedOutput):
    def list_parts(self):
        return {"single"}

    def assign_part(self, item_key):
        return "single"

    def build_part(for_part, resume_state):
        return PsqlSink()


op.output("pg-out", PsqlOutput())
```

#### Execution

Bytewax dataflows can be executed on a single host with multiple Python processes, or on multiple hosts. When processing data in a distributed fashion, Bytewax will ensure that all items with the same key are routed to the same host.

```sh
# a single worker locally
python -m bytewax.run my_dataflow:flow

# multiple workers in a single process
python -m bytewax.run my_dataflow:flow -w 2

# multiple workers in more than one process
# first we start the first process running in cluster_one port 2101
python -m bytewax.run my_dataflow:flow -w 2 -i0 -a "cluster_one:2101;cluster_two:2101"

# then the second in cluster_two
python -m bytewax.run simple -w 3 -i1 -a "cluster_one:2101;cluster_two:2101"
```

It can also be run in a Docker container as described further in the [documentation](https://bytewax.io/docs/deployment/container).

#### Kubernetes

The recommended way to run dataflows at scale is to leverage the [kubernetes ecosystem](https://bytewax.io/docs/deployment/k8s-ecosystem). To help manage deployment, we built [waxctl](https://bytewax.io/docs/deployment/waxctl), which allows you to easily deploy dataflows that will run at huge scale across multiple compute nodes.

```sh
waxctl df deploy my_dataflow.py --name my-dataflow
```

## Why Bytewax?

At a high level, Bytewax provides a few major benefits:

* The operators in Bytewax are largely “data-parallel”, meaning they can operate on independent parts of the data concurrently.
* Bytewax offers the ability to express higher-level control constructs, like iteration.
* Bytewax allows you to develop and run your code locally, and then easily scale that code to multiple workers or processes without changes.
* Bytewax can be used in both a streaming and batch context
* Ability to leverage the Python ecosystem directly

## Community

[Slack](https://join.slack.com/t/bytewaxcommunity/shared_invite/zt-vkos2f6r-_SeT9pF2~n9ArOaeI3ND2w) Is the main forum for communication and discussion.

[GitHub Issues](https://github.com/bytewax/bytewax/issues) is reserved only for actual issues. Please use the slack community for discussions.

[Code of Conduct](https://github.com/bytewax/bytewax/blob/main/CODE_OF_CONDUCT.md)

## Usage

Install the [latest release](https://github.com/bytewax/bytewax/releases/latest) with pip:

```shell
pip install bytewax
```

### Building From Source

To build a specific branch, you will need to use Maturin and have Rust installed on your machine. Once those have been installed run

```shell
maturin develop -E dev
```

*Important*: If you are testing with a maturin built version from source, you should use `maturin build --release` since `maturin develop` will be slower.

## More Examples

For a more complete example, and documentation on the available operators, check out the [User Guide](https://bytewax.io/docs) and the [/examples](/examples) folder.

## License

Bytewax is licensed under the [Apache-2.0](https://opensource.org/licenses/APACHE-2.0) license.

## Contributing

Contributions are welcome! This community and project would not be what it is without the [contributors](https://github.com/bytewax/bytewax/graphs/contributors). All contributions, from bug reports to new features, are welcome and encouraged. Please view the [contribution guidelines](/CONTRIBUTING.md) before getting started.

</br>
</br>

<p align="center"> With ❤️ Bytewax</p>
<p align="center"><img src="https://user-images.githubusercontent.com/6073079/157482621-331ad886-df3c-4c92-8948-9e50accd38c9.png" /> </p>
<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=07749572-3e76-4ac0-952b-d5dcf3bff737" />
