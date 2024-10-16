[![Actions Status](https://github.com/bytewax/bytewax/workflows/CI/badge.svg)](https://github.com/bytewax/bytewax/actions)
[![PyPI](https://img.shields.io/pypi/v/bytewax.svg?style=flat-square)](https://pypi.org/project/bytewax/)
[![Bytewax User Guide](https://img.shields.io/badge/user-guide-brightgreen?style=flat-square)](https://docs.bytewax.io/stable/guide/index.html)
[![](https://img.shields.io/badge/Gurubase-Ask%20Bytewax%20Guru-006BFF)](https://gurubase.io/g/bytewax)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/bytewax/bytewax/assets/53014647/cd47293b-72c9-423c-b010-2c4990206c60" width="350">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/bytewax/bytewax/assets/53014647/f376c9e8-5bd4-4563-ba40-3df8761b13fc" width="350">
  <img alt="Bytewax">
</picture>

## Python Stateful Stream Processing Framework

Bytewax is a Python framework that simplifies event and stream processing. Because Bytewax couples the stream and event processing capabilities of Flink, Spark, and Kafka Streams with the friendly and familiar interface of Python, you can re-use the Python libraries you already know and love. Connect data sources, run stateful transformations and write to various different downstream systems with built-in connectors or existing Python libraries.
<img width="1303" alt="Bytewax Dataflow Animation" src="https://github.com/bytewax/bytewax/assets/156834296/4e314f17-38ab-4e72-9268-a48ddee7a201">

### How it all works

Bytewax is a Python framework and Rust distributed processing engine that uses a dataflow computational model to provide parallelizable stream processing and event processing capabilities similar to Flink, Spark, and Kafka Streams. You can use Bytewax for a variety of workloads from moving data à la Kafka Connect style all the way to advanced online machine learning workloads. Bytewax is not limited to streaming applications but excels anywhere that data can be distributed at the input and output.

Bytewax has an accompanying command line interface, [waxctl](https://docs.bytewax.io/stable/guide/deployment/waxctl.html), which supports the deployment of dataflows on cloud servers or Kubernetes. You can download it [here](https://bytewax.io/waxctl).

---

### Getting Started with Bytewax

```sh
pip install bytewax
```

[_Install waxctl_](https://bytewax.io/waxctl)

#### Dataflow, Input and Operators

A Bytewax dataflow is Python code that will represent an input, a series of processing steps, and an output. The inputs could range from a Kafka stream to a WebSocket and the outputs could vary from a data lake to a key-value store.

```python
import json

from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
```

Bytewax has input and output helpers for common input and output data sources but you can also create your own with the [Sink and Source API](https://docs.bytewax.io/stable/guide/advanced-concepts/custom-connectors.html).

At a high-level, the dataflow compute model is one in which a program execution is conceptualized as data flowing through a series of operator-based steps. Operators like `map` and `filter` are the processing primitives of Bytewax. Each of them gives you a “shape” of data transformation, and you give them regular Python functions to customize them to a specific task you need. See the documentation for a list of the [available operators](https://docs.bytewax.io/stable/api/bytewax/bytewax.operators.html)

```python
BROKERS = ["localhost:19092"]
IN_TOPICS = ["in_topic"]
OUT_TOPIC = "out_topic"
ERR_TOPIC = "errors"


def deserialize(kafka_message):
    return json.loads(kafka_message.value)


def anonymize_email(event_data):
    event_data["email"] = "@".join(["******", event_data["email"].split("@")[-1]])
    return event_data


def remove_bytewax(event_data):
    return "bytewax" not in event_data["email"]


flow = Dataflow("kafka_in_out")
stream = kop.input("inp", flow, brokers=BROKERS, topics=IN_TOPICS)
# we can inspect the stream coming from the kafka topic to view the items within on std out for debugging
op.inspect("inspect-oks", stream.oks)
# we can also inspect kafka errors as a separate stream and raise an exception when one is encountered
errs = op.inspect("errors", stream.errs).then(op.raises, "crash-on-err")
deser_msgs = op.map("deserialize", stream.oks, deserialize)
anon_msgs = op.map("anon", deser_msgs, anonymize_email)
filtered_msgs = op.filter("filter_employees", anon_msgs, remove_bytewax)
processed = op.map("map", anon_msgs, lambda m: KafkaSinkMessage(None, json.dumps(m)))
# and finally output the cleaned data to a new topic
kop.output("out1", processed, brokers=BROKERS, topic=OUT_TOPIC)
```

#### Windowing, Reducing and Aggregating

Bytewax is a stateful stream processing framework, which means that some operations remember information across multiple events. Windows and aggregations are also stateful, and can be reconstructed in the event of failure. Bytewax can be configured with different [state recovery mechanisms](https://docs.bytewax.io/stable/api/bytewax/bytewax.recovery.html) to durably persist state in order to recover from failure.

There are multiple stateful operators available like `reduce`, `stateful_map` and `fold_window`. The complete list can be found in the [API documentation for all operators](https://docs.bytewax.io/stable/api/bytewax/bytewax.operators.html). Below we use the `fold_window` operator with a tumbling window based on system time to gather events and calculate the number of times events have occurred on a per-user basis.

```python
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
import bytewax.operators.windowing as win
from bytewax.operators.windowing import EventClock, TumblingWindower
from bytewax.testing import TestingSource

flow = Dataflow("window_eg")

src = [
    {"user_id": "123", "value": 5, "time": "2023-1-1T00:00:00Z"},
    {"user_id": "123", "value": 7, "time": "2023-1-1T00:00:01Z"},
    {"user_id": "123", "value": 2, "time": "2023-1-1T00:00:07Z"},
]
inp = op.input("inp", flow, TestingSource(src))
keyed_inp = op.key_on("keyed_inp", inp, lambda x: x["user_id"])


# This function instructs the event clock on how to retrieve the
# event's datetime from the input.
# Note that the datetime MUST be UTC. If the datetime is using a different
# representation, we would have to convert it here.
def get_event_time(event):
    return datetime.fromisoformat(event["time"])


# Configure the `fold_window` operator to use the event time.
clock = EventClock(get_event_time, wait_for_system_duration=timedelta(seconds=10))

# And a 5 seconds tumbling window
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
windower = TumblingWindower(align_to=align_to, length=timedelta(seconds=5))

five_sec_buckets_win_out = win.collect_window(
    "five_sec_buckets", keyed_inp, clock, windower
)


def calc_avg(bucket):
    values = [event["value"] for event in bucket]
    if len(values) > 0:
        return sum(values) / len(values)
    else:
        return None


five_sec_avgs = op.map_value("avg_in_bucket", five_sec_buckets_win_out.down, calc_avg)
```

#### Merges and Joins

Merging or Joining multiple input streams is a common task for stream processing, Bytewax enables different types of joins to facilitate different patters.

##### Merging Streams

Merging streams is like concatenating, there is no logic and the resulting stream will potentially include heterogeneous records.

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

Output in Bytewax is described as a sink and these are grouped into [connectors](https://docs.bytewax.io/stable/api/bytewax/bytewax.connectors.html). There are a number of basic connectors in the Bytewax repo to help you during development. In addition to the built-in connectors, it is possible to use the input and output API to build a custom sink and source. There is also a hub for connectors built by the community, partners and Bytewax. Below is an example of a custom connector for Postgres using the psycopg2 library.

% skip: next

```python
import psycopg2

from bytewax import operators as op
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition


class PsqlSink(StatefulSinkPartition):
    def __init__(self):
        self.conn = psycopg2.connect("dbname=website user=bytewax")
        self.conn.set_session(autocommit=True)
        self.cur = self.conn.cursor()

    def write_batch(self, values):
        query_string = """
            INSERT INTO events (user_id, data)
            VALUES (%s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET data = %s;
        """
        self.cur.execute_values(query_string, values)

    def snapshot(self):
        pass

    def close(self):
        self.conn.close()


class PsqlOutput(FixedPartitionedSink):
    def list_parts(self):
        return ["single"]

    def build_part(step_id, for_part, resume_state):
        return PsqlSink()
```

#### Execution

Bytewax dataflows can be executed in a single Python process, or on multiple processes on multiple hosts with multiple worker threads. When processing data in a distributed fashion, Bytewax uses routing keys to ensure your state is updated in a correct way automatically.

```sh
# a single worker locally
python -m bytewax.run my_dataflow:flow

# Start two worker threads in a single process.
python -m bytewax.run my_dataflow -w 2

# Start a process on two separate machines to form a Bytewax cluster.
# Start the first process with two worker threads on `machine_one`.
machine_one$ python -m bytewax.run my_dataflow -w 2 -i0 -a "machine_one:2101;machine_two:2101"

# Then start the second process with three worker threads on `machine_two`.
machine_two$ python -m bytewax.run my_dataflow -w 3 -i1 -a "machine_one:2101;machine_two:2101"
```

It can also be run in a Docker container as described further in the [documentation](https://docs.bytewax.io/stable/guide/deployment/container.html).

#### Kubernetes

The recommended way to run dataflows at scale is to leverage the [kubernetes ecosystem](https://docs.bytewax.io/stable/guide/deployment/waxctl.html). To help manage deployment, we built [waxctl](https://docs.bytewax.io/stable/guide/deployment/waxctl.html), which allows you to easily deploy dataflows that will run at huge scale across multiple compute nodes.

```sh
waxctl df deploy my_dataflow.py --name my-dataflow
```

## Why Bytewax?

At a high level, Bytewax provides a few major benefits:

- The operators in Bytewax are largely “data-parallel”, meaning they can operate on independent parts of the data concurrently.
- Bytewax offers the ability to express higher-level control constructs, like iteration.
- Bytewax allows you to develop and run your code locally, and then easily scale that code to multiple workers or processes without changes.
- Bytewax can be used in both a streaming and batch context
- Ability to leverage the Python ecosystem directly

## Community

[Slack](https://join.slack.com/t/bytewaxcommunity/shared_invite/zt-vkos2f6r-_SeT9pF2~n9ArOaeI3ND2w) is the main forum for communication and discussion.

[GitHub Issues](https://github.com/bytewax/bytewax/issues) is reserved only for actual issues. Please use the community Slack for discussions.

[Code of Conduct](https://github.com/bytewax/bytewax/blob/main/CODE_OF_CONDUCT.md)

## More Examples

For a more complete example, and documentation on the available operators, check out the [User Guide](https://docs.bytewax.io/stable/guide/index.html) and the [/examples](examples) folder.

## License

Bytewax is licensed under the [Apache-2.0](https://opensource.org/licenses/APACHE-2.0) license.

## Contributing

Contributions are welcome! This community and project would not be what it is without the [contributors](https://github.com/bytewax/bytewax/graphs/contributors). All contributions, from bug reports to new features, are welcome and encouraged.

Please view the [Contribution Guide](https://docs.bytewax.io/stable/guide/contributing/contributing.html) for how to get started.

</br>
</br>

<p align="center"> With ❤️ Bytewax</p>
<p align="center"><img src="https://user-images.githubusercontent.com/6073079/157482621-331ad886-df3c-4c92-8948-9e50accd38c9.png" /> </p>
<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=07749572-3e76-4ac0-952b-d5dcf3bff737" />
