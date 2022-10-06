Processing Events from Redpanda to Postgres
===========================

This example will cover building a data product with Bytewax. We will write a data flow that will aggregate event data generated via a user's interaction with a software product. The event data in this example will stream into a Redpanda Topic and our dataflow will consume from this topic. The dataflow will anonymize user data, filter out employee generated events and then count the number of events per type of event, per user over a time window. The results will be written to a postgres database that will serve the final application.

Prerequisites
---------

**Kafka/Redpanda**
To get started you will need a Kafka ([Docker setup](https://developer.confluent.io/quickstart/kafka-docker/)) or Redpanda ([Docker setup](https://docs.redpanda.com/docs/quickstart/quick-start-docker/)) cluster running.

**Postgres**
Follow the correct installation guide for your OS from those listed on the [wiki](https://wiki.postgresql.org/wiki/Detailed_installation_guides). Once you have completed the installation, you will need to create a db and a table with the correct schema.

```sql doctest:SKIP
CREATE DATABASE website;
CREATE USER bytewax;
ALTER USER bytewax WITH SUPERUSER;
\c website;
CREATE TABLE events (
    user_id VARCHAR unique PRIMARY KEY,
    data JSON
);
```

**Python Modules**
You should also have the following installed:

```bash
pip install bytewax==0.11.1 psycopg2==2.9.4 kafka-python==2.0.2
```

**Data**
To get started with some data we will use the code below to generate some web events in our topic for processing in the dataflow we are going to write. Create a new file named `fake_events.py` with the code below included.

```python doctest:SKIP
import json
from random import randint
from time import sleep

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

input_topic_name = "web_events"
localhost_bootstrap_server = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=[localhost_bootstrap_server])
admin = KafkaAdminClient(bootstrap_servers=[localhost_bootstrap_server])

# Create input topic
try:
    input_topic = NewTopic(input_topic_name, num_partitions=3, replication_factor=1)
    admin.create_topics([input_topic])
    print(f"input topic {input_topic_name} created successfully")
except:
    print(f"Topic {input_topic_name} already exists")

# Add data to input topic
users = [
    {"user_id":"a12", "email":"joe@mail.com"}, 
    {"user_id":"a34", "email":"jane@mail.com"}, 
    {"user_id":"a56", "email":"you@mail.com"}, 
    {"user_id":"a78", "email":"the@mail.com"}, 
    {"user_id":"a99", "email":"employee@bytewax.io"}
    ]

event_types = [
    "click",
    "post",
    "comment",
    "login"
]

try:
    for i in range(500):
        event = users[randint(0,4)]
        event['type'] = event_types[randint(0,3)]
        event_ = json.dumps(event).encode()
        producer.send(input_topic_name, value=event_)
        sleep(0.1)
    print(f"input topic {input_topic_name} populated successfully")
except KafkaError:
    print("A kafka error occurred")
```

Now run `fake_events.py` to load our topic up with some event data.

```python doctest:SKIP
python fake_events.py
```

Dataflow
-----------

The dataflow will start with us defining the input. In this case, consuming from a Redpanda topic.

```python doctest:SKIP

​​from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig 

flow = Dataflow()
flow.input("inp", KafkaInputConfig(brokers=["localhost:9092"], topic="web_events"))
```

At a high-level, dataflow programming is a programming paradigm where program execution is conceptualized as data flowing through a series of operator-based steps. Operators like `map` and `filter` are the processing primitives of bytewax. Each of them gives you a “shape” of data transformation, and you give them regular Python functions to customize them to a specific task you need. See the documentation for a list of the [available operators](https://docs.bytewax.io/apidocs/bytewax.dataflow#bytewax.dataflow.Dataflow)

```python doctest:SKIP
import json
def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    key = json.loads(key_bytes) if key_bytes else None
    event_data = json.loads(payload_bytes) if payload_bytes else None
    return event_data["user_id"], event_data

def anonymize_email(user_id__event_data):
    user_id, event_data = user_id__event_data
    event_data["email"] = "@".join(["******", event_data["email"].split("@")[-1]])
    return user_id, event_data

def remove_bytewax(user_id__event_data):
    user_id, event_data = user_id__event_data
    return "bytewax" not in event_data["email"]

flow.map(deserialize)
flow.map(anonymize_email)
flow.filter(remove_bytewax)
```

Bytewax is a stateful stream processor, which means that you can do things like aggregations and windowing. With Bytewax, state is stored in memory on the workers by default and can also be persisted with different [state recovery mechanisms](https://docs.bytewax.io/apidocs/bytewax.recovery). There are different stateful operators available like `reduce`, `stateful_map` and `fold_window`. The complete list can be found in the [API documentation for all operators](https://docs.bytewax.io/apidocs/bytewax.dataflow). Below we use the `fold_window` operator with a tumbling window based on system time to gather events and calculate the number of times different events happen per user.

```python doctest:SKIP
import datetime
from collections import defaultdict

from bytewax.window import TumblingWindowConfig, SystemClockConfig

cc = SystemClockConfig()
wc = TumblingWindowConfig(length=datetime.timedelta(seconds=5))

def build():
    return defaultdict(lambda: 0)

def count_events(results, event):
    results[event["type"]] += 1
    return results

flow.fold_window("session_state_recovery", cc, wc, build, count_events)
```

Output mechanisms in Bytewax are managed in the [capture operator](https://docs.bytewax.io/apidocs/bytewax.dataflow#bytewax.dataflow.Dataflow.capture). There are a number of helpers that allow you to easily connect and write to other systems ([output docs](https://docs.bytewax.io/apidocs/bytewax.outputs)). If there isn’t a helper built, it is easy to build a custom version. Like the input, Bytewax output can be parallelized and connection will occur on the worker.

```python doctest:SKIP
import json

import psycopg2
from bytewax.outputs import ManualOutputConfig

def output_builder(worker_index, worker_count):
    # create the connection at the worker level
    conn = psycopg2.connect("dbname=website user=bytewax")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    def write_to_postgres(user_id__user_data):
        user_id, user_data = user_id__user_data
        query_string = '''
                    INSERT INTO events (user_id, data)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id)
                    DO
                        UPDATE SET data = %s;'''
        cur.execute(query_string, (user_id, json.dumps(user_data), json.dumps(user_data)))
    return write_to_postgres

flow.capture(ManualOutputConfig(output_builder))
```

Bytewax comes with a few different execution models. They are used to run the dataflow in different manners, like running across a cluster or running on a local machine. Below is an example of running on across a manual managed cluster

```python doctest:SKIP
if __name__ == "__main__":
    addresses = [
    "localhost:2101"
    ]

    cluster_main(
        flow,
        addresses=addresses,
        proc_id=0,
        worker_count_per_proc=2)
```

**Deploying and Scaling**
--------

Bytewax can be run as a single worker thread, single worker process on a local machine, or remote machine with multiple worker threads and worker processes just like a regular python script.

```sh doctest:SKIP
python my_dataflow.py
```

It can also be run in a docker container as described further in the [documentation](https://docs.bytewax.io/deployment/container).

**Kubernetes**

The recommended way to run dataflows at scale is to leverage the [kubernetes ecosystem](https://docs.bytewax.io/deployment/k8s-ecosystem). To help manage deployment, we built [wactl](https://docs.bytewax.io/deployment/waxctl), which allows you to easily deploy dataflows that will run at huge scale across pods.

```sh doctest:SKIP
waxctl df deploy my_dataflow.py --name my-dataflow
```
