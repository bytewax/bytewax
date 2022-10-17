# Analyzing Sensor Data With Kafka and DynamoDB

In this example, we will walk through processing sensor data from IOT devices using Bytewax.

In this scenario, IOT devices are streaming real-time data that is being written to Kafka. Our goals are to:

- Read data from Kafka
- Build up a window of events from each sensor, based on its id
- Write those updated windows to DynamoDB every 5 seconds

In order to run this example, we'll need to install Bytewax with the optional `dynamodb` dependency:

```shell
pip install bytewax[dynamodb]
```

Great, let's start configuring Bytewax to read from Kafka. We'll begin by importing some dependencies and instantiating our Bytewax Dataflow.

```python
import json

from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig

flow = Dataflow()
flow.input("inp", KafkaInputConfig(brokers=["localhost:9092"], topic="sensor_topic"))
```

Our sensor payload is a JSON encoded string in Kafka. Let's write a function that deserializes data from Kafka, and returns it as a dictionary. In addition to the payload, we are also returning the `id` of the sensor that captured this data. Bytewax will use this key to route all of the data for a sensor to the same worker.

```python
def deserialize(key_bytes__payload_bytes):
    _key_bytes, payload_bytes = key_bytes__payload_bytes
    payload = json.loads(payload_bytes)
    return payload["id"], payload

flow.map(deserialize)
```

The goal of our example Dataflow is to accumulate sensor data over a defined period of time to produce an average reading. To do that, we'll use one of Bytewax's stateful windowing operators, `fold_window`.

In this example, we want our sensor readings to be grouped together based on the timestamp that the sensor recorded when the data was produced. To do that, we need to define a function that will read that time from the sensor event and convert it to UTC.

```python
from datetime import datetime

def get_event_time(event):
    return datetime.fromisoformat(event["time"])
```

There are two important considerations when using event times to construct windows:

Event Orderliness: We cannot guarantee that sensor events will be read from Kafka in order.

Event Lateness: We can't guarantee that we will have received all of the events that we should consider in a timely fashion, since sensors could lose their connection to the Internet and send data well after it was recorded.

In this way, event time processing is a tradeoff between latency and correctness.

We need a way to tell our window operator how long we are willing to wait for all of the data to arrive before returning windows. To do that, we create an `EventClockConfiguration` that uses our time getting function, and defines how long we are willing to wait, in system time.

```python
from datetime import timedelta
from bytewax.window import EventClockConfig

# Configure the `fold_window` operator to use the event time.
cc = EventClockConfig(get_event_time, wait_for_system_duration=timedelta(seconds=10))
```

We'll need to configure Bytewax to capture windows of data for a key every five seconds. Windows with a fixed size that do not overlap are called tumbling windows. The following code shows how to configure a bytewax `TumblingWindow` with 5 second intervals that are aligned to the time when the dataflow is started.

```python
from datetime import timezone
from bytewax.window import TumblingWindowConfig

start_at = datetime.now(timezone.utc)
wc = TumblingWindowConfig(start_at=start_at, length=timedelta(seconds=5))
```

Our stateful window operator, `fold_window` takes two functions in its list of arguments: a builder and a folder. The builder function is called when a new key is encountered for a window, and the folder is called on every item in the window.

In our case, the folder function and the builder function are simple. We would like to accumulate all of the events for a sensor into a single list. Our builder function can be the built-in Python function `list`, and our folder can just call `append` on that list with every event within a window.

```python
def acc_values(acc, event):
    acc.append(event)
    return acc
```

With those two functions and our time configuration, we can now use our `fold_window` operator:

```python
flow.fold_window("running_average", cc, wc, list, acc_values)
```

For each completed window, we'll need to process the data to compute an average of all of the values collected during that period, and format the results as a dictionary, along with the key for that sensor. Let's define our formatting step and add it to the dataflow.

When using DynamoDB as our output, we'll need to structure our output from this step as a dictionary, and add the values we want written to DynamoDB under the **Item** key.

We'll also need to format any floating point values as [Decimal](https://docs.python.org/3/library/decimal.html) values.

```python
from decimal import Decimal

def format(event):
    key, data = event
    values = [x[0] for x in data]
    dates = [datetime.fromisoformat(x[1]) for x in data]
    return {
        "Item": {
            "id": key,
            "average": Decimal(str(sum(values) / len(values))),
            "num_events": len(values),
            "from": Decimal(str((min(dates) - start_at).total_seconds())),
            "to": Decimal(str((max(dates) - start_at).total_seconds())),
        }
    }

flow.map(format)
```

With that completed, all that we need now is to write the finished data to DynamoDB. We can configure the built-in `DynamoDBOutputConfig` to do this:

```python
from bytewax.connectors.dynamodb.outputs import DynamoDBOutputConfig

flow.capture(
    DynamoDBOutputConfig(
        table="example",
    )
)
```

For more information about configuring AWS credentials and region for DynamoDB, please see the [AWS documentation](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html).
