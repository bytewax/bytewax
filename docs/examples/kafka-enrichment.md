Enriching Streaming Data from Kafka
===========================

This example will cover how to write a dataflow to support in-line data enrichment. Data enrichment is the process of adding to or enhancing data to make it more suitable or useful for a specific purpose. It is most often the result of joining additional data from a 3rd party database or another internal data source. For this specific example we will consume a stream of ip addresses from Kafka as input, enrich them with third party data to determine the location of the IP address and then produce data to Kafka. This example will leverage the `KafkaInputConfig` and `KafkaOutputConfig` to do so.

Prerequisites
---------

**Kafka**

To get started you will need a Kafka ([Docker setup](https://developer.confluent.io/quickstart/kafka-docker/)) or Redpanda ([Docker setup](https://docs.redpanda.com/docs/quickstart/quick-start-docker/)) cluster running.

**Python Modules**
You should also have the following installed:

pip install bytewax==0.11.1 requests==2.28.0 kafka-python==2.0.2

**Data**

The data source for this example is under the [examples directory](https://github.com/bytewax/bytewax/tree/main/docs/examples) in the [bytewax repo](https://github.com/bytewax/bytewax) and you can load it the data to the running kafka cluster with the code shown below. This code is outside of the scope of this example. It is creating an input and output topic and then writing some sample data to the topic.

```python doctest:SKIP
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
from time import sleep

input_topic_name = "ip_address_by_country"
output_topic_name = "ip_address_by_location"
localhost_bootstrap_server = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=[localhost_bootstrap_server])
admin = KafkaAdminClient(bootstrap_servers=[localhost_bootstrap_server])

# Create input topic
try:
    input_topic = NewTopic(input_topic_name, num_partitions=20, replication_factor=1)
    admin.create_topics([input_topic])
    print(f"input topic {input_topic_name} created successfully")
except:
    print(f"Topic {input_topic_name} already exists")

# Create output topic
try:
    output_topic = NewTopic(output_topic_name, num_partitions=20, replication_factor=1)
    admin.create_topics([output_topic])
    print(f"output topic {output_topic_name} created successfully")
except:
    print(f"Topic {output_topic_name} already exists")

# Add data to input topic
try:
    for line in open("ip_address_with_country.txt"):
        ip_address, country_raw = line.split(",")
        country = country_raw[:-1]
        producer.send(input_topic_name, key=f"{country}".encode('ascii'), value=f"{ip_address}".encode('ascii'))
        sleep(0.1)
    print(f"input topic {output_topic_name} populated successfully")
except KafkaError:
    print("A kafka error occured")
```

Building the Dataflow
--------

With some data loaded and our infrastructure running. We can move onto the fun part :).

Every dataflow will contain, at the very least an input and an output. In this example the data input source will be a kafka topic and the output sink will be another kafka topic. Between the input and output lies the code used to transform the data. This is illustrated by the diagram below.

![dataflow diagram]()

Let's walk through constructing the input, the transformation code and the output.

**Kafka Input**

Bytewax has a concept around input configurations. At a high level, these are code that can be configured and will be used as the input source for the dataflow. The [`KafkaInputConfig`](https://docs.bytewax.io/apidocs/bytewax.inputs#bytewax.inputs.KafkaInputConfig) is one of the more popular input configurations. It is important to note that the input connection will be made on each worker.

```python doctest:SKIP
from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import KafkaOutputConfig

flow = Dataflow()
flow.input(
    step_id = "ip_address", 
    input_config = KafkaInputConfig(
        brokers=["localhost:9092"], 
        topic="ip_address_by_country",
        tail=False
        )
    )
```

After initializing a Dataflow object, we use the `input` method to define our input. The input method takes two arguments, the `step_id` and the `input_config`. The `step_id` is used for recovery purposes and the input configuration is where we will use the `KafkaInputConfig` to set up our dataflow to consume from Kafka.

_A Quick Aside on Recovery: With Bytewax you can persist state in more durable formats. This is so that in the case that the dataflow fails, you can recover state and the dataflow will not be required to recompute from the beginning. This is oftentimes referred to as checkpointing for other processing frameworks. With the `KafkaInputConfig` this will also handle the offset and consumer groups for you. This makes it easy to get started working with data in Kafka._

**Data Transformation**

[Operators](https://docs.bytewax.io/apidocs/bytewax.dataflow) are Dataflow class methods that define how data will flow through the dataflow. Whether it will be filtered, modified, aggregated or accumulated. In this example we are modifying our data in-flight and will use the `map` operator. The `map` operator will receive a Python function as an argument and this will contain the code to modify the data payload.

```python doctest:SKIP
import json
import requests

def get_location(data):
    key, value = data
    ip_address = value.decode('ascii')
    response = requests.get(f'https://ipapi.co/{ip_address}/json/')
    response_json = response.json()
    location_data = {
        "ip": ip_address,
        "city": response_json.get("city"),
        "region": response_json.get("region"),
        "country_name": response_json.get("country_name")
    }
    return key, json.loads(location_data)

# corresponding flow module for the Map operator
flow.map(get_location)
```

In the code above, we are making an http request to an external service, this is only for demonstration purposes. You should use something that is lower latency so you do not risk having the http request as a bottleneck or having network errors.

**Kafka Output**

To capture data that is transformed in a dataflow, the capture method is used.Similarly to the input method, it takes a configuration as the argument. Bytewax has built-in output configurations and [`KafkaOutputConfig`](https://docs.bytewax.io/apidocs/bytewax.outputs#bytewax.outputs.KafkaOutputConfig) is one of those. We are going to use it in this example to write out the enriched data to a new topic.

```python doctest:SKIP
flow.capture(
    KafkaOutputConfig(
        brokers=["localhost:9092"],
        topic="ip_address_by_location"
    )
)
```

### Kicking off execution

With the above dataflow written the final step is to specify the execution method. Whether it will run as a single threaded process on our local machine or be capable of scaling across a kubernetes cluster. The methods used to define the execution are part of the execution module and more detail can be found in the long format documentation as well as in the API documentation.

```python doctest:SKIP
from bytewax.execution import cluster_main

if __name__ == "__main__":
    addresses = [
    "localhost:2101"
    ]

    cluster_main(
        flow, 
        addresses=addresses,
        proc_id=0,
        worker_count_per_proc=1)
```

There are two types of workers, worker threads and worker processes. In most use cases where you are running Python code to transform and enrich data, you will want to use processes.

Deploying the Enrichment Dataflow
---------

Bytewax dataflows can be run as you would a regular python script.

```bash doctest:SKIP
> python kafka_enrich.py
```

It is also possible to run a bytewax dataflow on one of the public clouds, like AWS with Waxctl with very little configuration. You will need to have the AWS cli install and configured.

```bash doctest:SKIP
waxctl aws deploy kafka-enrichment.py --name kafka-enrichment \
  --requirements-file-name requirements-ke.txt
```

Waxctl will configure and start an AWS EC2 instance and run your dataflow on the instance. To see the default parameters, you can run the help command and see them in the command line:

```bash doctest:SKIP
waxctl aws deploy -h                                               
Deploy a dataflow to a new EC2 instance.
​
The deploy command expects one argument, which is the path of your python dataflow file.
By default, Waxctl creates a policy and a role that will allow the EC2 instance to store Cloudwatch logs and start sessions through Systems Manager.
​
Examples:
  # The default command to deploy a dataflow named "bytewax" in a new EC2 instance running my-dataflow.py file.
  waxctl aws deploy my-dataflow.py
​
  # Deploy a dataflow named "custom" using specific security groups and instance profile
  waxctl aws deploy dataflow.py --name custom \
    --security-groups-ids "sg-006a1re044efb2d23" \
    --principal-arn "arn:aws:iam::1111111111:instance-profile/my-profile"
​
Usage:
  waxctl aws deploy [PATH] [flags]
​
Flags:
  -P, --associate-public-ip-address     associate a public IP address to the EC2 instance (default true)
  -m, --detailed-monitoring             specifies whether detailed monitoring is enabled for the EC2 instance
  -e, --extra-tags strings              extra tags to apply to the EC2 instance. The format must be KEY=VALUE
  -h, --help                            help for deploy
  -t, --instance-type string            EC2 instance type to be created (default "t2.micro")
  -k, --key-name string                 name of an existing key pair
  -n, --name string                     name of the EC2 instance to deploy the dataflow (default "bytewax")
  -p, --principal-arn string            principal ARN to assign to the EC2 instance
      --profile string                  AWS cli configuration profile
  -f, --python-file-name string         python script file to run. Only needed when [PATH] is a tar file
      --region string                   AWS region
  -r, --requirements-file-name string   requirements.txt file if needed
      --save-cloud-config               save cloud-config file to disk for troubleshooting purposes
  -S, --security-groups-ids strings     security groups Ids to assign to the EC2 instance
  -s, --subnet-id string                the ID of the subnet to launch the EC2 instance into
​
Global Flags:
      --debug   enable verbose output
```
