Every bytewax dataflow needs to be able to connect to and receive or retrieve data and also requires instruction on where to send and what to do with processed data. 
There are two operators that allow you to configure this: `Dataflow.input` and `Dataflow.output`.

## Input

The first thing we need to define on a Dataflow is the `input`.
We do this by passing a specific input configuration to the `input` operator.

Bytewax offers two input connectors (more to come in the future):
- KafkaInput
- FileInput
- DirInput


[KafkaInput](/apidocs/bytewax.connectors/kafka#bytewax.connectors.kafka.KafkaInput) is a
specific input configuration tailored for Apache Kafka (and kafka-api compatible
platforms, like Redpanda).  
This input generator uses the Python confluent-kafka library and automatically handles
recovery.  
`KafkaInput` accepts four parameters: a list of brokers, a list of topics, a tail
boolean and a starting_offset.  
Additional configuration can be passed as a dictionary with the keyword argument
`add_conf`.  
See our API docs for `bytewax.connectors.kafka` for more on Kafka configuration.

[FileInput](/apidocs/bytewax.connectors/files#bytewax.connectors.files.FileInput) is an
input source that emits lines from a file as input elements.  
It also handles recovery by keeping track of the last line that was read.

[DirInput](/apidocs/bytewax.connectors/files#bytewax.connectors.files.DirInput) is an
input source that emits lines from all the files in the given directory.  
It also handles recovery by keeping track of the last line that was read.

Bytewax also exposes the [DynamicInput]() and [PartitionedInput]() base classes so that
you can build your own input connector.  
See the [blog post]() and the [examples]() for how to build your own connectors.

## Output

Output, similarly to input, is configurable and this is accomplished with the `output` operator.

Bytewax offers four output configurations:
- StdOutput
- KafkaOutput
- FileOutput
- DirOutput

[StdOutput]() simply prints all of the
output to standard out, and it does not require any other configuration.

[KafkaOutput]() is a specific output configuration tailored for Apache Kafka (and
kafka-api compatible platforms, like Redpanda).  
This output connector is provided by the Python library confluent-kafka.
`KafkaOutput` expects only two parameters: a list of brokers and a topic.
Its input must take the form of a two-tuple of bytes, where the second element
is the payload and the first is an optional key.  
Additional configuration can be passed as a dictionary with the keyword arg `add_config`.
See our API docs for `bytewax.connectors.kafka` for more on Kafka configuration.

[FileOutput](/apidocs/bytewax.connectors/files#bytewax.connectors.files.FileOutput) is an
output source that writes emitted items as lines on a file.  

[DirOutput](/apidocs/bytewax.connectors/files#bytewax.connectors.files.DirOutput) is an
output source that writes to a set of files in a directory line-by-line.
