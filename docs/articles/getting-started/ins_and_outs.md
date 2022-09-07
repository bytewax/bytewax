Every bytewax dataflow needs to be able to connect to and receive or retrieve data and also requires instruction on where to send and what to do with processed data. 
There are two operators that allow you to configure this: `Dataflow.input` and `Dataflow.capture`.

## Input

The first thing we need to define on a Dataflow is the `input`.
We do this by passing a specific input configuration to the `input` operator.

Bytewax offers two input configurations (more to come in the future):
- ManualInputConfig
- KafkaInputConfig

[ManualInputConfig](/apidocs/bytewax.inputs#bytewax.inputs.ManualInputConfig) allows you to define the input **builder** as a python function.  
The input builder is a function that is called on each worker and will produce the input for that worker.  
It accepts three parameters: `worker_index, worker_count, resume_state`. The input builder function should return an iterable that yields a tuple of `(state, event)`. If your dataflow is interrupted, the third argument passed to your input function can be used to reconstruct the state of your input at the last recovery snapshot, provided you write your input logic accordingly.
You can use any existing python library to extract the data you need inside the builder function.  
You then pass the builder to `ManualInputConfig` and the generator will be polled to retrieve new items.

[KafkaInputConfig](/apidocs/bytewax.inputs#bytewax.inputs.KafkaInputConfig) is a specific input configuration tailored for Apache Kafka (and kafka-api compatible platforms, like Redpanda).  
This input generator is provided by the Rust Bytewax library, and while you can build a custom Kafka input using the `ManualInputConfig` and any python library to connect to Kafka, using the `KafkaInputConfig` is the recommended approach since it also automatically handles recovery.  
`KafkaInputConfig` accepts four parameters: a list of brokers, a topic, a tail boolean and a starting_offset. Additional configuration can be passed as a dictionary with the keyword `additional_properties`. See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md. See our API docs for `bytewax.inputs` for more on a Kafka configuration.

## Output

Output, similarly to input, is configurable and this is accomplished with the `capture` operator.

Bytewax offers three output configurations:
- ManualOutputConfig
- StdOutputConfig
- KafkaOutputConfig

[ManualOutputConfig](/apidocs/bytewax.outputs#bytewax.outputs.ManualOutputConfig) is the most flexible output configuration. It requires a **builder** function that is called on each worker and will handle the output for that worker. The output builder function should return a callback **output handler** function that can be called with each item of output produced. This can be used with any existing Python library to connect with various downstream systems. Pay attention to whether the downstream system can accept concurrent writes in the case of parallelism.

[StdOutputConfig](/apidocs/bytewax.outputs#bytewax.outputs.StdOutputConfig) simply prints all of the output to standard out, and it does not require any other configuration.

[KafkaOutputConfig](/apidocs/bytewax.inputs#bytewax.outputs.KafkaOutputConfig) is a specific output configuration tailored for Apache Kafka (and kafka-api compatible platforms, like Redpanda).  
This output generator is provided by the Rust Bytewax library. `KafkaOutputConfig` expects only two parameters: a list of brokers and a topic. Its input must take the form of a two-tuple of bytes, where the second element is the payload and the first is an optional key. Additional configuration can be passed as a dictionary with the keyword `additional_properties`. See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md. See our API docs for `bytewax.outputs` for more on a Kafka configuration.