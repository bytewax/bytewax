Each bytewax dataflow needs to know how to get the input data, and what to do with the processed data.
There are 2 operators that allow you to configure this: `Dataflow.input` and `Dataflow.capture`.

## Input

The first thing we need to define on a Dataflow is the `input`.
We do this by passing a specific input configuration to the `input` operator.

Bytewax offers two input configurations (more to come in the future):
- ManualInputConfig
- KafkaInputConfig

[ManualInputConfig](/apidocs/bytewax.inputs#bytewax.inputs.ManualInputConfig) allows you to define the input **builder** as a python function.  
The input builder is a function that is called on each worker and will produce the input for that worker.  
It accepts three parameters: `worker_index, worker_count, resume_state`. The input builder function should return an iterable that yields a tuple of `(state, event)`, and it must know how to skip ahead in its input data to start at that `resume_state`. The `resume_state` parameter allows you to configure recovery should a failure interrupt a stateful operation, such as `stateful_map`.  
You can use any existing python library to extract the data you need inside the builder function.  
You then pass the builder to `ManualInputConfig` and the generator will be polled to retrieve new items.

[KafkaInputConfig](/apidocs/bytewax.inputs#bytewax.inputs.KafkaInputConfig) is a specific input configuration tailored for Apache Kafka (and kafka-api compatible platforms, like Redpanda).  
This input generator is provided by the Rust Bytewax library, and while you can build a custom Kafka input using the `ManualInputConfig` and any python library to talk to Kafka, this is the recommended approach, since it also automatically handles recovery.  
`KafkaInputConfig` accepts four parameters: a list of brokers, a topic, a tail boolean and a starting_offset. See our API docs for `bytewax.inputs` for more on a Kafka configuration.

## Output

Output too is configured using a configuration and the `capture` operator.

Bytewax offers three output configurations:
- ManualOutputConfig
- StdOutputConfig

[ManualOutputConfig](/apidocs/bytewax.outputs#bytewax.outputs.ManualOutputConfig) requires a **builder** function that is called on each worker and will handle the output for that worker. The output builder function should return a callback **output handler** function that can be called with each item of output produced.

[StdOutputConfig](/apidocs/bytewax.outputs#bytewax.outputs.StdOutputConfig) just prints all the output to the standard output, and it does not require any other configuration.
