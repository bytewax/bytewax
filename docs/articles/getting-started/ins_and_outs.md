# Inputs

The first thing we need to define on a Dataflow is the `input`.
We do this by passing a specific input configuration to the `input` operator.

Bytewax offers two input configurations (more to come in the future):
- ManualInputConfig
- KafkaInputConfig

[ManualInputConfig]() allows you to define the input **builder** as a python function.
The input builder is a function that is called on each worker and will produce the input for that worker.
This input builder accepts three parameters: `worker_index, worker_count, resume_state`. The input builder function should return an iterable that yields a tuple of `(state, event)`.
The input builder must know how to skip ahead in its input data to start at that `resume_state`. The `resume_state` parameter allows you to configure recovery should a failure interrupt a stateful operation, such as `stateful_map`.

You can use any existing python library to extract the data you need.
You then pass the builder to `ManualInputConfig` and that will be polled to retrieve new items.

[KafkaInputConfig]() is a specific input configuration tailored for Kafka (and kafka-api compatible platforms, like Redpanda).
This input generator is in the Rust side of Bytewax, and while you can build a custom Kafka input using the [ManualInputConfig]() and any python library to talk to Kafka, this is the recommended approach, since it also automatically handles recovery.
`KafkaInputConfig` accepts four parameters: a list of brokers, a topic, a tail boolean and a starting_offset. See our API docs for `bytewax.inputs` for more on a Kafka configuration.



### Input Configuration

You can manually configure your dataflow's input with an input builder and a `ManualInputConfig` or use a Kafka stream with `KafkaInputConfig`. You'll configure these with the `Dataflow.input` operator.

For a manual input source, you'll define an input **builder**, a function that is called on each worker and will produce the input for that worker. This input builder accepts three parameters: `worker_index, worker_count, resume_state`. The input builder function should return an iterable that yields a tuple of `(state, event)` and will be the `input_builder` parameter on the `ManualInputConfig` passed to the dataflow. The input builder must know how to skip ahead in its input data to start at that `resume_state`. The `resume_state` parameter allows you to configure recovery should a failure interrupt a stateful operation, such as `stateful_map`.

A `KafkaInputConfig` accepts four parameters: a list of brokers, a topic, a tail boolean and a starting_offset. See our API docs for `bytewax.inputs` for more on a Kafka configuration.


As in `spawn_cluster()`, the input builder function passed to a manual input configuration should return an iterable that yields `(state, item)` tuples. If you are using a `KafkaInputConfig`, state recovery will be managed for you.

### Output

Output is configured similarly to input, using a configuration and the `capture` operator. Akin to the `ManualInputConfig`, the `ManualOutputConfig` requires **builder** function that is called on each worker and will handle the output for that worker. However, the output builder function should return a callback **output handler** function that can be called with each item of output produced.

(ManualOutputConfig) `run_main()` collects all output into a list internally first, thus it will not support infinite, larger than memory datasets.
