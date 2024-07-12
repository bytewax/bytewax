# Handling Missing Values in Data Streams

![Thumbnail](missing_values.svg)


Given that the real world is never ideal, our datasets are often far from perfect and contain missing values. In order to build accurate machine learning models, we must address missing values. When data is missing, our understanding of the system is incomplete, potentially due to issues such as sensor failure, network issues, or optional data fields. In real-time applications like self-driving cars, heating systems, and smart devices, incorrect interpretation of missing data can have severe consequences. The process for dealing with missing value is called imputation and we will demonstrate how you can build a custom window to deal with this in Bytewax.

| Skill Level | Time to Complete | Level |
| ----------- | ---------------- | ----- |
| Basic, no prior knowledge requirement | Approx. 15 Min | Beginner |


## Your Takeaway

Learn how to create a custom sliding window with the stateful map operator to impute, or fill in missing values, with an estimate using numpy

## Resources

<gh-path:/docs/tutorials/search-logs/missing_data_dataflow.py>

## Important Concepts
Bytewax is based around the concept of a dataflow. A dataflow is made up of a sequence of operators that interact with data that is “flowing” through it. The dataflow is a directed graph where the nodes are operators and the edges are the data that flows between them. The dataflow is a powerful abstraction that allows you to build complex data processing pipelines with ease.

* **Stateless vs. Stateful** - In Bytewax, operators can be either stateless or stateful. A stateless operator is one that processes each value it sees in isolation. A stateful operator, on the other hand, maintains some state between items and allows you to modify the state. This state can be used to store information about the data that has been seen so far, or to store the results of some computation.

* **Workers** - A worker is a single thread of execution that runs a dataflow. Workers are responsible for executing the operators in the dataflow and passing data between them. Workers can run on a single machine, or they can be distributed across multiple machines. See <project:#execution-model> for more information.

## Goal

Generate a dataflow that will impute missing values in a stream of data of random integers and nan values.

We can represent our dataflow - called map_eg through this diagram, in which the data flows through three key steps:

* input: includes a random integer between 0 and 10 or a numpy nan value for every 5th value
* stateful map to impute the values: we will create a custom window to impute the missing values
* output: output the data and the imputed value to standard output

```mermaid
graph TD;
    subgraph map_eg_input
        map_eg_input-->input_down[map_eg.input_down portout]
    end
    input_down-->impute_up[map_eg.impute_up]
    subgraph map_eg_impute
        impute_up-->map_eg_impute
        map_eg_impute-->impute_down[map_eg.impute_down portout]
    end
    impute_down-->output_up[map_eg.output_up]
    subgraph map_eg_output
        output_up-->map_eg_output
    end
```


During data input, we will generate random integers and `NaN` values. In the stateful map, we will create a custom window to impute the missing values. Finally, we will output the data and the imputed value to standard output.

Let's get started!

## Imports and Setup

Before we begin, let's import the necessary modules and set up the environment for building the dataflow.

Complete installation - we recommend using a virtual environment to manage your Python dependencies. You can install Bytewax using pip:

```{code-block} console
:substitutions:
$ python -m venv venv
$ ./venv/bin/activate
(venv) $ pip install bytewax==|version|
```

Now, let's import the required modules and set up the environment for building the dataflow.

```{literalinclude} missing_data_dataflow.py
:caption: dataflow.py
:language: python
:start-after: start-imports
:end-before: end-imports
:lineno-match:
```

## Input Code

For this example we will mock up some data that will yield either a random integer between 0 and 10, or a numpy nan value for every 5th value we generate.

To simulate the generation of random numbers and `NaN` values, we will create a class called `RandomNumpyData`. This class will generate a random integer between 0 and 10, or a `NaN` value for every 5th value. We will design this class to inherit from {py:obj}`~bytewax.inputs.StatelessSourcePartition`, allowing us to create our input as a stateless Bytewax input partition.

Next, we will create the `RandomNumpyInput` class, which will act as a wrapper for `RandomNumpyData`. This wrapper facilitates dynamic data generation based on the distribution of work across multiple workers in a distributed processing system. When the data source needs to be instantiated (e.g., at the start of a processing step or when distributed across workers), each worker will create and return an instance of `RandomNumpyData`.

```{literalinclude} missing_data_dataflow.py
:caption: dataflow.py
:language: python
:start-after: start-random-data
:end-before: end-random-data
:lineno-match:
```

When the data source needs to be built (e.g., at the start of a processing step or when distributed across workers), it simply creates and returns an instance of `RandomNumpyData`.

We can then initialize our dataflow with `RandomNumpyInput` as the input source.

With this we complete the input part of our dataflow. We will now turn our attention to how we can set up custom windowing using the stateful map operator.

```{literalinclude} missing_data_dataflow.py
:caption: dataflow.py
:language: python
:start-after: start-dataflow
:end-before: end-dataflow
:lineno-match:
```

## Custom Window Using Stateful Map

Before we dive into the code, it is important to understand the stateful map operator. Stateful map is a one-to-one transformation of values in (key, value) pairs, but allows you to reference a persistent state for each key when doing the transformation. For more information see the API docs {py:obj}`~bytewax.operators.stateful_map`.

In our case our key will be the same for the entire stream because we only have one stream of data in this example. So, we have some code that will create a `WindowedArray` object in the builder function and then use the update function to impute the mean. This class allows us to maintain a sliding window of the most recent values in a sequence, allowing for the computation of window-based statistics.

Let’s unpack the code. When our class `WindowedArray` is initialized, it will create an empty Numpy array with `dtype` of object. The reason the the object datatype is that this will allow us to add both integers and `Nan` values. For each new data point that we receive, we will instruct the stateful map operator to use the impute_value method that will check if the value is nan and then calculate the mean from the last n objects, n being the size of array of values we've "remembered". In other words, how many of the values we care about and want to use in our calculation. this will vary on the application itself. It will also add the value to our window (`last_n`).


```{literalinclude} missing_data_dataflow.py
:caption: dataflow.py
:language: python
:start-after: start-windowed-array
:end-before: end-windowed-array
:lineno-match:
```

We also create a `StatefulImputer` wrapper class that will create an instance of `WindowedArray` and return it when the stateful map operator needs to be built. This is useful as the stateful map operator {py:obj}`~bytewax.operators.stateful_map` requires stateful operations to be encapsulated in objects (for example, in a streaming data processing framework where state needs to be maintained across batches of data), thus the StatefulImputer provides a convenient wrapper to maintain the state.

```{literalinclude} missing_data_dataflow.py
:caption: dataflow.py
:language: python
:start-after: start-stateful-imputer
:end-before: end-stateful-imputer
:lineno-match:
```

We can then add the {py:obj}`~bytewax.operators.stateful_map` step to our dataflow.

```{literalinclude} missing_data_dataflow.py
:caption: dataflow.py
:language: python
:start-after: start-dataflow-inpute
:end-before: end-dataflow-inpute
:lineno-match:
```

## Output Results

Next up we will use the {py:obj}`~bytewax.operators.output` operator to write our code to a sink, in this case {py:obj}`~bytewax.connectors.stdio.StdOutSink`. This is not going to do anything sophisticated, just print the data and the imputed value to standard output.

```{literalinclude} missing_data_dataflow.py
:caption: dataflow.py
:language: python
:start-after: start-output
:end-before: end-output
:lineno-match:
```

## Running our dataflow

That’s it! To run the code, use the following invocation:

```console
> python -m bytewax.run dataflow:flow
```

This yields:

```console
('data', (nan, nan))
('data', (10, 10))
('data', (1, 1))
('data', (4, 4))
('data', (10, 10))
('data', (nan, 6.25))
('data', (5, 5))
('data', (10, 10))
('data', (8, 8))
('data', (5, 5))
('data', (nan, 6.625))
```

On the left hand side of the tuple we see the original value, and on the right hand side the imputed value. Note that the imputed value is calculated based on the last 4 values in the window. Note also that if the first value in the stream is an empty value, the imputed value will be the same as the first value in the stream.

## Summary

In this example, we learned how to impute missing values from a datastream using Bytewax and numpy through custom classes, the dataflow and stateful map operators.
