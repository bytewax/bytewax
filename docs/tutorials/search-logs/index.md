# Building Sessions from Search Logs

Here is an introductory example of using Bytewax to turn an incoming stream of event logs from a hypothetical search engine into metrics over search sessions. In this example, we're going to focus on the dataflow itself and aggregating state.

| Skill Level | Time to Complete | Level |
| ----------- | ---------------- | ----- |
| Basic, no prior knowledge requirement | Approx. 25 Min | Beginner |


## Your Takeaway

This tutorial will teach you how to use Bytewax to detect and calculate the Click-Through Rate (CTR) on a custom session window on streaming data using a window and then calculate metrics downstream.

## Resources

<https://github.com/bytewax/bytewax/blob/main/docs/tutorials/search-logs/dataflow.py>

## Introduction and problem statement

One of the most critical metrics in evaluating the effectiveness of online platforms, particularly search engines, is the Click-Through Rate (CTR). The CTR is a measure of how frequently users engage with search results or advertisements, making it an indispensable metric for digital marketers, web developers, and data analysts.

This relevance of CTR extends to any enterprise aiming to understand user behavior, refine content relevancy, and ultimately, increase the profitability of online activities. As such, efficiently calculating and analyzing CTR is not only essential for enhancing user experience but also for driving strategic business decisions. The challenge, however, lies in accurately aggregating and processing streaming data to generate timely and actionable insights.

Our focus on developing a dataflow using Bytewax—an open-source Python framework for streaming data processing—addresses this challenge head-on. Bytewax allows for the real-time processing of large volumes of event data, which is particularly beneficial for organizations dealing with continuous streams of user interactions. This tutorial is specifically relevant for:

- Digital Marketers: Who need to analyze user interaction to optimize ad placements and content strategy effectively.

- Data Analysts and Scientists: Who require robust tools to process and interpret user data to derive insights that drive business intelligence.

- Web Developers: Focused on improving site architecture and user interface to enhance user engagement and satisfaction.

- Product Managers: Who oversee digital platforms and are responsible for increasing user engagement and retention through data-driven methodologies.

## Strategy and Assumptions

In this tutorial, we will demonstrate how to build a dataflow using Bytewax to process streaming data from a hypothetical search engine. The dataflow will be designed to calculate the Click-Through Rate (CTR) for each search session, providing a comprehensive overview of user engagement with search results.

The key steps involved in this process include:

- Defining a data model/schema for incoming events.
- Generating input data to simulate user interactions.
- Implementing logic functions to calculate CTR for each search session.
- Creating a dataflow that incorporates windowing to process the incoming event stream.
- Executing the dataflow to generate actionable insights.


### Assumptions

- Searches are per-user, so we need to divvy up events by user.
- Searches don't span user sessions, so we should calculate user sessions first.
- Sessions without a search shouldn't contribute.
- Calculate one metric: **click through rate** (or **CTR**), if a user clicked on any result in a search.

</details>


## Imports and Setup

Before we begin, let's import the necessary modules and set up the environment for building the dataflow.

Complete installation - we recommend using a virtual environment to manage your Python dependencies. You can install Bytewax using pip:

```{code-block} console
:substitutions:
$ pip install bytewax==|version|
```

Now, let's import the required modules and set up the environment for building the dataflow.

```{literalinclude} dataflow.py
:language: python
:start-after: start-imports
:end-before: end-imports
:lineno-match:
```

## Creating our Dataflow

A dataflow is the unit of work in Bytewax. Dataflows are data-parallel directed acyclic graphs that are made up of processing steps. Each step in the dataflow is an operator that processes data in some way. In this example, we will create a dataflow to process the incoming event stream and calculate the Click-Through Rate (CTR) for each search session.

We can initialize the dataflow as follows:

```{literalinclude} dataflow.py
:language: python
:start-after: start-dataflow
:end-before: end-dataflow
:lineno-match:
```

## Data Model

In this example, we will define a data model for the incoming events, generate input data to simulate user interactions, and implement logic functions to calculate the Click-Through Rate (CTR) for each search session. We will then create a dataflow to process the incoming event stream and execute it to generate actionable insights.


Let's start by defining a data model/schema for our incoming events. We'll make model classes for all the relevant events we'd want to monitor.

```{literalinclude} dataflow.py
:language: python
:start-after: start-dataclasses
:end-before: end-dataclasses
:lineno-match:
```

In a production system, these might come from external schema or be auto generated.

Once the data model is defined, we can move on to generating input data to simulate user interactions. This will allow us to test our dataflow and logic functions before deploying them in a live environment. Let's create 2 users and simulate their click activity as follows:

```{literalinclude} dataflow.py
:language: python
:start-after: start-simulated-events
:end-before: end-simulated-events
:lineno-match:
```

The client events will constitute the data input for our dataflow, simulating user interactions with the search engine. The events will include user IDs, search queries, search results, and click activity. This data will be used to calculate the Click-Through Rate (CTR) for each search session.

Once the events have been created, we can add them to the dataflow as input data. This will allow us to process the events and calculate the CTR for each search session.

Bytewax has a {py:obj}`~bytewax.testing.TestingSource` class that takes an enumerable list of events that it will emit, one at a time into our dataflow. {py:obj}`~bytewax.testing.TestingSource` will be initialized with the list of events we created earlier in the variable `client_events`. This will be our input source for the dataflow.

```{literalinclude} dataflow.py
:language: python
:start-after: start-feed-input
:end-before: end-feed-input
:lineno-match:
```

The next step is to define the logic functions that will process the incoming events and calculate the CTR for each search session. We will define two helper functions: `user_event`, and `calculate_ctr`, these functions will be used to process the incoming events and calculate the CTR for each search session.

1. The `user_event` function will extract the user ID from the incoming event and use it as the key for grouping the events by user.

```{literalinclude} dataflow.py
:language: python
:start-after: start-user-event
:end-before: end-user-event
:lineno-match:
```

All of Bytewax's operators are in the {py:obj}`bytewax.operators` module, which we've imported here by a shorter name, `op`. We are using the {py:obj}`~bytewax.operators.map` operator - it takes each event from the input and applies the `user_event` function. This function is transforming each event into a format suitable for grouping by user (key-value pairs where the key is the user ID).


2. The `calculate_ctr` function will calculate the Click-Through Rate (CTR) for each search session based on the click activity in the session.

```{literalinclude} dataflow.py
:language: python
:start-after: start-calc-ctr
:end-before: end-calc-ctr
:lineno-match:
```

We will now turn our attention to windowing the data. In a dataflow pipeline, the role of collecting windowed data, particularly after mapping user events, is crucial for segmenting the continuous stream of events into manageable, discrete chunks based on time or event characteristics. This step enables the aggregation and analysis of events within specific time frames or sessions, which is essential for understanding patterns, behaviors, and trends over time.

After user events are mapped, typically transforming each event into a tuple of (user_id, event_data), the next step is to group these events into windows. In this example, we will use a {py:obj}`~bytewax.operators.windowing.SessionWindower` to group events by user sessions. We will also use an {py:obj}`~bytewax.operators.windowing.EventClock` to manage the timing and order of events as they are processed through the dataflow.



```{literalinclude} dataflow.py
:language: python
:start-after: start-windowing
:end-before: end-windowing
:lineno-match:
```

* The {py:obj}`~bytewax.operators.windowing.EventClock` is responsible for managing the timing and order of events as they are processed through the dataflow. It's crucial for ensuring that events are handled accurately in real-time or near-real-time streaming applications.

* The {py:obj}`~bytewax.operators.windowing.SessionWindower` specifies how to group these timestamped events into sessions. A session window collects all events that occur within a specified gap of each other, allowing for dynamic window sizes based on the flow of incoming data

These configurations ensure that your dataflow can handle streaming data effectively, capturing user behavior in sessions and calculating relevant metrics like CTR in a way that is timely and reflective of actual user interactions. This setup is ideal for scenarios where user engagement metrics over time are critical, such as in digital marketing analysis, website optimization, or interactive application monitoring.

Once the events are grouped into windows, further processing can be performed on these grouped events, such as calculating metrics like CTR within each session. This step often involves applying additional functions to the windowed data to extract insights, such as counting clicks and searches to compute the CTR.

We can apply the `calculate_ctr` function to the windowed data to calculate the CTR for each search session. This step will provide a comprehensive overview of user engagement with search results, enabling data analysts, marketers, and developers to evaluate the effectiveness of search algorithms, content relevance, and user experience.

```{literalinclude} dataflow.py
:language: python
:start-after: start-calc-map
:end-before: end-calc-map
:lineno-match:
```

### Returning results

Finally, we can add an output step to our dataflow to return the results of the CTR calculation. This step will emit the CTR for each search session, providing a comprehensive overview of user engagement with search results.

```{literalinclude} dataflow.py
:language: python
:start-after: start-output
:end-before: end-output
:lineno-match:
```

## Execution

Now we're done with defining the dataflow. Let's run it! We can see that the CTR for each search session is calculated based on the simulated user interactions.

```console
$ python -m bytewax.run dataflow:flow
User 1: 1 searches, 1 clicks
User 2: 1 searches, 2 clicks
('1', 1.0)
('2', 2.0)
```

## Summary

That’s it, now you have an understanding of how you can build custom session windows, how you can define data classes to be used in Bytewax, and how to calculate the click-through rate on a stream of logs.
