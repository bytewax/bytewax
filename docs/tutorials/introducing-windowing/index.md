# Introducing Windowing: Processing smoothie orders using windowing operators

![Currency symbol](windowing-smoothie.svg)

In this example we are going to walk through how you can incorporate windowing in your Bytewax dataflow through Bytewax windowing operators. This is part II in our operator introductory tutorial series, for the first part visit [Processing smoothie orders with Bytewax operators](../introducing-operators/index.md).


| Skill Level | Time to Complete | Level |
| ----------- | ---------------- | ----- |
| Intermediate Python programming and Bytewax operators | Approx. 25 Min | Intermediate |

## Your Takeaway

This tutorial will teach you how to chain Bytewax windowing operators to wrangle streams of data. By the end of this tutorial you will have an understanding of how to incorporate windowing techniques into a Bytewax dataflow.

## Resources

<gh-path:/docs/tutorials/introducing-windowing/tumbling_dataflow.py>
<gh-path:/docs/tutorials/introducing-windowing/sliding_dataflow.py>
<gh-path:/docs/tutorials/introducing-windowing/session_dataflow.py>
<gh-path:/docs/tutorials/introducing-windowing/smoothie_order_l.csv>

In [our blog on windowing](https://bytewax.io/blog/windowing-in-streaming-applications), we cover in depth key concepts and assumptions we make to ensure a smooth experience incorporating windowing techniques. If you're new to windowing, we recommend you take a look at the blog and the [cheatsheet our team prepared](https://images.production.bytewax.io/wind1_74099d4d0d.png).

## Set up and imports

Before we begin, let's import the necessary modules and set up the environment for building the dataflow.

Complete installation - we recommend using a virtual environment to manage your Python dependencies. You can install Bytewax using pip:

```{code-block} console
:substitutions:
$ python -m venv venv
$ ./venv/bin/activate
(venv) $ pip install bytewax==|version|
```

Now, let's import the required modules and set up the environment for building the dataflow.

```{literalinclude} tumbling_dataflow.py
:caption: tumbling_dataflow.py
:language: python
:start-after: start-imports
:end-before: end-imports
:lineno-match:
```

We're now ready! Let's jump in.

## Processing smoothie orders with windowing

Here is a mock dataset containing smoothie order data. Let's apply windowing techniques on this data to answer specific questions.

<gh-path:/docs/tutorials/introducing-windowing/smoothie_order_l.csv>

**Objectives:**

1. Count the number of smoothie orders every 30 minutes starting from 08:00 AM until 12:30 PM.
2. Track smoothie orders using overlapping windows to smooth fluctuations in order rates and calculate moving number of orders.
3. Group smoothie orders that occur closely together in time into dynamic session windows, which close after a period of inactivity.

Let's first initialize our dataflow:

```{literalinclude} tumbling_dataflow.py
:caption: tumbling_dataflow.py
:language: python
:start-after: start-dataflow
:end-before: end-dataflow
:lineno-match:
```

We can then read the content of the CSV file through the {py:obj}`~bytewax.operators.input` operator along with the {py:obj}`~bytewax.connectors.files.CSVSource`

```{literalinclude} tumbling_dataflow.py
:caption: tumbling_dataflow.py
:language: python
:start-after: start-input
:end-before: end-input
:lineno-match:
```

By executing

```console
python -m bytewax.run tumbling_dataflow:flow
```

we can see the stream of processed information (showing first few entries):

```console
windowing_operators_examples.see_data: {'order_id': '1', 'time': '2024-08-29 08:00:00', 'order_requested': 'Green Machine', 'ingredients': 'spinach;banana;almond milk;chia seeds'}
windowing_operators_examples.see_data: {'order_id': '2', 'time': '2024-08-29 08:05:00', 'order_requested': 'Berry Blast', 'ingredients': 'strawberry;blueberry;greek yogurt;honey'}
windowing_operators_examples.see_data: {'order_id': '3', 'time': '2024-08-29 08:10:00', 'order_requested': 'Tropical Twist', 'ingredients': 'mango;pineapple;coconut water;flax seeds'}
```

Let's define our clock and window type next.

## Choosing the appropriate windowing technique according to the objective

For each of our three objectives, we will choose to work with a specific type of window as outlined in the table below:

| Objective| Type of Window| Specifications| Aggregation or computation |
|-|-|-|-|
| Count number of orders every 30 minutes from 8:00am to 12:30pm | Tumbling Window | Length of window is 30 minutes, aligned to the date of the events | Counting |
| Track smoothie orders using overlapping windows for moving number of orders | Sliding Window | Length of window can be chosen depending on the desired window size | Counting|
|  Group smoothie orders that occur closely together in time into dynamic session windows, windows close after inactivity | Session Window | Period if inactivity can be defined by the user |Collecting |

Let's now define the appropriate code for each objective.

### Objective 1: Count the number of smoothie orders every 30 minutes from 08:00 AM to 12:30 PM

We can incorporate our clock definition and window definition as follows:

```{literalinclude} tumbling_dataflow.py
:caption: tumbling_dataflow.py
:language: python
:start-after: start-objective-1
:end-before: end-objective-1
:lineno-match:
```

Executing our dataflow then returns the following:

```console
python -m bytewax.run tumbling_dataflow:flow

windowing_operators_examples.windowed_count_orders: ('total_orders', (0, 5))
windowing_operators_examples.windowed_count_orders: ('total_orders', (1, 7))
windowing_operators_examples.windowed_count_orders: ('total_orders', (2, 6))
windowing_operators_examples.windowed_count_orders: ('total_orders', (3, 5))
windowing_operators_examples.windowed_count_orders: ('total_orders', (4, 5))
windowing_operators_examples.windowed_count_orders: ('total_orders', (5, 4))
windowing_operators_examples.windowed_count_orders: ('total_orders', (6, 6))
windowing_operators_examples.windowed_count_orders: ('total_orders', (7, 4))
windowing_operators_examples.windowed_count_orders: ('total_orders', (8, 8))
```

The first element in the tuple corresponds to the window ID, and the second element corresponds to the number of orders. Let's format our output:

```{literalinclude} tumbling_dataflow.py
:caption: tumbling_dataflow.py
:language: python
:start-after: start-format-1
:end-before: end-format-1
:lineno-match:
```

Executing our dataflow then returns the following:

```console
python -m bytewax.run tumbling_dataflow:flow

windowing_operators_examples.formatted_objective_1: 'Window 0    (08:00    - 08:30): 5 orders'
windowing_operators_examples.formatted_objective_1: 'Window 1    (08:30    - 09:00): 7 orders'
windowing_operators_examples.formatted_objective_1: 'Window 2    (09:00    - 09:30): 6 orders'
windowing_operators_examples.formatted_objective_1: 'Window 3    (09:30    - 10:00): 5 orders'
windowing_operators_examples.formatted_objective_1: 'Window 4    (10:00    - 10:30): 5 orders'
windowing_operators_examples.formatted_objective_1: 'Window 5    (10:30    - 11:00): 4 orders'
windowing_operators_examples.formatted_objective_1: 'Window 6    (11:00    - 11:30): 6 orders'
windowing_operators_examples.formatted_objective_1: 'Window 7    (11:30    - 12:00): 4 orders'
windowing_operators_examples.formatted_objective_1: 'Window 8    (12:00    - 12:30): 8 orders'
```

With the {py:obj}`~bytewax.operators.windowing.TumblingWindower` and {py:obj}`~bytewax.operators.windowing.count_window` operators, we were able to create 30 minute chunks and count the orders within those chunks. Let's now take a look at our second objective.

### Objective 2: Track smoothie orders using overlapping windows for moving number of orders

We will use the {py:obj}`~bytewax.operators.windowing.SlidingWindower` and {py:obj}`~bytewax.operators.windowing.count_window` operators to acomplish our goal. In the code below, we defined a sliding window with a length of 1 hour with an offset of 15 minutes. It will count the total orders in each window.
We've also added a formatting function.

```{literalinclude} sliding_dataflow.py
:caption: sliding_dataflow.py
:language: python
:start-after: start-objective-2
:end-before: end-objective-2
:lineno-match:
```

Executing our dataflow then returns the following:

```console
python -m bytewax.run sliding_dataflow:flow

windowing_operators_examples.formatted_objective_2: 'Window -3    (07:15 -    08:15): 3 orders'
windowing_operators_examples.formatted_objective_2: 'Window -2    (07:30 -    08:30): 5 orders'
windowing_operators_examples.formatted_objective_2: 'Window -1    (07:45 -    08:45): 9 orders'
windowing_operators_examples.formatted_objective_2: 'Window 0    (08:00 -    09:00): 12 orders'
windowing_operators_examples.formatted_objective_2: 'Window 1    (08:15 -    09:15): 12 orders'
windowing_operators_examples.formatted_objective_2: 'Window 2    (08:30 -    09:30): 13 orders'
windowing_operators_examples.formatted_objective_2: 'Window 3    (08:45 -    09:45): 11 orders'
windowing_operators_examples.formatted_objective_2: 'Window 4    (09:00 -    10:00): 11 orders'
windowing_operators_examples.formatted_objective_2: 'Window 5    (09:15 -    10:15): 11 orders'
windowing_operators_examples.formatted_objective_2: 'Window 6    (09:30 -    10:30): 10 orders'
windowing_operators_examples.formatted_objective_2: 'Window 7    (09:45 -    10:45): 10 orders'
windowing_operators_examples.formatted_objective_2: 'Window 8    (10:00 -    11:00): 9 orders'
windowing_operators_examples.formatted_objective_2: 'Window 9    (10:15 -    11:15): 8 orders'
windowing_operators_examples.formatted_objective_2: 'Window 10    (10:30 -    11:30): 10 orders'
windowing_operators_examples.formatted_objective_2: 'Window 11    (10:45 -    11:45): 10 orders'
windowing_operators_examples.formatted_objective_2: 'Window 12    (11:00 -    12:00): 10 orders'
windowing_operators_examples.formatted_objective_2: 'Window 13    (11:15 -    12:15): 12 orders'
windowing_operators_examples.formatted_objective_2: 'Window 14    (11:30 -    12:30): 12 orders'
windowing_operators_examples.formatted_objective_2: 'Window 15    (11:45 -    12:45): 10 orders'
windowing_operators_examples.formatted_objective_2: 'Window 16    (12:00 -    13:00): 8 orders'
windowing_operators_examples.formatted_objective_2: 'Window 17    (12:15 -    13:15): 4 orders'
```

There seem to be peak times - early morning and around lunch time. Let's complete our final objective.

### Objective 3: Group smoothie orders that occur closely together in time into dynamic session windows, windows close after inactivity

We will use the {py:obj}`~bytewax.operators.windowing.SessionWindower` and {py:obj}`~bytewax.operators.windowing.collect_window` operators to acomplish our goal.

```{literalinclude} session_dataflow.py
:caption: session_dataflow.py
:language: python
:start-after: start-objective-3
:end-before: end-objective-3
:lineno-match:
```

Executing our dataflow then returns the following:

```console
python -m bytewax.run session_dataflow:flow

windowing_operators_examples.output: 'Session 0    (08:00 - 08:15): Order ID [1, 2, 3, 4]'
windowing_operators_examples.output: 'Session 1    (08:23 - 08:23): Order ID [5]'
windowing_operators_examples.output: 'Session 2    (08:31 - 08:38): Order ID [6, 7, 8]'
windowing_operators_examples.output: 'Session 3    (08:44 - 08:56): Order ID [9, 10, 11, 12]'
windowing_operators_examples.output: 'Session 4    (09:02 - 09:22): Order ID [13, 14, 15, 16, 17]'
windowing_operators_examples.output: 'Session 5    (09:28 - 09:28): Order ID [18]'
windowing_operators_examples.output: 'Session 6    (09:36 - 09:39): Order ID [19, 20]'
windowing_operators_examples.output: 'Session 7    (09:45 - 09:45): Order ID [21]'
windowing_operators_examples.output: 'Session 8    (09:51 - 09:51): Order ID [22]'
windowing_operators_examples.output: 'Session 9    (09:58 - 09:58): Order ID [23]'
windowing_operators_examples.output: 'Session 10    (10:05 - 10:14): Order ID [24, 25, 26]'
windowing_operators_examples.output: 'Session 11    (10:20 - 10:20): Order ID [27]'
windowing_operators_examples.output: 'Session 12    (10:28 - 10:33): Order ID [28, 29]'
windowing_operators_examples.output: 'Session 13    (10:41 - 10:41): Order ID [30]'
windowing_operators_examples.output: 'Session 14    (10:48 - 10:48): Order ID [31]'
windowing_operators_examples.output: 'Session 15    (10:56 - 10:56): Order ID [32]'
windowing_operators_examples.output: 'Session 16    (11:02 - 11:02): Order ID [33]'
windowing_operators_examples.output: 'Session 17    (11:09 - 11:09): Order ID [34]'
windowing_operators_examples.output: 'Session 18    (11:16 - 11:19): Order ID [35, 36]'
windowing_operators_examples.output: 'Session 19    (11:25 - 11:28): Order ID [37, 38]'
windowing_operators_examples.output: 'Session 20    (11:36 - 11:41): Order ID [39, 40]'
windowing_operators_examples.output: 'Session 21    (11:48 - 11:48): Order ID [41]'
windowing_operators_examples.output: 'Session 22    (11:56 - 12:29): Order ID [42, 43, 44, 45, 46, 47, 48, 49, 50]'
```

We can see that the busiest window was between 11:56 and 12:29, around lunch time!

## Summary
That’s it, now you have an understanding of how you can chain Bytewax operators along with windowing operators to perform data transformations, aggregations and obtain insights into time-series-based data.
