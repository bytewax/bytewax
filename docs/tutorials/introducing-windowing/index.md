# Introducing Windowing: Processing smoothie orders using windowing operators

![Currency symbol](smoothie-orders.svg)

In this example we are going to walk through how you can incorporate windowing in your Bytewax dataflow through Bytewax windowing operators. This is part II in our operator introductory tutorial series, for the first part visit [Processing smoothie orders with Bytewax operators](../introducing-operators/index.md).


| Skill Level | Time to Complete | Level |
| ----------- | ---------------- | ----- |
| Intermediate Python programming and Bytewax operators | Approx. 25 Min | Intermediate |

## Your Takeaway

This tutorial will teach you how to chain Bytewax windowing operators to wrangle streams of data. By the end of this tutorial you will have an understanding of how to incorporate windowing techniques into a Bytewax dataflow.

## Resources

<gh-path:/docs/tutorials/introducing-windowing/windowing_dataflow.py>
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

```{literalinclude} windowing_dataflow.py
:caption: windowing_dataflow.py
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

```{literalinclude} windowing_dataflow.py
:caption: windowing_dataflow.py
:language: python
:start-after: start-dataflow
:end-before: end-dataflow
:lineno-match:
```

We can then read the content of the CSV file through the {py:obj}`~bytewax.operators.input` operator along with the {py:obj}`~bytewax.connectors.files.CSVSource`

```{literalinclude} windowing_dataflow.py
:caption: windowing_dataflow.py
:language: python
:start-after: start-input
:end-before: end-input
:lineno-match:
```

By executing

```console
python -m bytewax.run windowing_dataflow:flow
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
| Count number of orders every 30 minutes from 8 to 12:30 | Tumbling Window | Length of window is 30 minutes, aligned to the date of the events | Counting |
| Track smoothie orders using overlapping windows for moving number of orders | Sliding Window | Length of window can be chosen depending on the desired window size | Counting|
|  Group smoothie orders that occur closely together in time into dynamic session windows, windows close after inactivity | Session Window | Period if inactivity can be defined by the user |Collecting |

Let's now define the appropriate code for each objective.

### Objective 1: Count the number of smoothie orders every 30 minutes from 08:00 AM to 12:30 PM

We can incorporate our clock definition and window definition as follows:

```{literalinclude} windowing_dataflow.py
:caption: windowing_dataflow.py
:language: python
:start-after: start-objective-1
:end-before: end-objective-1
:lineno-match:
```

Executing our dataflow then returns the following:

```console
python -m bytewax.run windowing_dataflow:flow

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

```{literalinclude} windowing_dataflow.py
:caption: windowing_dataflow.py
:language: python
:start-after: start-format-1
:end-before: end-format-1
:lineno-match:
```

Executing our dataflow then returns the following:

```console
python -m bytewax.run windowing_dataflow:flow

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
