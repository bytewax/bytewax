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
2. Track smoothie orders using overlapping windows to smooth fluctuations in order rates and calculate moving averages.
3. Group smoothie orders that occur closely together in time into dynamic session windows, which close after a period of inactivity.
