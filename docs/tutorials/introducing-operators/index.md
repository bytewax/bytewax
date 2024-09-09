# Processing smoothie orders with Bytewax operators

In this example we are going to walk through how you can combine operators to wrangle data through a streaming approach.


| Skill Level | Time to Complete | Level |
| ----------- | ---------------- | ----- |
| Understanding of Python programming | Approx. 25 Min | Beginner |

## Your Takeaway

This tutorial will teach you how to chain Bytewax operators to wrangle streams of data, for example by filtering specific information, augmenting the data and doing aggregations.

## Resources

<gh-path:/docs/tutorials/introducing-operators/stateless_operators_dataflow.py>
<gh-path:/docs/tutorials/introducing-operators/smoothie_orders.csv>


## Introduction to operators

Operators in Bytewax are functions that operate on elements in the stream and can be classified into different types, depending on the type of transformation.

When working with distributed data streams and pipelines, the concept of state becomes crucial, especially for tasks that require tracking information across multiple events. In simple terms, state refers to data that persists across operations and stream elements, which allows for more complex and meaningful transformations beyond stateless operations like map or filter.

* **Stateless transformations** operate independently on each incoming data element without any memory of the past. For example, using a map operator to convert text to uppercase or filtering numbers based on a condition.

* **Stateful transformations**, on the other hand, require memory to maintain information about previous elements. This memory is what we call the state. Every time a new event comes in, the state is updated.

In this tutorial, we will focus on stateless transformations. Let's get started.

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

```{literalinclude} stateless_operators_dataflow.py
:caption: stateless_operators_dataflow.py
:language: python
:start-after: start-imports
:end-before: end-imports
:lineno-match:
```

We're now ready! Let's jump in.

## Example 1: Filter orders based on a specific ingredient

The dataflow will search for orders whose smoothies included bananas and cocoa powder.

We will begin by initializing the dataflow:

```{literalinclude} stateless_operators_dataflow.py
:caption: stateless_operators_dataflow.py
:language: python
:start-after: start-dataflow
:end-before: end-dataflow
:lineno-match:
```

We can then add our data as input. To achieve this, we will use the {py:obj}`~bytewax.operators.input` operator along with the {py:obj}`~bytewax.connectors.files.CSVSource` connector - this will enable us to read data from our CSV file `smoothie_orders.csv`.


```{literalinclude} stateless_operators_dataflow.py
:caption: stateless_operators_dataflow.py
:language: python
:start-after: start-input
:end-before: end-input
:lineno-match:
```

By executing

```console
python -m bytewax.run stateless_operators_dataflow:flow
```

we can see the stream of processed information (showing first few entries):

```console
init_smoothie.see_data: {'order_id': '1', 'time': '2024-08-29 08:00:00', 'order_requested': 'Green Machine', 'ingredients': 'Spinach;Banana;Almond Milk;Chia Seeds'}
init_smoothie.see_data: {'order_id': '2', 'time': '2024-08-29 08:05:00', 'order_requested': 'Berry Blast', 'ingredients': 'Strawberry;Blueberry;Greek Yogurt;Honey'}
init_smoothie.see_data: {'order_id': '3', 'time': '2024-08-29 08:10:00', 'order_requested': 'Tropical Twist', 'ingredients': 'Mango;Pineapple;Coconut Water;Flax Seeds'}
```

Let's do a simple filter. In the function below, we will check whether "Banana" and "Almond Milk" is in the "ingredients" column in the dataset. We can apply our function through the Bytewax {py:obj}`~bytewax.operators.filter` operator as follows:

```{literalinclude} stateless_operators_dataflow.py
:caption: stateless_operators_dataflow.py
:language: python
:start-after: start-filter
:end-before: end-filter
:lineno-match:
```

By executing the dataflow in the same way as before we see the following entries (showing first few):

```console
init_smoothie.filter_results: {'order_id': '1', 'time': '2024-08-29 08:00:00', 'order_requested': 'Green Machine', 'ingredients': 'Spinach;Banana;Almond Milk;Chia Seeds'}
init_smoothie.filter_results: {'order_id': '4', 'time': '2024-08-29 08:15:00', 'order_requested': 'Protein Power', 'ingredients': 'Peanut Butter;Banana;Protein Powder;Almond Milk'}
init_smoothie.filter_results: {'order_id': '5', 'time': '2024-08-29 08:23:00', 'order_requested': 'Green Machine', 'ingredients': 'Spinach;Banana;Almond Milk;Chia Seeds'}
init_smoothie.filter_results: {'order_id': '9', 'time': '2024-08-29 08:44:00', 'order_requested': 'Mocha Madness', 'ingredients': 'Coffee;Cocoa Powder;Almond Milk;Banana'}
```

## Example 2: data augmentation through caching and mapping

Let's now augment our data as a next example. We will use the Bytewax {py:obj}`~bytewax.operators.enrich_cached` operator. We will set up a `mock_price_service` function, although you may also choose to ping a remote service instead. We can use the cached price and augment the data.

```{literalinclude} stateless_operators_dataflow.py
:caption: stateless_operators_dataflow.py
:language: python
:start-after: start-enrich
:end-before: end-enrich
:lineno-match:
```

This includes a new key-value pair in the data with the price:

```console
init_smoothie.enrich_results: {'order_id': '3', 'time': '2024-08-29 08:10:00', 'order_requested': 'Tropical Twist', 'ingredients': 'Mango;Pineapple;Coconut Water;Flax Seeds', 'price': 7.49}
init_smoothie.enrich_results: {'order_id': '4', 'time': '2024-08-29 08:15:00', 'order_requested': 'Protein Power', 'ingredients': 'Peanut Butter;Banana;Protein Powder;Almond Milk', 'price': 8.99}
init_smoothie.enrich_results: {'order_id': '5', 'time': '2024-08-29 08:23:00', 'order_requested': 'Green Machine', 'ingredients': 'Spinach;Banana;Almond Milk;Chia Seeds', 'price': 5.99}
init_smoothie.enrich_results: {'order_id': '6', 'time': '2024-08-29 08:31:00', 'order_requested': 'Berry Blast', 'ingredients': 'Strawberry;Blueberry;Greek Yogurt;Honey', 'price': 6.49}
init_smoothie.enrich_results: {'order_id': '7', 'time': '2024-08-29 08:34:00', 'order_requested': 'Citrus Zing', 'ingredients': 'Orange;Lemon;Ginger;Turmeric', 'price': 6.99}
init_smoothie.enrich_results: {'order_id': '8', 'time': '2024-08-29 08:38:00', 'order_requested': 'Berry Blast', 'ingredients': 'Strawberry;Blueberry;Greek Yogurt;Honey', 'price': 6.49}
```

We can also perform specific computations on the data through the Bytewax {py:obj}`~bytewax.operators.map` operator in combination with a user-defined function like `calculate_total_price` as seen as follows:


```{literalinclude} stateless_operators_dataflow.py
:caption: stateless_operators_dataflow.py
:language: python
:start-after: start-map
:end-before: end-map
:lineno-match:
```

This adds a new key-value pair to the data called `'total_price'`:

```console
init_smoothie.inspect_final: {'order_id': '47', 'time': '2024-08-29 12:17:00', 'order_requested': 'Tropical Twist', 'ingredients': 'Mango;Pineapple;Coconut Water;Flax Seeds', 'price': 7.49, 'total_price': 8.61}
init_smoothie.inspect_final: {'order_id': '48', 'time': '2024-08-29 12:21:00', 'order_requested': 'Mocha Madness', 'ingredients': 'Coffee;Cocoa Powder;Almond Milk;Banana', 'price': 7.99, 'total_price': 9.19}
init_smoothie.inspect_final: {'order_id': '49', 'time': '2024-08-29 12:24:00', 'order_requested': 'Green Machine', 'ingredients': 'Spinach;Banana;Almond Milk;Chia Seeds', 'price': 5.99, 'total_price': 6.89}
init_smoothie.inspect_final: {'order_id': '50', 'time': '2024-08-29 12:29:00', 'order_requested': 'Protein Power', 'ingredients': 'Peanut Butter;Banana;Protein Powder;Almond Milk', 'price': 8.99, 'total_price': 10.34}
```

## Example 3: calculating total number of orders and total revenue per type of smoothie

Let's now take a look at the total number of smoothies ordered on the dataset through the Bytewax {py:obj}`~bytewax.operators.count_final` operator

```{literalinclude} stateless_operators_dataflow.py
:caption: stateless_operators_dataflow.py
:language: python
:start-after: start-count-final
:end-before: end-count-final
:lineno-match:
```

This returns:

```console
init_smoothie.inspect_final_count: ('Berry Blast', 7)
init_smoothie.inspect_final_count: ('Citrus Zing', 10)
init_smoothie.inspect_final_count: ('Green Machine', 8)
init_smoothie.inspect_final_count: ('Mocha Madness', 8)
init_smoothie.inspect_final_count: ('Morning Glow', 1)
init_smoothie.inspect_final_count: ('Nutty Delight', 2)
init_smoothie.inspect_final_count: ('Protein Power', 9)
init_smoothie.inspect_final_count: ('Tropical Twist', 5)
```

To finalize, we will calculate the total revenue by taking the total number of each smoothie ordered, multiplying the count by the price and apply a tax rate:

```{literalinclude} stateless_operators_dataflow.py
:caption: stateless_operators_dataflow.py
:language: python
:start-after: start-total-revenue
:end-before: end-total-revenue
:lineno-match:
```

This returns:

```console
init_smoothie.inspect_total_revenue: ('Berry Blast', 52.24)
init_smoothie.inspect_total_revenue: ('Citrus Zing', 80.39)
init_smoothie.inspect_total_revenue: ('Green Machine', 55.11)
init_smoothie.inspect_total_revenue: ('Mocha Madness', 73.51)
init_smoothie.inspect_total_revenue: ('Morning Glow', 6.31)
init_smoothie.inspect_total_revenue: ('Nutty Delight', 19.53)
init_smoothie.inspect_total_revenue: ('Protein Power', 93.05)
init_smoothie.inspect_total_revenue: ('Tropical Twist', 43.07)
```

## Summary
That’s it, now you have an understanding of how you can chain Bytewax operators to perform stateless transformations on the data through a streaming approach.
