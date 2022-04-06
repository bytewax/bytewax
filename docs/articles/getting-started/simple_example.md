Now that we've installed bytewax, let's begin with an end-to-end example. We'll start by building out a simple dataflow that performs count of words in a file.

To begin, save a copy of this text in a file called `wordcount.txt`:

```
To be, or not to be, that is the question:
Whether 'tis nobler in the mind to suffer
The slings and arrows of outrageous fortune,
Or to take arms against a sea of troubles
And by opposing end them.
```

And a copy of the code in a file called `wordcount.py`.

``` python
import re

from bytewax import Dataflow, run


def file_input():
    for line in open("wordcount.txt"):
        yield 1, line


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1
    
    
def add(count1, count2):
    return count1 + count2


flow = Dataflow()
flow.map(str.lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_epoch(add)
flow.inspect(print)


if __name__ == "__main__":
    for epoch, item in run(flow, file_input()):
        print(item)
```

## Running the example

Now that we have our program and our input, we can run our example and see the completed result:

``` bash
> python ./wordcount.py

("'tis", 1)
('that', 1)
('fortune', 1)
('opposing', 1)
('the', 3)
('end', 1)
('be', 2)
...
```

## Unpacking the program

Now that we've run our first bytewax program, let's walk through the components that we used. 

In a dataflow program, each step added to the flow will occur in the order that it is added. For our wordcount dataflow, we'll want the following steps:

- Take a line from the file
- Lowercase all characters in the line
- Split the line into words
- Count the occurrence of each word in the file
- Print out the result after all the lines have been processed

We'll start with how to get input we'll push through our dataflow.

### Take a line from the file

``` python
def file_input():
    for line in open("wordcount.txt"):
        yield 1, line
```

To provide input to our program, we've defined a [Python generator](https://docs.python.org/3/glossary.html#term-generator) that will read our input file.

This generator yields two-tuples of `1` and a line from our file. The `1` in this example is significant, but we'll talk more about it when we discuss [epochs](/getting-started/epochs/).

Let's define the steps that we want to execute for each line of input that we receive. We will add these steps to a **dataflow object**.

### Lowercase all characters in the line

If you look closely at our input, we have instances of both `To` and `to`. Let's add a step to our dataflow that transforms each line into lowercase letters. At the same time, we'll introduce our first operator, [map](/operators/operators/#map).

``` python
def lower(line):
    return line.lower()

flow.map(lower)
```

For each item that our generator produces, the map operator will use the [built-in string function `lower()`](https://docs.python.org/3.8/library/stdtypes.html#str.lower) to emit downstream a copy of the string with all characters converted to lowercase.

### Split the line into words

When our `file_input()` function is called, it will receive an entire line from our file. In order to count the words in the file, we'll need to break that line up into individual words.

Enter our `tokenize` function:

``` python
def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)
```

Here, we use a Python regular expression to split the line of input into a list of words:


``` python
line = "To be, or not to be, that is the question:"
tokenize(line)
['To', 'be', 'or', 'not', 'to', 'be', 'that', 'is', 'the', 'question']
```

To make use of `tokenize` function, we'll use the [flat map operator](/operators/operators/#flat-map):

``` python
flow.flat_map(tokenize)
```

The flat map operator defines a step which calls a function on each input item. Each word in the list we return from our function will then be emitted downstream individually.

### Build up counts

At this point in the dataflow, the items of data are the individual words.

Let's skip ahead slightly and look at one of the more useful operators in bytewax, [reduce epoch](/operators/operators#reduce-epoch).

``` python
def add(count1, count2):
    return count1 + count2
    
flow.reduce_epoch(add)
```

Its super power is that it can repeatedly combine together items into a single, aggregate value via a reducing function. Think about it like reducing a sauce while cooking; you are boiling all of the values down to something more concentrated.

In this case, we pass it the reducing function `add` which will sum together the counts of words so that the final aggregator value is the total.

How does reduce epoch know which items to combine? Part of its requirements are that the input items from the previous step in the dataflow are `(key, value)` two-tuples, and it will make sure that all values for a given key are passed to the reducing function, but two separate keys will never have their values mixed. Thus, if we make the word the key, we'll be able to get separate counts!

That explains the previous step in the dataflow:

``` python
def initial_count(word):
    return word, 1
    
flow.map(initial_count)
```

This map sets up the shape that reduce epoch needs: two-tuples where the key is the word, and the value is something we can add together. In this case, since we have a copy of a word for each instance, it represents that we should add `1` to the total count, so label that here.

The "epoch" part of reduce epoch means that we repeat the reduction in each epoch. We'll gloss over that here, but know we'll be counting all the words from the input. Epochs will be further [explained later](/getting-started/epochs/).

### Print out the counts

The last part of our dataflow program will use the [inspect operator](/operators/operators#inspect) to see the results of our reduction. For this example, we're supplying the built-in Python function `print`.

Here is the complete output when running the example:

```
("'tis", 1)
('that', 1)
('fortune', 1)
('opposing', 1)
('the', 3)
('end', 1)
('be', 2)
('and', 2)
('of', 2)
('in', 1)
('take', 1)
('arrows', 1)
('outrageous', 1)
('troubles', 1)
('is', 1)
('slings', 1)
('not', 1)
('a', 1)
('by', 1)
('them', 1)
('sea', 1)
('whether', 1)
('question', 1)
('to', 4)
('mind', 1)
('nobler', 1)
('suffer', 1)
('arms', 1)
('or', 2)
('against', 1)
```

### Running

To run the example, we'll need to introduce one more function, `run`:

``` python
if __name__ == "__main__":
    for epoch, item in run(flow, file_input()):
        print(item)
```

When we call `run`, our dataflow program will begin running, Bytewax will read the input items and epoch from your input generator, push the data through each step in the dataflow, and return the output. We then print the output of the final step.

To learn more about possible modes of execution, [read our page on execution](/getting-started/execution).
