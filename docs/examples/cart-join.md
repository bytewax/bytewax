In this example, we're going to build a small online order fulfillment
system. It will join two event streams: one containing customer orders
and another containing successful payments, and then emit which orders
for each customer have been paid. We're also going to design it to be
**recoverable** and restart-able after invalid data.

## Sample Data

First let's make a local file containing our sample event data in JSON
format. Save this as `cart-join.json`.

```json
{"user_id": "a", "type": "order", "order_id": 1}
{"user_id": "a", "type": "order", "order_id": 2}
{"user_id": "b", "type": "order", "order_id": 3}
{"user_id": "a", "type": "payment", "order_id": 2}
{"user_id": "b", "type": "order", "order_id": 4}
FAIL HERE
{"user_id": "a", "type": "payment", "order_id": 1}
{"user_id": "b", "type": "payment", "order_id": 4}
```

## Input Builders

Let's start with the most basic version of our input builder which
doesn't support recovery yet. It will read each line of JSON, use the
line index as the epoch, and emit the parsed JSON.

```python
import json


def input_builder(worker_index, worker_count):
    assert worker_index == 0  # We're not going to worry about multiple workers yet.
    with open("cart-join.json") as f:
        for epoch, line in enumerate(f):
            obj = json.loads(line)
            yield epoch, obj
```

## Dataflow

Let's get our empty dataflow set up.

```python
from bytewax import Dataflow

flow = Dataflow()
```

Now, our plan is to use the stateful map operator to actually do the
join. All stateful operators require their input data to be a `(key,
value)` tuple so that the system can route all keys so they access the
same state.  Let's add that key field using the `user_id` field
present in every event.

```python
def key_off_user_id(event):
    return event["user_id"], event
    
    
flow.map(key_off_user_id)
```

Now onto the join itself. Stateful map needs two callbacks: One that
builds the initial, empty state. And one that combines new items into
the state and emits a value downstream.

We'll make a quick dictionary that holds the relevant data.

```python
def build_state(user_id_as_key):
    return {"unpaid_order_ids": [], "paid_order_ids": []}
```

The constructor of the class can be re-used as the initial state
builder.

Now we need the join logic, which will return two values: the updated
state and the item to emit downstream. Since we'd like to continuously
be emitting the most updated join info, we'll emit the state again.

```python
def joiner(state, event):
    e_type = event["type"]
    order_id = event["order_id"]
    if e_type == "order":
        state["unpaid_order_ids"].append(order_id)
    elif e_type == "payment":
        state["unpaid_order_ids"].remove(order_id)
        state["paid_order_ids"].append(order_id)
    return state, state


flow.stateful_map("joiner", build_state, joiner)
```

The items stateful operators emit also have the relevant key still
attached, so in this case we have `(user_id, joined_state)`. Let's
format that into a dictionary for output.

```python
def format_output(user_id__joined_state):
    user_id, joined_state = user_id__joined_state
    return {
        "user_id": user_id,
        "paid_order_ids": joined_state["paid_order_ids"],
        "unpaid_order_ids": joined_state["unpaid_order_ids"],
    }

flow.map(format_output)
```

And finally, capture this output and send it to our output builder.

```python
flow.capture()
```

On that note, we'll serialize the final output as JSON and print it.

```python
def output_builder(worker_index, worker_count):
    def output_handler(epoch_item):
        epoch, item = epoch_item
        line = json.dumps(item)
        print(epoch, line)
    return output_handler
```

## Execution

Now let's run our dataflow without state recovery:

```python doctest:IGNORE_EXCEPTION_DETAIL doctest:ELLIPSIS
from bytewax import run_main

run_main(flow, input_builder, output_builder)
```

Cool, we're chugging along and then OH NO!

```{testoutput}
0 {"user_id": "a", "paid_order_ids": [], "unpaid_order_ids": [1]}
1 {"user_id": "a", "paid_order_ids": [], "unpaid_order_ids": [1, 2]}
2 {"user_id": "b", "paid_order_ids": [], "unpaid_order_ids": [3]}
Traceback (most recent call last):
...
json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

Something went wrong! In this case it was that we had a non-JSON line
`FAIL HERE` in the input, but you could imagine that Kafka consumer
breaks or the VM is killed or something else bad happens!

We've also built up very valuable state in our stateful map operator
and we don't want to pay the penalty of having to re-read our input
all the way from the beginning to build it back up. Let's see how we
could have implemented state recovery to allow that to happen in the
future!

## Recovery Prep

Following our checklist in [Bytewax's documentation on
recovery](/getting-started/recovery/) we need to enhance our input
builder with a few things.

First, we need the ability to resume from an arbitrary epoch. Since
we're using the line in the file as the epoch, we can skip ahead to
that line.

```python
RESUME_FROM_EPOCH = 0


def input_builder(worker_index, worker_count):
    assert worker_index == 0  # We're not going to worry about multiple workers yet.
    with open("cart-join.jsons") as f:
        for epoch, line in enumerate(f):
            if epoch < RESUME_FROM_EPOCH:
                continue
            obj = json.loads(line)
            yield epoch, obj
```


We need a **recovery store**. For this example we'll use a local
[SQLite](https://sqlite.org/index.html) database because we're running
on a single machine.


```python
from tempfile import TemporaryDirectory
from bytewax.recovery import SqliteRecoveryConfig

recovery_dir = TemporaryDirectory()  # We'll store this somewhere temporary for this test.
recovery_config = SqliteRecoveryConfig(recovery_dir.name + "/state-recovery.sqlite3")
```

Now if we run the dataflow, the internal state will be persisted at
the end of each epoch so we can recover from mid-way. Since we didn't
run with any of the recovery systems activated last time, let's run
the dataflow again with them enabled.

```python doctest:IGNORE_EXCEPTION_DETAIL doctest:ELLIPSIS
run_main(flow, input_builder, output_builder, recovery_config=recovery_config)
```

As expected, we have the same error.

```{testoutput}
0 {"user_id": "a", "paid_order_ids": [], "unpaid_order_ids": [1]}
1 {"user_id": "a", "paid_order_ids": [], "unpaid_order_ids": [1, 2]}
2 {"user_id": "b", "paid_order_ids": [], "unpaid_order_ids": [3]}
Traceback (most recent call last):
...
json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

But this time, we've persisted state into the recovery store! If we
fix whatever is causing the exception, we can resume the dataflow and
still get the correct output. Let's fix up the input handler one more
time.

```python
def input_builder(worker_index, worker_count):
    assert worker_index == 0  # We're not going to worry about multiple workers yet.
    with open("cart-join.jsons") as f:
        for epoch, line in enumerate(f):
            if epoch < RESUME_FROM_EPOCH:
                continue
            if line.startswith("FAIL"):  # Fix the bug.
                continue
            obj = json.loads(line)
            yield epoch, obj
```

How do we know which epoch to resume from? Use the epoch _before_ the
last epoch seen in the output. In this case, that would be epoch `1`.

```python
RESUME_FROM_EPOCH = 1


run_main(flow, input_builder, output_builder, recovery_config=recovery_config)
```

```{testoutput}
1 {"user_id": "a", "paid_order_ids": [], "unpaid_order_ids": [1, 2]}
2 {"user_id": "b", "paid_order_ids": [], "unpaid_order_ids": [3]}
3 {"user_id": "a", "paid_order_ids": [2], "unpaid_order_ids": [1]}
4 {"user_id": "b", "paid_order_ids": [], "unpaid_order_ids": [3, 4]}
6 {"user_id": "a", "paid_order_ids": [2, 1], "unpaid_order_ids": []}
7 {"user_id": "b", "paid_order_ids": [4], "unpaid_order_ids": [3]}
```

Notice how the system did not forget the information in epoch `0`; we
still see that user `a` has order `1`.
