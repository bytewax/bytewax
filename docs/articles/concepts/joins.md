## Joins

A **join** is a way to combine items in two or more streams into a
single stream, matching them by a **join key**.

This is a very similar concept to the joins available in SQL, but
there are some new edge cases and behaviors you have to be aware of
when handling unbounded streams of data.

For example, you might have a stream of user names:

```
{"user_id": 12345, "name": "Bee"}
{"user_id": 12345, "name": "Hive"}
```

And another stream of email addresses.

```
{"user_id": 12345, "email": "bee@bytewax.io"}
{"user_id": 12345, "email": "hive@bytewax.io"}
```

And you'd like to be able to have access to both simultaneously,
joined onto the same object:

```
{"user_id": 12345, "name": "Bee", "email": "bee@bytewax.io"}
{"user_id": 12345, "name": "Hive", "email": "hive@bytewax.io"}
```

Let's setup a sample dataflow that gets that data into some streams:

```python
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource

flow = Dataflow("join_eg")

names_l = [
    {"user_id": 12345, "name": "Bee"},
    {"user_id": 12345, "name": "Hive"},
]
names = op.input("names", flow, TestingSource(names_l))

emails_l = [
    {"user_id": 12345, "email": "bee@bytewax.io"},
    {"user_id": 12345, "email": "hive@bytewax.io"},
]
emails = op.input("emails", flow, TestingSource(emails_l))
```

Bytewax provides the
[`join`](/apidocs/bytewax.operators/index#bytewax.operators.join),
[`join_named`](/apidocs/bytewax.operators/index#bytewax.operators.join_named),
[`join_window`](/apidocs/bytewax.operators/window/index#bytewax.operators.window.join_window),
and
[`join_window_named`](/apidocs/bytewax.operators/window/index#bytewax.operators.window.join_window)
operators to provide this functionality.

### Join Keys

All the operators above are stateful operators and so require all of
the upstreams to contain 2-tuples with the first element being a
string. All items that have the same key will be joined together.

Make this key unique enough so you don't bring together too much.

This key must be a string, so if you have a different data type,
you'll need to convert that type to a string. The
[`key_on`](/apidocs/bytewax.operators/index#bytewax.operators.key_on)
and [`map`](/apidocs/bytewax.operators/index#bytewax.operators.map)
operators can help you with this.

If we wanted to join the data in the above streams, let's key it by
the `user_id` since that is what we want to bring the data together by.

Since `user_id` is an `int` in our input data, we need to pass it
through `str`. And since it'd be redundant to keep the `"user_id"` key
in the dict, we'll map the value to just include the relevant field.

```python
keyed_names = op.map("key_names", names, lambda x: (str(x["user_id"]), x["name"]))
keyed_emails = op.map("key_emails", emails, lambda x: (str(x["user_id"]), x["email"]))
```

Let's
[`inspect`](/apidocs/bytewax.operators/index#bytewax.operators.inspect)
our streams to double check we know what they look like:

```python
import bytewax.testing

op.inspect("check_names", keyed_names)
op.inspect("check_emails", keyed_emails)

bytewax.testing.run_main(flow)
```

Looks like we see our 2-tuples!

```{testoutput}
join_eg.check_names: ('12345', 'Bee')
join_eg.check_emails: ('12345', 'bee@bytewax.io')
join_eg.check_names: ('12345', 'Hive')
join_eg.check_emails: ('12345', 'hive@bytewax.io')
```

### Streaming Joins

When doing a join in a SQL database, you look at the keys contained in
two tables and bring the other columns together. In a streaming
context, we have the concept of "key", we have the concept of "column"
(in the values for each key), but we don't have the exact concept of
"table". Each stream is sort of like a table, but for a SQL table
there's an obvious end to the table; when you run the join it takes
the state of the table when you run the command.

In a streaming system, there is no guaranteed end to the stream, so
our joins have to have slightly different semantics. The default
behavior of the
[`join`](/apidocs/bytewax.operators/index#bytewax.operators.join)
operator takes any number of upstream **sides** and waits until we
have seen a value for a key on all sides of the join, and only then do
we emit the gathered values downstream as a `tuple` of the values in
the same order as the sides stream arguments. This is similar to an
_inner join_ in SQL.

Let's see that in action. To recap our example:

```python
flow = Dataflow("join_eg")

names_l = [
    {"user_id": 123, "name": "Bee"},
    {"user_id": 456, "name": "Hive"},
]
names = op.input("names", flow, TestingSource(names_l))

emails_l = [
    {"user_id": 123, "email": "bee@bytewax.io"},
    {"user_id": 456, "email": "hive@bytewax.io"},
]
emails = op.input("emails", flow, TestingSource(emails_l))

keyed_names = op.map("key_names", names, lambda x: (str(x["user_id"]), x["name"]))
keyed_emails = op.map("key_emails", emails, lambda x: (str(x["user_id"]), x["email"]))
```

Then let's add a join operator taking the `keyed_names` stream as one
side and `keyed_emails` stream as the other and view the output:

```python
joined = op.join("join", keyed_names, keyed_emails)

op.inspect("check_join", joined)

bytewax.testing.run_main(flow)
```

Alright! It looks like we gathered the names and emails for each key.

```{testoutput}
join_eg.check_join: ('123', ('Bee', 'bee@bytewax.io'))
join_eg.check_join: ('456', ('Hive', 'hive@bytewax.io'))
```

What happens if we don't have a value for a key? Let's update our
names input to add a name that won't have an email. Then run the
dataflow again.

```python
names_l.clear()
names_l.extend(
    [
        {"user_id": 123, "name": "Bee"},
        {"user_id": 456, "name": "Hive"},
        {"user_id": 789, "name": "Pooh Bear"},
    ]
)

bytewax.testing.run_main(flow)
```

Hmm. It seems we didn't get any output for Pooh.

```{testoutput}
join_eg.check_join: ('123', ('Bee', 'bee@bytewax.io'))
join_eg.check_join: ('456', ('Hive', 'hive@bytewax.io'))
```

This is because the inner join by default will wait forever until it
gets a value for all sides for a key. It has no idea how long it
should wait for Pooh's email to arrive, so it stays there.

Another ramification of this, is that if you see a second value on one
side for a key, you will also not see any output. For example, let's
update our input to have an email update for the Bee.

```python
names_l.clear()
names_l.extend(
    [
        {"user_id": 123, "name": "Bee"},
        {"user_id": 456, "name": "Hive"},
    ]
)

emails_l.clear()
emails_l.extend(
    [
        {"user_id": 123, "email": "bee@bytewax.io"},
        {"user_id": 456, "email": "hive@bytewax.io"},
        {"user_id": 123, "email": "queen@bytewax.io"},
    ]
)

bytewax.testing.run_main(flow)
```

Notice we still don't see any new output for that user.

```{testoutput}
join_eg.check_join: ('123', ('Bee', 'bee@bytewax.io'))
join_eg.check_join: ('456', ('Hive', 'hive@bytewax.io'))
```

For our streaming inner join, by default, as soon as we see all the
values for a key, we _discard the join state_ and send it down stream.
Thus when the second "email" value comes in, there's no "name" value
for the key `"123"`, and the join operator waits until another value
comes in.

Let's visualize the state of the
[`join`](/apidocs/bytewax.operators/index#bytewax.operators.join)
operator evolving as it sees new items as a table. The table starts
empty.

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |

Then it sees the first name value and emits nothing downstream.

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 123 | `"Bee"` | |

Then it sees the second name value and emits nothing downstream.

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 123 | `"Bee"` | |
| 456 | `"Hive"` | |

Then finally it sees the first email value.

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 123 | `"Bee"` | `"bee@bytewax.io"` |
| 456 | `"Hive"` | |

Right after this point, it gathers together each of the values, and
then clears that "row" in the "table" and the tuple is emitted
downstream.

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 456 | `"Hive"` | |

```
('123', ('Bee', 'bee@bytewax.io'))
```

Let's continue. We then see the second email come through and clear
that row.

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |

```
('456', ('Hive', 'hive@bytewax.io'))
```

So note, now that the table is empty, when we see the email update for
the Bee, there's no name state. So nothing is emitted!

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 123 | | `"bee@bytewax.io"` |

Hopefully this helps clarify how basic streaming joins work. Realizing
that the
[`join`](/apidocs/bytewax.operators/index#bytewax.operators.join)
operator only keeps the state around while it is waiting for all sides
to complete is the trick to remember.

### Running Joins

The streaming join semantics are useful in some cases, but on infinite
data you could imagine other join semantics. A **running join** emits
downstream the values for all sides of the join whenever any value
comes in, _and keeps the state around_. This is similar to a _full
outer join_ in SQL. The way to enable a running join with the
[`join`](/apidocs/bytewax.operators/index#bytewax.operators.join)
operator is to set `running=True`.

Let's review what the dataflow would look like then:

```python
flow = Dataflow("join_eg")

names_l = [
    {"user_id": 123, "name": "Bee"},
    {"user_id": 456, "name": "Hive"},
]
names = op.input("names", flow, TestingSource(names_l))

emails_l = [
    {"user_id": 123, "email": "bee@bytewax.io"},
    {"user_id": 456, "email": "hive@bytewax.io"},
    {"user_id": 123, "email": "queen@bytewax.io"},
]
emails = op.input("emails", flow, TestingSource(emails_l))

keyed_names = op.map("key_names", names, lambda x: (str(x["user_id"]), x["name"]))
keyed_emails = op.map("key_emails", emails, lambda x: (str(x["user_id"]), x["email"]))

joined = op.join("join", keyed_names, keyed_emails, running=True)
```

Now let's run the dataflow again an inspect the output.

```python
op.inspect("check_join", joined)

bytewax.testing.run_main(flow)
```

Here's what we get. Let's visualize the progress and outputs of the
[`join`](/apidocs/bytewax.operators/index#bytewax.operators.join)
state table again but with `running=True`.

```{testoutput}
join_eg.check_join: ('123', ('Bee', None))
join_eg.check_join: ('123', ('Bee', 'bee@bytewax.io'))
join_eg.check_join: ('456', ('Hive', None))
join_eg.check_join: ('456', ('Hive', 'hive@bytewax.io'))
join_eg.check_join: ('123', ('Bee', 'queen@bytewax.io'))
```

First the table starts empty.

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |

Then we see the first name value. Since the table was updated in any
way, it emits the current values for that key and keeps them in the
table! If it doesn't know a value yet, it fills it in as `None`.

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 123 | `"Bee"` | |

```
('123', ({'user_id': 123, 'name': 'Bee'}, None))
```

Then we see the second name value. The same thing happens

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 123 | `"Bee"` | |
| 456 | `"Hive"` | |

```
('456', ({'user_id': 456, 'name': 'Hive'}, None))
```

Now we see the first email value. The same rules apply, but now since
there are values for all the sides, we see them all in the output. The
state for that key is not cleared!

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 123 | `"Bee"` | `"bee@bytewax.io"` |
| 456 | `"Hive"` | |

```
('123', ('Bee', 'bee@bytewax.io'))
```

Then we see the second email value.

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 123 | `"Bee"` | `"bee@bytewax.io"` |
| 456 | `"Hive"` | `"hive@bytewax.io"` |

```
('456', ('Hive', 'hive@bytewax.io'))
```

Note that none of the state we have seen has been cleared. This now
means when we see the updated Bee email, we'll see some output!

| Key | Name Value | Email Value |
| --- | ---------- | ----------- |
| 123 | `"Bee"` | `"queen@bytewax.io"` |
| 456 | `"Hive"` | `"hive@bytewax.io"` |

```
('123', ('Bee', 'queen@bytewax.io'))
```

So the running join is cool in that you can track updates to changes
in values over time. _But this comes with a downside!_ Because we
never throw away the state for a key, this state keeps growing in
memory _forever_ if you keep adding keys. This might be the behavior
you want, but realize that it does not come for free. A non-running
streaming join is better if you know you'll only get one value for
each side for each key, since it knows it can discard the state after
sending the values downstream.

## Windowed Joins

The streaming join assumes that a key could come anywhere in the
entire lifetime of a stream. This means it could possibly wait forever
for a value to come on a side that will never come. Another option is
to use a **windowed join** that always flushes out the values for a
key whenever the time-based window closes. You can use this if you
need to know the join values over an infinite stream when you aren't
sure that you'll see values on all sides of the join.

Bytewax provides the operators
[`join_window`](/apidocs/bytewax.operators/window/index#bytewax.operators.window.join_window),
and
[`join_window_named`](/apidocs/bytewax.operators/window/index#bytewax.operators.window.join_window)
to implement this.

For the details of all the types of windows you can define and
explanation of the parameters, see our [windowing
documentation](windowing.md). I'm going to use a simple 1 hour
tumbling window; the previous window closes and the next window starts
at the top of each hour. We'll be using event time.

```python
from datetime import timedelta, datetime, timezone
from bytewax.operators.window import EventClockConfig, TumblingWindow

clock = EventClockConfig(
    dt_getter=lambda x: x["at"], wait_for_system_duration=timedelta(0)
)
windower = TumblingWindow(
    length=timedelta(hours=1),
    align_to=datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
)
```

Let's assume we have input sources that are similar in shape to
before, but now have timestamps.

```python
flow = Dataflow("join_eg")

names_l = [
    {
        "user_id": 123,
        "at": datetime(2023, 12, 14, 0, 0, tzinfo=timezone.utc),
        "name": "Bee",
    },
    {
        "user_id": 456,
        "at": datetime(2023, 12, 14, 0, 0, tzinfo=timezone.utc),
        "name": "Hive",
    },
]
names = op.input("names", flow, TestingSource(names_l))

emails_l = [
    {
        "user_id": 123,
        "at": datetime(2023, 12, 14, 0, 15, tzinfo=timezone.utc),
        "email": "bee@bytewax.io",
    },
    {
        "user_id": 456,
        "at": datetime(2023, 12, 14, 1, 15, tzinfo=timezone.utc),
        "email": "hive@bytewax.io",
    },
]
emails = op.input("emails", flow, TestingSource(emails_l))
```

Since our objects are no longer just strings, let's keep the values
untouched so we have access to the timestamps.

```python
keyed_names = op.map("key_names", names, lambda x: (str(x["user_id"]), x))
keyed_emails = op.map("key_emails", emails, lambda x: (str(x["user_id"]), x))
```

Let's inspect this just to double check we understand the shape.

```python
op.inspect("check_names", keyed_names)
op.inspect("check_emails", keyed_emails)

bytewax.testing.run_main(flow)
```

The values are entire `dict`s and we'll still access the `"at"` key to
use the event timestamp.

```{testoutput}
join_eg.check_names: ('123', {'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 0, tzinfo=datetime.timezone.utc), 'name': 'Bee'})
join_eg.check_emails: ('123', {'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 15, tzinfo=datetime.timezone.utc), 'email': 'bee@bytewax.io'})
join_eg.check_names: ('456', {'user_id': 456, 'at': datetime.datetime(2023, 12, 14, 0, 0, tzinfo=datetime.timezone.utc), 'name': 'Hive'})
join_eg.check_emails: ('456', {'user_id': 456, 'at': datetime.datetime(2023, 12, 14, 1, 15, tzinfo=datetime.timezone.utc), 'email': 'hive@bytewax.io'})
```

Let's visualize what these windows and events look like on a timeline.
Window start times are inclusive, so something right on a window
border is in the next window.

```mermaid
gantt
    dateFormat  YYYY-MM-DDTHH:mm
    axisFormat %Y-%m-%d %H:%M

    section Windows
    Zero: w0, 2023-12-13T23:00, 1h
    One: w1, after w0, 1h
    Two: w2, after w1, 1h

    section Names
    ('123', {'name'= 'Bee'}): milestone, 2023-12-14T00:00, 0d
    ('456', {'name'= 'Hive'}): milestone, 2023-12-14T00:00, 0d

    section Emails
    ('123', {'email'= 'bee@bytewax.io'}): milestone, 2023-12-14T00:15, 0d
    ('456', {'email'= 'hive@bytewax.io'}): milestone, 2023-12-14T01:15, 0d
```

So it looks like for key `"123"`, we should see the name and email be
joined because they occur in the same window. But for key `"456"` we
should see one downstream item of just the name (because no email came
for `"456"` in that window), and another downstream item of just the
email (because no name came in that window).

Now let's set up the windowed join and inspect the results to see if
it matches that. To review, the entire dataflow is as follows.

```python
import bytewax.operators.window as op_w

flow = Dataflow("join_eg")

names_l = [
    {
        "user_id": 123,
        "at": datetime(2023, 12, 14, 0, 0, tzinfo=timezone.utc),
        "name": "Bee",
    },
    {
        "user_id": 456,
        "at": datetime(2023, 12, 14, 0, 0, tzinfo=timezone.utc),
        "name": "Hive",
    },
]
names = op.input("names", flow, TestingSource(names_l))

emails_l = [
    {
        "user_id": 123,
        "at": datetime(2023, 12, 14, 0, 15, tzinfo=timezone.utc),
        "email": "bee@bytewax.io",
    },
    {
        "user_id": 456,
        "at": datetime(2023, 12, 14, 1, 15, tzinfo=timezone.utc),
        "email": "hive@bytewax.io",
    },
]
emails = op.input("emails", flow, TestingSource(emails_l))

keyed_names = op.map("key_names", names, lambda x: (str(x["user_id"]), x))
keyed_emails = op.map("key_emails", emails, lambda x: (str(x["user_id"]), x))

joined = op_w.join_window("join", clock, windower, keyed_names, keyed_emails)

op.inspect("check_join", joined)

bytewax.testing.run_main(flow)
```

Looks like that's what we see! Notice the `None` in the output for key
`"456"`. All window operators also emit the metadata about the window
for analysis, but you can ignore that.

```{testoutput}
join_eg.check_join: ('456', (WindowMetadata(open_time: 2023-12-14 00:00:00 UTC, close_time: 2023-12-14 01:00:00 UTC), ({'user_id': 456, 'at': datetime.datetime(2023, 12, 14, 0, 0, tzinfo=datetime.timezone.utc), 'name': 'Hive'}, None)))
join_eg.check_join: ('123', (WindowMetadata(open_time: 2023-12-14 00:00:00 UTC, close_time: 2023-12-14 01:00:00 UTC), ({'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 0, tzinfo=datetime.timezone.utc), 'name': 'Bee'}, {'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 15, tzinfo=datetime.timezone.utc), 'email': 'bee@bytewax.io'})))
join_eg.check_join: ('456', (WindowMetadata(open_time: 2023-12-14 01:00:00 UTC, close_time: 2023-12-14 02:00:00 UTC), (None, {'user_id': 456, 'at': datetime.datetime(2023, 12, 14, 1, 15, tzinfo=datetime.timezone.utc), 'email': 'hive@bytewax.io'})))
```

Bytewax currently does not support running windowed joins.

### Product Joins

Window operators, because they have a defined close time, also support
another join type. The **product join** emits all of the combinations
of _all_ of the input values seen on a side.

For example, if we don't change the join parameters, but update the
input in the above dataflow to include multiple values in a window for
a key.

```python
names_l.clear()
names_l.extend(
    [
        {
            "user_id": 123,
            "at": datetime(2023, 12, 14, 0, 0, tzinfo=timezone.utc),
            "name": "Bee",
        },
    ]
)

emails_l.clear()
emails_l.extend(
    [
        {
            "user_id": 123,
            "at": datetime(2023, 12, 14, 0, 15, tzinfo=timezone.utc),
            "email": "bee@bytewax.io",
        },
        {
            "user_id": 123,
            "at": datetime(2023, 12, 14, 0, 30, tzinfo=timezone.utc),
            "email": "queen@bytewax.io",
        },
    ]
)

bytewax.testing.run_main(flow)
```

Notice how now we only have the latest email for the bee:

```{testoutput}
join_eg.check_join: ('123', (WindowMetadata(open_time: 2023-12-14 00:00:00 UTC, close_time: 2023-12-14 01:00:00 UTC), ({'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 0, tzinfo=datetime.timezone.utc), 'name': 'Bee'}, {'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 30, tzinfo=datetime.timezone.utc), 'email': 'queen@bytewax.io'})))
```

Now if we re-define the dataflow and use `product=True`, we can see
all of the values for the Bee's email in that window.

```python
import bytewax.operators.window as op_w

flow = Dataflow("join_eg")

names = op.input("names", flow, TestingSource(names_l))
emails = op.input("emails", flow, TestingSource(emails_l))

keyed_names = op.map("key_names", names, lambda x: (str(x["user_id"]), x))
keyed_emails = op.map("key_emails", emails, lambda x: (str(x["user_id"]), x))

joined = op_w.join_window(
    "join", clock, windower, keyed_names, keyed_emails, product=True
)

op.inspect("check_join", joined)

bytewax.testing.run_main(flow)
```

Notice there are now two output values for that key.

```{testoutput}
join_eg.check_join: ('123', (WindowMetadata(open_time: 2023-12-14 00:00:00 UTC, close_time: 2023-12-14 01:00:00 UTC), ({'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 0, tzinfo=datetime.timezone.utc), 'name': 'Bee'}, {'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 15, tzinfo=datetime.timezone.utc), 'email': 'bee@bytewax.io'})))
join_eg.check_join: ('123', (WindowMetadata(open_time: 2023-12-14 00:00:00 UTC, close_time: 2023-12-14 01:00:00 UTC), ({'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 0, tzinfo=datetime.timezone.utc), 'name': 'Bee'}, {'user_id': 123, 'at': datetime.datetime(2023, 12, 14, 0, 30, tzinfo=datetime.timezone.utc), 'email': 'queen@bytewax.io'})))
```

## Named Joins

The two previously described join operators have **named** versions.
[`join`](/apidocs/bytewax.operators/index#bytewax.operators.join), has
[`join_named`](/apidocs/bytewax.operators/index#bytewax.operators.join_named)
and
[`join_window`](/apidocs/bytewax.operators/window/index#bytewax.operators.window.join_window),
has
[`join_window_named`](/apidocs/bytewax.operators/window/index#bytewax.operators.window.join_window).
The named versions have identical parameters and join semantics, but
in stead of emitting `tuple`s down stream, they emit `dict`s in which
the keys are the names of the keyword arguments you use to specify the
upstream sides. This helps out a lot when you many-sided joins and it
can be difficult to keep track of which values are in which index of
the resulting tuple.

For example, given our original streaming join. If you remember the
output was 2-tuples because there were two sides to the join.

```python
flow = Dataflow("join_eg")

names_l = [
    {"user_id": 123, "name": "Bee"},
    {"user_id": 456, "name": "Hive"},
]
names = op.input("names", flow, TestingSource(names_l))

emails_l = [
    {"user_id": 123, "email": "bee@bytewax.io"},
    {"user_id": 456, "email": "hive@bytewax.io"},
    {"user_id": 123, "email": "queen@bytewax.io"},
]
emails = op.input("emails", flow, TestingSource(emails_l))

keyed_names = op.map("key_names", names, lambda x: (str(x["user_id"]), x["name"]))
keyed_emails = op.map("key_emails", emails, lambda x: (str(x["user_id"]), x["email"]))

joined = op.join("join", keyed_names, keyed_emails)

op.inspect("check_join", joined)

bytewax.testing.run_main(flow)
```

```{testoutput}
join_eg.check_join: ('123', ('Bee', 'bee@bytewax.io'))
join_eg.check_join: ('456', ('Hive', 'hive@bytewax.io'))
```

If we change this to use an equivalent
[`join_named`](/apidocs/bytewax.operators/index#bytewax.operators.join_named)
instead:

```python
flow = Dataflow("join_eg")

names_l = [
    {"user_id": 123, "name": "Bee"},
    {"user_id": 456, "name": "Hive"},
]
names = op.input("names", flow, TestingSource(names_l))

emails_l = [
    {"user_id": 123, "email": "bee@bytewax.io"},
    {"user_id": 456, "email": "hive@bytewax.io"},
    {"user_id": 123, "email": "queen@bytewax.io"},
]
emails = op.input("emails", flow, TestingSource(emails_l))

keyed_names = op.map("key_names", names, lambda x: (str(x["user_id"]), x["name"]))
keyed_emails = op.map("key_emails", emails, lambda x: (str(x["user_id"]), x["email"]))

joined = op.join_named("join", name=keyed_names, email=keyed_emails)

op.inspect("check_join", joined)

bytewax.testing.run_main(flow)
```

Notice how the output is now `dict`s and how the kwargs `name` and
`email` are represented as keys in the output dictionary.

```{testoutput}
join_eg.check_join: ('123', {'name': 'Bee', 'email': 'bee@bytewax.io'})
join_eg.check_join: ('456', {'name': 'Hive', 'email': 'hive@bytewax.io'})
```

Hopefully using named joins will help you keep your code less fragile.
