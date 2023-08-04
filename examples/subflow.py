import operator
from datetime import datetime, timedelta, timezone

from bytewax.connectors.files import FileInput
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.window import SystemClockConfig, TumblingWindow


# You can define your own functions which add groupings of steps to a
# dataflow. This allows you to repeat a pattern of steps easily.
def calc_counts(flow):
    """Add steps to this flow which counts the frequencies of input
    items and emits (item, count) tuples downstream."""
    # `str` format required for reduce_window key
    flow.map(lambda x: (str(x), 1))
    flow.reduce_window(
        "sum",
        SystemClockConfig(),
        TumblingWindow(
            length=timedelta(seconds=5),
            align_to=datetime(2023, 1, 1, tzinfo=timezone.utc),
        ),
        operator.add,
    )


def get_count(word_count):
    word, count = word_count
    return count


def format_output(count_count):
    times_appearing, num_words_with_the_same_count = count_count
    if num_words_with_the_same_count == 1:
        return f"There was one word with a count of {times_appearing}"
    else:
        return (
            f"There were {num_words_with_the_same_count} different words "
            f"with a count of {times_appearing}"
        )


flow = Dataflow()
flow.input("inp", FileInput("examples/sample_data/wordcount.txt"))
# "at this point we have full sentences as items in the dataflow"
flow.flat_map(str.split)
# "words"
calc_counts(flow)
# ("word", count)
flow.map(get_count)
# count
calc_counts(flow)
flow.map(format_output)
# (that_same_count, num_words_with_the_same_count)
flow.output("out", StdOutput())
