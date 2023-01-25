import operator
from datetime import timedelta

from bytewax.connectors.files import FileInput
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.outputs import ManualOutputConfig
from bytewax.window import SystemClockConfig, TumblingWindowConfig


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
        TumblingWindowConfig(length=timedelta(seconds=5)),
        operator.add,
    )


def get_count(word_count):
    word, count = word_count
    return count


def output_builder(worker_index, worker_count):
    def format_and_print_output(count_count):
        times_appearing, num_words_with_the_same_count = count_count
        if num_words_with_the_same_count == 1:
            print(f"There was one word with a count of {times_appearing}")
        else:
            print(
                f"There were {num_words_with_the_same_count} different words with a count of {times_appearing}"
            )

    return format_and_print_output


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
# (that_same_count, num_words_with_the_same_count)
flow.capture(ManualOutputConfig(output_builder))


if __name__ == "__main__":
    run_main(flow)
