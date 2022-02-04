import collections

import bytewax


def read_lines():
    with open("pyexamples/sample_data/wordcount.txt") as lines:
        for line in lines:
            yield (1, line)


def count(acc, xs):
    for x in xs:
        acc[x] += 1
    return acc


# You can define your own functions which add groupings of steps to a
# dataflow. This allows you to repeat a pattern of steps easily.
def calc_counts(flow):
    """Add steps to this flow which counts the frequencies of input
    items and emits (item, count) tuples downstream."""
    flow.exchange(hash)
    flow.accumulate(lambda: collections.defaultdict(int), count)
    flow.flat_map(dict.items)


def get_count(word_count):
    word, count = word_count
    return count


def inspector(count_count):
    that_same_count, num_words_with_the_same_count = count_count
    print(
        f"There were {num_words_with_the_same_count} different words with a count of {that_same_count}"
    )


ec = bytewax.Executor()
flow = ec.Dataflow(read_lines())
# "at this point we have full sentences as items in the dataflow"
flow.flat_map(str.split)
# "words"
calc_counts(flow)
# ("word", count)
flow.map(get_count)
# count
calc_counts(flow)
# (that_same_count, num_words_with_the_same_count)
flow.inspect(inspector)


if __name__ == "__main__":
    ec.build_and_run()
