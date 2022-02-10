import re
from collections import defaultdict

import bytewax

file = open("pyexamples/sample_data/wordcount.txt", "r").readlines()


def file_input():
    for line in file:
        yield (1, line)


def tokenize(x):
    x = x.lower()
    return re.findall(r'[^\s!,.?":;0-9]+', x)


def acc(word_to_count, words):
    for word in words:
        word_to_count[word] += 1
    return word_to_count


exec = bytewax.Executor()
flow = exec.Dataflow(file_input())
flow.flat_map(tokenize)
flow.accumulate(lambda: defaultdict(int), acc)

if __name__ == "__main__":
    exec.build_and_run(ctrlc=False)
