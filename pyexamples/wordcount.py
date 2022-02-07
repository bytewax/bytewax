import re
import collections

import bytewax
from nltk.tokenize import RegexpTokenizer
from bytewax import inp


def tokenize(x):
    return re.findall(r'[^\s!,.?":;0-9]+', x)


def build_new_accumulator():
    word_to_count = {}
    return word_to_count


def acc(word_to_count, words):
    for word in words:
        if word not in word_to_count:
            word_to_count[word] = 0
        word_to_count[word] += 1
    return word_to_count


ec = bytewax.Executor()
flow = ec.Dataflow(inp.single_batch(open("pyexamples/sample_data/wordcount.txt")))
flow.flat_map(tokenize)
flow.accumulate(build_new_accumulator, acc)
flow.inspect(print)

if __name__ == "__main__":
    exec.build_and_run()
