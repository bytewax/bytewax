#!/usr/bin/env python3

import json
from collections import defaultdict
import time
from utils import twitter

import bytewax

# batch the input for every n seconds
def tick_every(interval_sec):
    timely_t = 0
    last_bump_sec = time.time() - interval_sec
    while True:
        yield timely_t
        now_sec = time.time()
        frac_intervals = (now_sec - last_bump_sec) / interval_sec
        if frac_intervals >= 1.0:
            timely_t += int(frac_intervals)
            last_bump_sec = now_sec


def gen_input():
    for t, event in zip(tick_every(2), twitter.get_stream()):
        yield t, event


def decode(x):
    # add try/except to handle nulls
    try:
        return json.loads(x)
    except ValueError:
        return {'matching_rules':[]}

# get all of the mentioned hashtags        
def coin_name(data_dict):
    return [x['tag'] for x in data_dict["matching_rules"]]

# count the mentions
def count_edits(acc, tags):
    for tag in tags:
        acc[tag] += 1
    return acc


ex = bytewax.Executor()
df = ex.DataflowBlueprint(gen_input())
df.map(decode)
df.flat_map(coin_name)
df.exchange(hash)
df.accumulate(lambda: defaultdict(int), count_edits)
df.inspect(print)


if __name__ == "__main__":
    ex.build_and_run()
