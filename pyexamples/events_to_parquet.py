import json
import time
from datetime import date

from utils import fake_events
import bytewax
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

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
    for t, event in zip(tick_every(5), fake_events.generate_web_events()):
        yield t, event


def key_on_page(event):
    return (event['page_url_path'], event)


def collect_events(acc, events):
    for event in events:
        acc.append(event)
    return acc

# go dataframe --> pyarrow Table --> parquet
def convert_to_parquet(x):
    df = pd.DataFrame(x)
    table = pa.Table.from_pandas(df)
    today = date.today()
    file_partition = today.strftime("year=%Y/month=%m/day=%d")
    pq.write_to_dataset(
        table, 
        root_path=file_partition,
        partition_cols=['page_url_path'])

ex = bytewax.Executor()
flow = ex.DataflowBlueprint(gen_input())
flow.map(json.loads)
# in order to partition on this key, we need to exchange 
# across the workers so the write will work properly
flow.exchange(lambda x: hash(x['page_url_path']))
flow.accumulate(lambda: list(), collect_events)
flow.map(convert_to_parquet)


if __name__ == "__main__":
    ex.build_and_run()
