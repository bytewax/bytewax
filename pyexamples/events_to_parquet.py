import json
import time
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import bytewax
from bytewax import inp

from utils import fake_events


def key_on_page(event):
    return (event["page_url_path"], event)


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
        table, root_path=file_partition, partition_cols=["page_url_path"]
    )


ec = bytewax.Executor()
flow = ec.Dataflow(inp.tumbling_epoch(5.0, fake_events.generate_web_events()))
flow.map(json.loads)
# in order to partition on this key, we need to exchange
# across the workers so the write will work properly
flow.exchange(lambda x: hash(x["page_url_path"]))
flow.accumulate(lambda: list(), collect_events)
flow.map(convert_to_parquet)


if __name__ == "__main__":
    ec.build_and_run()
