import json
import logging
import math
import os

from datetime import datetime, timedelta
from random import random, randrange, choice

from bytewax.bigquery import BigqueryOutputConfig
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig

logging.basicConfig(level=logging.INFO)
flow = Dataflow()
project = os.environ["PROJECT"]
dataset = os.environ["DATASET"]
table = os.environ["TABLE"]

table_ref = f"{project}.{dataset}.{table}"

def input_builder(worker_index, worker_count, resume_state):
    def random_new_users():
        for i in range(100):
            dt = datetime.utcnow()
            if random() > 0.8:
                dt = dt - timedelta(minutes=random())
            user = {
                "user_id": str(math.floor(random() * 10)),
                "age": randrange(100),
                "pet": {
                    "name": choice(["Fluffy", "Egghead", "Sir Snarlsalot"]),
                    "furry": choice([True, False]),
                },
                "created_at": dt.isoformat(),
            }
            yield None, [user]

    return random_new_users()


flow.input(
    "inp",
    ManualInputConfig(input_builder),
)

flow.capture(BigqueryOutputConfig(table_ref=table_ref))

if __name__ == "__main__":
    run_main(flow)
