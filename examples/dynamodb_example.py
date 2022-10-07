import json
import logging
import boto3
import math

from datetime import datetime, timezone, timedelta
from random import random

from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import DynamoDBOutputConfig
from bytewax.window import TumblingWindowConfig, EventClockConfig

#logging.basicConfig(level=logging.DEBUG)


def input_builder(worker_index, worker_count, resume_state):
    def random_events():
        for i in range(100):
            dt = datetime.now(timezone.utc)
            if random() > 0.8:
                dt = dt - timedelta(minutes=random())
            event = {
                "id": str(math.floor(random() * 10)),
                "type": "temp",
                "value": i,
                "time": dt.isoformat(),
            }
            yield None, event

    return random_events()


def deserialize(payload):
    return str(payload["id"]), payload


flow = Dataflow()
flow.input("inp", ManualInputConfig(input_builder))
flow.map(deserialize)
flow.capture(DynamoDBOutputConfig(
    table="example"
))

if __name__ == "__main__":
    cluster_main(flow, [], 0)
