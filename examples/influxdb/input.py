import os
import logging
from datetime import timedelta, datetime, timezone

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.influxdb import InfluxDBSource

TOKEN = os.getenv(
    "INFLUXDB_TOKEN",
    "my-token",
)
DATABASE = os.getenv("INFLUXDB_DATABASE", "testing")
ORG = os.getenv("INFLUXDB_ORG", "dev")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

flow = Dataflow("a_simple_example")

inp = op.input(
    "inp",
    flow,
    InfluxDBSource(
        timedelta(minutes=30),
        "https://us-east-1-1.aws.cloud2.influxdata.com",
        DATABASE,
        TOKEN,
        "home",
        ORG,
        datetime.fromtimestamp(1724258000, tz=timezone.utc),
    ),
)
op.inspect("input", inp)
