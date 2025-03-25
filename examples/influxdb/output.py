import logging
import os
from datetime import timedelta

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.influxdb import InfluxDBSink
from bytewax.testing import TestingSource

TOKEN = os.getenv(
    "INLFUXDB_TOKEN",
    "my-token",
)
DATABASE = os.getenv("INFLUXDB_DATABASE", "testing")
ORG = os.getenv("INFLUXDB_ORG", "dev")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

lines = [
    "home,room=Living\\ Room temp=21.1,hum=35.9,co=0i 1724258000",
    "home,room=Kitchen temp=21.0,hum=35.9,co=0i 1724258000",
    "home,room=Living\\ Room temp=21.4,hum=35.9,co=0i 1724259000",
    "home,room=Kitchen temp=23.0,hum=36.2,co=0i 1724259000",
    "home,room=Living\\ Room temp=21.8,hum=36.0,co=0i 1724260000",
    "home,room=Kitchen temp=22.7,hum=36.1,co=0i 1724260000",
    "home,room=Living\\ Room temp=22.2,hum=36.0,co=0i 1724261000",
    "home,room=Kitchen temp=22.4,hum=36.0,co=0i 1724261000",
    "home,room=Living\\ Room temp=22.2,hum=35.9,co=0i 1724262000",
    "home,room=Kitchen temp=22.5,hum=36.0,co=0i 1724262000",
    "home,room=Living\\ Room temp=22.4,hum=36.0,co=0i 1724263000",
    "home,room=Kitchen temp=22.8,hum=36.5,co=1i 1724263000",
    "home,room=Living\\ Room temp=22.3,hum=36.1,co=0i 1724264000",
    "home,room=Kitchen temp=22.8,hum=36.3,co=1i 1724264000",
    "home,room=Living\\ Room temp=22.3,hum=36.1,co=1i 1724265000",
    "home,room=Kitchen temp=22.7,hum=36.2,co=3i 1724265000",
    "home,room=Living\\ Room temp=22.4,hum=36.0,co=4i 1724266000",
    "home,room=Kitchen temp=22.4,hum=36.0,co=7i 1724266000",
    "home,room=Living\\ Room temp=22.6,hum=35.9,co=5i 1724267000",
    "home,room=Kitchen temp=22.7,hum=36.0,co=9i 1724267000",
    "home,room=Living\\ Room temp=22.8,hum=36.2,co=9i 1724268000",
    "home,room=Kitchen temp=23.3,hum=36.9,co=18i 1724268000",
    "home,room=Living\\ Room temp=22.5,hum=36.3,co=14i 1724269000",
    "home,room=Kitchen temp=23.1,hum=36.6,co=22i 1724269000",
    "home,room=Living\\ Room temp=22.2,hum=36.4,co=17i 1724270000",
    "home,room=Kitchen temp=22.7,hum=36.5,co=26i 1724270000",
]

flow = Dataflow("simple_output")

stream = op.input("input", flow, TestingSource(lines))
keyed_stream = op.key_on("key_location", stream, lambda x: x.split(",")[0])
op.inspect("check_stream", stream)
batch_readings = op.collect(
    "lines", keyed_stream, max_size=10, timeout=timedelta(milliseconds=50)
)

# Use the custom sink to write the output stream back to InfluxDB
op.output(
    "out",
    batch_readings,
    InfluxDBSink(
        host="https://us-east-1-1.aws.cloud2.influxdata.com",
        database=DATABASE,
        org=ORG,
        token=TOKEN,
        write_precision="s",
    ),
)
