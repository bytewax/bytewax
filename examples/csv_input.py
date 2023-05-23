from bytewax.connectors.files import FileInput
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput


flow = Dataflow()
flow.input("inp", FileInput("examples/sample_data/ec2_metrics.csv"))
flow.output("out", StdOutput())

from bytewax.testing import run_main

run_main(flow)