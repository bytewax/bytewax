from pathlib import Path

from bytewax import operators as op
from bytewax.connectors.files import CSVSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow("csv_input")
stream = op.input("inp", flow, CSVSource(Path("examples/sample_data/ec2_metrics.csv")))
op.output("out", stream, StdOutSink())
