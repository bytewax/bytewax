from bytewax.connectors.files import CSVSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow()
flow.input("inp", CSVSource("examples/sample_data/ec2_metrics.csv", delimiter=","))
flow.output("out", StdOutSink())
