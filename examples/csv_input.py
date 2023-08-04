from bytewax.connectors.files import CSVInput
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow

flow = Dataflow()
flow.input("inp", CSVInput("examples/sample_data/ec2_metrics.csv", delimiter=","))
flow.output("out", StdOutput())
