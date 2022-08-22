from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingInputConfig
from bytewax.outputs import StdOutputConfig

flow = Dataflow()
flow.input("stateless_input", TestingInputConfig(range(10)))
flow.map(lambda x: x * x)
flow.capture(StdOutputConfig())

if __name__ == "__main__":
    run_main(flow)
