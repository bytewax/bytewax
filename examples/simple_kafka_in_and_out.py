import json

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import KafkaOutputConfig

flow = Dataflow()

flow.input(
    "inp",
    KafkaInputConfig(
        brokers=["localhost:9092"],
        topic="input_topic",
    ),
)

flow.capture(
    KafkaOutputConfig(
        brokers=["localhost:9092"],
        topic="output_topic",
    )
)

if __name__ == "__main__":
    run_main(flow)
