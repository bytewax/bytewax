from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import MqttInputConfig
from bytewax.outputs import StdOutputConfig

flow = Dataflow()
flow.input(
    "input",
    MqttInputConfig(
        host="tcp://broker.emqx.io:1883",
        topics=["rust/mqtt", "rust/test"],
        qos=[0, 1],
        client_id="rust_subscribe"
    )
)
flow.capture(StdOutputConfig())


if __name__ == "__main__":
    spawn_cluster(flow)
