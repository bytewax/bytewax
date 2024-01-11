from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow

BROKERS = ["localhost:19092"]
IN_TOPICS = ["in_topic"]
OUT_TOPIC = "out_topic"

flow = Dataflow("kafka_in_out")
kinp = kop.input("inp", flow, brokers=BROKERS, topics=IN_TOPICS)
op.inspect("inspect-errors", kinp.errs)
op.inspect("inspect-oks", kinp.oks)
kop.output("out1", kinp.oks, brokers=BROKERS, topic=OUT_TOPIC)
