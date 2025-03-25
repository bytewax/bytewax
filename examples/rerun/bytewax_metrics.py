from random import random
from time import sleep, time

import rerun as rr
from bytewax import operators as op
from bytewax.bytewax_rerun import RerunMessage, RerunSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

sink = RerunSink(application_id="metrics_app", recording_id="metrics_recording")

flow = Dataflow("rerun-test")
inp = op.input("inp", flow, TestingSource(list(range(100))))
# Redistribute messages to see them moving between workers
inp = op.redistribute("scale", inp)


@sink.rerun_log(log_args=True, log_return=True)
def heavy_operation(item: int) -> int:
    sleep(random())

    return item * 2


@sink.rerun_log(log_args=True, log_return=True)
def make_message(item: int) -> RerunMessage:
    return RerunMessage(
        entity_path="message",
        entity=rr.Scalar(item + (random() * 2 - 1)),
        timeline="messages",
        time=time(),
    )


operated = op.map("operated", inp, heavy_operation)
messages = op.map("message", operated, make_message)
op.output("to_rerun", messages, sink)
