#!/usr/bin/env python
# coding: utf-8

###################
# ---IMPORTANT--- #
###################
# To run this example you will need to run a Redpanda cluster -
# https://docs.redpanda.com/ and create a stream using the file in
# examples/utils/topics_helper.py

import json

from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from river import anomaly

# Define the dataflow object and kafka input.
flow = Dataflow("anomaly detection")
stream = op.input("inp", flow, KafkaSource(["localhost:19092"], ["ec2_metrics"]))


def normalize(msg):
    """
    We require the data to be normalized so it falls
    between 0 and 1. Since this is a percentage already, we
    just need to divide it by 100
    """
    data = json.loads(msg.value)
    data["value"] = float(data["value"]) / 100
    return data["instance"], data


normalized_stream = op.map("normalize", stream, normalize)


class AnomalyDetector(anomaly.HalfSpaceTrees):
    """
    Our anomaly detector inherits from the HalfSpaceTrees
    object from the river package and has the following inputs


    n_trees – defaults to 10
    height – defaults to 8
    window_size – defaults to 250
    limits (Dict[Hashable, Tuple[float, float]]) – defaults to None
    seed (int) – defaults to None

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, n_trees=5, height=3, window_size=5, seed=42, **kwargs)

    def update(self, data):
        self.learn_one({"value": data["value"]})

    def score(self, data):
        return self.score_one({"value": data["value"]})


def mapper(state, data):
    if state is None:
        state = AnomalyDetector()

    state.update(data)

    data["score"] = state.score(data)
    if data["score"] > 0.7:
        data["anom"] = 1
    else:
        data["anom"] = 0

    emit = (
        data["index"],
        data["timestamp"],
        data["value"],
        data["score"],
        data["anom"],
    )
    return (state, emit)


anomaly_stream = op.stateful_map("anom", normalized_stream, mapper)
# (("fe7f93", {"index": "1", "value":0.08, "instance":"fe7f93", "score":0.02}))


def format_output(event):
    instance, (index, t, value, score, is_anomalous) = event
    return (
        f"{instance}: time = {t}, "
        f"value = {value:.3f}, "
        f"score = {score:.2f}, "
        f"{is_anomalous}"
    )


formatted_stream = op.map("format", anomaly_stream, format_output)
op.output("out", formatted_stream, StdOutSink())
