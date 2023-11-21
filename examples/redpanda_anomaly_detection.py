#!/usr/bin/env python
# coding: utf-8

###################
# ---IMPORTANT--- #
###################
# To run this example you will need to run a Redpanda cluster -
# https://docs.redpanda.com/ and create a stream using the file in
# examples/utils/topics_helper.py

import json

from bytewax.connectors.kafka import KafkaSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from river import anomaly

# Define the dataflow object and kafka input.
flow = Dataflow()
flow.input("inp", KafkaSource(["localhost:9092"], ["ec2_metrics"]))


def group_instance_and_normalize(msg):
    """
    In this function, we will take input data and reformat it
    so that we have the shape (key, value), which is required
    for the aggregation step where we will aggregate by the key.

    We will also require the data to be normalized so it falls
    between 0 and 1. Since this is a percentage already, we
    just need to divide it by 100
    """
    data = json.loads(msg.value)
    data["value"] = float(data["value"]) / 100
    return data["instance"], data


flow.map("group_and_normalize", group_instance_and_normalize)


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
        data["score"] = self.score_one({"value": data["value"]})
        if data["score"] > 0.7:
            data["anom"] = 1
        else:
            data["anom"] = 0
        return self, (
            data["index"],
            data["timestamp"],
            data["value"],
            data["score"],
            data["anom"],
        )


flow.stateful_map("detector", lambda: AnomalyDetector(), AnomalyDetector.update)
# (("fe7f93", {"index": "1", "value":0.08, "instance":"fe7f93", "score":0.02}))


def format_output(event):
    instance, (index, t, value, score, is_anomalous) = event
    return (
        f"{instance}: time = {t}, "
        f"value = {value:.3f}, "
        f"score = {score:.2f}, "
        f"{is_anomalous}"
    )


flow.filter("filter_anomalies", lambda x: bool(x[1][4]))
flow.map("format", format_output)
flow.output("out", StdOutSink())
