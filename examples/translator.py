from transformers import pipeline

import bytewax
from bytewax import Dataflow, inputs, parse, run_cluster


def predict(en):
    de = translator(en)[0]["translation_text"]
    return (en, de)


def inspector(en_de):
    en, de = en_de
    print(f"{en} -> {de}")


flow = Dataflow()
flow.map(str.strip)
flow.map(predict)
flow.capture()


if __name__ == "__main__":
    translator = pipeline("translation_en_to_de")

    for epoch, item in run_cluster(
        flow,
        inputs.single_batch(open("examples/sample_data/lyrics.txt")),
        **parse.cluster_args(),
    ):
        inspector(item)
