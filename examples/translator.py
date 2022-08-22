from transformers import pipeline

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig


def input_builder(worker_index, worker_count, resume_state):
    state = None  # ignore recovery
    for line in open("examples/sample_data/lyrics.txt"):
        yield state, line


def build_predict(translator):
    def predict(en):
        de = translator(en)[0]["translation_text"]
        return (en, de)

    return predict


def format_output(en_de):
    en, de = en_de
    return f"{en} -> {de}"


translator = pipeline("translation_en_to_de")

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(str.strip)
flow.map(build_predict(translator))
flow.map(format_output)
flow.capture(StdOutputConfig())

if __name__ == "__main__":
    run_main(flow)
