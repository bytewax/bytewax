import bytewax
from bytewax import inp
from transformers import pipeline


translator = pipeline("translation_en_to_de")


def predict(en):
    de = translator(en)[0]["translation_text"]
    return (en, de)


def inspector(en_de):
    en, de = en_de
    print(f"{en} -> {de}")


ec = bytewax.Executor()
flow = ec.Dataflow(inp.single_batch(open("pyexamples/sample_data/lyrics.txt")))
flow.map(str.strip)
flow.map(predict)
flow.inspect(inspector)


if __name__ == "__main__":
    ec.build_and_run()
