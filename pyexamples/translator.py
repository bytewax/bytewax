import bytewax
from bytewax import inp
from transformers import pipeline


# Load my fancy model translator
translator = pipeline("translation_en_to_de")


def predict(x):
    y = translator(x)[0]["translation_text"]
    print(f"{x} -> {y}")
    return y


ec = bytewax.Executor()
flow = ec.Dataflow(inp.single_batch(open("pyexamples/sample_data/lyrics.txt")))
flow.map(str.strip)
flow.map(predict)
flow.inspect(print)


if __name__ == "__main__":
    ec.build_and_run()
