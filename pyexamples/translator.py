import bytewax
from transformers import pipeline


# Load my fancy model translator
translator = pipeline("translation_en_to_de")


def gen_input():
    with open("pyexamples/sample_data/lyrics.txt") as lines:
        for line in lines:
            yield (1, line)


def predict(x):
    y = translator(x)[0]["translation_text"]
    print(f"{x} -> {y}")
    return y


ec = bytewax.Executor()
flow = ec.Dataflow(gen_input())
flow.map(str.strip)
flow.map(predict)
flow.inspect(print)


if __name__ == "__main__":
    ec.build_and_run()
