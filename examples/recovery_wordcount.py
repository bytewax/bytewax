import re

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.recovery import KafkaRecoveryConfig, SqliteRecoveryConfig


def input_builder(worker_index, worker_count, resume_state):
    with open("examples/sample_data/wordcount.txt") as lines:
        resume_state = resume_state or 0
        for i, line in enumerate(lines):
            if i < resume_state:
                continue
            if i % worker_count != worker_index:
                continue
            # "Fix" by commenting out these two lines below and re-run
            if i == 3:
                raise RuntimeError("boom")
            resume_state += 1
            yield (resume_state, line)


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


def count_builder():
    return 0


def add(running_count, new_count):
    running_count += new_count
    return running_count, running_count


flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
# "Here, we have FULL sentences."
flow.map(lower)
# "here, we have lowercase sentences."
flow.flat_map(tokenize)
# "words"
flow.map(initial_count)
# ("word", 1)
flow.stateful_map("running_count", count_builder, add)
# ("word", running_count)
flow.capture(StdOutputConfig())


recovery_config = KafkaRecoveryConfig(
    ["127.0.0.1:9092"],
    "wordcount",
)
# recovery_config = SqliteRecoveryConfig(
#     ".",
# )

if __name__ == "__main__":
    spawn_cluster(
        flow,
        recovery_config=recovery_config,
        **parse.cluster_args(),
    )
