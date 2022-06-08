import re

from bytewax import AdvanceTo, Dataflow, Emit, parse, ManualInputConfig, spawn_cluster
from bytewax.recovery import KafkaRecoveryConfig


def input_builder(worker_index, worker_count, resume_epoch):
    with open("examples/sample_data/wordcount.txt") as lines:
        for epoch, line in enumerate(lines):
            if epoch < resume_epoch:
                continue
            if epoch % worker_count != worker_index:
                continue
            # if epoch == 12:
            #     raise RuntimeError("boom")
            yield AdvanceTo(epoch)
            yield Emit(line)


def output_builder(worker_index, worker_count):
    return print


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


def count_builder(word):
    return 0


def add(running_count, new_count):
    running_count += new_count
    return running_count, running_count


flow = Dataflow()
# "Here, we have FULL sentences."
flow.map(lower)
# "here, we have lowercase sentences."
flow.flat_map(tokenize)
# "words"
flow.map(initial_count)
# ("word", 1)
flow.stateful_map("running_count", count_builder, add)
# ("word", running_count)
flow.capture()


if __name__ == "__main__":
    recovery_config = KafkaRecoveryConfig(
        ["localhost:9092"], "bytewax-state", create=True
    )
    spawn_cluster(
        flow,
        ManualInputConfig(input_builder),
        output_builder,
        recovery_config=recovery_config,
        **parse.cluster_args()
    )
