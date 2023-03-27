import sys
import argparse
import pathlib

from bytewax.execution import run
from bytewax.recovery import SqliteRecoveryConfig, KafkaRecoveryConfig


#  Take kwargs and serialize them to a list of arguments for Popen
def _make_command(
    file_path,
    dataflow_name,
    dataflow_args,
    processes,
    workers_per_process,
    epoch_interval,
    recovery_engine,
    kafka_topic,
    kafka_brokers,
    sqlite_directory,
):
    args = [sys.executable, "-m", "bytewax.run", file_path]
    if workers_per_process is not None:
        args.extend(["-w", f"{workers_per_process}"])
    if processes is not None:
        args.extend(["-p", f"{processes}"])
    if dataflow_name is not None:
        args.extend(["-d", f"{dataflow_name}"])
    if dataflow_args is not None:
        args.extend(["--dataflow-args", *dataflow_args])
    if epoch_interval is not None:
        args.extend(["--epoch-interval", f"{epoch_interval}"])
    if recovery_engine is not None:
        args.extend(["--recovery-engine", f"{recovery_engine}"])
    if kafka_brokers is not None:
        args.extend(["--kafka-brokers", f"{kafka_brokers}"])
    if kafka_topic is not None:
        args.extend(["--kafka-topic", f"{kafka_topic}"])
    if sqlite_directory is not None:
        args.extend(["--sqlite-directory", f"{sqlite_directory}"])
    return args


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.run", description="Run a bytewax dataflow"
    )
    parser.add_argument("file_path", metavar="FILE_PATH", type=pathlib.Path)
    parser.add_argument(
        "-d",
        "--dataflow-name",
        type=str,
        default="flow",
        help="Name of the Dataflow variable",
    )
    parser.add_argument(
        "--dataflow-args",
        type=str,
        nargs="*",
        help="Args to pass to the dataflow getter",
    )
    scaling = parser.add_argument_group("Scaling")
    scaling.add_argument(
        "-p",
        "--processes",
        type=int,
        help="Number of separate processes to run",
    )
    scaling.add_argument(
        "-w",
        "--workers-per-process",
        type=int,
        help="Number of workers for each process",
    )
    # Config options for recovery
    recovery = parser.add_argument_group("Recovery")
    recovery.add_argument(
        "-r", "--recovery-engine", type=str, choices=["kafka", "sqlite"]
    )
    kafka_config = parser.add_argument_group("Kafka recovery config")
    kafka_config.add_argument(
        "--kafka-brokers", type=list[str], default=["localhost:9092"]
    )
    kafka_config.add_argument("--kafka-topic", type=str)
    sqlite_config = parser.add_argument_group("SQLite recovery config")
    sqlite_config.add_argument("--sqlite-directory", type=pathlib.Path)

    # Epoch configuration
    parser.add_argument("-s", "--epoch-interval", type=int, default=10)

    args = parser.parse_args()
    return args


def get_recovery_config(
    recovery_engine=None,
    kafka_topic=None,
    kafka_brokers=None,
    sqlite_directory=None,
):
    recovery_config = None
    if recovery_engine is not None:
        if recovery_engine == "kafka":
            recovery_config = KafkaRecoveryConfig(
                brokers=kafka_brokers, topic_prefix=kafka_topic
            )
        elif recovery_engine == "sqlite":
            recovery_config = SqliteRecoveryConfig(sqlite_directory or "./")
    return recovery_config


def main():
    kwargs = vars(_parse_args())
    recovery_engine = kwargs.pop("recovery_engine")
    kafka_topic = kwargs.pop("kafka_topic")
    kafka_brokers = kwargs.pop("kafka_brokers")
    sqlite_directory = kwargs.pop("sqlite_directory")
    recovery_config = get_recovery_config(
        recovery_engine,
        kafka_topic,
        kafka_brokers,
        sqlite_directory,
    )
    kwargs["recovery_config"] = recovery_config
    run(**kwargs)


if __name__ == "__main__":
    main()
