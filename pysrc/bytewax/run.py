import argparse
import pathlib

from bytewax.recovery import SqliteRecoveryConfig, KafkaRecoveryConfig

from .bytewax import spawn_cluster


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.run", description="Run a bytewax dataflow"
    )
    parser.add_argument("file_path", metavar="FILE_PATH", type=pathlib.Path)
    parser.add_argument(
        "-d",
        "--dataflow-name",
        type=str,
        default="get_flow",
        help="Name of the Dataflow getter function",
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
    parser.add_argument(
        "--epoch-interval",
        type=int,
        default=10,
        help="Number of seconds between state snapshots",
    )

    args = parser.parse_args()
    return args


def _get_recovery_config(
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


def _main():
    kwargs = vars(_parse_args())
    recovery_engine = kwargs.pop("recovery_engine")
    kafka_topic = kwargs.pop("kafka_topic")
    kafka_brokers = kwargs.pop("kafka_brokers")
    sqlite_directory = kwargs.pop("sqlite_directory")
    recovery_config = _get_recovery_config(
        recovery_engine,
        kafka_topic,
        kafka_brokers,
        sqlite_directory,
    )
    kwargs["recovery_config"] = recovery_config
    spawn_cluster(**kwargs)


if __name__ == "__main__":
    _main()
