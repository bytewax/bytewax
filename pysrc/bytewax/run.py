import argparse
import pathlib

from bytewax.recovery import SqliteRecoveryConfig

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
        default="flow",
        help="Name of the Dataflow getter. "
        "Either a variable or a function name in the Python file",
    )
    parser.add_argument(
        "--dataflow-args",
        type=str,
        nargs="*",
        help="Args for the dataflow getter function, "
        "if --dataflow-name is a function",
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
        "--sqlite-directory",
        type=pathlib.Path,
        help="Passing this argument enables sqlite recovery in the specified folder",
    )

    # Epoch configuration
    parser.add_argument(
        "--epoch-interval",
        type=int,
        default=10,
        help="Number of seconds between state snapshots",
    )

    args = parser.parse_args()
    return args


def _main():
    kwargs = vars(_parse_args())
    sqlite_directory = kwargs.pop("sqlite_directory")
    recovery_config = None
    if sqlite_directory:
        recovery_config = SqliteRecoveryConfig(sqlite_directory or "./")
    kwargs["recovery_config"] = recovery_config
    spawn_cluster(**kwargs)


if __name__ == "__main__":
    _main()
