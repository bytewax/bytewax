import argparse
import pathlib

from bytewax.recovery import SqliteRecoveryConfig

from .bytewax import spawn_cluster, cluster_main

__all__ = [
    "cluster_main"
]


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.run", description="Run a bytewax dataflow"
    )
    parser.add_argument("file_path", metavar="FILE_PATH", type=pathlib.Path)
    run = parser.add_argument_group("Run options")
    run.add_argument(
        "-d",
        "--dataflow-builder",
        type=str,
        default="get_flow",
        help="Name of the function that returns the Dataflow.",
    )
    run.add_argument(
        "--dataflow-args",
        type=str,
        nargs="*",
        help="Args for the dataflow builder function.",
    )
    scaling = parser.add_argument_group(
        "Scaling",
        "You should use either '-p/-w' to spawn multiple processes on this same machine,"
        " or '-i/-a' to spawn a single process on different machines",
    )
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
    scaling.add_argument("-i", "--process-id", type=int, help="Process id")
    scaling.add_argument(
        "-a", "--addresses",
        action="append",
        help="Addresses of other processes"
    )

    # Config options for recovery
    recovery = parser.add_argument_group("Recovery")
    recovery.add_argument(
        "--sqlite-directory",
        type=pathlib.Path,
        help="Passing this argument enables sqlite recovery in the specified folder",
    )
    recovery.add_argument(
        "--epoch-interval",
        type=int,
        default=10,
        help="Number of seconds between state snapshots",
    )

    args = parser.parse_args()
    print(args)
    if (args.processes is not None or args.workers_per_process is not None) and (
        args.process_id is not None or args.addresses is not None
    ):
        parser.error("Can't use both '-w/-p' and '-a/-i'")
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
