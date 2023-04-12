import argparse
import pathlib
import importlib

from bytewax.recovery import SqliteRecoveryConfig

from .bytewax import spawn_cluster, cluster_main

__all__ = ["cluster_main"]


class ImportFromStringError(Exception):
    pass


def import_from_string(import_str):
    if not isinstance(import_str, str):
        return import_str

    help_msg = f"Import string '{import_str}' must be in format "
    "'<module>:<attribute>' or '<module>:<factory function>:<optional string arg>."

    if import_str.count(":") > 2:
        raise ImportFromStringError(help_msg)

    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        raise ImportFromStringError(help_msg)

    try:
        module = importlib.import_module(module_str)
    except ImportError as exc:
        if exc.name != module_str:
            raise exc from None
        raise ImportFromStringError(f'Could not import module "{module_str}".')

    instance = module
    try:
        for attr_str in attrs_str.split("."):
            if ":" in attr_str:
                attr_str, _, arg = attr_str.partition(":")
                instance = getattr(instance, attr_str)(arg)
            else:
                instance = getattr(instance, attr_str)
    except AttributeError:
        raise ImportFromStringError(
            f"Attribute '{attrs_str}' not found in module '{module_str}'."
        ) from None

    return instance


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.run", description="Run a bytewax dataflow"
    )
    parser.add_argument(
        "import_str",
        type=str,
        help="dataflow import string in the format "
        "<module name>:<dataflow variable name or factory function> "
        "or <module>:<dataflow factory function>:<string argument for factory>)",
    )
    scaling = parser.add_argument_group(
        "Scaling",
        "You should use either '-p/-w' to spawn multiple processes "
        "on this same machine, or '-i/-a' to spawn a single process "
        "on different machines",
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
        "-a", "--addresses", action="append", help="Addresses of other processes"
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
    if (args.processes is not None or args.workers_per_process is not None) and (
        args.process_id is not None or args.addresses is not None
    ):
        parser.error("Can't use both '-w/-p' and '-a/-i'")
    return args


if __name__ == "__main__":
    kwargs = vars(_parse_args())
    sqlite_directory = kwargs.pop("sqlite_directory")
    recovery_config = None
    if sqlite_directory:
        recovery_config = SqliteRecoveryConfig(sqlite_directory or "./")
    kwargs["recovery_config"] = recovery_config
    kwargs["flow"] = import_from_string(kwargs.pop("import_str"))
    spawn_cluster(**kwargs)
