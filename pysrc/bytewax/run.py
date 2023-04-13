import argparse
import importlib
import pathlib
import os

from bytewax.recovery import SqliteRecoveryConfig

from .bytewax import cluster_main, spawn_cluster

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
                func = getattr(instance, attr_str)
                if not callable(func):
                    raise ImportFromStringError(
                        f"Factory function `{attr_str}` is not a function"
                    ) from None
                instance = func(arg)
            else:
                instance = getattr(instance, attr_str)
    except AttributeError:
        raise ImportFromStringError(
            f"Attribute '{attrs_str}' not found in module '{module_str}'."
        ) from None

    return instance


class EnvDefault(argparse.Action):
    """Action that uses env variable as default if nothing else was set."""

    def __init__(self, envvar, default=None, **kwargs):
        if envvar:
            default = os.environ.get(envvar, default)
        super(EnvDefault, self).__init__(default=default, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.run", description="Run a bytewax dataflow"
    )
    parser.add_argument(
        "import_str",
        type=str,
        help="Dataflow import string in the formats:\n"
        "<module_name>:<dataflow_variable_name_or_factory_function>\n"
        "<module_name>:<dataflow_factory_function>:<string_argument_for_factory>\n"
        "Example: 'src.dataflow:flow' or 'src.dataflow:get_flow:string_argument'",
        action=EnvDefault,
        envvar="BYTEWAX_IMPORT_STR",
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
        action=EnvDefault,
        envvar="BYTEWAX_PROCESSES",
    )
    scaling.add_argument(
        "-w",
        "--workers-per-process",
        type=int,
        help="Number of workers for each process",
        action=EnvDefault,
        envvar="BYTEWAX_WORKERS_PER_PROCESS",
    )
    scaling.add_argument(
        "-i",
        "--process-id",
        type=int,
        help="Process id",
        action=EnvDefault,
        envvar="BYTEWAX_PROCESS_ID",
    )
    scaling.add_argument(
        "-a",
        "--addresses",
        help="Addresses of other processes, separated by comma:\n"
        "-a localhost:2021,localhost:2022,localhost:2023 ",
        action=EnvDefault,
        envvar="BYTEWAX_ADDRESSES",
    )

    # Config options for recovery
    recovery = parser.add_argument_group("Recovery")
    recovery.add_argument(
        "--sqlite-directory",
        type=pathlib.Path,
        help="Passing this argument enables sqlite recovery in the specified folder",
        action=EnvDefault,
        envvar="BYTEWAX_SQLITE_DIRECTORY",
    )
    recovery.add_argument(
        "--epoch-interval",
        type=int,
        default=10,
        help="Number of seconds between state snapshots",
        action=EnvDefault,
        envvar="BYTEWAX_EPOCH_INTERVAL",
    )

    args = parser.parse_args()
    if (args.processes is not None or args.workers_per_process is not None) and (
        args.process_id is not None or args.addresses is not None
    ):
        parser.error("Can't use both '-w/-p' and '-a/-i'")
    return args


if __name__ == "__main__":
    kwargs = vars(_parse_args())

    # Prepare recovery config
    sqlite_directory = kwargs.pop("sqlite_directory")
    kwargs["recovery_config"] = None
    if sqlite_directory:
        kwargs["recovery_config"] = SqliteRecoveryConfig(sqlite_directory or "./")

    # Prepare addresses
    addresses = kwargs.pop("addresses")
    if addresses:
        kwargs["addresses"] = addresses.split(",")

    # Import the dataflow
    kwargs["flow"] = import_from_string(kwargs.pop("import_str"))
    spawn_cluster(**kwargs)
