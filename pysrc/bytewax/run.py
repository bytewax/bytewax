import argparse
import importlib
import pathlib
import os
import sys

from bytewax.recovery import SqliteRecoveryConfig

from .bytewax import cluster_main, spawn_cluster

__all__ = ["cluster_main"]


class ImportFromStringError(Exception):
    pass


def import_from_string(import_str):
    if not isinstance(import_str, str):
        return import_str

    help_msg = (
        f"Import string '{import_str}' must be in format "
        "'<module>:<attribute>' or '<module>:<factory function>:<optional string arg>."
    )

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


def _prepare_import(import_str):
    """Given a filename this will try to calculate the python path, add it
    to the search path and return the actual module name that is expected.

    This was taken from Flask's codebase.
    """
    path, _, flow_name = import_str.partition(":")
    if flow_name == "":
        flow_name = "flow"
    path = os.path.realpath(path)

    fname, ext = os.path.splitext(path)
    if ext == ".py":
        path = fname

    if os.path.basename(path) == "__init__":
        path = os.path.dirname(path)

    module_name = []

    # move up until outside package structure (no __init__.py)
    while True:
        path, name = os.path.split(path)
        module_name.append(name)

        if not os.path.exists(os.path.join(path, "__init__.py")):
            break

    if sys.path[0] != path:
        sys.path.insert(0, path)

    return ".".join(module_name[::-1]) + f":{flow_name}"


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
    )
    scaling = parser.add_argument_group(
        "Scaling",
        "You should use either '-p' to spawn multiple processes "
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
        help="Addresses of other processes, separated by semicolumn:\n"
        '-a "localhost:2021;localhost:2022;localhost:2023" ',
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
    args.import_str = _prepare_import(args.import_str)

    # First of all check if a process_id was set with a different
    # env var, used in the helm chart for deploy
    env = os.environ
    if args.process_id is None:
        if "BYTEWAX_POD_NAME" in env and "BYTEWAX_STATEFULSET_NAME" in env:
            args.process_id = env["BYTEWAX_POD_NAME"].replace(
                env["BYTEWAX_STATEFULSET_NAME"] + "-", ""
            )

    # If process_id is set, check if the addresses parameter is correctly set.
    # Again, we check for a different env var that can be used by the helm chart,
    # which specifies a file with host addresses. We read the file and populate
    # the argument if needed.
    # Not using else since we might have modified the condition inside the first if.
    if args.process_id is not None and args.addresses is None:
        if "BYTEWAX_HOSTFILE_PATH" in env:
            with open(env["BYTEWAX_HOSTFILE_PATH"]) as hostfile:
                args.addresses = ";".join(
                    [address.strip() for address in hostfile if address.strip() != ""]
                )
        else:
            parser.error("the addresses option is required if a process_id is passed")

    # The dataflow should either run as a local multiprocess cluster,
    # or a single process with urls for the others, so we manually
    # validate the options to avoid confusion.
    if args.processes is not None and (
        args.process_id is not None or args.addresses is not None
    ):
        import warnings

        warnings.warn(
            "Both '-p' and '-a/-i' specified. "
            "Ignoring the '-p' option, but this should be fixed"
        )
        args.processes = None
        # parser.error("Can't use both '-p' and '-a/-i'")

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
        kwargs["addresses"] = addresses.split(";")

    # Import the dataflow
    kwargs["flow"] = import_from_string(kwargs.pop("import_str"))
    spawn_cluster(**kwargs)
