"""Executing dataflows and the Bytewax runtime.

Dataflows are run for local development or production by executing
this module as as script with `python -m bytewax.run`.

See `python -m bytewax.run --help` for more info.

If you need to execute a dataflow as part of running unit tests, see
{py:obj}`bytewax.testing`.

"""

import argparse
import ast
import inspect
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Tuple

from bytewax._bytewax import cli_main
from bytewax.backup import Backup
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig, SnapshotMode

__all__ = [
    "cli_main",
]


def _locate_subclass(module_name: str, object_name: str, superclass):
    """Import a module and try to find a Dataflow within it.

    Check if the given string is a variable name or a function.
    Call a function to get the dataflow instance, or return the
    variable directly.

    This is adapted from Flask's codebase.
    """
    try:
        __import__(module_name)
    except ImportError as ex:
        # Reraise the ImportError if it occurred within the imported module.
        # Determine this by checking whether the trace has a depth > 1.
        if ex.__traceback__ is not None:
            raise
        else:
            msg = f"Could not import {module_name!r}."
            raise ImportError(msg) from None

    module = sys.modules[module_name]

    # Parse dataflow_name as a single expression to determine if it's a valid
    # attribute name or function call.
    try:
        expr = ast.parse(object_name.strip(), mode="eval").body
    except SyntaxError:
        msg = f"Failed to parse {object_name!r} as an attribute name or function call"
        raise SyntaxError(msg) from None

    if isinstance(expr, ast.Name):
        name = expr.id
        args = []
        kwargs = {}
    elif isinstance(expr, ast.Call):
        # Ensure the function name is an attribute name only.
        if not isinstance(expr.func, ast.Name):
            msg = f"Function reference must be a simple name: {object_name!r}."
            raise TypeError(msg)

        name = expr.func.id

        # Parse the positional and keyword arguments as literals.
        try:
            args = [ast.literal_eval(arg) for arg in expr.args]
            kwargs = {str(kw.arg): ast.literal_eval(kw.value) for kw in expr.keywords}
        except ValueError:
            # literal_eval gives cryptic error messages, show a generic
            # message with the full expression instead.
            msg = f"Failed to parse arguments as literal values: {object_name!r}"
            raise ValueError(msg) from None
    else:
        msg = f"Failed to parse {object_name!r} as an attribute name or function call"
        raise ValueError(msg)

    try:
        attr = getattr(module, name)
    except AttributeError as e:
        msg = f"Failed to find attribute {name!r} in {module.__name__!r}."
        raise AttributeError(msg) from e

    # If the attribute is a function, call it with any args and kwargs
    # to get the real application.
    if inspect.isfunction(attr):
        try:
            instance = attr(*args, **kwargs)
        except TypeError as e:
            if not _called_with_wrong_args(attr):
                raise

            msg = (
                f"The factory {object_name!r} in module {module.__name__!r} "
                "could not be called with the specified arguments"
            )
            raise TypeError(msg) from e
    else:
        instance = attr

    if isinstance(instance, superclass):
        return instance

    msg = "A valid object was not obtained from " f"'{module.__name__}:{object_name}'"
    raise RuntimeError(msg)


def _called_with_wrong_args(f):
    # This is taken from Flask's codebase.
    tb = sys.exc_info()[2]

    try:
        while tb is not None:
            if tb.tb_frame.f_code is f.__code__:
                # In the function, it was called successfully.
                return False

            tb = tb.tb_next

        # Didn't reach the function.
        return True
    finally:
        # Delete tb to break a circular reference.
        # https://docs.python.org/2/library/sys.html#sys.exc_info
        del tb


class _EnvDefault(argparse.Action):
    """Action that uses env variable as default if nothing else was set."""

    def __init__(self, envvar, default=None, **kwargs):
        if envvar:
            default = os.environ.get(envvar, default)
            kwargs["help"] += f" [env: {envvar}]"
        super(_EnvDefault, self).__init__(default=default, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def _prepare_import(import_str: str, default_name: str = "flow") -> Tuple[str, str]:
    """Given a filename this will try to calculate the python path.

    Add it to the search path and return the actual module name that
    is expected.

    This is adapted from Flask's codebase.

    """
    path, _, object_name = import_str.partition(":")
    if not object_name:
        object_name = default_name
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

    mod_str = ".".join(module_name[::-1])
    attr_str = object_name

    return (mod_str, attr_str)


def _parse_timedelta(s):
    return timedelta(seconds=int(s))


def _create_arg_parser():
    """Create and return an argparse instance.

    This function returns the parser, as we add options for scaling
    that are used only for testing in the testing namespace.
    """
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.run",
        description="Run a bytewax dataflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "import_str",
        type=str,
        help="Dataflow import string in the format "
        "<module_name>[:<dataflow_variable_or_factory>] "
        "Example: src.dataflow or src.dataflow:flow or "
        "src.dataflow:get_flow('string_argument')",
    )
    recovery = parser.add_argument_group(
        "Recovery", """See the `bytewax.recovery` module docstring for more info."""
    )
    recovery.add_argument(
        "-r",
        "--recovery-directory",
        type=Path,
        help="""Local file system directory to use to save recovery partitions""",
        action=_EnvDefault,
        envvar="BYTEWAX_RECOVERY_DIRECTORY",
    )
    recovery.add_argument(
        "--snapshot-mode",
        choices=["immediate", "batch"],
        default="immediate",
        help="Whether to take snapshots at every state change or "
        "in batch at the end of each epoch.",
        action=_EnvDefault,
        envvar="BYTEWAX_SNAPSHOT_MODE",
    )
    recovery.add_argument(
        "--backup",
        type=str,
        help="Backup import string in the format "
        "<module_name>[:<backup_variable_or_factory>] "
        "Example: src.dataflow or src.dataflow:backup_object or "
        "src.dataflow:get_backup('string_argument')",
    )
    recovery.add_argument(
        "-s",
        "--snapshot-interval",
        type=_parse_timedelta,
        help="""System time duration in seconds to snapshot state for recovery;
        on resume, dataflow might need to rewind and replay all the data processed
        in one of these intervals""",
        action=_EnvDefault,
        envvar="BYTEWAX_SNAPSHOT_INTERVAL",
    )
    return parser


def _parse_args():
    arg_parser = _create_arg_parser()

    # Add scaling arguments for the run namespace
    scaling = arg_parser.add_argument_group(
        "Scaling",
        "You should use either '-w' to spawn multiple workers "
        "within a process, or '-i/-a' to manage multiple processes",
    )
    scaling.add_argument(
        "-w",
        "--workers-per-process",
        type=int,
        help="Number of workers for each process",
        action=_EnvDefault,
        envvar="BYTEWAX_WORKERS_PER_PROCESS",
    )
    scaling.add_argument(
        "-i",
        "--process-id",
        type=int,
        help="Process id",
        action=_EnvDefault,
        envvar="BYTEWAX_PROCESS_ID",
    )
    scaling.add_argument(
        "-a",
        "--addresses",
        help="Addresses of other processes, separated by semicolon:\n"
        '-a "localhost:2021;localhost:2022;localhost:2023" ',
        action=_EnvDefault,
        envvar="BYTEWAX_ADDRESSES",
    )

    args = arg_parser.parse_args()

    # First of all check if a process_id was set with a different
    # env var, used in the helm chart for deploy
    env = os.environ
    if args.process_id is None:
        if "BYTEWAX_POD_NAME" in env and "BYTEWAX_STATEFULSET_NAME" in env:
            args.process_id = int(
                env["BYTEWAX_POD_NAME"].replace(
                    env["BYTEWAX_STATEFULSET_NAME"] + "-", ""
                )
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
                    [address.strip() for address in hostfile if address.strip()]
                )
        else:
            arg_parser.error(
                "the addresses option is required if a process_id is passed"
            )

    # If recovery is configured, make sure that the snapshot_interval and
    # backup_interval are set.
    if args.recovery_directory is not None and args.snapshot_interval is None:
        arg_parser.error(
            "when running with recovery, the `-s/--snapshot_interval` "
            "value must be set. For more information please see "
            "https://bytewax.io/docs/concepts/recovery."
        )

    return args


if __name__ == "__main__":
    kwargs = vars(_parse_args())

    snapshot_interval = kwargs.pop("snapshot_interval")
    recovery_directory = kwargs.pop("recovery_directory")
    backup_import_str = kwargs.pop("backup")
    snapshot_mode = kwargs.pop("snapshot_mode")
    if snapshot_mode == "immediate":
        snapshot_mode = SnapshotMode.Immediate
    elif snapshot_mode == "batch":
        snapshot_mode = SnapshotMode.Batch
    else:
        msg = f"Invalid snapshot mode passed: {snapshot_mode}"
        raise ValueError(msg)

    # Recovery config
    kwargs["recovery_config"] = None
    if recovery_directory is not None:
        backup = None
        if backup_import_str is not None:
            mod_str, attrs_str = _prepare_import(backup_import_str, "backup")
            backup = _locate_subclass(mod_str, attrs_str, Backup)
        kwargs["epoch_interval"] = snapshot_interval
        kwargs["recovery_config"] = RecoveryConfig(
            recovery_directory,
            backup=backup,
            snapshot_mode=snapshot_mode,
        )
    else:
        # Default epoch interval if there is no recovery setup. Since
        # there's no recovery, this needs not be coordinated with
        # anything else.
        kwargs["epoch_interval"] = snapshot_interval or timedelta(seconds=10)

    # Prepare addresses
    addresses = kwargs.pop("addresses")
    if addresses is not None:
        kwargs["addresses"] = addresses.split(";")

    # Import the dataflow
    mod_str, attrs_str = _prepare_import(kwargs.pop("import_str"))
    kwargs["flow"] = _locate_subclass(mod_str, attrs_str, Dataflow)

    cli_main(**kwargs)
