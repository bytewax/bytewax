import argparse
import pathlib
import os
import sys
import ast
import traceback
import inspect

from bytewax.recovery import SqliteRecoveryConfig
from .bytewax import cli_main


def _locate_dataflow(module_name, dataflow_name):
    """Import a module and try to find a Dataflow within it.

    Check if the given string is a variable name or a function.
    Call a function to get the dataflow instance, or return the
    variable directly.

    This is adapted from Flask's codebase.
    """
    from bytewax.dataflow import Dataflow

    try:
        __import__(module_name)
    except ImportError:
        # Reraise the ImportError if it occurred within the imported module.
        # Determine this by checking whether the trace has a depth > 1.
        if sys.exc_info()[2].tb_next:
            raise ImportError(
                f"While importing {module_name!r}, an ImportError was"
                f" raised:\n\n{traceback.format_exc()}"
            ) from None
        else:
            raise ImportError(f"Could not import {module_name!r}.") from None

    module = sys.modules[module_name]

    # Parse dataflow_name as a single expression to determine if it's a valid
    # attribute name or function call.
    try:
        expr = ast.parse(dataflow_name.strip(), mode="eval").body
    except SyntaxError:
        raise SyntaxError(
            f"Failed to parse {dataflow_name!r} as an attribute name or function call."
        ) from None

    if isinstance(expr, ast.Name):
        name = expr.id
        args = []
        kwargs = {}
    elif isinstance(expr, ast.Call):
        # Ensure the function name is an attribute name only.
        if not isinstance(expr.func, ast.Name):
            raise TypeError(
                f"Function reference must be a simple name: {dataflow_name!r}."
            )

        name = expr.func.id

        # Parse the positional and keyword arguments as literals.
        try:
            args = [ast.literal_eval(arg) for arg in expr.args]
            kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in expr.keywords}
        except ValueError:
            # literal_eval gives cryptic error messages, show a generic
            # message with the full expression instead.
            raise ValueError(
                f"Failed to parse arguments as literal values: {dataflow_name!r}."
            ) from None
    else:
        raise ValueError(
            f"Failed to parse {dataflow_name!r} as an attribute name or function call."
        )

    try:
        attr = getattr(module, name)
    except AttributeError as e:
        raise AttributeError(
            f"Failed to find attribute {name!r} in {module.__name__!r}."
        ) from e

    # If the attribute is a function, call it with any args and kwargs
    # to get the real application.
    if inspect.isfunction(attr):
        try:
            dataflow = attr(*args, **kwargs)
        except TypeError as e:
            if not _called_with_wrong_args(attr):
                raise

            raise TypeError(
                f"The factory {dataflow_name!r} in module"
                f" {module.__name__!r} could not be called with the"
                " specified arguments."
            ) from e
    else:
        dataflow = attr

    if isinstance(dataflow, Dataflow):
        return dataflow

    raise RuntimeError(
        "A valid Bytewax dataflow was not obtained from"
        f" '{module.__name__}:{dataflow_name}'."
    )


def _called_with_wrong_args(f):
    """Check whether calling a function raised a ``TypeError`` because
    the call failed or because something in the factory raised the
    error.

    This is taken from Flask's codebase.

    :param f: The function that was called.
    :return: ``True`` if the call failed.
    """
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


class EnvDefault(argparse.Action):
    """Action that uses env variable as default if nothing else was set."""

    def __init__(self, envvar, default=None, **kwargs):
        if envvar:
            default = os.environ.get(envvar, default)
            kwargs["help"] += f" [env: {envvar}]"
        super(EnvDefault, self).__init__(default=default, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def _prepare_import(import_str):
    """Given a filename this will try to calculate the python path, add it
    to the search path and return the actual module name that is expected.

    This is adapted from Flask's codebase.
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
        help="Dataflow import string in the format "
        "<module_name>:<dataflow_variable_or_factory> "
        "Example: src.dataflow:flow or src.dataflow:get_flow('string_argument')",
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

    return args


if __name__ == "__main__":
    kwargs = vars(_parse_args())

    # Prepare recovery config
    sqlite_directory = kwargs.pop("sqlite_directory")
    kwargs["recovery_config"] = None
    if sqlite_directory:
        kwargs["recovery_config"] = SqliteRecoveryConfig(sqlite_directory)

    # Prepare addresses
    addresses = kwargs.pop("addresses")
    if addresses:
        kwargs["addresses"] = addresses.split(";")

    # Import the dataflow
    module_str, _, attrs_str = kwargs.pop("import_str").partition(":")
    kwargs["flow"] = _locate_dataflow(module_str, attrs_str)
    cli_main(**kwargs)
