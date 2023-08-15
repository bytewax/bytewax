"""Executing dataflows.

Dataflows are run for local development or production by executing
this module as as script with `python -m bytewax.run`.

See `python -m bytewax.run --help` for more info.

If you need to execute a dataflow as part of running unit tests, see
`bytewax.testing`.

Execution
---------

You can run your Dataflow in 3 different ways
The first argument passed to this script is a dataflow getter string.
It should point to the python module containing the dataflow, and the
name of the variable holding the dataflow, or a function call that
returns a dataflow.

For example, if you are at the root of this repository, you can run the
"simple.py" example by calling the script with the following argument:

```
$ python -m bytewax.run examples.simple:flow
```

If instead of a variable, you have a function that returns a dataflow,
you can use a string after the `:` to call the function, possibly with args:


```
$ python -m bytewax.run "my_dataflow:get_flow('/tmp/file')"
```

By default this script will run a single worker on a single process.
You can modify this by using other parameters:

### Local cluster

You can run multiple processes, and multiple workers for each process, by
adding the `-p/--processes` and `-w/--workers-per-process` parameters, without
changing anything in the code:

```
# Runs 3 processes, with 2 workers each, for a total of 6 workers
$ python -m bytewax.run my_dataflow -p3 -w2
```

Bytewax will handle the communication setup between processes/workers.

### Manual cluster

You can also manually handle the multiple processes, and run them on different
machines, by using the `-a/--addresses` and `-i/--process-id` parameters.

Each process should receive a list of addresses of all the processes (the `-a`
parameter) and the id of the current process (a number starting from 0):

```
# First process
$ python -m bytewax.run my_dataflow \
    --addresses "localhost:2021;localhost:2022" \
    --process-id 0
```

```
# Second process
$ python -m bytewax.run my_dataflow \
    --addresses "localhost:2021;localhost:2022" \
    --process-id 1
```

Recovery
--------

See the `bytewax.recovery` module docstring for how to setup recovery.

"""

import argparse
import ast
import inspect
import os
import sys
import traceback
from datetime import timedelta
from pathlib import Path

from bytewax.recovery import RecoveryConfig

from .bytewax import cli_main

__all__ = [
    "cli_main",
]


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
            msg = f"While importing {module_name!r}, an ImportError was raised:\n\n"
            f"{traceback.format_exc()}"
            raise ImportError(msg) from None
        else:
            msg = f"Could not import {module_name!r}."
            raise ImportError(msg) from None

    module = sys.modules[module_name]

    # Parse dataflow_name as a single expression to determine if it's a valid
    # attribute name or function call.
    try:
        expr = ast.parse(dataflow_name.strip(), mode="eval").body
    except SyntaxError:
        msg = f"Failed to parse {dataflow_name!r} as an attribute name or function call"
        raise SyntaxError(msg) from None

    if isinstance(expr, ast.Name):
        name = expr.id
        args = []
        kwargs = {}
    elif isinstance(expr, ast.Call):
        # Ensure the function name is an attribute name only.
        if not isinstance(expr.func, ast.Name):
            msg = f"Function reference must be a simple name: {dataflow_name!r}."
            raise TypeError(msg)

        name = expr.func.id

        # Parse the positional and keyword arguments as literals.
        try:
            args = [ast.literal_eval(arg) for arg in expr.args]
            kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in expr.keywords}
        except ValueError:
            # literal_eval gives cryptic error messages, show a generic
            # message with the full expression instead.
            msg = f"Failed to parse arguments as literal values: {dataflow_name!r}"
            raise ValueError(msg) from None
    else:
        msg = f"Failed to parse {dataflow_name!r} as an attribute name or function call"
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
            dataflow = attr(*args, **kwargs)
        except TypeError as e:
            if not _called_with_wrong_args(attr):
                raise

            msg = f"The factory {dataflow_name!r} in module {module.__name__!r} "
            "could not be called with the specified arguments"
            raise TypeError(msg) from e
    else:
        dataflow = attr

    if isinstance(dataflow, Dataflow):
        return dataflow

    msg = "A valid Bytewax dataflow was not obtained from "
    f"'{module.__name__}:{dataflow_name}'"
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


def _prepare_import(import_str):
    """Given a filename this will try to calculate the python path.

    Add it to the search path and return the actual module name that
    is expected.

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


def _parse_timedelta(s):
    return timedelta(seconds=int(s))


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.run", description="Run a bytewax dataflow"
    )
    parser.add_argument(
        "import_str",
        type=str,
        help="Dataflow import string in the format "
        "<module_name>[:<dataflow_variable_or_factory>] "
        "Example: src.dataflow or src.dataflow:flow or "
        "src.dataflow:get_flow('string_argument')",
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
        action=_EnvDefault,
        envvar="BYTEWAX_PROCESSES",
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

    recovery = parser.add_argument_group(
        "Recovery", """See the `bytewax.recovery` module docstring for more info."""
    )
    recovery.add_argument(
        "-r",
        "--recovery-directory",
        type=Path,
        help="""Local file system directory to look for pre-initialized recovery
        partitions; see `python -m bytewax.recovery` for how to init partitions""",
        action=_EnvDefault,
        envvar="BYTEWAX_RECOVERY_DIRECTORY",
    )
    parser.add_argument(
        "-s",
        "--snapshot-interval",
        type=_parse_timedelta,
        default=timedelta(seconds=10),
        help="""System time duration in seconds to snapshot state for recovery;
        defaults to 10 sec""",
        action=_EnvDefault,
        envvar="BYTEWAX_SNAPSHOT_INTERVAL",
    )
    recovery.add_argument(
        "-b",
        "--backup-interval",
        type=_parse_timedelta,
        default=timedelta(days=1),
        help="""System time duration in seconds to keep extra state snapshots around;
        set this to the interval at which you are backing up recovery partitions;
        defaults to 1 day""",
        action=_EnvDefault,
        envvar="BYTEWAX_RECOVERY_BACKUP_INTERVAL",
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
            "Ignoring the '-p' option, but this should be fixed",
            stacklevel=1,
        )
        args.processes = None

    return args


if __name__ == "__main__":
    kwargs = vars(_parse_args())

    kwargs["epoch_interval"] = kwargs.pop("snapshot_interval")

    recovery_directory, backup_interval = kwargs.pop("recovery_directory"), kwargs.pop(
        "backup_interval"
    )
    kwargs["recovery_config"] = None
    if recovery_directory is not None:
        kwargs["recovery_config"] = RecoveryConfig(recovery_directory, backup_interval)

    # Prepare addresses
    addresses = kwargs.pop("addresses")
    if addresses is not None:
        kwargs["addresses"] = addresses.split(";")

    # Import the dataflow
    module_str, _, attrs_str = kwargs.pop("import_str").partition(":")
    kwargs["flow"] = _locate_dataflow(module_str, attrs_str)

    cli_main(**kwargs)
