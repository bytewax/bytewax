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

Bytewax allows you to **recover** a stateful dataflow; it will let you
resume processing and output due to a failure without re-processing
all initial data to re-calculate all internal state.

It does this by snapshoting state and progress information for a
single dataflow instance in a distributed set of SQLite databases in
the **recovery directory** periodically. The **epoch** is period of
this snapshotting. Bytewax defaults to a new epoch every 10 seconds,
but you can change it with the `epoch_interval` parameter.

When you run your dataflow it will start backing up recovery data
automatically. Recovery data for multiple dataflows _must not_ be
mixed together.

If the dataflow fails, first you must fix whatever underlying fault
caused the issue. That might mean deploying new code which fixes a bug
or resolving an issue with a connected system.

Once that is done, re-run the dataflow using the _same recovery
directory_. Bytewax will automatically read the progress of the
previous dataflow execution and determine the most recent epoch that
processing can resume at. Output should resume from that
epoch. Because snapshotting only happens periodically, the dataflow
can only resume on epoch boundaries.

It is possible that your output systems will see duplicate data around
the resume epoch; design your systems to support at-least-once
processing.

If you want to fully restart a dataflow and ignore previous state,
delete the data in the recovery directory.

Currently it is not possible to recover a dataflow with a different
number of workers than when it failed.

"""
import argparse
import ast
import inspect
import os
import pathlib
import sys
import traceback

from bytewax.recovery import SqliteRecoveryConfig

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

    # Config options for recovery
    recovery = parser.add_argument_group("Recovery")
    recovery.add_argument(
        "--sqlite-directory",
        type=pathlib.Path,
        help="Passing this argument enables sqlite recovery in the specified folder",
        action=_EnvDefault,
        envvar="BYTEWAX_SQLITE_DIRECTORY",
    )
    recovery.add_argument(
        "--epoch-interval",
        type=int,
        default=10,
        help="Number of seconds between state snapshots",
        action=_EnvDefault,
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
