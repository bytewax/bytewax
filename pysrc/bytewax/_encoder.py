"""Dataflow JSON encoding."""
import datetime
import inspect
import json
import types
from collections import ChainMap
from typing import List, Protocol, runtime_checkable

from bytewax.dataflow import Dataflow, Operator


class DataflowEncoder(json.JSONEncoder):
    """Encoder that can handle a `bytewax.Dataflow`."""

    def default(self, obj):
        if hasattr(obj, "__json__"):
            return obj.__json__()

        # Check if the object is a class, and return its name.
        # If the object is a class the call to __getstate__ below
        # WILL fail since we are not passing a `self` parameter.
        if inspect.isclass(obj):
            return obj.__qualname__

        if isinstance(obj, types.BuiltinFunctionType):
            return obj.__name__
        if isinstance(obj, types.MethodDescriptorType):
            return obj.__name__
        if isinstance(obj, types.FunctionType):
            return obj.__name__
        if isinstance(obj, types.BuiltinMethodType):
            return obj.__name__
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, datetime.timedelta):
            return str(obj)
        if isinstance(obj, type):  # For callable types like `list` and `dict`
            return obj.__name__

        # Call the default encoder method for any other instance types.
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError as err:
            msg = f"{obj} can not be JSON encoded"
            raise TypeError(msg) from err


def encode_dataflow(dataflow: Dataflow):
    """Encode this dataflow into JSON."""
    return json.dumps(dataflow, cls=DataflowEncoder, sort_keys=True)


@runtime_checkable
class _Graphable(Protocol):
    substeps: List[Operator]

    def _get_id(self) -> str:
        ...


def _to_plantuml_step(
    step: _Graphable,
    stream_to_orig_port: ChainMap[str, str],
    recursive: bool = False,
) -> List[str]:
    step_id = step.step_id
    lines = [
        f"component {step_id} [",
        f"    {step_id} ({type(step).__name__})",
        "]",
        f"component {step_id} " "{",  # noqa: ISC001
    ]

    inner_lines = []

    for port in step.inp_ports.values():
        inner_lines.append(f"portin {port.port_id}")
    for port in step.out_ports.values():
        inner_lines.append(f"portout {port.port_id}")
        stream_to_orig_port[port.stream_id] = port.port_id

    for port in step.inp_ports.values():
        from_port_id = stream_to_orig_port[port.stream_id]
        if recursive:
            inner_lines.append(f"{from_port_id} --> {port.port_id} : {port.stream_id}")
        else:
            inner_lines.append(f"{from_port_id} --> {port.port_id}")

    if recursive:
        # Add in an "inner scope". Rewrite the port that originated a
        # stream from the true output port to the fake input port on
        # this containing step.
        stream_to_orig_port = stream_to_orig_port.new_child()
        for port in step.inp_ports.values():
            stream_to_orig_port[port.stream_id] = port.port_id

        for substep in step.substeps:
            inner_lines += _to_plantuml_step(substep, stream_to_orig_port, recursive)

        # Now also connect all the inner outputs to the containing
        # outputs.
        if len(step.substeps) > 0:
            for port in step.out_ports.values():
                from_port_id = stream_to_orig_port[port.stream_id]
                inner_lines.append(
                    f"{from_port_id} --> {port.port_id} : {port.stream_id}"
                )

    lines += ["    " + line for line in inner_lines]

    lines.append("}")
    return lines


def to_plantuml(step: _Graphable, recursive: bool = False) -> str:
    lines = [
        "@startuml",
    ]
    stream_to_orig_port = ChainMap()
    for substep in step.substeps:
        lines += _to_plantuml_step(substep, stream_to_orig_port, recursive)
    lines.append("@enduml")
    return "\n".join(lines)
