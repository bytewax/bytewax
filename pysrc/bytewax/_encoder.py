"""Serialization of the dataflow data model."""
import json
from collections import ChainMap
from functools import singledispatch
from typing import Any, Dict, List

from bytewax.dataflow import Dataflow, MultiPort, Operator, SinglePort


@singledispatch
def json_for(obj) -> Any:
    """Hook to extend the JSON serialization.

    Register new types via `@json_for.register`. See `singledispatch`
    for more info.

    If this contains nested un-serializeable types, this will be
    re-called with them later by `json.dumps`; you don't have to
    recurse yourself.

    Args:
        obj: Un-handled type to attempt to encode.

    Returns:
        A new value that is JSON serializable.

    """
    return obj


@json_for.register
def _(df: Dataflow) -> Dict:
    return {
        "type": "Dataflow",
        "flow_id": df.flow_id,
        "substeps": df.substeps,
    }


@json_for.register
def _(step: Operator) -> Dict:
    inp_ports = {
        name: list(port.stream_ids.values()) for name, port in step.inp_ports().items()
    }
    out_ports = {
        name: list(port.stream_ids.values()) for name, port in step.out_ports().items()
    }
    return {
        "type": step.__class__.__name__,
        "step_id": step.step_id,
        "inp_ports": inp_ports,
        "out_ports": out_ports,
        "substeps": step.substeps,
    }


class _Encoder(json.JSONEncoder):
    def default(self, obj):
        return json_for(obj)


def to_json(flow: Dataflow) -> str:
    """Encode this dataflow into JSON.

    Args:
        flow: Dataflow.

    Returns:
        JSON string.
    """
    return json.dumps(flow, cls=_Encoder, indent=2)


def _to_plantuml_step(
    step: Operator,
    stream_to_orig_port: ChainMap[str, str],
    recursive: bool = False,
) -> List[str]:
    step_id = step.get_id()
    lines = [
        f"component {step_id} [",
        f"    {step_id} ({type(step).__name__})",
        "]",
        f"component {step_id} " "{",  # noqa: ISC001
    ]

    inner_lines = []

    for port in step.inp_ports().values():
        inner_lines.append(f"portin {port.port_id}")
    for port in step.out_ports().values():
        inner_lines.append(f"portout {port.port_id}")
        for stream_id in port.stream_ids.values():
            stream_to_orig_port[stream_id] = port.port_id

    for port in step.inp_ports().values():
        if not recursive:
            for stream_id in port.stream_ids.values():
                from_port_id = stream_to_orig_port[stream_id]
                inner_lines.append(f"{from_port_id} --> {port.port_id}")
        elif isinstance(port, SinglePort):
            from_port_id = stream_to_orig_port[port.stream_id]
            inner_lines.append(f"{from_port_id} --> {port.port_id} : {port.stream_id}")
        elif isinstance(port, MultiPort):
            for stream_name, stream_id in port.stream_ids.items():
                from_port_id = stream_to_orig_port[stream_id]
                inner_lines.append(
                    f"{from_port_id} --> {port.port_id} "
                    f': "{stream_id} @ {stream_name}"'
                )

    if recursive:
        # Add in an "inner scope". Rewrite the port that originated a
        # stream from the true output port to the fake input port on
        # this containing step.
        stream_to_orig_port = stream_to_orig_port.new_child()
        for port in step.inp_ports().values():
            for stream_id in port.stream_ids.values():
                stream_to_orig_port[stream_id] = port.port_id

        for substep in step.substeps:
            inner_lines += _to_plantuml_step(substep, stream_to_orig_port, recursive)

        # Now also connect all the inner outputs to the containing
        # outputs.
        if len(step.substeps) > 0:
            for port in step.out_ports().values():
                for stream_id in port.stream_ids.values():
                    from_port_id = stream_to_orig_port[stream_id]
                    inner_lines.append(
                        f"{from_port_id} --> {port.port_id} : {stream_id}"
                    )

    lines += ["    " + line for line in inner_lines]

    lines.append("}")
    return lines


def to_plantuml(flow: Dataflow, recursive: bool = False) -> str:
    """Return a PlantUML diagram of part of a `Dataflow`.

    Args:
        flow: Dataflow.

        recursive: Wheither to show sub-steps.

    Returns:
        PlantUML diagram string.
    """
    lines = [
        "@startuml",
    ]
    stream_to_orig_port: ChainMap = ChainMap()
    for substep in flow.substeps:
        lines += _to_plantuml_step(substep, stream_to_orig_port, recursive)
    lines.append("@enduml")
    return "\n".join(lines)
