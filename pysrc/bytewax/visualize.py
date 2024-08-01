"""Visualize dataflow structure.

This most easily used via running this module as a script. Run `python
-m bytewax.visualize --help` or see <project:#xref-visualize> for more
info.

"""

import argparse
import json
import typing
from collections import ChainMap
from dataclasses import dataclass
from functools import singledispatch
from typing import Any, Dict, List, Literal

from bytewax.dataflow import Dataflow, Operator
from bytewax.run import _locate_dataflow, _prepare_import
from typing_extensions import Self


@dataclass(frozen=True)
class RenderedPort:
    """Port with streams resolved to globally-unique IDs."""

    port_name: str
    port_id: str
    from_port_ids: List[str]
    from_stream_ids: List[str]


@dataclass(frozen=True)
class RenderedOperator:
    """Operator with all ports resolved to globally-unique IDs."""

    op_type: str
    step_name: str
    step_id: str
    inp_ports: List[RenderedPort]
    out_ports: List[RenderedPort]
    substeps: List[Self]


@dataclass(frozen=True)
class RenderedDataflow:
    """Dataflow with streams, ports resolved to globally-unique IDs."""

    flow_id: str
    substeps: List[RenderedOperator]


def _to_rendered(step: Operator, stream_to_orig_port_id: ChainMap) -> RenderedOperator:
    inp_ports = {name: getattr(step, name) for name in step.ups_names}
    inp_rports = [
        RenderedPort(
            port_name,
            port.port_id,
            [
                stream_to_orig_port_id[stream_id]
                for stream_name, stream_id in port.stream_ids.items()
            ],
            [stream_id for stream_name, stream_id in port.stream_ids.items()],
        )
        for port_name, port in inp_ports.items()
    ]

    out_ports = {name: getattr(step, name) for name in step.dwn_names}
    stream_to_orig_port_id.update(
        {
            stream_id: port.port_id
            for port in out_ports.values()
            for stream_id in port.stream_ids.values()
        }
    )

    # Add in an "inner scope". Rewrite the port that originated a
    # stream from the true output port to the fake input port on
    # this containing step.
    stream_to_orig_port_id = stream_to_orig_port_id.new_child(
        {
            stream_id: port.port_id
            for port in inp_ports.values()
            for stream_id in port.stream_ids.values()
        }
    )

    substeps = [
        _to_rendered(substep, stream_to_orig_port_id) for substep in step.substeps
    ]

    out_rports = [
        RenderedPort(
            port_name,
            port.port_id,
            [
                stream_to_orig_port_id[stream_id]
                for stream_id in port.stream_ids.values()
                if len(substeps) > 0
            ],
            [
                stream_id
                for stream_name, stream_id in port.stream_ids.items()
                if len(substeps) > 0
            ],
        )
        for port_name, port in out_ports.items()
    ]

    return RenderedOperator(
        type(step).__name__,
        step.step_name,
        step.step_id,
        inp_rports,
        out_rports,
        substeps,
    )


def to_rendered(flow: Dataflow) -> RenderedDataflow:
    """Convert a dataflow into the "rendered" data model.

    This resolves all port links for you. All you have to do is set up
    the links by connecting {py:obj}`RenderedPort.port_id` to all
    {py:obj}`RenderedPort.from_port_ids`.

    :arg flow: Dataflow.

    :returns: Rendered dataflow.

    """
    stream_to_orig_port_id: ChainMap = ChainMap()

    substeps = [_to_rendered(step, stream_to_orig_port_id) for step in flow.substeps]

    return RenderedDataflow(
        flow.flow_id,
        substeps,
    )


@singledispatch
def _json_for(obj) -> Any:
    """Hook to extend the JSON serialization.

    Register new types via `@json_for.register`. See
    {py:obj}`functools.singledispatch` for more info.

    If this contains nested un-serializeable types, this will be
    re-called with them later by {py:obj}`json.dumps`; you don't have
    to recurse yourself.

    :arg obj: Un-handled type to attempt to encode.

    :returns: A new value that is JSON serializable.

    """
    raise TypeError()


@_json_for.register
def _json_for_df(df: RenderedDataflow) -> Dict:
    return {
        "typ": "RenderedDataflow",
        "flow_id": df.flow_id,
        "substeps": df.substeps,
    }


@_json_for.register
def _json_for_op(step: RenderedOperator) -> Dict:
    return {
        "typ": "RenderedOperator",
        "op_type": step.op_type,
        "step_name": step.step_name,
        "step_id": step.step_id,
        "inp_ports": step.inp_ports,
        "out_ports": step.out_ports,
        "substeps": step.substeps,
    }


@_json_for.register
def _json_for_port(port: RenderedPort) -> Dict:
    return {
        "typ": "RenderedPort",
        "port_name": port.port_name,
        "port_id": port.port_id,
        "from_port_ids": port.from_port_ids,
        "from_stream_ids": port.from_stream_ids,
    }


class _Encoder(json.JSONEncoder):
    def default(self, o):
        try:
            return _json_for(o)
        except TypeError:
            return super().default(o)


def to_json(flow: Dataflow) -> str:
    """Encode this dataflow into JSON.

    :arg flow: Dataflow.

    :returns: JSON string.

    """
    return json.dumps(to_rendered(flow), cls=_Encoder, indent=2)


def _to_plantuml_step(
    step: RenderedOperator,
    recursive: bool,
) -> List[str]:
    lines = [
        f"component {step.step_id} [",
        f"    {step.step_id} ({step.op_type})",
        "]",
        f"component {step.step_id} {{",
    ]

    inner_lines = []

    for port in step.inp_ports:
        inner_lines.append(f"portin {port.port_id}")
    for port in step.out_ports:
        inner_lines.append(f"portout {port.port_id}")

    for port in step.inp_ports:
        for from_port_id, stream_id in zip(port.from_port_ids, port.from_stream_ids):
            inner_lines.append(f"{from_port_id} --> {port.port_id} : {stream_id}")

    if recursive:
        for substep in step.substeps:
            inner_lines += _to_plantuml_step(substep, recursive)

        # Now also connect all the inner outputs to the containing
        # outputs.
        for port in step.out_ports:
            for from_port_id, stream_id in zip(
                port.from_port_ids, port.from_stream_ids
            ):
                inner_lines.append(f"{from_port_id} --> {port.port_id} : {stream_id}")

    lines += ["    " + line for line in inner_lines]

    lines.append("}")
    return lines


def to_plantuml(flow: Dataflow, recursive: bool = False) -> str:
    """Generate a PlantUML diagram of a dataflow.

    This most easily used via running this module as a script. Run
    `python -m bytewax.visualize --help` or see
    <project:#xref-visualize> for more info.

    See [the PlantUML website](https://plantuml.com/) for more
    info on PlantUML.

    :arg flow: Dataflow.

    :arg recursive: Wheither to show sub-steps. Defaults to `False`.

    :returns: PlantUML diagram string.

    """
    rflow = to_rendered(flow)
    lines = [
        "@startuml",
    ]
    for substep in rflow.substeps:
        lines += _to_plantuml_step(substep, recursive)
    lines.append("@enduml")
    return "\n".join(lines)


def _to_mermaid_step(
    step: RenderedOperator,
    port_id_to_port: Dict[str, RenderedPort],
    port_id_to_step: Dict[str, RenderedOperator],
) -> List[str]:
    lines = [
        f'{step.step_id}["{step.step_name} ({step.op_type})"]',
    ]

    for port in step.inp_ports:
        for from_port_id in port.from_port_ids:
            from_step_id = port_id_to_step[from_port_id].step_id
            from_port_name = port_id_to_port[from_port_id].port_name
            lines.append(
                f"{from_step_id} -- "
                f'"{from_port_name} → {port.port_name}" '
                f"--> {step.step_id}"
            )

    return lines


def to_mermaid(flow: Dataflow) -> str:
    """Generate a Mermaid diagram of a dataflow.

    This most easily used via running this module as a script. Run
    `python -m bytewax.visualize --help` or see
    <project:#xref-visualize> for more info.

    See [the Mermaid docs](https://mermaid.js.org/intro/) for more
    info on Mermaid.

    Does not show any sub-steps.

    :arg flow: Dataflow.

    :returns: Mermaid diagram string.

    """
    rflow = to_rendered(flow)
    lines = [
        "flowchart TD",
        f'subgraph "{flow.flow_id} (Dataflow)"',
    ]

    port_id_to_port = {
        port.port_id: port
        for step in rflow.substeps
        for port in step.inp_ports + step.out_ports
    }
    port_id_to_step = {
        port.port_id: step
        for step in rflow.substeps
        for port in step.inp_ports + step.out_ports
    }

    for substep in rflow.substeps:
        lines += _to_mermaid_step(substep, port_id_to_port, port_id_to_step)
    lines.append("end")
    return "\n".join(lines)


_Formats = Literal[
    "json",
    "mermaid",
    "plantuml",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.visualize",
        description="""Generate a diagram of dataflow structure.

        You can pipe the output into a file to save it. You'll need to
        use the relevant program to convert the diagram code into a
        visualization.

        See https://mermaid.js.org/ and https://plantuml.com/ . The
        JSON format is bespoke and for debugging.""",
    )
    parser.add_argument(
        "import_str",
        type=str,
        help="Dataflow import string in the format "
        "<module_name>[:<dataflow_variable_or_factory>] "
        "Example: src.dataflow or src.dataflow:flow or "
        "src.dataflow:get_flow('string_argument')",
    )
    parser.add_argument(
        "-o",
        "--output-format",
        choices=typing.get_args(_Formats),
        default="mermaid",
        help="Output format to use; defaults to 'mermaid'",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="""Show nested inner operators; defaults to not; always
        false for MermaidJS; always true for JSON""",
    )

    return parser.parse_args()


def _visualize_main(import_str: str, output_format: _Formats, recursive: bool) -> None:
    mod_str, attr_str = _prepare_import(import_str)
    flow = _locate_dataflow(mod_str, attr_str)

    if output_format == "json":
        out = to_json(flow)
    elif output_format == "mermaid":
        out = to_mermaid(flow)
    elif output_format == "plantuml":
        out = to_plantuml(flow, recursive)
    else:
        msg = f"unknown visualization type {output_format!r}"
        raise ValueError(msg)

    print(out)


if __name__ == "__main__":
    args = vars(_parse_args())
    _visualize_main(**args)
