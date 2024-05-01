import json
import textwrap

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource
from bytewax.visualize import to_json, to_mermaid


def test_to_json_linear():
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("add_one", s, lambda x: x + 1)
    op.output("out", s, TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "typ": "RenderedDataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "typ": "RenderedOperator",
                "op_type": "input",
                "step_name": "inp",
                "step_id": "test_df.inp",
                "inp_ports": [],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.inp.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "map",
                "step_name": "add_one",
                "step_id": "test_df.add_one",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.add_one.up",
                        "from_port_ids": ["test_df.inp.down"],
                        "from_stream_ids": ["test_df.inp.down"],
                    }
                ],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.add_one.down",
                        "from_port_ids": ["test_df.add_one.flat_map_batch.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map_batch.down"],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "flat_map_batch",
                        "step_name": "flat_map_batch",
                        "step_id": "test_df.add_one.flat_map_batch",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.add_one.flat_map_batch.up",
                                "from_port_ids": ["test_df.add_one.up"],
                                "from_stream_ids": ["test_df.inp.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.add_one.flat_map_batch.down",
                                "from_port_ids": [],
                                "from_stream_ids": [],
                            }
                        ],
                        "substeps": [],
                    }
                ],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "output",
                "step_name": "out",
                "step_id": "test_df.out",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.out.up",
                        "from_port_ids": ["test_df.add_one.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map_batch.down"],
                    }
                ],
                "out_ports": [],
                "substeps": [],
            },
        ],
    }


def test_to_json_nonlinear():
    flow = Dataflow("test_df")
    nums = op.input("nums", flow, TestingSource([1, 2, 3]))
    ones = op.map("add_one", nums, lambda x: x + 1)
    twos = op.map("add_two", nums, lambda x: x + 2)
    op.output("out_one", ones, TestingSink([]))
    op.output("out_two", twos, TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "typ": "RenderedDataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "typ": "RenderedOperator",
                "op_type": "input",
                "step_name": "nums",
                "step_id": "test_df.nums",
                "inp_ports": [],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.nums.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "map",
                "step_name": "add_one",
                "step_id": "test_df.add_one",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.add_one.up",
                        "from_port_ids": ["test_df.nums.down"],
                        "from_stream_ids": ["test_df.nums.down"],
                    }
                ],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.add_one.down",
                        "from_port_ids": ["test_df.add_one.flat_map_batch.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map_batch.down"],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "flat_map_batch",
                        "step_name": "flat_map_batch",
                        "step_id": "test_df.add_one.flat_map_batch",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.add_one.flat_map_batch.up",
                                "from_port_ids": ["test_df.add_one.up"],
                                "from_stream_ids": ["test_df.nums.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.add_one.flat_map_batch.down",
                                "from_port_ids": [],
                                "from_stream_ids": [],
                            }
                        ],
                        "substeps": [],
                    }
                ],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "map",
                "step_name": "add_two",
                "step_id": "test_df.add_two",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.add_two.up",
                        "from_port_ids": ["test_df.nums.down"],
                        "from_stream_ids": ["test_df.nums.down"],
                    }
                ],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.add_two.down",
                        "from_port_ids": ["test_df.add_two.flat_map_batch.down"],
                        "from_stream_ids": ["test_df.add_two.flat_map_batch.down"],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "flat_map_batch",
                        "step_name": "flat_map_batch",
                        "step_id": "test_df.add_two.flat_map_batch",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.add_two.flat_map_batch.up",
                                "from_port_ids": ["test_df.add_two.up"],
                                "from_stream_ids": ["test_df.nums.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.add_two.flat_map_batch.down",
                                "from_port_ids": [],
                                "from_stream_ids": [],
                            }
                        ],
                        "substeps": [],
                    }
                ],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "output",
                "step_name": "out_one",
                "step_id": "test_df.out_one",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.out_one.up",
                        "from_port_ids": ["test_df.add_one.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map_batch.down"],
                    }
                ],
                "out_ports": [],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "output",
                "step_name": "out_two",
                "step_id": "test_df.out_two",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.out_two.up",
                        "from_port_ids": ["test_df.add_two.down"],
                        "from_stream_ids": ["test_df.add_two.flat_map_batch.down"],
                    }
                ],
                "out_ports": [],
                "substeps": [],
            },
        ],
    }


def test_to_json_multistream_inp():
    flow = Dataflow("test_df")
    ones = op.input("ones", flow, TestingSource([2, 3, 4]))
    twos = op.input("twos", flow, TestingSource([3, 4, 5]))
    s = op.merge("merge", ones, twos)
    op.output("out", s, TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "typ": "RenderedDataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "typ": "RenderedOperator",
                "op_type": "input",
                "step_name": "ones",
                "step_id": "test_df.ones",
                "inp_ports": [],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.ones.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "input",
                "step_name": "twos",
                "step_id": "test_df.twos",
                "inp_ports": [],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.twos.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "merge",
                "step_name": "merge",
                "step_id": "test_df.merge",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "ups",
                        "port_id": "test_df.merge.ups",
                        "from_port_ids": ["test_df.ones.down", "test_df.twos.down"],
                        "from_stream_ids": ["test_df.ones.down", "test_df.twos.down"],
                    }
                ],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.merge.down",
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
            },
            {
                "typ": "RenderedOperator",
                "op_type": "output",
                "step_name": "out",
                "step_id": "test_df.out",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.out.up",
                        "from_port_ids": ["test_df.merge.down"],
                        "from_stream_ids": ["test_df.merge.down"],
                    }
                ],
                "out_ports": [],
                "substeps": [],
            },
        ],
    }


def test_to_mermaid_linear():
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    s = op.map("add_one", s, lambda x: x + 1)
    op.output("out", s, TestingSink([]))

    assert to_mermaid(flow) == textwrap.dedent(
        """\
        flowchart TD
        subgraph "test_df (Dataflow)"
        test_df.inp["inp (input)"]
        test_df.add_one["add_one (map)"]
        test_df.inp -- "down → up" --> test_df.add_one
        test_df.out["out (output)"]
        test_df.add_one -- "down → up" --> test_df.out
        end"""
    )


def test_to_mermaid_nonlinear():
    flow = Dataflow("test_df")
    nums = op.input("nums", flow, TestingSource([1, 2, 3]))
    ones = op.map("add_one", nums, lambda x: x + 1)
    twos = op.map("add_two", nums, lambda x: x + 2)
    op.output("out_one", ones, TestingSink([]))
    op.output("out_two", twos, TestingSink([]))

    assert to_mermaid(flow) == textwrap.dedent(
        """\
        flowchart TD
        subgraph "test_df (Dataflow)"
        test_df.nums["nums (input)"]
        test_df.add_one["add_one (map)"]
        test_df.nums -- "down → up" --> test_df.add_one
        test_df.add_two["add_two (map)"]
        test_df.nums -- "down → up" --> test_df.add_two
        test_df.out_one["out_one (output)"]
        test_df.add_one -- "down → up" --> test_df.out_one
        test_df.out_two["out_two (output)"]
        test_df.add_two -- "down → up" --> test_df.out_two
        end"""
    )


def test_to_mermaid_multistream_inp():
    flow = Dataflow("test_df")
    ones = op.input("ones", flow, TestingSource([2, 3, 4]))
    twos = op.input("twos", flow, TestingSource([3, 4, 5]))
    s = op.merge("merge", ones, twos)
    op.output("out", s, TestingSink([]))

    assert to_mermaid(flow) == textwrap.dedent(
        """\
        flowchart TD
        subgraph "test_df (Dataflow)"
        test_df.ones["ones (input)"]
        test_df.twos["twos (input)"]
        test_df.merge["merge (merge)"]
        test_df.ones -- "down → ups" --> test_df.merge
        test_df.twos -- "down → ups" --> test_df.merge
        test_df.out["out (output)"]
        test_df.merge -- "down → up" --> test_df.out
        end"""
    )
