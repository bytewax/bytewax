import json

import bytewax.operators as op
from bytewax._encoder import to_json
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource


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
                        "from_port_ids": ["test_df.add_one.down"],
                        "from_stream_ids": ["test_df.add_one.down"],
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
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
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
                        "from_port_ids": [],
                        "from_stream_ids": [],
                    }
                ],
                "substeps": [],
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
                        "from_stream_ids": ["test_df.add_one.down"],
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
                        "from_stream_ids": ["test_df.add_two.down"],
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
