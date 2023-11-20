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
                        "from_port_ids": ["test_df.add_one.flat_map.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map.down"],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "flat_map",
                        "step_name": "flat_map",
                        "step_id": "test_df.add_one.flat_map",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.add_one.flat_map.up",
                                "from_port_ids": ["test_df.add_one.up"],
                                "from_stream_ids": ["test_df.inp.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.add_one.flat_map.down",
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
                        "from_stream_ids": ["test_df.add_one.flat_map.down"],
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
                        "from_port_ids": ["test_df.add_one.flat_map.down"],
                        "from_stream_ids": ["test_df.add_one.flat_map.down"],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "flat_map",
                        "step_name": "flat_map",
                        "step_id": "test_df.add_one.flat_map",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.add_one.flat_map.up",
                                "from_port_ids": ["test_df.add_one.up"],
                                "from_stream_ids": ["test_df.nums.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.add_one.flat_map.down",
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
                        "from_port_ids": ["test_df.add_two.flat_map.down"],
                        "from_stream_ids": ["test_df.add_two.flat_map.down"],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "flat_map",
                        "step_name": "flat_map",
                        "step_id": "test_df.add_two.flat_map",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.add_two.flat_map.up",
                                "from_port_ids": ["test_df.add_two.up"],
                                "from_stream_ids": ["test_df.nums.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.add_two.flat_map.down",
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
                        "from_stream_ids": ["test_df.add_one.flat_map.down"],
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
                        "from_stream_ids": ["test_df.add_two.flat_map.down"],
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


def test_to_json_multistream_out():
    flow = Dataflow("test_df")
    nums = op.input("nums", flow, TestingSource([1, 2, 3]))
    ones, twos = op.key_split(
        "split", nums, lambda x: "ALL", lambda x: x + 1, lambda x: x + 2
    )
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
                "op_type": "key_split",
                "step_name": "split",
                "step_id": "test_df.split",
                "inp_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "up",
                        "port_id": "test_df.split.up",
                        "from_port_ids": ["test_df.nums.down"],
                        "from_stream_ids": ["test_df.nums.down"],
                    }
                ],
                "out_ports": [
                    {
                        "typ": "RenderedPort",
                        "port_name": "down",
                        "port_id": "test_df.split.down",
                        "from_port_ids": [
                            "test_df.split.value_0.down",
                            "test_df.split.value_1.down",
                        ],
                        "from_stream_ids": [
                            "test_df.split.value_0.flat_map_value.keyed.noop.down",
                            "test_df.split.value_1.flat_map_value.keyed.noop.down",
                        ],
                    }
                ],
                "substeps": [
                    {
                        "typ": "RenderedOperator",
                        "op_type": "key_on",
                        "step_name": "key",
                        "step_id": "test_df.split.key",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.split.key.up",
                                "from_port_ids": ["test_df.split.up"],
                                "from_stream_ids": ["test_df.nums.down"],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.split.key.down",
                                "from_port_ids": ["test_df.split.key.keyed.down"],
                                "from_stream_ids": [
                                    "test_df.split.key.keyed.noop.down"
                                ],
                            }
                        ],
                        "substeps": [
                            {
                                "typ": "RenderedOperator",
                                "op_type": "map",
                                "step_name": "map",
                                "step_id": "test_df.split.key.map",
                                "inp_ports": [
                                    {
                                        "typ": "RenderedPort",
                                        "port_name": "up",
                                        "port_id": "test_df.split.key.map.up",
                                        "from_port_ids": ["test_df.split.key.up"],
                                        "from_stream_ids": ["test_df.nums.down"],
                                    }
                                ],
                                "out_ports": [
                                    {
                                        "typ": "RenderedPort",
                                        "port_name": "down",
                                        "port_id": "test_df.split.key.map.down",
                                        "from_port_ids": [
                                            "test_df.split.key.map.flat_map.down"
                                        ],
                                        "from_stream_ids": [
                                            "test_df.split.key.map.flat_map.down"
                                        ],
                                    }
                                ],
                                "substeps": [
                                    {
                                        "typ": "RenderedOperator",
                                        "op_type": "flat_map",
                                        "step_name": "flat_map",
                                        "step_id": "test_df.split.key.map.flat_map",
                                        "inp_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "up",
                                                "port_id": "test_df.split.key.map.flat_map.up",
                                                "from_port_ids": [
                                                    "test_df.split.key.map.up"
                                                ],
                                                "from_stream_ids": [
                                                    "test_df.nums.down"
                                                ],
                                            }
                                        ],
                                        "out_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "down",
                                                "port_id": "test_df.split.key.map.flat_map.down",
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
                                "op_type": "key_assert",
                                "step_name": "keyed",
                                "step_id": "test_df.split.key.keyed",
                                "inp_ports": [
                                    {
                                        "typ": "RenderedPort",
                                        "port_name": "up",
                                        "port_id": "test_df.split.key.keyed.up",
                                        "from_port_ids": ["test_df.split.key.map.down"],
                                        "from_stream_ids": [
                                            "test_df.split.key.map.flat_map.down"
                                        ],
                                    }
                                ],
                                "out_ports": [
                                    {
                                        "typ": "RenderedPort",
                                        "port_name": "down",
                                        "port_id": "test_df.split.key.keyed.down",
                                        "from_port_ids": [
                                            "test_df.split.key.keyed.noop.down"
                                        ],
                                        "from_stream_ids": [
                                            "test_df.split.key.keyed.noop.down"
                                        ],
                                    }
                                ],
                                "substeps": [
                                    {
                                        "typ": "RenderedOperator",
                                        "op_type": "_noop",
                                        "step_name": "noop",
                                        "step_id": "test_df.split.key.keyed.noop",
                                        "inp_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "up",
                                                "port_id": "test_df.split.key.keyed.noop.up",
                                                "from_port_ids": [
                                                    "test_df.split.key.keyed.up"
                                                ],
                                                "from_stream_ids": [
                                                    "test_df.split.key.map.flat_map.down"
                                                ],
                                            }
                                        ],
                                        "out_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "down",
                                                "port_id": "test_df.split.key.keyed.noop.down",
                                                "from_port_ids": [],
                                                "from_stream_ids": [],
                                            }
                                        ],
                                        "substeps": [],
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "typ": "RenderedOperator",
                        "op_type": "map_value",
                        "step_name": "value_0",
                        "step_id": "test_df.split.value_0",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.split.value_0.up",
                                "from_port_ids": ["test_df.split.key.down"],
                                "from_stream_ids": [
                                    "test_df.split.key.keyed.noop.down"
                                ],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.split.value_0.down",
                                "from_port_ids": [
                                    "test_df.split.value_0.flat_map_value.down"
                                ],
                                "from_stream_ids": [
                                    "test_df.split.value_0.flat_map_value.keyed.noop.down"
                                ],
                            }
                        ],
                        "substeps": [
                            {
                                "typ": "RenderedOperator",
                                "op_type": "flat_map_value",
                                "step_name": "flat_map_value",
                                "step_id": "test_df.split.value_0.flat_map_value",
                                "inp_ports": [
                                    {
                                        "typ": "RenderedPort",
                                        "port_name": "up",
                                        "port_id": "test_df.split.value_0.flat_map_value.up",
                                        "from_port_ids": ["test_df.split.value_0.up"],
                                        "from_stream_ids": [
                                            "test_df.split.key.keyed.noop.down"
                                        ],
                                    }
                                ],
                                "out_ports": [
                                    {
                                        "typ": "RenderedPort",
                                        "port_name": "down",
                                        "port_id": "test_df.split.value_0.flat_map_value.down",
                                        "from_port_ids": [
                                            "test_df.split.value_0.flat_map_value.keyed.down"
                                        ],
                                        "from_stream_ids": [
                                            "test_df.split.value_0.flat_map_value.keyed.noop.down"
                                        ],
                                    }
                                ],
                                "substeps": [
                                    {
                                        "typ": "RenderedOperator",
                                        "op_type": "flat_map",
                                        "step_name": "flat_map",
                                        "step_id": "test_df.split.value_0.flat_map_value.flat_map",
                                        "inp_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "up",
                                                "port_id": "test_df.split.value_0.flat_map_value.flat_map.up",
                                                "from_port_ids": [
                                                    "test_df.split.value_0.flat_map_value.up"
                                                ],
                                                "from_stream_ids": [
                                                    "test_df.split.key.keyed.noop.down"
                                                ],
                                            }
                                        ],
                                        "out_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "down",
                                                "port_id": "test_df.split.value_0.flat_map_value.flat_map.down",
                                                "from_port_ids": [],
                                                "from_stream_ids": [],
                                            }
                                        ],
                                        "substeps": [],
                                    },
                                    {
                                        "typ": "RenderedOperator",
                                        "op_type": "key_assert",
                                        "step_name": "keyed",
                                        "step_id": "test_df.split.value_0.flat_map_value.keyed",
                                        "inp_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "up",
                                                "port_id": "test_df.split.value_0.flat_map_value.keyed.up",
                                                "from_port_ids": [
                                                    "test_df.split.value_0.flat_map_value.flat_map.down"
                                                ],
                                                "from_stream_ids": [
                                                    "test_df.split.value_0.flat_map_value.flat_map.down"
                                                ],
                                            }
                                        ],
                                        "out_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "down",
                                                "port_id": "test_df.split.value_0.flat_map_value.keyed.down",
                                                "from_port_ids": [
                                                    "test_df.split.value_0.flat_map_value.keyed.noop.down"
                                                ],
                                                "from_stream_ids": [
                                                    "test_df.split.value_0.flat_map_value.keyed.noop.down"
                                                ],
                                            }
                                        ],
                                        "substeps": [
                                            {
                                                "typ": "RenderedOperator",
                                                "op_type": "_noop",
                                                "step_name": "noop",
                                                "step_id": "test_df.split.value_0.flat_map_value.keyed.noop",
                                                "inp_ports": [
                                                    {
                                                        "typ": "RenderedPort",
                                                        "port_name": "up",
                                                        "port_id": "test_df.split.value_0.flat_map_value.keyed.noop.up",
                                                        "from_port_ids": [
                                                            "test_df.split.value_0.flat_map_value.keyed.up"
                                                        ],
                                                        "from_stream_ids": [
                                                            "test_df.split.value_0.flat_map_value.flat_map.down"
                                                        ],
                                                    }
                                                ],
                                                "out_ports": [
                                                    {
                                                        "typ": "RenderedPort",
                                                        "port_name": "down",
                                                        "port_id": "test_df.split.value_0.flat_map_value.keyed.noop.down",
                                                        "from_port_ids": [],
                                                        "from_stream_ids": [],
                                                    }
                                                ],
                                                "substeps": [],
                                            }
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "typ": "RenderedOperator",
                        "op_type": "map_value",
                        "step_name": "value_1",
                        "step_id": "test_df.split.value_1",
                        "inp_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "up",
                                "port_id": "test_df.split.value_1.up",
                                "from_port_ids": ["test_df.split.key.down"],
                                "from_stream_ids": [
                                    "test_df.split.key.keyed.noop.down"
                                ],
                            }
                        ],
                        "out_ports": [
                            {
                                "typ": "RenderedPort",
                                "port_name": "down",
                                "port_id": "test_df.split.value_1.down",
                                "from_port_ids": [
                                    "test_df.split.value_1.flat_map_value.down"
                                ],
                                "from_stream_ids": [
                                    "test_df.split.value_1.flat_map_value.keyed.noop.down"
                                ],
                            }
                        ],
                        "substeps": [
                            {
                                "typ": "RenderedOperator",
                                "op_type": "flat_map_value",
                                "step_name": "flat_map_value",
                                "step_id": "test_df.split.value_1.flat_map_value",
                                "inp_ports": [
                                    {
                                        "typ": "RenderedPort",
                                        "port_name": "up",
                                        "port_id": "test_df.split.value_1.flat_map_value.up",
                                        "from_port_ids": ["test_df.split.value_1.up"],
                                        "from_stream_ids": [
                                            "test_df.split.key.keyed.noop.down"
                                        ],
                                    }
                                ],
                                "out_ports": [
                                    {
                                        "typ": "RenderedPort",
                                        "port_name": "down",
                                        "port_id": "test_df.split.value_1.flat_map_value.down",
                                        "from_port_ids": [
                                            "test_df.split.value_1.flat_map_value.keyed.down"
                                        ],
                                        "from_stream_ids": [
                                            "test_df.split.value_1.flat_map_value.keyed.noop.down"
                                        ],
                                    }
                                ],
                                "substeps": [
                                    {
                                        "typ": "RenderedOperator",
                                        "op_type": "flat_map",
                                        "step_name": "flat_map",
                                        "step_id": "test_df.split.value_1.flat_map_value.flat_map",
                                        "inp_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "up",
                                                "port_id": "test_df.split.value_1.flat_map_value.flat_map.up",
                                                "from_port_ids": [
                                                    "test_df.split.value_1.flat_map_value.up"
                                                ],
                                                "from_stream_ids": [
                                                    "test_df.split.key.keyed.noop.down"
                                                ],
                                            }
                                        ],
                                        "out_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "down",
                                                "port_id": "test_df.split.value_1.flat_map_value.flat_map.down",
                                                "from_port_ids": [],
                                                "from_stream_ids": [],
                                            }
                                        ],
                                        "substeps": [],
                                    },
                                    {
                                        "typ": "RenderedOperator",
                                        "op_type": "key_assert",
                                        "step_name": "keyed",
                                        "step_id": "test_df.split.value_1.flat_map_value.keyed",
                                        "inp_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "up",
                                                "port_id": "test_df.split.value_1.flat_map_value.keyed.up",
                                                "from_port_ids": [
                                                    "test_df.split.value_1.flat_map_value.flat_map.down"
                                                ],
                                                "from_stream_ids": [
                                                    "test_df.split.value_1.flat_map_value.flat_map.down"
                                                ],
                                            }
                                        ],
                                        "out_ports": [
                                            {
                                                "typ": "RenderedPort",
                                                "port_name": "down",
                                                "port_id": "test_df.split.value_1.flat_map_value.keyed.down",
                                                "from_port_ids": [
                                                    "test_df.split.value_1.flat_map_value.keyed.noop.down"
                                                ],
                                                "from_stream_ids": [
                                                    "test_df.split.value_1.flat_map_value.keyed.noop.down"
                                                ],
                                            }
                                        ],
                                        "substeps": [
                                            {
                                                "typ": "RenderedOperator",
                                                "op_type": "_noop",
                                                "step_name": "noop",
                                                "step_id": "test_df.split.value_1.flat_map_value.keyed.noop",
                                                "inp_ports": [
                                                    {
                                                        "typ": "RenderedPort",
                                                        "port_name": "up",
                                                        "port_id": "test_df.split.value_1.flat_map_value.keyed.noop.up",
                                                        "from_port_ids": [
                                                            "test_df.split.value_1.flat_map_value.keyed.up"
                                                        ],
                                                        "from_stream_ids": [
                                                            "test_df.split.value_1.flat_map_value.flat_map.down"
                                                        ],
                                                    }
                                                ],
                                                "out_ports": [
                                                    {
                                                        "typ": "RenderedPort",
                                                        "port_name": "down",
                                                        "port_id": "test_df.split.value_1.flat_map_value.keyed.noop.down",
                                                        "from_port_ids": [],
                                                        "from_stream_ids": [],
                                                    }
                                                ],
                                                "substeps": [],
                                            }
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
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
                        "from_port_ids": ["test_df.split.down"],
                        "from_stream_ids": [
                            "test_df.split.value_0.flat_map_value.keyed.noop.down"
                        ],
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
                        "from_port_ids": ["test_df.split.down"],
                        "from_stream_ids": [
                            "test_df.split.value_1.flat_map_value.keyed.noop.down"
                        ],
                    }
                ],
                "out_ports": [],
                "substeps": [],
            },
        ],
    }
