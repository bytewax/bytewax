import json

from bytewax._encoder import to_json
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource


def test_to_json_linear():
    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource([1, 2, 3]))
    s = s.map("add_one", lambda x: x + 1)
    s.output("out", TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "type": "Dataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "type": "input",
                "step_id": "test_df.inp",
                "inp_ports": {},
                "out_ports": {"down": ["test_df.inp.out"]},
                "substeps": [],
            },
            {
                "type": "map",
                "step_id": "test_df.add_one",
                "inp_ports": {"up": ["test_df.inp.out"]},
                "out_ports": {"down": ["test_df.add_one.flat_map.out"]},
                "substeps": [
                    {
                        "type": "flat_map",
                        "step_id": "test_df.add_one.flat_map",
                        "inp_ports": {"up": ["test_df.inp.out"]},
                        "out_ports": {"down": ["test_df.add_one.flat_map.out"]},
                        "substeps": [],
                    },
                ],
            },
            {
                "type": "output",
                "step_id": "test_df.out",
                "inp_ports": {"up": ["test_df.add_one.flat_map.out"]},
                "out_ports": {},
                "substeps": [],
            },
        ],
    }


def test_to_json_nonlinear():
    flow = Dataflow("test_df")
    nums = flow.input("nums", TestingSource([1, 2, 3]))
    ones = nums.map("add_one", lambda x: x + 1)
    twos = nums.map("add_two", lambda x: x + 2)
    ones.output("out_one", TestingSink([]))
    twos.output("out_two", TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "type": "Dataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "type": "input",
                "step_id": "test_df.nums",
                "inp_ports": {},
                "out_ports": {"down": ["test_df.nums.out"]},
                "substeps": [],
            },
            {
                "type": "map",
                "step_id": "test_df.add_one",
                "inp_ports": {"up": ["test_df.nums.out"]},
                "out_ports": {"down": ["test_df.add_one.flat_map.out"]},
                "substeps": [
                    {
                        "type": "flat_map",
                        "step_id": "test_df.add_one.flat_map",
                        "inp_ports": {"up": ["test_df.nums.out"]},
                        "out_ports": {"down": ["test_df.add_one.flat_map.out"]},
                        "substeps": [],
                    },
                ],
            },
            {
                "type": "map",
                "step_id": "test_df.add_two",
                "inp_ports": {"up": ["test_df.nums.out"]},
                "out_ports": {"down": ["test_df.add_two.flat_map.out"]},
                "substeps": [
                    {
                        "type": "flat_map",
                        "step_id": "test_df.add_two.flat_map",
                        "inp_ports": {"up": ["test_df.nums.out"]},
                        "out_ports": {"down": ["test_df.add_two.flat_map.out"]},
                        "substeps": [],
                    },
                ],
            },
            {
                "type": "output",
                "step_id": "test_df.out_one",
                "inp_ports": {"up": ["test_df.add_one.flat_map.out"]},
                "out_ports": {},
                "substeps": [],
            },
            {
                "type": "output",
                "step_id": "test_df.out_two",
                "inp_ports": {"up": ["test_df.add_two.flat_map.out"]},
                "out_ports": {},
                "substeps": [],
            },
        ],
    }


def test_to_json_multistream_inp():
    flow = Dataflow("test_df")
    ones = flow.input("ones", TestingSource([2, 3, 4]))
    twos = flow.input("twos", TestingSource([3, 4, 5]))
    s = flow.merge_all("merge", ones, twos)
    s.output("out", TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "type": "Dataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "type": "input",
                "step_id": "test_df.ones",
                "inp_ports": {},
                "out_ports": {"down": ["test_df.ones.out"]},
                "substeps": [],
            },
            {
                "type": "input",
                "step_id": "test_df.twos",
                "inp_ports": {},
                "out_ports": {"down": ["test_df.twos.out"]},
                "substeps": [],
            },
            {
                "type": "merge_all",
                "step_id": "test_df.merge",
                "inp_ports": {"ups": ["test_df.ones.out", "test_df.twos.out"]},
                "out_ports": {"down": ["test_df.merge.out"]},
                "substeps": [],
            },
            {
                "type": "output",
                "step_id": "test_df.out",
                "inp_ports": {"up": ["test_df.merge.out"]},
                "out_ports": {},
                "substeps": [],
            },
        ],
    }


def test_to_json_multistream_out():
    flow = Dataflow("test_df")
    nums = flow.input("nums", TestingSource([1, 2, 3]))
    ones, twos = nums.key_split(
        "split", lambda x: "ALL", lambda x: x + 1, lambda x: x + 2
    )
    ones.output("out_one", TestingSink([]))
    twos.output("out_two", TestingSink([]))

    assert json.loads(to_json(flow)) == {
        "type": "Dataflow",
        "flow_id": "test_df",
        "substeps": [
            {
                "type": "input",
                "step_id": "test_df.nums",
                "inp_ports": {},
                "out_ports": {"down": ["test_df.nums.out"]},
                "substeps": [],
            },
            {
                "type": "key_split",
                "step_id": "test_df.split",
                "inp_ports": {"up": ["test_df.nums.out"]},
                "out_ports": {
                    "down": [
                        "test_df.split.value_0.flat_map_value.keyed.noop.out",
                        "test_df.split.value_1.flat_map_value.keyed.noop.out",
                    ]
                },
                "substeps": [
                    {
                        "type": "key_on",
                        "step_id": "test_df.split.key",
                        "inp_ports": {"up": ["test_df.nums.out"]},
                        "out_ports": {"down": ["test_df.split.key.keyed.noop.out"]},
                        "substeps": [
                            {
                                "type": "map",
                                "step_id": "test_df.split.key.map",
                                "inp_ports": {"up": ["test_df.nums.out"]},
                                "out_ports": {
                                    "down": ["test_df.split.key.map.flat_map.out"]
                                },
                                "substeps": [
                                    {
                                        "type": "flat_map",
                                        "step_id": "test_df.split.key.map.flat_map",
                                        "inp_ports": {"up": ["test_df.nums.out"]},
                                        "out_ports": {
                                            "down": [
                                                "test_df.split.key.map.flat_map.out"
                                            ]
                                        },
                                        "substeps": [],
                                    }
                                ],
                            },
                            {
                                "type": "key_assert",
                                "step_id": "test_df.split.key.keyed",
                                "inp_ports": {
                                    "up": ["test_df.split.key.map.flat_map.out"]
                                },
                                "out_ports": {
                                    "down": ["test_df.split.key.keyed.noop.out"]
                                },
                                "substeps": [
                                    {
                                        "type": "_noop",
                                        "step_id": "test_df.split.key.keyed.noop",
                                        "inp_ports": {
                                            "up": ["test_df.split.key.map.flat_map.out"]
                                        },
                                        "out_ports": {
                                            "down": ["test_df.split.key.keyed.noop.out"]
                                        },
                                        "substeps": [],
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "type": "map_value",
                        "step_id": "test_df.split.value_0",
                        "inp_ports": {"up": ["test_df.split.key.keyed.noop.out"]},
                        "out_ports": {
                            "down": [
                                "test_df.split.value_0.flat_map_value.keyed.noop.out"
                            ]
                        },
                        "substeps": [
                            {
                                "type": "flat_map_value",
                                "step_id": "test_df.split.value_0.flat_map_value",
                                "inp_ports": {
                                    "up": ["test_df.split.key.keyed.noop.out"]
                                },
                                "out_ports": {
                                    "down": [
                                        "test_df.split.value_0.flat_map_value.keyed.noop.out"
                                    ]
                                },
                                "substeps": [
                                    {
                                        "type": "flat_map",
                                        "step_id": "test_df.split.value_0.flat_map_value.flat_map",
                                        "inp_ports": {
                                            "up": ["test_df.split.key.keyed.noop.out"]
                                        },
                                        "out_ports": {
                                            "down": [
                                                "test_df.split.value_0.flat_map_value.flat_map.out"
                                            ]
                                        },
                                        "substeps": [],
                                    },
                                    {
                                        "type": "key_assert",
                                        "step_id": "test_df.split.value_0.flat_map_value.keyed",
                                        "inp_ports": {
                                            "up": [
                                                "test_df.split.value_0.flat_map_value.flat_map.out"
                                            ]
                                        },
                                        "out_ports": {
                                            "down": [
                                                "test_df.split.value_0.flat_map_value.keyed.noop.out"
                                            ]
                                        },
                                        "substeps": [
                                            {
                                                "type": "_noop",
                                                "step_id": "test_df.split.value_0.flat_map_value.keyed.noop",
                                                "inp_ports": {
                                                    "up": [
                                                        "test_df.split.value_0.flat_map_value.flat_map.out"
                                                    ]
                                                },
                                                "out_ports": {
                                                    "down": [
                                                        "test_df.split.value_0.flat_map_value.keyed.noop.out"
                                                    ]
                                                },
                                                "substeps": [],
                                            }
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "type": "map_value",
                        "step_id": "test_df.split.value_1",
                        "inp_ports": {"up": ["test_df.split.key.keyed.noop.out"]},
                        "out_ports": {
                            "down": [
                                "test_df.split.value_1.flat_map_value.keyed.noop.out"
                            ]
                        },
                        "substeps": [
                            {
                                "type": "flat_map_value",
                                "step_id": "test_df.split.value_1.flat_map_value",
                                "inp_ports": {
                                    "up": ["test_df.split.key.keyed.noop.out"]
                                },
                                "out_ports": {
                                    "down": [
                                        "test_df.split.value_1.flat_map_value.keyed.noop.out"
                                    ]
                                },
                                "substeps": [
                                    {
                                        "type": "flat_map",
                                        "step_id": "test_df.split.value_1.flat_map_value.flat_map",
                                        "inp_ports": {
                                            "up": ["test_df.split.key.keyed.noop.out"]
                                        },
                                        "out_ports": {
                                            "down": [
                                                "test_df.split.value_1.flat_map_value.flat_map.out"
                                            ]
                                        },
                                        "substeps": [],
                                    },
                                    {
                                        "type": "key_assert",
                                        "step_id": "test_df.split.value_1.flat_map_value.keyed",
                                        "inp_ports": {
                                            "up": [
                                                "test_df.split.value_1.flat_map_value.flat_map.out"
                                            ]
                                        },
                                        "out_ports": {
                                            "down": [
                                                "test_df.split.value_1.flat_map_value.keyed.noop.out"
                                            ]
                                        },
                                        "substeps": [
                                            {
                                                "type": "_noop",
                                                "step_id": "test_df.split.value_1.flat_map_value.keyed.noop",
                                                "inp_ports": {
                                                    "up": [
                                                        "test_df.split.value_1.flat_map_value.flat_map.out"
                                                    ]
                                                },
                                                "out_ports": {
                                                    "down": [
                                                        "test_df.split.value_1.flat_map_value.keyed.noop.out"
                                                    ]
                                                },
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
                "type": "output",
                "step_id": "test_df.out_one",
                "inp_ports": {
                    "up": ["test_df.split.value_0.flat_map_value.keyed.noop.out"]
                },
                "out_ports": {},
                "substeps": [],
            },
            {
                "type": "output",
                "step_id": "test_df.out_two",
                "inp_ports": {
                    "up": ["test_df.split.value_1.flat_map_value.keyed.noop.out"]
                },
                "out_ports": {},
                "substeps": [],
            },
        ],
    }
