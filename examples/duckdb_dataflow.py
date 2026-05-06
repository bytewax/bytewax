from pathlib import Path
from typing import Dict, Tuple, Union

import bytewax.duckdb.operators as duck_op
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

db_path = Path("example_db.db")

flow = Dataflow("duckdb")


def create_dict(value: int) -> Tuple[str, Dict[str, Union[int, str]]]:
    return ("1", {"id": value, "name": "Alice"})


inp = op.input("inp", flow, TestingSource(range(100)))
dict_stream = op.map("dict", inp, create_dict)
duck_op.output(
    "out",
    dict_stream,
    db_path,
    "example_table",
    "CREATE TABLE example_table (id INTEGER, name TEXT)",
)
