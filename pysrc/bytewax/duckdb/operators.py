"""Operators for the DuckDB sink.

This module provides operators for writing data to DuckDB or MotherDuck
using the Bytewax DuckDB sink.

Usage:

```
import bytewax.duckdb.operators as duck_op
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingSource
import random
from typing import Dict, Tuple, Union

# Initialize the dataflow
flow = Dataflow("duckdb-names-cities")

# Define sample data for names and locations
names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
locations = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]

# Function to create a dictionary with more varied data
def create_dict(value: int) -> Tuple[str, Dict[str, Union[int, str]]]:
    name = random.choice(names)
    age = random.randint(20, 60)  # Random age between 20 and 60
    location = random.choice(locations)
    # This tuple represents the key and the dictionary to be written
    return (str(value), {"id": value, "name": name, "age": age, "location": location})

# Generate input data
inp = op.input("inp", flow, TestingSource(range(50)))
dict_stream = op.map("dict", inp, create_dict)

# Output the data to DuckDB, creating a table with multiple columns
duck_op.output(
    "out",
    dict_stream,
    db_path,
    "names_cities",
    "CREATE TABLE IF NOT EXISTS names_cities\
        (id INTEGER, name TEXT, age INTEGER, location TEXT)"
)

# Run the dataflow
run_main(flow)
```

We can verify it was written to the database by querying the table:

```
import duckdb

con = duckdb.connect(db_path)
con.execute("SELECT * FROM names_cities").fetchdf()
```
"""

from datetime import timedelta
from typing import List, Optional

import bytewax.operators as op
from bytewax.dataflow import operator
from bytewax.duckdb import DuckDBSink
from bytewax.operators import KeyedStream, V


@operator
def _to_sink(
    step_id: str,
    up: KeyedStream[V],
    timeout: timedelta,
    batch_size: int,
) -> KeyedStream[List[V]]:
    """Collect batches of items to be inserted into DuckDB."""
    return op.collect("batch", up, timeout=timeout, max_size=batch_size)


@operator
def output(
    step_id: str,
    up: KeyedStream[V],
    db_path: str,
    table_name: str,
    create_table_sql: Optional[str],
    timeout: timedelta = timedelta(seconds=1),
    batch_size: int = 122_880,
) -> None:
    r"""Produce to DuckDB as an output sink.

    :arg step_id: Unique ID.

    :arg up: Stream of records. Key must be a `String`
        and value must be a Python Dictionary that is
        serializable into a PyArrow table.

    :arg create_table_sql: Optional SQL statement to create DuckDB table
        if it does not already exist.

    :arg timeout: a timedelta of the amount of time to wait for
        new data before writing. Defaults to 1 second.

    :arg batch_size: the number of items to wait for before writing.
        Defaults to 122_880, an optimal size for DuckDB.

    """
    return _to_sink(
        "to_sink",
        up,
        timeout=timeout,
        batch_size=batch_size,
    ).then(
        op.output,
        "duckdb_output",
        DuckDBSink(db_path, table_name, create_table_sql),
    )
