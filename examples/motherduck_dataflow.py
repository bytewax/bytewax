from pathlib import Path
from typing import Dict, Tuple, Union

import bytewax.duckdb.operators as duck_op
from bytewax.duckdb import DuckDBSink
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

import os
from dotenv import load_dotenv

load_dotenv(".env")

md_token = os.getenv("MOTHERDUCK_TOKEN")
db_path = f"md:my_db?motherduck_token={md_token}"

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
    # The following marks batch '1' to be written to MotherDuck
    return ("batch_1", {"id": value, "name": name, "age": age, "location": location})


# Generate input data
inp = op.input("inp", flow, TestingSource(range(50)))
dict_stream = op.map("dict", inp, create_dict)

# Output the data to DuckDB, creating a table with multiple columns
duck_op.output(
    "out",
    dict_stream,
    db_path,
    "names_cities",
    "CREATE TABLE IF NOT EXISTS names_cities (id INTEGER, name TEXT, age INTEGER, location TEXT)",
)

# Run the dataflow
run_main(flow)
