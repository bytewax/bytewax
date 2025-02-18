"""Tests for the bytewax.duckdb module."""

import os
from pathlib import Path
from typing import Dict, List, Tuple, Union

import duckdb
import pytest

import bytewax.duckdb.operators as duck_op
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.duckdb import DuckDBSink
from bytewax.testing import TestingSource, run_main


# Skip the license warning in tests
@pytest.fixture(autouse=True)
def suppress_license_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    """Suppress the license warning in tests."""
    monkeypatch.setitem(os.environ, "BYTEWAX_LICENSE", "1")


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Generate a temporary path for the DuckDB database file."""
    return tmp_path / "test_duckdb.db"


@pytest.fixture
def table_name() -> str:
    """Fixture for the table name."""
    return "test_table"


@pytest.fixture
def create_table_sql() -> str:
    """Fixture for the SQL statement to create a table."""
    return "CREATE TABLE test_table (id INTEGER, name TEXT)"


def test_duckdb_sink(db_path: Path, table_name: str) -> None:
    """Test that DuckDBSink writes all items correctly."""
    flow = Dataflow("duckdb")

    def create_dict(value: int) -> Tuple[str, List[Dict[str, Union[int, str]]]]:
        return (str(value), [{"id": value, "name": f"Name_{value}"}])

    inp = op.input("inp", flow, TestingSource(range(100)))
    dict_stream = op.map("dict", inp, create_dict)

    op.output(
        "out",
        dict_stream,
        DuckDBSink(
            str(db_path),  # Convert Path to string
            table_name,
            f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER, name TEXT)",
        ),
    )
    run_main(flow)

    conn = duckdb.connect(str(db_path))  # Convert Path to string
    result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    assert result == (100,)


def test_duckdb_operator(db_path: Path, table_name: str) -> None:
    """Test that the DuckDB operator appends data correctly across runs."""
    flow = Dataflow("duckdb")

    def create_dict(value: int) -> Tuple[str, Dict[str, Union[int, str]]]:
        return (str(value), {"id": value, "name": f"Name_{value}"})

    inp = op.input("inp", flow, TestingSource(range(100)))
    dict_stream = op.map("dict", inp, create_dict)

    # Use IF NOT EXISTS to avoid duplicate table creation errors
    duck_op.output(
        "out",
        dict_stream,
        str(db_path),  # Convert Path to string
        table_name,
        f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER, name TEXT)",
    )

    # First run
    run_main(flow)
    conn = duckdb.connect(str(db_path))  # Convert Path to string
    first_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    assert first_result == (100,)

    # Second run: reinitialize the dataflow and append
    flow = Dataflow("duckdb")
    inp = op.input("inp", flow, TestingSource(range(100)))
    dict_stream = op.map("dict", inp, create_dict)

    duck_op.output(
        "out",
        dict_stream,
        str(db_path),  # Convert Path to string
        table_name,
        f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER, name TEXT)",
    )
    run_main(flow)

    second_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    assert second_result == (200,)
