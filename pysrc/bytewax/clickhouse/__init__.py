"""ClickHouse Sink Implementation.

This module provides a dynamic sink for writing data to a ClickHouse
database using Bytewax's streaming data processing framework. The
sink is capable of creating and managing a connection to ClickHouse,
checking for the existence of the target table, and creating it if
necessary using a ReplacingMergeTree engine.

Classes:
    ClickHouseSink: A dynamic sink that connects to a ClickHouse database,
                manages table creation, and writes data in batches.
    _ClickHousePartition: A stateless partition responsible for writing batches of
                        data to the ClickHouse database.

Usage:
    - The `ClickHouseSink` class is used to define a sink that can be
    connected to a Bytewax dataflow.
    - The `build` method of `ClickHouseSink` creates a `_ClickHousePartition`
    that handles the actual data writing process.
    - The sink supports creating a table with a specified schema
    if it does not exist, and verifies the existing table's engine
    type for compatibility with ReplacingMergeTree.


Logging:
    The module uses Python's logging library to log important events such
    as table existence checks, schema details, and warnings about potential
    performance issues.

## Usage

Before running any workload, you will need to start ClickHouse if you are not already running it.

```bash
docker compose up -d
```

ClickHouse Sink requires a PyArrow table, Schema and keys for order and partition.

The Sink is eventually consistent based on the keys.

Add the import

```python
from bytewax.clickhouse import operators as chop
```

Add A schema and order by string to your code.

```python
CH_SCHEMA = \"\"\"
        metric String,
        value Float64,
        ts DateTime,
        \"\"\"

ORDER_BY = "metric, ts"
```

define a pyarrow schema

```python
PA_SCHEMA = pa.schema(
    [
        ("metric", pa.string()),
        ("value", pa.float64()),
        ("ts", pa.timestamp("us")),  # microsecond
    ]
)
```

Use the ClickHouse Sink to write data to ClickHouse

```python
chop.output(
    "output_clickhouse",
    metrics,
    "metrics",
    "admin",
    "password",
    database="bytewax",
    port=8123,
    ch_schema=CH_SCHEMA,
    order_by=ORDER_BY,
    pa_schema=PA_SCHEMA,
    timeout=timedelta(seconds=1),
    max_size=10,
)
```

"""

import logging
from typing import List, TypeVar

from bytewax.outputs import DynamicSink, StatelessSinkPartition
from clickhouse_connect import get_client
from pyarrow import Table, concat_tables  # type: ignore
from typing_extensions import override


K = TypeVar("K")
"""Type of key in Kafka message."""

V = TypeVar("V")
"""Type of value in a Kafka message."""

# Configure logging
logger = logging.getLogger(__name__)


class _ClickHousePartition(StatelessSinkPartition):
    def __init__(
        self,
        table_name: str,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
    ):
        self.table_name = table_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )

    @override
    def write_batch(self, batch: List[Table]) -> None:
        """Write a batch of data to the ClickHouse table.

        This method concatenates the incoming batch of PyArrow Tables and inserts the
        resulting table into the specified ClickHouse table.

        Args:
            batch (List[Table]): A list of PyArrow Table objects representing the batch
            of data to be written.
        """
        arrow_table = concat_tables(batch)
        self.client.insert_arrow(
            f"{self.database}.{self.table_name}",
            arrow_table,
            settings={"buffer_size": 0},
        )


class ClickHouseSink(DynamicSink):
    """A dynamic sink for writing data to a ClickHouse database in a Bytewax dataflow.

    The ClickHouseSink class provides functionality to connect to a ClickHouse database,
    check for the existence of a specified table, and create it if it doesn't exist.
    The class ensures that the table uses the ReplacingMergeTree engine and writes data
    in batches using the PyArrow format.

    Methods:
        build(step_id, worker_index, worker_count) -> _ClickHousePartition:
            Constructs a _ClickHousePartition instance that manages the actual data
            writing process.
    """

    def __init__(
        self,
        table_name: str,
        schema: str,
        username: str,
        password: str,
        host: str = "localhost",
        port: int = 8123,
        database: str = "default",
        order_by: str = "",
    ):
        """Initialize the ClickHouseSink.

        Sets up the connection parameters for the ClickHouse database and verifies
        the existence of the target table. If the table does not exist, it will be
        created using the specified schema.

        Args:
            table_name (str): Name of the table in ClickHouse.
            username (str): Username for authentication with the ClickHouse server.
            password (str): Password for authentication with the ClickHouse server.
            host (str, optional): Hostname of ClickHouse server. Default is "localhost".
            port (int, optional): Port number of the ClickHouse server. Default is 8123.
            database (Optional[str], optional): Name of the database in ClickHouse.
                                    If not provided, uses "default".
            schema (Optional[str], optional): Schema definition for the table if needs
                                    to be created. Defaults to None.
            order_by (str, optional): Comma-separated list of columns to order by in the
                                    ReplacingMergeTree engine. Defaults to "".

        Raises:
            ValueError: If the schema is not provided and the table does not exist,
            a ValueError is raised.
        """
        self.table_name = table_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.schema = schema

        # init client
        client = get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )

        # Check if the table exists
        table_exists_query = f"EXISTS {self.database}.{self.table_name}"
        table_exists = client.command(table_exists_query)
        if not table_exists:
            logger.info(
                f"""Table '{self.table_name}' does not exist.
                        Attempting to create with provided schema"""
            )
            if schema:
                # Create the table with ReplacingMergeTree
                create_table_query = f"""
                CREATE TABLE {database}.{table_name} (
                    {self.schema}
                ) ENGINE = ReplacingMergeTree()
                ORDER BY tuple({order_by});
                """
                client.command(create_table_query)
                logger.info(f"Table '{table_name}' created successfully.")
            else:
                msg = """Bad Schema. Can't complete execution without schema of format
                        column1 UInt32,
                        column2 String,
                        column3 Date"""
                raise ValueError(msg)
        else:
            logger.info(f"Table '{self.table_name}' exists.")

            # Check the MergeTree type
            mergetree_type_query = f"""SELECT engine FROM system.tables
                    WHERE database = '{self.database}' AND name = '{self.table_name}'"""
            mergetree_type = client.command(mergetree_type_query)
            logger.info(f"MergeTree type of the table '{table_name}': {mergetree_type}")

            if "ReplacingMergeTree" not in str(mergetree_type):
                logger.warning(
                    f"""Table '{table_name}' is not using ReplacingMergeTree.
                    Consider modifying the table to avoid performance degredation
                    and/or duplicates on restart"""
                )

            # Get the table schema
            schema_query = f"""
            SELECT name, type FROM system.columns
            WHERE database = '{self.database}' AND table = '{self.table_name}'
            """
            schema_result = client.query(schema_query)
            columns = schema_result.result_rows
            logger.info(f"Schema of the table '{self.table_name}':")
            for column in columns:
                logger.info(f"Column: {column[0]}, Type: {column[1]}")

        client.close()

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _ClickHousePartition:
        """Build a sink partition for writing to ClickHouse.

        This method constructs an instance of `_ClickHousePartition`, which will
        handle the actual data writing to the ClickHouse table for the specified
        worker in a distributed Bytewax dataflow.

        Args:
            step_id (str): The ID of the step in the Bytewax dataflow.
            worker_index (int): The index of the worker in the dataflow.
            worker_count (int): The total number of workers in the dataflow.

        Returns:
            _ClickHousePartition: An instance of `_ClickHousePartition` that will manage
            the data writing for this worker.
        """
        return _ClickHousePartition(
            self.table_name,
            self.host,
            self.port,
            self.username,
            self.password,
            self.database,
        )
