"""Operators for the ClickHouse sink.

It's suggested to import operators like this:

```
from bytewax.connectors.clickhouse import operators as chop
```

And then you can use the operators like this:

```
from bytewax.dataflow import Dataflow

flow = Dataflow("clickhouse-out")
input = kop.input("kafka_inp", flow, brokers=[...], topics=[...])
chop.output(
    "ch-out",
    input,
)
```
"""

from datetime import timedelta
from typing import List, Tuple

import bytewax.operators as op
import pyarrow as pa  # type: ignore
from bytewax.clickhouse import ClickHouseSink, V
from bytewax.dataflow import Stream, operator
from typing_extensions import TypeAlias

KeyedStream: TypeAlias = Stream[Tuple[str, V]]
"""A {py:obj}`~bytewax.dataflow.Stream` of `(key, value)` 2-tuples."""


@operator
def _to_sink(
    step_id: str,
    up: KeyedStream[V],
    timeout: timedelta,
    max_size: int,
    pa_schema: pa.Schema,
) -> KeyedStream[List[V]]:
    """Convert records to PyArrow Table."""

    def shim_mapper(key__batch: Tuple, pa_schema: pa.Schema) -> pa.Table:
        key, batch = key__batch
        columns = list(zip(*batch))
        arrays = []
        for i, f in enumerate(pa_schema):
            array = pa.array(columns[i], f.type)
            arrays.append(array)
        t = pa.Table.from_arrays(arrays, schema=pa_schema)

        return t

    return op.collect("batch", up, timeout=timeout, max_size=max_size).then(
        op.map, "map", lambda x: shim_mapper(x, pa_schema)
    )


@operator
def output(
    step_id: str,
    up: KeyedStream[V],
    pa_schema: pa.Schema,
    table_name: str,
    ch_schema: str,
    username: str,
    password: str,
    host: str = "localhost",
    port: int = 8123,
    database: str = "default",
    order_by: str = "",
    timeout: timedelta = timedelta(seconds=1),
    max_size: int = 50,
) -> None:
    r"""Produce to ClickHouse as an output sink.

    Uses Arrow format, must be arrow serializiable.

    Default partition routing is used.

    Workers are the unit of parallelism.

    Can support at-least-once processing depending on
    the MergeTree used for downstream queries.

    :arg step_id: Unique ID.

    :arg up: Stream of records. Key must be a `String`
        and value must be serializable into an arrow table.

    :arg pa_schema: Arrow schema.

    :arg table_name: Table name for the writes.

    :arg ch_schema: schema string of format
                        ```column1 UInt32,\\n column2 String,\\n column3 Date```,

    :arg username: database username, user must have
        correct permissions.

    :arg password:

    :arg host: host name, defaults to "localhost".

    :arg port: port name, defaults to 8123.

    :arg database: optional database name. If omitted
        this will use the default database.

    :arg order_by: order by string that determines the sort of
        the table for deduplication. Should be of format:
        `column1, column2`

    :arg timeout: a timedelta of the amount of time to wait for
        new data before writing. Defaults to 1 second.

    :arg batch_size: the number of items to wait before writing
        defaults to 50.

    """
    return _to_sink(
        "to_sink", up, timeout=timeout, max_size=max_size, pa_schema=pa_schema
    ).then(
        op.output,
        "kafka_output",
        ClickHouseSink(
            table_name, ch_schema, username, password, host, port, database, order_by
        ),
    )
