import bytewax.operators as op
from bytewax.connectors.sql import DynamicSQLOutput
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, insert
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert

url = "sqlite+pysqlite:///:memory:"
engine = create_engine(url, echo=True)
metadata_obj = MetaData()
tbl = Table(
    "numbers",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("num", Integer),
)
metadata_obj.create_all(engine)

# Derive the table from the existing db using reflection
metadata_obj = MetaData()
numbers = Table("numbers", metadata_obj, autoload_with=engine)

with engine.connect() as conn:
    conn.execute(insert(numbers), [{"id": 1, "num": 1}])
    conn.commit()

flow = Dataflow("sqlite-out")
inp = op.input(
    "inp",
    flow,
    TestingSource([{"id": 1, "num": 2}, {"id": 2, "num": 2}, {"id": 3, "num": 1}]),
)
upsert = sqlite_upsert(numbers)
upsert = upsert.on_conflict_do_update(
    index_elements=[numbers.c.id], set_=dict(num=upsert.excluded.num)
)

op.output(
    "sqlite_out",
    inp,
    DynamicSQLOutput(engine, upsert),
)
