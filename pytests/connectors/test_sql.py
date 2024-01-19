import bytewax.operators as op
from bytewax.connectors.sql import DynamicSQLOutput
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, insert, select
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert


def test_sqlite_output():
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

    with engine.connect() as conn:
        conn.execute(insert(tbl), [{"id": 1, "num": 1}])
        conn.commit()

    flow = Dataflow("sqlite-out")
    inp = op.input(
        "inp",
        flow,
        TestingSource([{"id": 1, "num": 2}, {"id": 2, "num": 2}]),
    )
    stmt = sqlite_upsert(tbl)
    stmt = stmt.on_conflict_do_update(
        index_elements=[tbl.c.id], set_=dict(num=stmt.excluded.num)
    )

    op.output(
        "sqlite_out",
        inp,
        DynamicSQLOutput(engine, stmt),
    )

    run_main(flow)

    with engine.connect() as conn:
        res = conn.execute(select(tbl))
        assert [r._asdict() for r in res.all()] == [
            {"id": 1, "num": 2},
            {"id": 2, "num": 2},
        ]
