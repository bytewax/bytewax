"""Connectors for SQLAlchemy compatible sinks."""
from typing import List

from sqlalchemy import (
    Engine,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import Executable
from typing_extensions import override

from bytewax.outputs import DynamicSink, StatelessSinkPartition


class _StatelessSQLSinkPartition(StatelessSinkPartition[Dict[str, Any]]):
    def __init__(self, engine: Engine, stmt: Executable):
        self._engine = engine
        self._stmt = stmt
        self._conn = engine.connect()

    @override
    def write_batch(self, items: List[dict]):
        try:
            self._conn.execute(self._stmt, items)
            self._conn.commit()
        # SQLAlchemy Errors fail to initialize, catch and rethrow here.
        # similar to https://github.com/celery/celery/issues/5057
        except SQLAlchemyError as e:
            raise RuntimeError(e._message()) from e

    @override
    def close(self):
        self._conn.close()


class DynamicSQLOutput(DynamicSink):
    """Write output to a SQL database using SQLAlchemy's core API.

    Each worker will construct it's own connection to the supplied Engine.

    The supplied `Executable` statement is used as the first argument to
    `sqlalchemy.engine.Connection.execute` along with a list of
    dictionaries to be used as the values for the statement.

    """

    def __init__(self, engine: Engine, stmt: Executable):
        """Init.

        Args:
            engine: The SQLAlchemy engine to use when building a connection.

            stmt: A SQLAlchemy executable statement to be run for each batch of output.
        """
        self._engine = engine
        self._stmt = stmt

    @override
    def build(self, worker_index: int, worker_count: int) -> _StatelessSQLSinkPartition:
        return _StatelessSQLSinkPartition(self._engine, self._stmt)
