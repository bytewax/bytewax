"""Connectors for [SQL](https://en.wikipedia.org/wiki/SQL).

Importing this module requires the
[`SQLAlchemy`](https://github.com/sqlalchemy/sqlalchemy)
package to be installed.

It also requires any database-specific packages to be installed, such as
[`psycopg2`](https://github.com/psycopg/psycopg2) for PostgreSQL.

"""
from time import sleep

from bytewax.inputs import PartitionedInput, StatefulSource
from sqlalchemy import create_engine, text

__all__ = [
    "SQLInput",
]


class _SQLSource(StatefulSource):
    def __init__(
        self,
        connection,
        query,
        starting_cursors,
        resume_state,
        backoff_start_delay,
        backoff_factor,
        backoff_max_delay,
    ):
        self._cursor_dict = resume_state or starting_cursors
        self._connection = connection
        self._query = query
        self._result = None
        self._backoff_start_delay = backoff_start_delay
        self._backoff_factor = backoff_factor
        self._backoff_max_delay = backoff_max_delay

    def next(self):
        delay = self._backoff_start_delay

        while True:
            if self._result is None or not self._result.returns_rows:
                self._result = self._connection.execution_options(
                    stream_results=True
                ).execute(text(self._query), self._cursor_dict)

            row = self._result.fetchone()

            if row is not None:
                item = row._asdict()
                # update all cursor columns
                for key in self._cursor_dict.keys():
                    self._cursor_dict[key] = item[key]
                return item
            else:
                # If there are no more rows to fetch, sleep for the specified interval
                # and then try again
                self._result.close()
                self._result = None

                sleep(delay)
                delay = min(delay * self._backoff_factor, self._backoff_max_delay)

    def snapshot(self):
        return self._cursor_dict

    def close(self):
        self._connection.close()


class SQLInput(PartitionedInput):
    def __init__(
        self,
        connection_string: str,
        query: str,
        starting_cursors: dict,
        backoff_start_delay: float = 1.0,
        backoff_factor: float = 2.0,
        backoff_max_delay: float = 60.0,
    ):
        self._connection_string = connection_string
        self._query = query
        self._starting_cursors = starting_cursors
        self._backoff_start_delay = backoff_start_delay
        self._backoff_factor = backoff_factor
        self._backoff_max_delay = backoff_max_delay

    def list_parts(self):
        return {"single"}  # A single partition in this case

    def build_part(self, for_part, resume_state):
        engine = create_engine(
            self._connection_string, connect_args={"options": "-c timezone=utc"}
        )
        connection = engine.connect()
        # Setting the transaction to READ ONLY mode
        connection.execution_options(isolation_level="READ COMMITTED", readonly=True)
        return _SQLSource(
            connection,
            self._query,
            self._starting_cursors,
            resume_state,
            self._backoff_start_delay,
            self._backoff_factor,
            self._backoff_max_delay,
        )
