"""A source and sink connector for Bytewax.

You will need an influxdb account and the API key to use the connector.

To test the connector, use `examples/input.py` and `examples/output.py`. You may need to modify the date parameters for it to work.

To use the source and sink connector use them in the input and output operators like shown below.

```python
inp = op.input(
    "inp",
    flow,
    InfluxDBSource(
        timedelta(seconds=5),
        "https://us-east-1-1.aws.cloud2.influxdata.com",
        DATABASE,
        TOKEN,
        "home",
        ORG,
        datetime.now(timezone.utc) - timedelta(days=5),
    ),
)
op.output(
    "out",
    inp,
    InfluxDBSink(
        host="https://us-east-1-1.aws.cloud2.influxdata.com",
        database=DATABASE,
        org=ORG,
        token=TOKEN,
        write_precision="s",
    ),
)
```
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, NamedTuple, Optional, Union

from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3.write_client.client.write.point import DEFAULT_WRITE_PRECISION
from pyarrow import RecordBatch  # type: ignore

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class _InfluxDBSourcePartition(
    StatefulSourcePartition[RecordBatch, Optional[datetime]]
):
    def __init__(
        self,
        interval: timedelta,
        now: datetime,
        last_time: datetime,
        host: str,
        database: str,
        token: str,
        measurement: str,
        org: Union[str, None],
    ) -> None:
        self.interval = interval
        self.last_time = last_time
        self._next_awake = now
        self.client = InfluxDBClient3(
            host=host, database=database, token=token, org=org
        )
        self.host = host
        self.database = database
        self.token = token
        self.measurement = measurement
        self.org = org

    def next_batch(self) -> List[RecordBatch]:
        """Retrieve the next batch of data from InfluxDB based on the time interval.

        This method constructs and executes a query to fetch data from InfluxDB
        between the last known time and the current time. The last_time attribute is
        updated to the current time after fetching the data.

        Returns:
            List[RecordBatch]: A list of data points retrieved from InfluxDB returned
            as Arrow Record Batches. That can convert into dataframes or similar.
        """
        head_time = self.last_time + self.interval
        query = f"""SELECT * from "{self.measurement}"
        WHERE time >= '{self.last_time.isoformat()}'
        and time < '{head_time.isoformat()}'"""
        logger.info(f"query is: {query}")
        data = self.client.query(query=query)
        self.last_time = head_time
        logger.info(f"last_time:{self.last_time}")
        if head_time < self._next_awake:
            self._next_awake = datetime.now(timezone.utc)
        else:
            self._next_awake = head_time + self.interval
        return data.to_batches()

    def snapshot(self) -> Optional[datetime]:
        """Take a snapshot of the current state of the partition.

        This method captures the current state of the partition, specifically the
        last_time, which can be used to resume the source from the last read point.

        Returns:
            datetime: the last time bounded on the query.
        """
        return self.last_time

    def next_awake(self) -> datetime:
        """Get the time when the partition should next be polled.

        This method returns the next scheduled time for polling based on the interval.

        Returns:
            datetime: The next polling time.
        """
        return self._next_awake


class InfluxDBSource(FixedPartitionedSource[RecordBatch, Optional[datetime]]):
    """A source for reading data from an InfluxDB instance in fixed partitions."""

    def __init__(
        self,
        interval: timedelta,
        host: str,
        database: str,
        token: str,
        measurement: str,
        org: Optional[str] = None,
        start_time: Union[datetime, None] = None,
    ):
        """Influx DB Source Connector.

        Manages the connection to an InfluxDB instance and reads data at fixed
        intervals, partitioning the data flow using a single partition (singleton).
        It supports resuming from a specific state based on the last read time.

        Args:
            interval (timedelta): The time interval between polls.
            host (str): The InfluxDB host URL.
            database (str): The name of the InfluxDB database.
            token (str): The authentication token for InfluxDB.
            measurement (str): The InfluxDB measurement to query.
            org (Optional[str]): The InfluxDB organization name.
            start_time (Union[datetime, None], optional): The starting point for the
                data query. If None, it defaults to the current time minus the interval.

        Methods:
            list_parts(): Lists the partitions managed by this source.
            build_part(step_id, for_part, resume_state): Builds and returns a specific
                partition.
        """
        self.interval = interval
        self.host = host
        self.database = database
        self.token = token
        self.measurement = measurement
        self.org = org
        self.start_time = start_time

    def list_parts(self) -> List[str]:
        """List the partitions managed by this source.

        This Source is limited to a single client connection/query.

        Returns:
            List[str]: A list containing the partition identifier(s).
        """
        return ["singleton"]

    def build_part(
        self, step_id: str, for_part: str, resume_state: Union[Any, None]
    ) -> _InfluxDBSourcePartition:
        """Build and return a specific partition for the data source.

        This method creates an _InfluxDBPartition instance that handles the actual
        data retrieval. It resumes from the last known state if available, or starts
        from the specified start_time or interval.

        Args:
            step_id (str): The ID of the current step in the dataflow.
            for_part (str): The partition identifier, should always be "singleton".
            resume_state (Optional[dict]): The state to resume from, typically
            containing the last read time.

        Returns:
            _InfluxDBPartition: An instance of the partition that handles data
            retrieval.
        """
        assert for_part == "singleton"
        now = datetime.now(timezone.utc)
        if resume_state:
            last_time = resume_state
        elif self.start_time:
            last_time = self.start_time
        else:
            last_time = now - self.interval
        return _InfluxDBSourcePartition(
            self.interval,
            now,
            last_time,
            self.host,
            self.database,
            self.token,
            self.measurement,
            self.org,
        )


class _InfluxDBSinkPartition(StatelessSinkPartition):
    def __init__(
        self,
        host: str,
        database: str,
        token: str,
        org: str,
        write_precision: str,
        **kwargs: Optional[Any],
    ) -> None:
        self.client = InfluxDBClient3(
            host=host, database=database, token=token, org=org
        )
        self.bucket = database
        self.org = org
        self.write_precision = write_precision
        self.kwargs = kwargs

    def write_batch(
        self,
        items: List[
            Union[
                str,
                Point,
                Dict[str, Any],
                bytes,
                NamedTuple,
                Any,
            ]
        ],
    ) -> None:
        """Write a batch of items to the InfluxDB instance.

        Args:
            items: The items to write to InfluxDB.
            See the influx db client documentation for a full list of the accepted
            formats.

            https://influxdb-client.readthedocs.io/en/stable/usage.html#the-data-could-be-written-as
        """
        self.client.write(
            items, self.bucket, write_precision=self.write_precision, **self.kwargs
        )

    def close(self) -> None:
        self.client.close()


# Custom Sink class that wraps the SinkPartition
class InfluxDBSink(DynamicSink):
    """A dynamic sink for writing data to InfluxDB."""

    def __init__(
        self,
        host: str,
        database: str,
        token: str,
        org: str,
        write_precision: str = DEFAULT_WRITE_PRECISION,
        **kwargs: Optional[Any],
    ) -> None:
        """Influx DB sink connector.

        This class handles the creation of sink partitions that perform the actual
        data writing to InfluxDB. It is used within the Bytewax dataflow to send data
        to InfluxDB. Can be used with multiple workers.

        Args:
            host (str): The InfluxDB host URL.
            database (str): The name of the InfluxDB database.
            token (str): The authentication token for InfluxDB.
            org (str): The InfluxDB organization name.
            write_precision (str, optional): The precision for writing points.
                Defaults to the clients default precision.
            **kwargs: Additional parameters passed to the InfluxDB client.

        Methods:
            build(step_id, worker_index, worker_count): Builds and returns an
            _InfluxDBSinkPartition.
        """
        self.host = host
        self.database = database
        self.token = token
        self.org = org
        self.write_precision = write_precision
        self.kwargs = kwargs

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _InfluxDBSinkPartition:
        """Build the individual sink partitions.

        creates an _InfluxDBPartition on each worker
        """
        return _InfluxDBSinkPartition(
            self.host,
            self.database,
            self.token,
            self.org,
            self.write_precision,
            **self.kwargs,
        )
