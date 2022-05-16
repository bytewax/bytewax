# Feature definition file. Establishes the shape of the feature view for
# storage.
import datetime as dt

from feast import Entity, Feature, FeatureView, FileSource, ValueType
from google.protobuf.duration_pb2 import Duration

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = FileSource(
    path="feast/data/driver_stats.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(
    name="driver_id",
    value_type=ValueType.INT64,
    description="driver id",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_daily_stats_view = FeatureView(
    name="driver_daily_stats",
    entities=["driver_id"],
    ttl=Duration().FromTimedelta(dt.timedelta(days=600)),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)
