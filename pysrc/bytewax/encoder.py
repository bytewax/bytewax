import datetime
import json
import types

from .bytewax import Dataflow
from .inputs import ManualInputConfig, InputConfig
from .window import ClockConfig, WindowConfig, SystemClockConfig
from .outputs import (
    ManualOutputConfig,
    ManualEpochOutputConfig,
    StdOutputConfig,
    KafkaOutputConfig,
)

# A list of all types that should use vars(obj)
# which uses the Python `__getstate__` method
# to return a JSON encodable representation.
config_types = (
    InputConfig,
    ClockConfig,
    WindowConfig,
    ManualOutputConfig,
    ManualInputConfig,
    ManualEpochOutputConfig,
    StdOutputConfig,
    KafkaOutputConfig,
    SystemClockConfig,
)


class DataflowEncoder(json.JSONEncoder):
    """Custom JSON encoder for a Dataflow

    This class is used in conjunction with the Rust `webserver` module to
    produce a JSON representation of a bytewax Dataflow.
    """

    def default(self, obj):
        if isinstance(obj, Dataflow):
            return {"Dataflow": {"steps": obj.steps}}
        if isinstance(obj, config_types):
            return obj.__getstate__()
        if isinstance(obj, types.BuiltinFunctionType):
            return obj.__name__
        if isinstance(obj, types.FunctionType):
            return obj.__name__
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, datetime.timedelta):
            return str(obj)

        # Call the default encoder method for any other instance types.
        return json.JSONEncoder.default(self, obj)


def encode_dataflow(dataflow: Dataflow):
    """Convenience method for calling `json.dumps` with our custom
    DataflowEncoder class from Rust.
    """
    return json.dumps(dataflow, cls=DataflowEncoder, sort_keys=True)
