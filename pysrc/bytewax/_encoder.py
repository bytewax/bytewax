import datetime
import inspect
import json
import types

from .bytewax import Dataflow


class DataflowEncoder(json.JSONEncoder):
    """Custom JSON encoder for a Dataflow

    This class is used in conjunction with the Rust `webserver` module to
    produce a JSON representation of a bytewax Dataflow.

    __getstate__() is a method defined on all of the Python classes we
    create in Rust to return a PyDict representation of that class.
    """

    def default(self, obj):
        if hasattr(obj, "__json__"):
            return obj.__json__()

        # Check if the object is a class, and return its name.
        # If the object is a class the call to __getstate__ below
        # WILL fail since we are not passing a `self` parameter.
        if inspect.isclass(obj):
            return obj.__qualname__

        if isinstance(obj, types.BuiltinFunctionType):
            return obj.__name__
        if isinstance(obj, types.MethodDescriptorType):
            return obj.__name__
        if isinstance(obj, types.FunctionType):
            return obj.__name__
        if isinstance(obj, types.BuiltinMethodType):
            return obj.__name__
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, datetime.timedelta):
            return str(obj)
        if isinstance(obj, type):  # For callable types like `list` and `dict`
            return obj.__name__

        # Call the default encoder method for any other instance types.
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError as err:
            msg = f"{obj} can not be JSON encoded"
            raise TypeError(msg) from err


def encode_dataflow(dataflow: Dataflow):
    """Convenience method for calling `json.dumps` with our custom
    DataflowEncoder class from Rust.
    """
    return json.dumps(dataflow, cls=DataflowEncoder, sort_keys=True)
