"""Setup a dataflow for handling missing data in a stream of numbers.

This example demonstrates how to use the bytewax library to create a dataflow
that processes a stream of numbers, where every 5th number is missing (represented
by np.nan). The dataflow uses a stateful operator to impute the missing values
using a windowed mean imputation strategy.
"""

# start-imports
import random

import bytewax.operators as op
import numpy as np
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition

# end-imports


# start-random-data
class RandomNumpyData(StatelessSourcePartition):
    """Generate a random sequence of numbers with missing values.

    Data Source that generates a sequence
    of 100 numbers, where every 5th number is
    missing (represented by np.nan),
    and the rest are random integers between 0 and 10.
    """

    def __init__(self):
        """Initialize the data source."""
        self._it = enumerate(range(100))

    def next_batch(self):
        """Generate the next batch of data.

        Returns:
            list: A list of tuples containing the data.
                If the index of the item is divisible by 5,
                the data is np.nan, otherwise it is a random
        """
        i, item = next(self._it)
        if i % 5 == 0:
            return [("data", np.nan)]
        else:
            return [("data", random.randint(0, 10))]


class RandomNumpyInput(DynamicSource):
    """Generate random data based on worker distribution.

    Class encapsulating dynamic data generation
    based on worker distribution in distributed processing
    """

    def build(self, step_id, _worker_index, _worker_count):
        """Build the data source."""
        return RandomNumpyData()


# end-random-data

# start-dataflow
flow = Dataflow("map_eg")
input_stream = op.input("input", flow, RandomNumpyInput())
# end-dataflow


# start-windowed-array
class WindowedArray:
    """Windowed Numpy Array.

    Create a numpy array to run windowed statistics on.
    """

    def __init__(self, window_size: int) -> None:
        """Initialize the windowed array.

        Args:
            window_size (int): The size of the window.
        """
        self.last_n = np.empty(0, dtype=float)
        self.n = window_size

    def push(self, value: float) -> None:
        """Push a value into the windowed array.

        Args:
            value (float): The value to push into the array.
        """
        if np.isscalar(value) and np.isreal(value):
            self.last_n = np.insert(self.last_n, 0, value)
            try:
                self.last_n = np.delete(self.last_n, self.n)
            except IndexError:
                pass

    def impute_value(self) -> float:
        """Impute the next value in the windowed array.

        Returns:
            tuple: A tuple containing the original value and the imputed value.
        """
        return np.nanmean(self.last_n)


# end-windowed-array


# start-stateful-imputer
class StatefulImputer:
    """Impute values while maintaining state.

    This class is a stateful object that encapsulates a
    WindowedArray and provides a method that uses this
    array to impute values.
    The impute_value method of this object is passed to
    op.stateful_map, so the state is maintained across
    calls to this method.
    """

    def __init__(self, window_size):
        """Initialize the stateful imputer.

        Args:
            window_size (int): The size of the window.
        """
        self.windowed_array = WindowedArray(window_size)

    def impute_value(self, key, value):
        """Impute the value in the windowed array."""
        return self.windowed_array.impute_value(value)


# end-stateful-imputer

# start-dataflow-inpute
def mapper(window: Optional[WindowedArray], orig_value: float) -> Tuple[Optional[WindowedArray], Tuple[float, float]]:
    if window is None:
        window = WindowedArray(10)
    if not np.isnan(orig_value):
        window.push(value)
        new_value = orig_value
    else:
        new_value = window.impute()  # Calculate derived value.
        
    return (window, (orig_value, new_value))

imputed_stream = op.stateful_map("impute", input_stream, mapper)
# end-dataflow-inpute

# start-output
op.output("output", imputed_stream, StdOutSink())
# end-output
