"""This script demonstrates how to use windowing operators in a dataflow."""

# start-imports
from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    SlidingWindower,
    SessionWindower,
    collect_window,
    count_window,
)
from bytewax.testing import TestingSource, run_main
from bytewax.connectors.files import CSVSource
from pathlib import Path

# end-imports
