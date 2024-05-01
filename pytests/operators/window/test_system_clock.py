from datetime import datetime, timezone

from bytewax.operators.windowing import UTC_MAX, _SystemClockLogic
from bytewax.testing import TimeTestingGetter


def test_watermark_is_end_of_time_on_eof():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _SystemClockLogic(source.get)
    logic.on_eof()
    assert logic.on_eof() == UTC_MAX
