from datetime import datetime, timezone

from bytewax.testing import TimeTestingGetter
from bytewax.windowing import UTC_MAX, _SystemClockLogic


def test_watermark_is_end_of_time_on_eof():
    source = TimeTestingGetter(datetime(2024, 1, 1, tzinfo=timezone.utc))

    logic = _SystemClockLogic(source.get)
    logic.on_eof()
    assert logic.on_eof() == UTC_MAX
