from datetime import datetime, timedelta, timezone

from bytewax.operators.window import (
    LATE_SESSION_ID,
    WindowMetadata,
    _session_find_merges,
    _SessionWindowerLogic,
    _SessionWindowerState,
)


def test_initial_session():
    watermark = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    windows_in, windows_late = logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        watermark=watermark,
    )
    assert list(windows_in) == [0]
    assert list(windows_late) == []

    assert logic.metadata_for(0) == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
    )


def test_extend_forward_within_gap():
    watermark = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        watermark=watermark,
    )

    windows_in, windows_late = logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
        watermark=watermark,
    )
    assert list(windows_in) == [0]
    assert list(windows_late) == []

    assert logic.metadata_for(0) == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
    )


def test_extend_forward_exact_gap():
    watermark = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        watermark=watermark,
    )

    windows_in, windows_late = logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
        watermark=watermark,
    )
    assert list(windows_in) == [0]
    assert list(windows_late) == []

    assert logic.metadata_for(0) == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
    )


def test_extend_backward_within_gap():
    watermark = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        watermark=watermark,
    )

    windows_in, windows_late = logic.open_for(
        timestamp=datetime(2024, 1, 1, 8, 59, 55, tzinfo=timezone.utc),
        watermark=watermark,
    )
    assert list(windows_in) == [0]
    assert list(windows_late) == []

    assert logic.metadata_for(0) == WindowMetadata(
        open_time=datetime(2024, 1, 1, 8, 59, 55, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
    )


def test_extend_backward_exact_gap():
    watermark = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        watermark=watermark,
    )

    windows_in, windows_late = logic.open_for(
        timestamp=datetime(2024, 1, 1, 8, 59, 50, tzinfo=timezone.utc),
        watermark=watermark,
    )
    assert list(windows_in) == [0]
    assert list(windows_late) == []

    assert logic.metadata_for(0) == WindowMetadata(
        open_time=datetime(2024, 1, 1, 8, 59, 50, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
    )


def test_extend_merge():
    watermark = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        watermark=watermark,
    )
    logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
        watermark=watermark,
    )

    windows_in, windows_late = logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
        watermark=watermark,
    )
    assert list(windows_in) == [0]
    assert list(windows_late) == []
    assert logic.merged() == [(1, 0)]

    assert logic.metadata_for(0) == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
    )


def test_within_existing():
    watermark = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10),
        state=_SessionWindowerState(
            max_key=0,
            sessions={
                0: WindowMetadata(
                    open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
                    close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
                )
            },
        ),
    )

    windows_in, windows_late = logic.open_for(
        timestamp=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
        watermark=watermark,
    )
    assert list(windows_in) == [0]
    assert list(windows_late) == []

    assert logic.metadata_for(0) == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
    )


def test_late():
    watermark = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10),
        state=_SessionWindowerState(),
    )

    windows_in, windows_late = logic.open_for(
        timestamp=datetime(2023, 12, 1, 9, 0, 0, tzinfo=timezone.utc),
        watermark=watermark,
    )
    assert list(windows_in) == []
    assert list(windows_late) == [LATE_SESSION_ID]


def test_find_merges_none():
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == []
    assert sessions == sessions


def test_find_merges_within_gap():
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == [(1, 0)]
    assert sessions == {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
        ),
    }


def test_find_merges_exact_gap():
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == [(1, 0)]
    assert sessions == {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
        ),
    }


def test_find_merges_multi():
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
        ),
        2: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == [(1, 0), (2, 0)]
    assert sessions == {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
        ),
    }


def test_find_merges_no_yes_no():
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
        ),
        2: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 25, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 25, tzinfo=timezone.utc),
        ),
        3: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 40, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 40, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == [(2, 1)]
    assert sessions == {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 25, tzinfo=timezone.utc),
        ),
        3: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 40, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 40, tzinfo=timezone.utc),
        ),
    }
