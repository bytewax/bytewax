from threading import Event

from bytewax import AdvanceTo, Dataflow, Emit, run_main
from bytewax.recovery import SqliteRecoveryConfig

from pytest import fixture, raises


RECOVERY_CONFIG_TYPES = [
    "SqliteRecoveryConfig",
]


# Will run a version of each test in this file with each recovery
# store type.
def pytest_generate_tests(metafunc):
    if "recovery_config" in metafunc.fixturenames:
        metafunc.parametrize("recovery_config", RECOVERY_CONFIG_TYPES, indirect=True)


@fixture
def recovery_config(tmp_path, request):
    if request.param == "SqliteRecoveryConfig":
        yield SqliteRecoveryConfig(str(tmp_path / "state.sqlite3"), create=True)
    else:
        raise ValueError("unknown recovery config type: {request.param!r}")


def test_recover_with_latest_state(recovery_config):
    armed = Event()
    armed.set()

    def trigger(item):
        """Odd numbers cause exception if armed."""
        key, value = item
        if armed.is_set() and value % 2 == 1:
            raise RuntimeError("BOOM")
        return item

    def keep_max(previous_max, new_item):
        if previous_max is None:
            new_max = new_item
        else:
            new_max = max(previous_max, new_item)
        return new_max, new_max

    flow = Dataflow()
    flow.map(trigger)
    flow.stateful_map("keep_max", lambda key: None, keep_max)
    flow.capture()

    inp = [
        ("a", 4),
        ("b", 4),
        ("a", 1),  # Will fail here on first pass cuz odd.
        ("b", 1),
    ]

    def ib(i, n, r):
        for epoch, item in enumerate(inp):
            if epoch < r:
                continue
            yield Emit(item)
            yield AdvanceTo(epoch + 1)

    out = []

    def ob(i, n):
        return out.append

    # First pass.
    with raises(RuntimeError):
        run_main(flow, ib, ob, recovery_config=recovery_config)

    assert sorted(out) == sorted(
        [
            (0, ("a", 4)),
            (1, ("b", 4)),
        ]
    )

    # Disable bomb.
    armed.clear()
    out.clear()

    # Recover.
    run_main(flow, ib, ob, recovery_config=recovery_config)

    assert sorted(out) == sorted(
        [
            (1, ("b", 4)),
            (2, ("a", 4)),
            (3, ("b", 4)),
        ]
    )


def test_recover_doesnt_gc_last_write(recovery_config):
    armed = Event()
    armed.set()

    def trigger(item):
        """Odd numbers cause exception if armed."""
        key, value = item
        if armed.is_set() and value % 2 == 1:
            raise RuntimeError("BOOM")
        return item

    def keep_max(previous_max, new_item):
        if previous_max is None:
            new_max = new_item
        else:
            new_max = max(previous_max, new_item)
        return new_max, new_max

    flow = Dataflow()
    flow.map(trigger)
    flow.stateful_map("keep_max", lambda key: None, keep_max)
    flow.capture()

    inp = [
        (
            "a",
            4,
        ),  # "a" is old enough to be GCd by time failure happens, but shouldn't be because the key hasn't been seen again.
        ("b", 4),
        ("b", 4),
        ("b", 4),
        ("b", 4),
        ("b", 5),  # Will fail here on first pass cuz odd.
        ("a", 1),
    ]

    def ib(i, n, r):
        for epoch, item in enumerate(inp):
            if epoch < r:
                continue
            yield Emit(item)
            yield AdvanceTo(epoch + 1)

    out = []

    def ob(i, n):
        return out.append

    # First pass.
    with raises(RuntimeError):
        run_main(flow, ib, ob, recovery_config=recovery_config)

    assert sorted(out) == sorted(
        [
            (0, ("a", 4)),
            (1, ("b", 4)),
            (2, ("b", 4)),
            (3, ("b", 4)),
            (4, ("b", 4)),
        ]
    )

    # Disable bomb.
    armed.clear()
    out.clear()

    # Recover.
    run_main(flow, ib, ob, recovery_config=recovery_config)

    assert sorted(out) == sorted(
        [
            (4, ("b", 4)),
            (5, ("b", 5)),
            (6, ("a", 4)),  # Remembered "a" => 4
        ]
    )


def test_recover_respects_delete(recovery_config):
    armed = Event()
    armed.set()

    def trigger(item):
        """Odd numbers cause exception if armed."""
        key, value = item
        if armed.is_set() and value is not None and value % 2 == 1:
            raise RuntimeError("BOOM")
        return item

    def keep_max(previous_max, new_item):
        if previous_max is None:
            new_max = new_item
        else:
            if new_item is not None:
                new_max = max(previous_max, new_item)
            else:
                new_max = None
        return new_max, new_max

    flow = Dataflow()
    flow.map(trigger)
    flow.stateful_map("keep_max", lambda key: None, keep_max)
    flow.capture()

    inp = [
        ("a", 4),
        ("b", 4),
        ("a", None),
        ("b", 2),
        ("b", 5),  # Will fail here on first pass cuz odd.
        ("a", 2),  # Should be max for "a" on resume.
    ]

    def ib(i, n, r):
        for epoch, item in enumerate(inp):
            if epoch < r:
                continue
            yield Emit(item)
            yield AdvanceTo(epoch + 1)

    out = []

    def ob(i, n):
        return out.append

    # First pass.
    with raises(RuntimeError):
        run_main(flow, ib, ob, recovery_config=recovery_config)

    assert sorted(out) == sorted(
        [
            (0, ("a", 4)),
            (1, ("b", 4)),
            (2, ("a", None)),
            (3, ("b", 4)),
        ]
    )

    # Disable bomb.
    armed.clear()
    out.clear()

    # Recover.
    run_main(flow, ib, ob, recovery_config=recovery_config)

    assert sorted(out) == sorted(
        [
            (3, ("b", 4)),
            (4, ("b", 5)),
            (5, ("a", 2)),  # Notice not 4.
        ]
    )
