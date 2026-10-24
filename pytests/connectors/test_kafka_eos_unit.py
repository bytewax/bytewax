"""Broker-free unit tests for `KafkaEosSink`.

Partition listing, part keys and their order, `part_fn`, the partition
count fence, the client sets shared per worker thread, and the producer
settings. Also the publication rules on a fake producer: a window is
produced at the snapshot after it closes, `close()` publishes only when
the execution completed, the build checks, and the once-per-execution
resume wait. The broker-backed checks live in `test_kafka_eos.py` and
`test_kafka_eos_scale.py`.

"""

import re
import threading
import time
from concurrent.futures import Future
from datetime import timedelta
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSinkMessage, eos
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig
from bytewax.testing import TestingSource, run_main
from confluent_kafka import KafkaError, KafkaException
from pytest import fixture, mark, raises

TOPIC_UNIT = "eos-partitions"
BROKER_UNIT = "b1:9092"


class _Metadata:
    """Metadata of a topic whose partitions have the given leaders."""

    def __init__(self, leaders, error=None):
        self.error = error
        self.partitions = {
            p: SimpleNamespace(leader=leader) for p, leader in enumerate(leaders)
        }


class _MetadataAdmin:
    """An admin client whose `list_topics` answers each call from `answers`.

    The last answer repeats once the list is spent.

    """

    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = 0

    def poll(self, timeout=None):
        pass

    def list_topics(self, topic, timeout=None):
        self.calls += 1
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        return SimpleNamespace(topics={topic: answer})


def _admin_listing(monkeypatch, count: int) -> Dict:
    """Make the sink's AdminClient report a topic of `count` partitions.

    `list_parts` runs inside a worker, so the recovery check it makes first is
    stubbed to pass.

    """
    captured: Dict = {}

    def admin(config):
        captured.update(config)
        return _MetadataAdmin(_Metadata([0] * count))

    monkeypatch.setattr(eos, "AdminClient", admin)
    monkeypatch.setattr(eos, "execution_state", lambda: (True, False, True))
    return captured


@mark.parametrize("count", [1, 2, 10, 120])
def test_list_parts_gives_one_key_per_partition(monkeypatch, count):
    _admin_listing(monkeypatch, count)
    keys = eos.KafkaEosSink(["b1:9092"], TOPIC_UNIT).list_parts()
    assert keys == [f"{TOPIC_UNIT}#{p:06d}" for p in range(count)]


@mark.parametrize("count", [11, 100])
def test_part_keys_sort_in_partition_order(monkeypatch, count):
    _admin_listing(monkeypatch, count)
    keys = eos.KafkaEosSink(["b1:9092"], TOPIC_UNIT).list_parts()
    assert keys == sorted(keys)


def test_list_parts_uses_the_add_config_settings(monkeypatch):
    captured = _admin_listing(monkeypatch, 1)
    sink = eos.KafkaEosSink(
        ["b1:9092", "b2:9092"], TOPIC_UNIT, {"security.protocol": "SSL"}
    )
    sink.list_parts()
    assert captured == {
        "security.protocol": "SSL",
        "bootstrap.servers": "b1:9092,b2:9092",
    }


@mark.parametrize("routing_key", ["", "a", "key-1", "ключ"])
def test_part_fn_is_the_crc32_of_the_routing_key(routing_key):
    """The engine wraps this value with `% count`, which gives the partition."""
    import zlib

    sink = eos.KafkaEosSink(["b1:9092"], TOPIC_UNIT)
    assert sink.part_fn(routing_key) == zlib.crc32(routing_key.encode("utf-8"))


def test_parse_part_reads_the_partition_index():
    assert eos._parse_part(TOPIC_UNIT, f"{TOPIC_UNIT}#000007") == 7


def test_partition_count_fence_names_both_counts():
    with raises(RuntimeError) as excinfo:
        eos._check_partition_count("store-1", TOPIC_UNIT, 2, 3)
    msg = str(excinfo.value)
    assert "with 2 partitions" in msg and "has 3 partitions now" in msg


def test_partition_count_waits_for_every_leader(monkeypatch):
    admin = _MetadataAdmin(_Metadata([0, -1]), _Metadata([0, 1]))
    monkeypatch.setattr(eos.time, "sleep", lambda _seconds: None)
    assert eos._partition_count(admin, BROKER_UNIT, TOPIC_UNIT) == 2
    assert admin.calls == 2


def test_partition_count_refuses_a_missing_topic():
    admin = _MetadataAdmin(_Metadata([], error="unavailable"))
    with raises(RuntimeError, match="is not available for KafkaEosSink"):
        eos._partition_count(admin, BROKER_UNIT, TOPIC_UNIT)


class _RaisingAdmin:
    """An admin client whose `list_topics` raises `error`."""

    def __init__(self, config, error):
        self.config = config
        self.error = error

    def poll(self, timeout=None):
        pass

    def list_topics(self, topic, timeout=None):
        raise self.error


def test_metadata_failure_is_wrapped_as_kafka_source_wraps_it(monkeypatch):
    """`KafkaException` from `list_topics` is a RuntimeError that names the topic."""
    err = KafkaException(KafkaError(KafkaError._TRANSPORT, "Failed to get metadata"))
    monkeypatch.setattr(eos, "AdminClient", lambda config: _RaisingAdmin(config, err))
    monkeypatch.setattr(eos, "execution_state", lambda: (True, False, True))
    expect = (
        f"could not fetch metadata for Kafka topic `'{TOPIC_UNIT}'` from brokers "
        f"`'{BROKER_UNIT}'` within 30 seconds: Failed to get metadata"
    )
    with raises(RuntimeError, match=re.escape(expect)) as excinfo:
        eos.KafkaEosSink([BROKER_UNIT], TOPIC_UNIT).list_parts()
    assert excinfo.value.__cause__ is err


def test_list_parts_names_the_brokers_of_add_config(monkeypatch):
    """`add_config` can set `bootstrap.servers`, as it can for `KafkaSink`."""
    err = KafkaException(KafkaError(KafkaError._TRANSPORT))
    monkeypatch.setattr(eos, "AdminClient", lambda config: _RaisingAdmin(config, err))
    monkeypatch.setattr(eos, "execution_state", lambda: (True, False, True))
    sink = eos.KafkaEosSink(
        ["ignored:9092"], TOPIC_UNIT, {"bootstrap.servers": "real:9092"}
    )
    with raises(RuntimeError, match=re.escape("from brokers `'real:9092'`")):
        sink.list_parts()


def test_brokers_given_as_one_string_are_refused():
    """A `str` is iterable, so `list` would split it into characters."""
    expect = "brokers must be an iterable and not a string"
    with raises(TypeError, match=expect):
        eos.KafkaEosSink("b1:9092", TOPIC_UNIT)


def test_list_parts_refuses_without_recovery_before_any_kafka_call(monkeypatch):
    """Without a recovery config `list_parts` refuses, before it asks Kafka."""

    def no_admin(config):
        msg = "list_parts contacted Kafka before the refusal"
        raise AssertionError(msg)

    monkeypatch.setattr(eos, "AdminClient", no_admin)
    monkeypatch.setattr(eos, "execution_state", lambda: (False, False, False))
    with raises(RuntimeError, match="KafkaEosSink needs a recovery config"):
        eos.KafkaEosSink(["localhost:1"], TOPIC_UNIT).list_parts()


def test_build_part_refuses_without_recovery(monkeypatch):
    monkeypatch.setattr(eos, "execution_state", lambda: (False, False, False))
    sink = eos.KafkaEosSink(["localhost:1"], TOPIC_UNIT)
    with raises(RuntimeError, match="needs a recovery config"):
        sink.build_part("step", f"{TOPIC_UNIT}#000000", None)


@mark.parametrize(
    "overrides, message",
    [
        ({"enable.idempotence": "false"}, "enable.idempotence"),
        ({"enable.idempotence": False}, "enable.idempotence"),
        ({"enable.gapless.guarantee": "false"}, "enable.gapless.guarantee"),
        ({"enable.gapless.guarantee": 0}, "enable.gapless.guarantee"),
        ({"message.timeout.ms": "60000"}, "message.timeout.ms"),
        ({"message.timeout.ms": 300000}, "message.timeout.ms"),
    ],
)
def test_producer_overrides_that_conflict_with_a_pinned_setting_are_refused(
    overrides, message
):
    with raises(ValueError, match=message):
        eos.KafkaEosSink(["b1:9092"], TOPIC_UNIT, overrides)


@mark.parametrize(
    "overrides",
    [
        {"enable.idempotence": "true"},
        {"enable.idempotence": True},
        {"enable.gapless.guarantee": True},
        {"message.timeout.ms": "0"},
        {"linger.ms": "50"},
    ],
)
def test_producer_overrides_that_agree_with_the_pins_are_accepted(overrides):
    eos.KafkaEosSink(["b1:9092"], TOPIC_UNIT, overrides)


class _Client:
    """Stands in for `Producer` or `Consumer`, and records each one it makes."""

    made: list = []

    def __init__(self, config):
        self.config = config
        self.closed = False
        self.purged = 0
        _Client.made.append(self)

    def close(self):
        self.closed = True

    def poll(self, timeout=None):
        pass

    def purge(self):
        self.purged += 1

    def flush(self, timeout=None):
        return 0


@fixture
def fake_clients(monkeypatch):
    """Replace the client classes with `_Client`, and check the registry is empty."""
    _Client.made = []
    monkeypatch.setattr(eos, "Producer", _Client)
    monkeypatch.setattr(eos, "Consumer", _Client)
    assert eos._clients == {}
    yield _Client.made
    assert eos._clients == {}, "a client set was not released"


def test_producer_pins_the_settings_the_order_depends_on(fake_clients):
    """The pins win over `add_config`, and a message never times out.

    A message that expires is outside the gapless guarantee, so a window with
    an expired message would leave a gap. Infinite `message.timeout.ms` keeps
    every message in the queue until it is delivered or the producer stops.

    """
    key, _clients = eos._acquire_clients([BROKER_UNIT], {"linger.ms": "5"})
    try:
        producer = fake_clients[0].config
        assert producer["enable.idempotence"] is True
        assert producer["enable.gapless.guarantee"] is True
        assert producer["message.timeout.ms"] == 0
        assert producer["linger.ms"] == "5"
    finally:
        eos._release_clients(key)


def _recording_clients(calls: List[Tuple[str, str]]):
    """Fake AdminClient, Producer and Consumer that log each call to `calls`."""

    class _Client:
        kind = ""

        def __init__(self, config):
            self.config = config
            calls.append((self.kind, "created"))

        def poll(self, timeout=None):
            calls.append((self.kind, "poll"))

        def close(self):
            calls.append((self.kind, "closed"))

    class _Admin(_Client):
        kind = "admin"

        def list_topics(self, topic, timeout=None):
            calls.append((self.kind, "list_topics"))
            return SimpleNamespace(topics={topic: _Metadata([0])})

        def describe_configs(self, resources):
            calls.append((self.kind, "describe_configs"))
            future: Future = Future()
            future.set_result({"cleanup.policy": SimpleNamespace(value="delete")})
            return {resource: future for resource in resources}

    class _Producer(_Client):
        kind = "producer"

    class _Reader(_Client):
        kind = "reader"

    return _Admin, _Producer, _Reader


def test_every_client_is_polled_before_its_first_request(monkeypatch):
    """Each client eos.py makes starts its auth callbacks before any request.

    `KafkaSource` polls its admin client once after creating it, the
    workaround for librdkafka #3753. Without the poll, a configured
    `oauth_cb` is not started, and a request waits out its timeout.

    """
    calls: List[Tuple[str, str]] = []
    admin, producer, reader = _recording_clients(calls)
    monkeypatch.setattr(eos, "AdminClient", admin)
    monkeypatch.setattr(eos, "Producer", producer)
    monkeypatch.setattr(eos, "Consumer", reader)
    monkeypatch.setattr(eos, "execution_state", lambda: (True, False, True))
    eos.KafkaEosSink([BROKER_UNIT], TOPIC_UNIT).list_parts()
    key, _clients = eos._acquire_clients([BROKER_UNIT], {})
    eos._release_clients(key)
    for kind in ("admin", "producer", "reader"):
        events = [method for who, method in calls if who == kind]
        assert events[:2] == ["created", "poll"], f"{kind}: {events}"


def test_add_config_can_set_bootstrap_servers_for_producer_and_reader(fake_clients):
    key, _clients = eos._acquire_clients(
        [BROKER_UNIT], {"bootstrap.servers": "b9:9092"}
    )
    try:
        producer, reader = fake_clients
        assert producer.config["bootstrap.servers"] == "b9:9092"
        assert reader.config["bootstrap.servers"] == "b9:9092"
    finally:
        eos._release_clients(key)


def test_partitions_on_one_thread_share_one_client_set(fake_clients):
    """Fifty partitions on one thread make two clients, not 100.

    The client set is released with its last partition, and not before.

    """
    acquired = [eos._acquire_clients([BROKER_UNIT], {}) for _ in range(50)]
    assert len(fake_clients) == 2
    assert all(clients is acquired[0][1] for _key, clients in acquired)
    for key, _clients in acquired[:-1]:
        eos._release_clients(key)
    assert not any(client.closed for client in fake_clients[1:])
    eos._release_clients(acquired[-1][0])
    assert [client.closed for client in fake_clients] == [False, True]


def test_another_thread_gets_its_own_client_set(fake_clients):
    """Delivery callbacks run on the thread that polls, so threads never share."""
    results = []
    worker = threading.Thread(
        target=lambda: results.append(eos._acquire_clients([BROKER_UNIT], {}))
    )
    worker.start()
    worker.join()
    key, clients = eos._acquire_clients([BROKER_UNIT], {})
    assert results[0][1] is not clients
    assert len(fake_clients) == 4
    eos._release_clients(results[0][0])
    eos._release_clients(key)


def test_different_settings_get_different_client_sets(fake_clients):
    first = eos._acquire_clients([BROKER_UNIT], {"linger.ms": "5"})
    second = eos._acquire_clients([BROKER_UNIT], {"linger.ms": "6"})
    assert first[1] is not second[1]
    eos._release_clients(first[0])
    eos._release_clients(second[0])


def test_failed_publish_leaves_the_shared_producer_alone(monkeypatch):
    """A publish that raises mid-window does not purge the shared producer.

    The producer is shared with the other partitions of the thread. A purge
    drops their queued messages too, and their delivery reports then fail.
    The failure raises, and the producer stops on its own, with gapless
    delivery, so nothing after the failure is sent.

    """
    part = eos._EosPartition.__new__(eos._EosPartition)
    part._producer = _Client({})
    part._topic = TOPIC_UNIT
    part._partition = 0

    def produce(self, msg, window, seq):
        if seq == 1:
            msg_err = "produce refused"
            raise RuntimeError(msg_err)

    monkeypatch.setattr(eos._EosPartition, "_produce", produce)
    window = eos._Window(
        0,
        [
            KafkaSinkMessage(key=b"a", value=b"1"),
            KafkaSinkMessage(key=b"b", value=b"2"),
        ],
    )
    with raises(RuntimeError, match="produce refused"):
        part._publish(window)
    assert part._producer.purged == 0
    assert not window.produced


def test_bad_crash_hook_value_leaves_no_client_set(fake_clients, monkeypatch):
    """A crash hook that does not parse fails before any client is made.

    Clients made first would stay in the registry, and the reader would never
    close, because the partition that owns them never reaches its `try`.

    """
    monkeypatch.setenv(eos._CRASH_ENV, "soon")
    with raises(ValueError):
        eos._EosPartition([BROKER_UNIT], TOPIC_UNIT, {}, None)
    assert fake_clients == [], "clients were made before the crash hook was read"


class _AsyncProducer:
    """A producer whose delivery reports arrive only when the test releases them.

    `produce` queues one report per message, with offsets in produce order.
    `release(n)` lets the next `n` reports be delivered, and `poll` or `flush`
    delivers them, as librdkafka does. `flush` delivers everything queued.
    Offsets in `failing` are reported as delivery errors.

    """

    def __init__(self, config):
        self.config = config
        self.queued = []
        self.ready = 0
        self.next_offset = 0
        self.failing = set()
        self.polls = []
        self.flushes = 0
        self.purged = 0

    def produce(self, *, topic, partition, value, key, headers, timestamp, on_delivery):
        self.queued.append((on_delivery, self.next_offset))
        self.next_offset += 1

    def release(self, count):
        self.ready += count

    def poll(self, timeout=None):
        self.polls.append(timeout)
        self._deliver(self.ready)

    def flush(self, timeout=None):
        self.flushes += 1
        self._deliver(len(self.queued))
        return len(self.queued)

    def purge(self):
        self.purged += 1

    def _deliver(self, count):
        for _ in range(min(count, len(self.queued))):
            on_delivery, offset = self.queued.pop(0)
            self.ready = max(0, self.ready - 1)
            if offset in self.failing:
                on_delivery("injected: delivery failed", _Report(-1))
            else:
                on_delivery(None, _Report(offset))


class _Report:
    """A delivery report's message, which carries only the offset here."""

    def __init__(self, offset):
        self._offset = offset

    def offset(self):
        return self._offset


class _Reader(_Client):
    """A reader of a topic whose watermarks are set per test."""

    low = 0
    high = 0

    def get_watermark_offsets(self, partition, timeout=None, cached=False):
        return self.low, self.high

    def assign(self, partitions):
        pass

    def unassign(self):
        pass


@fixture
def async_partition(monkeypatch):
    """Build real sink partitions on a fake producer, with no broker.

    The constructor runs as in production. The broker reads are stubbed:
    the topic's watermarks come from `async_partition.topic` and the
    restore reads find no records. Set `execution_state` in a test to
    decide what `close()` sees. Every partition built is closed.

    """
    monkeypatch.setattr(eos, "Producer", _AsyncProducer)
    monkeypatch.setattr(eos, "Consumer", _Reader)
    monkeypatch.setattr(eos, "_check_not_compacted", lambda *_args: None)
    monkeypatch.setattr(eos, "_partition_count", lambda *_args: 1)
    monkeypatch.setattr(eos, "_read_records", lambda *_args: [])
    monkeypatch.setattr(eos, "execution_state", lambda: (True, False, True))
    parts = []

    class _Topic:
        low = 0
        high = 0

    def build(resume_state=None, resume_grace=None):
        _Reader.low = _Topic.low
        _Reader.high = _Topic.high
        part = eos._EosPartition(
            [BROKER_UNIT], TOPIC_UNIT, {}, resume_state, resume_grace
        )
        parts.append(part)
        return part

    build.topic = _Topic
    yield build
    for part in parts:
        part._release()
    assert eos._clients == {}, "a client set was not released"


def _items(*values):
    return [KafkaSinkMessage(key=b"k", value=value) for value in values]


def _window_numbers(state):
    return [number for number, _messages in state[3]]


def test_window_is_produced_at_the_snapshot_after_it_closes(async_partition):
    """A window closed by one snapshot is produced by the next, not by itself.

    Each snapshot is taken for a later boundary than the one before, so the
    window that the previous snapshot closed is durable when this one runs.
    Nothing is flushed on the way.

    """
    part = async_partition()
    producer = part._producer
    part.write_batch(_items(b"a", b"b"))
    state = part.snapshot()
    assert producer.next_offset == 0, "window 0 was produced in its own snapshot"
    assert _window_numbers(state) == [0]

    part.write_batch(_items(b"c"))
    state = part.snapshot()
    assert producer.next_offset == 2, "window 0 was not produced one snapshot later"
    assert _window_numbers(state) == [0, 1]

    part.write_batch(_items(b"d"))
    state = part.snapshot()
    assert producer.next_offset == 3
    assert _window_numbers(state) == [0, 1, 2]
    assert producer.flushes == 0
    assert producer.polls[-1] == 0


def test_window_leaves_the_state_only_once_confirmed(async_partition):
    """A snapshot never flushes. Windows leave the state only once confirmed."""
    part = async_partition()
    producer = part._producer
    part.write_batch(_items(b"a", b"b"))
    part.snapshot()
    part.write_batch(_items(b"c"))
    part.snapshot()
    state = part.snapshot()
    assert _window_numbers(state) == [0, 1, 2], "in-flight windows left the state"

    producer.release(1)
    state = part.snapshot()
    assert _window_numbers(state) == [0, 1, 2, 3], "window 0 left with one report"

    producer.release(1)
    state = part.snapshot()
    assert _window_numbers(state) == [1, 2, 3, 4], "confirmed window 0 stayed"
    assert producer.flushes == 0


def test_failed_delivery_fails_the_next_snapshot(async_partition):
    """A failed delivery raises at the next snapshot, and sends nothing after."""
    part = async_partition()
    producer = part._producer
    producer.failing = {0}
    part.write_batch(_items(b"a"))
    part.snapshot()
    part.write_batch(_items(b"b"))
    part.snapshot()  # produces window 0.
    producer.release(1)  # its failed report arrives on the next poll.
    with raises(RuntimeError, match="could not deliver"):
        part.snapshot()
    assert producer.purged == 0
    with raises(RuntimeError, match="already failed"):
        part.snapshot()


def test_restored_window_the_topic_lacks_is_produced_and_kept_until_confirmed(
    async_partition,
):
    """A restored window is produced at build, and kept until its reports arrive."""
    state = (1, "store", 2, [(0, _items(b"a0")), (1, _items(b"b0"))], 0, 1)
    part = async_partition(resume_state=state)
    producer = part._producer
    assert producer.next_offset == 2
    assert _window_numbers(part.snapshot()) == [0, 1, 2]
    producer.release(2)
    assert _window_numbers(part.snapshot()) == [2, 3]


def test_state_end_does_not_pass_a_partly_confirmed_window(async_partition):
    """`state_end` stops at a window with an unconfirmed message."""
    part = async_partition()
    producer = part._producer
    part.write_batch(_items(b"a", b"b"))
    part.snapshot()
    part.write_batch(_items(b"c"))
    part.snapshot()  # window 0 is produced, both reports pending.
    producer.release(1)
    part.snapshot()  # one report of window 0 harvested.
    state = part.snapshot()
    assert state[4] == 0, "state_end moved past a window still in flight"


def test_completed_close_publishes_and_flushes_pending_windows(
    async_partition, monkeypatch
):
    """A completed execution's `close()` produces the window still in the state."""
    monkeypatch.setattr(eos, "execution_state", lambda: (True, True, True))
    part = async_partition()
    producer = part._producer
    part.write_batch(_items(b"a", b"b"))
    part.snapshot()
    assert producer.next_offset == 0
    part.close()
    assert producer.next_offset == 2
    assert producer.flushes == 1


def test_close_of_an_incomplete_execution_publishes_nothing(async_partition):
    """Without completion the pending window stays in the state, but is flushed.

    The messages already produced are flushed, so they land before the
    process moves on. No window is produced by `close()` itself.

    """
    part = async_partition()
    producer = part._producer
    part.write_batch(_items(b"a", b"b"))
    part.snapshot()
    part.snapshot()
    produced = producer.next_offset
    part.close()
    assert producer.next_offset == produced
    assert producer.flushes == 1
    assert producer.queued == []


def test_close_raises_a_known_delivery_failure_before_publishing(
    async_partition, monkeypatch
):
    """A completed `close()` harvests the reports first, and publishes nothing new.

    Window 0 is produced, and its failed report is waiting to be polled. The
    close must see the failure before it produces window 1, and raise.

    """
    monkeypatch.setattr(eos, "execution_state", lambda: (True, True, True))
    part = async_partition()
    producer = part._producer
    producer.failing = {0}
    part.write_batch(_items(b"a"))
    part.snapshot()
    part.write_batch(_items(b"b"))
    part.snapshot()  # produces window 0, and closes window 1.
    producer.release(1)
    produced = producer.next_offset
    with raises(RuntimeError, match="could not deliver"):
        part.close()
    assert producer.next_offset == produced, "a window was produced after a failure"


def test_completed_close_raises_when_the_final_flush_fails(
    async_partition, monkeypatch
):
    """A completed run whose final flush fails raises from `close()`.

    The run must fail visibly, so its caller gets the error. The failure is
    not only logged.

    """
    monkeypatch.setattr(eos, "execution_state", lambda: (True, True, True))
    part = async_partition()
    producer = part._producer
    producer.failing = {0}
    part.write_batch(_items(b"a"))
    part.snapshot()
    part.write_batch(_items(b"b"))
    part.snapshot()  # window 0 is produced, and its report fails.
    with raises(RuntimeError, match="could not deliver"):
        part.close()


def test_completed_close_without_a_recovery_lock_leaves_windows_for_the_next_execution(
    async_partition, monkeypatch, caplog
):
    """A process with no recovery lock publishes nothing at close, and warns.

    A successor may start once the lock holders have returned, while this
    process could still be publishing. The windows stay in the state for the
    next execution, and the warning names the fix.

    """
    monkeypatch.setattr(eos, "execution_state", lambda: (True, True, False))
    part = async_partition()
    producer = part._producer
    part.write_batch(_items(b"a", b"b"))
    part.snapshot()
    part.snapshot()  # window 0 is produced, and closed window 1 waits in the state.
    produced = producer.next_offset
    part.close()
    assert producer.next_offset == produced
    assert producer.flushes == 0
    assert any(not window.produced for window in part._windows)
    assert "at least as many recovery partitions as processes" in caplog.text


def test_close_after_a_failed_snapshot_does_not_flush(async_partition):
    """A failed sink has already purged its producer, so `close()` leaves it be."""
    part = async_partition()
    producer = part._producer
    producer.failing = {0}
    part.write_batch(_items(b"a"))
    part.snapshot()
    part.write_batch(_items(b"b"))
    part.snapshot()
    producer.release(1)
    with raises(RuntimeError, match="could not deliver"):
        part.snapshot()
    flushes = producer.flushes
    part.close()
    assert producer.flushes == flushes


def test_fresh_store_refuses_a_topic_that_holds_messages(async_partition):
    async_partition.topic.low = 0
    async_partition.topic.high = 3
    with raises(RuntimeError, match="already holds 3 message"):
        async_partition()


def test_fresh_store_accepts_an_empty_topic(async_partition):
    async_partition.topic.low = 7
    async_partition.topic.high = 7
    part = async_partition()
    assert part._state_end == 7


def test_restored_store_refuses_a_topic_shorter_than_its_end(async_partition):
    state = (1, "store", 1, [(0, _items(b"a"))], 5, 1)
    with raises(RuntimeError, match="ends at offset 0, before the offset 5"):
        async_partition(resume_state=state)


def test_restored_store_with_no_windows_still_reads_the_topic(
    async_partition, monkeypatch
):
    """A restored store with no windows still checks the records after its end.

    A record of another store there must refuse the build.
    """
    foreign = eos._Eos(window=0, seq=0, store="someone-else", offset=4)
    monkeypatch.setattr(eos, "_read_records", lambda *_args: [foreign])
    async_partition.topic.high = 5
    state = (1, "store", 1, [], 0, 1)
    with raises(RuntimeError, match="written by eos-store someone-else"):
        async_partition(resume_state=state)


def _refuse_before_any_client(monkeypatch):
    """Fail if a partition makes a client before it refuses its restored state."""

    def clients(*_args):
        msg = "a client was made before the restored state was refused"
        raise AssertionError(msg)

    monkeypatch.setattr(eos, "_acquire_clients", clients)


def test_snapshot_is_a_version_one_state(async_partition):
    part = async_partition()
    part.write_batch(_items(b"a"))
    state = part.snapshot()
    assert state[0] == 1, "the snapshot does not carry format version 1"
    assert len(state) == 6


@mark.parametrize("version", [0, 2])
def test_restored_state_of_another_version_is_refused(
    async_partition, monkeypatch, version
):
    """A state of another format is refused, and the versions are named."""
    _refuse_before_any_client(monkeypatch)
    state = (version, "store", 1, [(0, _items(b"a0"))], 0, 1)
    with raises(RuntimeError) as excinfo:
        async_partition(resume_state=state)
    msg = str(excinfo.value)
    assert "KafkaEosSink cannot resume partition 0 of `eos-partitions`" in msg
    assert f"has version {version} and 6 fields" in msg
    assert "reads only version 1 states, which have 6 fields" in msg
    assert "written by a different bytewax version" in msg


def test_restored_unversioned_state_is_refused(async_partition, monkeypatch):
    """A state from before the version field has five fields and no version.

    Its fields cannot be told apart from those of a later format, so it is
    refused by its shape and not read.

    """
    _refuse_before_any_client(monkeypatch)
    legacy = ("store", 1, [(0, _items(b"a0"))], 0, 1)
    with raises(RuntimeError) as excinfo:
        async_partition(resume_state=legacy)
    msg = str(excinfo.value)
    assert "KafkaEosSink cannot resume partition 0 of `eos-partitions`" in msg
    assert "has no version and 5 fields" in msg
    assert "reads only version 1 states, which have 6 fields" in msg
    assert "written by a different bytewax version" in msg


class _SinkAdmin:
    """An admin client for `list_parts`, whose `describe_configs` counts its calls."""

    def __init__(self, config, policy="delete"):
        self.config = config
        self.policy = policy
        self.describes = 0

    def poll(self, timeout=None):
        pass

    def describe_configs(self, resources):
        self.describes += 1
        future: Future = Future()
        future.set_result({"cleanup.policy": SimpleNamespace(value=self.policy)})
        return {resource: future for resource in resources}


@mark.parametrize("partitions", [2, 5])
def test_compaction_is_checked_per_listing_not_per_partition(monkeypatch, partitions):
    """Listing the parts and building each of them reads the policy once."""
    admins: List[_SinkAdmin] = []

    def admin(config):
        made = _SinkAdmin(config)
        admins.append(made)
        return made

    monkeypatch.setattr(eos, "AdminClient", admin)
    monkeypatch.setattr(eos, "Producer", _AsyncProducer)
    monkeypatch.setattr(eos, "Consumer", _Reader)
    monkeypatch.setattr(eos, "_partition_count", lambda *_args: partitions)
    monkeypatch.setattr(eos, "_read_records", lambda *_args: [])
    monkeypatch.setattr(eos, "execution_state", lambda: (True, False, True))
    monkeypatch.setattr(_Reader, "low", 0)
    monkeypatch.setattr(_Reader, "high", 0)
    sink = eos.KafkaEosSink([BROKER_UNIT], TOPIC_UNIT)
    parts = [sink.build_part("step", key, None) for key in sink.list_parts()]
    assert len(parts) == partitions
    assert sum(made.describes for made in admins) == 1
    for part in parts:
        part.close()
    # The next execution lists the parts again and checks again.
    sink.list_parts()
    assert sum(made.describes for made in admins) == 2


def test_compacted_topic_is_refused_by_each_list_parts_until_it_passes(monkeypatch):
    """A refused check is not remembered, so the next listing refuses again."""
    admins: List[_SinkAdmin] = []

    def admin(config):
        made = _SinkAdmin(config, policy="compact")
        admins.append(made)
        return made

    monkeypatch.setattr(eos, "AdminClient", admin)
    monkeypatch.setattr(eos, "execution_state", lambda: (True, False, True))
    sink = eos.KafkaEosSink([BROKER_UNIT], TOPIC_UNIT)
    for _attempt in range(2):
        with raises(RuntimeError, match="includes compact"):
            sink.list_parts()
    assert sum(made.describes for made in admins) == 2


class _Clock:
    """A monotonic clock that `time.sleep` advances, so waits take no real time."""

    def __init__(self):
        self.now = 0.0
        self.sleeps: List[float] = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def test_resume_grace_is_paid_once_per_execution(async_partition, monkeypatch):
    """Restored partitions of one execution share one wait, whatever their stores.

    Each partition has its own store id, and the wait is still shared by the
    execution. A later execution waits again.

    """
    clock = _Clock()
    monkeypatch.setattr(eos, "time", clock)
    grace = timedelta(seconds=4)
    first_state = (1, "store-a", 1, [(0, _items(b"a"))], 0, 1)
    second_state = (1, "store-b", 1, [(0, _items(b"b"))], 0, 1)

    first = async_partition(resume_state=first_state, resume_grace=grace)
    second = async_partition(resume_state=second_state, resume_grace=grace)
    assert clock.sleeps == [4.0], "the second partition waited again"

    first.close()
    second.close()
    async_partition(resume_state=first_state, resume_grace=grace)
    assert clock.sleeps == [4.0, 4.0], "a later execution did not wait"


class _Msg:
    """A message as the reader sees it: its topic, partition, offset and headers."""

    def __init__(self, offset, partition=0, topic=TOPIC_UNIT):
        self._offset = offset
        self._partition = partition
        self._topic = topic

    def error(self):
        return None

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def topic(self):
        return self._topic

    def headers(self):
        return [
            ("eos-store", b"store"),
            ("eos-window", b"0"),
            ("eos-seq", str(self._offset).encode()),
        ]


class _ScriptedReader:
    """A reader that hands out its messages one per `consume`, after `pause`.

    The pause is taken on `clock`, so a read takes fake time and no real time.
    """

    def __init__(self, messages, pause, clock):
        self._messages = list(messages)
        self._pause = pause
        self._clock = clock

    def assign(self, partitions):
        pass

    def unassign(self):
        pass

    def consume(self, count, timeout):
        self._clock.sleep(self._pause)
        return self._messages[:1] and [self._messages.pop(0)]


def test_reader_waits_as_long_as_records_keep_arriving(monkeypatch):
    """The reader's deadline is for a stall, not for the whole read.

    The read takes longer than the deadline, but a record arrives every
    fraction of it, so nothing stalls and the read completes. The clock is
    fake, so each record arrives at an exact time.

    """
    clock = _Clock()
    monkeypatch.setattr(eos, "time", clock)
    monkeypatch.setattr(eos, "_TAIL_TIMEOUT_S", 0.3)
    reader = _ScriptedReader(
        [_Msg(offset) for offset in range(6)], pause=0.1, clock=clock
    )
    records = eos._read_records(reader, TOPIC_UNIT, 0, 0, 6)
    assert [rec.offset for rec in records] == list(range(6))
    assert clock.now > eos._TAIL_TIMEOUT_S, "the read did not outlast its deadline"


def test_reader_ignores_messages_of_another_partition():
    """A message of another partition, seen through a shared reader, is no record.

    The reader is reassigned to one partition at a time, so a message of
    another partition must not be counted, even with valid headers.

    """
    reader = _ScriptedReader(
        [_Msg(0), _Msg(1, partition=1), _Msg(2)], pause=0.0, clock=_Clock()
    )
    records = eos._read_records(reader, TOPIC_UNIT, 0, 0, 3)
    assert [rec.offset for rec in records] == [0, 2]


def test_produce_gives_up_when_the_producer_queue_stays_full(monkeypatch):
    """A queue that stays full fails the run, rather than waiting forever.

    With infinite `message.timeout.ms`, nothing leaves the queue while the
    broker is down, so the wait for room is bounded by the flush timeout.

    """
    monkeypatch.setattr(eos, "_FLUSH_TIMEOUT_S", 0.2)

    class _Stop(Exception):
        pass

    class _Full:
        polls = 0

        def produce(self, **_kwargs):
            raise BufferError

        def poll(self, timeout=None):
            _Full.polls += 1
            time.sleep(0.01)
            if _Full.polls > 1000:
                raise _Stop

    part = eos._EosPartition.__new__(eos._EosPartition)
    part._producer = _Full()
    part._topic = TOPIC_UNIT
    part._partition = 0
    part._store_id = "store"
    part._crash_after = None
    part._produced = 0
    window = eos._Window(0, [KafkaSinkMessage(key=b"a", value=b"1")])
    with raises(RuntimeError, match="stayed full"):
        part._produce(window.messages[0], window, 0)


def test_fresh_store_does_not_wait_for_resume_grace(async_partition, monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(eos, "time", clock)
    async_partition(resume_grace=timedelta(seconds=4))
    assert clock.sleeps == []


def _run_sink_flow(sink, recovery: Optional[RecoveryConfig]) -> None:
    flow = Dataflow("eos_unit")
    s = op.input("inp", flow, TestingSource([1, 2, 3]))
    keyed = op.map(
        "key", s, lambda x: ("route", KafkaSinkMessage(key=b"k", value=b"v"))
    )
    op.output("out", keyed, sink)
    if recovery is None:
        run_main(flow)
    else:
        run_main(flow, recovery_config=recovery)


def test_run_without_recovery_refuses_the_sink_at_build():
    """A real run without a recovery config fails before it asks Kafka anything.

    The broker is not running, so any Kafka call would fail with a different
    error.

    """
    sink = eos.KafkaEosSink(["localhost:1"], TOPIC_UNIT)
    with raises(RuntimeError) as excinfo:
        _run_sink_flow(sink, None)
    cause = excinfo.value
    while cause.__cause__ is not None:
        cause = cause.__cause__
    assert "KafkaEosSink needs a recovery config" in str(cause)


class _SilentProducer(_AsyncProducer):
    """A producer that never confirms a delivery.

    `flush` waits out its timeout and returns what is still queued.

    """

    def flush(self, timeout=None):
        self.flushes += 1
        time.sleep(timeout or 0)
        return len(self.queued)


def test_closes_on_one_producer_wait_one_flush_timeout_in_all(
    async_partition, monkeypatch
):
    # Three partitions share one producer on this thread, and none of their
    # messages is ever delivered. Each completed close fails, but the three
    # together wait one flush timeout, not one each.
    monkeypatch.setattr(eos, "Producer", _SilentProducer)
    monkeypatch.setattr(eos, "_FLUSH_TIMEOUT_S", 0.5)
    monkeypatch.setattr(eos, "execution_state", lambda: (True, True, True))
    parts = [async_partition() for _ in range(3)]
    for part in parts:
        part.write_batch(_items(b"a"))
        part.snapshot()

    start = time.monotonic()
    for part in parts:
        with raises(RuntimeError, match="could not deliver"):
            part.close()
    elapsed = time.monotonic() - start

    # One timeout is 0.5 s. Three timeouts would take 1.5 s.
    assert elapsed >= 0.45, f"three closes took {elapsed:.2f} s"
    assert elapsed < 0.9, f"three closes took {elapsed:.2f} s; want one timeout"


class _FlakyWatermarks:
    """A reader whose first `failures` watermark queries fail with `code`."""

    def __init__(self, failures, code):
        self.calls = 0
        self._failures = failures
        self._code = code

    def get_watermark_offsets(self, partition, timeout=None, cached=False):
        self.calls += 1
        if self.calls <= self._failures:
            raise KafkaException(KafkaError(self._code))
        return 0, 7


def test_watermark_query_retries_a_topic_without_a_leader_yet(monkeypatch):
    # Right after creation, a broker can list a topic before its leader
    # applies the creation: the query fails until the leader catches up.
    monkeypatch.setattr(eos.time, "sleep", lambda _seconds: None)
    reader = _FlakyWatermarks(2, KafkaError.UNKNOWN_TOPIC_OR_PART)

    assert eos._watermarks(reader, "topic", 0) == (0, 7)
    assert reader.calls == 3


def test_watermark_query_raises_other_errors_at_once(monkeypatch):
    monkeypatch.setattr(eos.time, "sleep", lambda _seconds: None)
    reader = _FlakyWatermarks(2, KafkaError._TRANSPORT)

    with raises(KafkaException):
        eos._watermarks(reader, "topic", 0)
    assert reader.calls == 1


def test_watermark_query_gives_up_after_the_leader_wait(monkeypatch):
    monkeypatch.setattr(eos, "_TAIL_TIMEOUT_S", 0.0)
    monkeypatch.setattr(eos.time, "sleep", lambda _seconds: None)
    reader = _FlakyWatermarks(10, KafkaError.UNKNOWN_TOPIC_OR_PART)

    with raises(KafkaException):
        eos._watermarks(reader, "topic", 0)
    assert reader.calls == 1
