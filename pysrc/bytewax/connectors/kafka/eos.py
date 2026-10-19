"""Exactly-once Kafka output without broker transactions.

{py:obj}`KafkaEosSink` writes every pipeline result to a Kafka topic
exactly once, across crashes, drains and resumes, without Kafka
transactions. It needs a recovery config, and the recovery store does
the work: a window of results is produced only after the recovery
snapshot that holds it is durable.

How it works:

- Items are buffered in the state of their sink partition. Each
  snapshot closes the buffer into a numbered window, and the state keeps
  the window until it is published and confirmed.

- A snapshot first produces the window that the previous snapshot
  closed. The engine takes a snapshot for a later boundary only once
  every recovery write for the earlier boundaries is durable, so that
  window is durable. Publication therefore trails one snapshot behind,
  and at most one window waits unpublished in the state.

- Produces do not wait for the broker. A later snapshot takes the
  delivery reports that have arrived, and drops a window once every one
  of its messages is confirmed. A delivery failure raises at the next
  snapshot, and the execution fails.

- Each message carries `eos-window`, `eos-seq` and `eos-store` headers:
  its window, its index in the window, and the store that wrote it. The
  state records `end`, the offset of the first message of its oldest
  window. One idempotent producer writes the topic and nothing else
  removes messages from its middle, so the windows in the state are one
  run of messages, from `end` on, in offset order.

- A build that restores state reads the topic from `end` to its end, and
  checks each record against its place in that run: its window, seq and
  store must match. The topic holds a prefix of the run, so the messages
  it lacks are produced. Retention removes the head of the topic, and a
  message it removed was delivered before the rest, so it is not produced
  again. A fresh build refuses a topic that already holds messages,
  because its store did not write them.

- When the execution completes, `close()` produces and flushes the
  windows that are still pending, after every recovery write is durable.
  A completed run whose flush fails raises. If the execution does not
  complete, the next execution on the same recovery store produces them
  from its restored state.

Partitions:

- Each partition of the output topic is written by one sink partition.
  Its windows, headers and checks are those of a one-partition topic,
  so the guarantee holds per partition. The dedup key is (partition,
  `eos-window`, `eos-seq`).

- An item is routed by its routing key, the first element of its
  `(key, KafkaSinkMessage)` tuple. It goes to partition
  `crc32(routing key bytes) % partitions`, which is the placement of
  librdkafka's `consistent` partitioner. The Kafka message key does not
  choose the partition. Set it to the routing key's bytes when other
  producers or consumers rely on key placement.

- There is no atomic visibility across partitions. A consumer may see a
  window of one partition before a window of another. Each partition
  resumes on its own, so every result still appears once.

- A partition that receives no items is not snapshotted, so its last
  window waits for its next items, for a completed execution, or for
  the next execution on the same recovery store.

- The partition count is part of the store. A store refuses to resume
  once the topic has a different number of partitions, and the refusal
  names both counts.

Limitations:

- Requires a recovery config, and refuses to build without one. A
  cluster of several processes is supported when they share one
  recovery directory on storage attached to one host. A process that
  holds no recovery partition lock publishes nothing at close, and logs
  a warning: run with at least as many recovery partitions as processes.
  A second execution on the same recovery store cannot start while any
  worker of the first holds one of the store's locks, so one store has
  one live execution.

- Only this sink, with one recovery store, may write to the topic. Kafka
  does not fence a second writer. A second store refuses the topic at
  its first build, because the topic holds messages it did not write.

- The topic must not be compacted. The sink refuses, when it lists the
  topic's partitions, a topic whose `cleanup.policy` includes `compact`,
  if the policy can be read.

- `enable.idempotence`, `enable.gapless.guarantee` and `message.timeout.ms`
  are fixed at `true`, `true` and `0`, since the publication order and the
  gapless guarantee depend on them. A different `add_config` value is
  refused at construction. Other producer settings can be overridden.
  librdkafka refuses `max.in.flight.requests.per.connection` above 5 under
  idempotence, when the producer is created. Without a message timeout a
  message waits for the broker, so a broker that stays down fails the run
  when the producer queue stays full or when the flush at close times out.

- A delivery failure raises and fails the execution. A failure during
  `close()` raises when the execution completed, and is logged otherwise,
  and the next execution publishes the windows.

- `resume_grace` covers records that a killed execution already handed
  to the network: they can land after a restart has read the topic, and
  without the wait they would duplicate a window.

- Publication waits one snapshot after a window closes, so output lags by
  up to one snapshot interval. On an idle stream the last window waits
  for the next item, the completed close, or the next execution.

"""

import logging
import os
import threading
import time
import uuid
import zlib
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from bytewax._bytewax import execution_state
from bytewax.connectors.kafka import (
    KafkaSinkMessage,
    _start_auth_callbacks,
    _topic_metadata,
)
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, ConfigResource

WINDOW_HEADER = "eos-window"
SEQ_HEADER = "eos-seq"
STORE_HEADER = "eos-store"
_EOS_HEADERS = (WINDOW_HEADER, SEQ_HEADER, STORE_HEADER)

# How long a read of the topic may go without a record, and how long a
# partition may take to get a leader. A read that keeps receiving records
# is not timed out.
_TAIL_TIMEOUT_S = 30.0
# Pause between metadata reads while a partition still has no leader.
_LEADER_POLL_S = 0.1
# Messages requested per poll while reading the topic.
_TAIL_BATCH = 1024
# How long one poll of the reader waits. `consume` returns at this timeout
# even when the partition end has already been reported, so it bounds the
# wait at the end of a read.
_READ_POLL_S = 0.05
# The broker holds a fetch at the end of a partition for `fetch.wait.max.ms`,
# 500 ms by default, before it reports the partition end. Every read that
# reaches the end waited that long, so the reader defaults to 10 ms. The
# reader reads only, and `add_config` can still override the default.
_READER_DEFAULTS = {"fetch.wait.max.ms": "10"}
_FLUSH_TIMEOUT_S = 60.0
_CRASH_ENV = "BYTEWAX_EOS_CRASH_AFTER"

# Producer settings that the sink's guarantees depend on. They are applied
# after `add_config`, and `_check_producer_config` refuses an `add_config`
# value that differs from them. librdkafka itself refuses
# `max.in.flight.requests.per.connection` above 5 under idempotence.
_PINNED_PRODUCER = {
    # Each message carries a sequence number, so a retry cannot duplicate it.
    "enable.idempotence": True,
    # A failed batch stops the producer instead of leaving a gap in the
    # sequence of messages. The guarantee does not cover a message that
    # expires, so messages do not expire.
    "enable.gapless.guarantee": True,
    "message.timeout.ms": 0,
}

_logger = logging.getLogger(__name__)


def _check_producer_config(add_config: Dict) -> None:
    """Refuse an `add_config` value that differs from a pinned setting.

    The pinned settings win over `add_config`, so a value that differs
    would be silently replaced. The comparison is on text, so a boolean
    `True` agrees with `"true"`.

    """
    for name, pinned in _PINNED_PRODUCER.items():
        if name not in add_config:
            continue
        value = str(add_config[name]).strip().lower()
        if value != str(pinned).lower():
            err = (
                f"KafkaEosSink needs `{name}={str(pinned).lower()}` to publish its "
                f"windows in order, but add_config sets `{name}={add_config[name]}`. "
                "Remove that setting."
            )
            raise ValueError(err)


def _require_recovery() -> None:
    """Refuse to run in an execution without a recovery config.

    Windows are published only after a later snapshot, and a snapshot is
    taken only with recovery. Without it nothing would ever be published.

    """
    recovering, _completed, _holds_lock = execution_state()
    if not recovering:
        err = (
            "KafkaEosSink needs a recovery config: it publishes each window only "
            "once the recovery store has made it durable, and an execution without "
            "recovery never does that. Pass `recovery_config` to the run."
        )
        raise RuntimeError(err)


@dataclass
class _Window:
    """A window of messages, from its close until every message is confirmed.

    `produced` is set once every message has been handed to the producer.
    `unconfirmed` counts the messages whose delivery report has not
    arrived.

    """

    number: int
    messages: List[KafkaSinkMessage]
    produced: bool = False
    unconfirmed: int = 0


# Recovery state, pickled into the snapshot. Its first element is the format
# version, so a state of another format is refused and never read by its
# shape. The rest are: the store's id, the next window number, every window
# still in the state, oldest first, `state_end`, the offset of the first
# message of the oldest window, and the topic's partition count.
_STATE_VERSION = 1
_STATE_FIELDS = 6
_Windows = List[Tuple[int, List[KafkaSinkMessage]]]
_State = Tuple[int, str, int, _Windows, int, int]


@dataclass(frozen=True)
class _Eos:
    """The EOS headers of one message, and its offset."""

    window: int
    seq: int
    store: str
    offset: int


def _eos_headers(topic: str, msg: Message) -> _Eos:
    """The EOS headers of a message, which every message must carry."""
    found: Dict[str, bytes] = {}
    for key, value in msg.headers() or []:
        found[key] = value or b""
    if any(name not in found for name in _EOS_HEADERS):
        err = (
            f"Kafka topic `{topic}` holds records not written by "
            "KafkaEosSink; a message has no eos headers"
        )
        raise RuntimeError(err)
    return _Eos(
        window=int(found[WINDOW_HEADER]),
        seq=int(found[SEQ_HEADER]),
        store=found[STORE_HEADER].decode(),
        offset=msg.offset(),
    )


def _check_not_compacted(admin: AdminClient, topic: str) -> None:
    """Refuse a compacted topic. A config that cannot be read only logs.

    Compaction removes superseded messages from the middle of a window,
    which the resume check would read as a hole. The policy is read
    through `admin`, the sink's AdminClient. A broker or ACL that denies
    the read leaves the check out rather than failing the sink, so the
    failure is logged and the listing continues.

    """
    try:
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        future = next(iter(admin.describe_configs([resource]).values()))
        entries = future.result(timeout=_TAIL_TIMEOUT_S)
    except Exception:
        _logger.warning(
            "KafkaEosSink could not read the config of `%s`, so it cannot "
            "check that the topic is not compacted; continuing",
            topic,
            exc_info=True,
        )
        return
    entry = entries.get("cleanup.policy")
    policy = "" if entry is None or entry.value is None else entry.value
    if "compact" in [part.strip() for part in policy.split(",")]:
        err = (
            f"Kafka topic `{topic}` has cleanup.policy={policy!r}, which includes "
            "compact. KafkaEosSink needs an uncompacted topic: compaction removes "
            "superseded messages from the middle of a window, which breaks the "
            "eos-seq contiguity the resume check relies on. Use a topic with "
            "cleanup.policy=delete."
        )
        raise RuntimeError(err)


# The resume wait of this process's current execution. The first restored
# partition to open starts it, and the last to close ends it, so one
# execution waits once however many partitions it restores.
_resume_wait_lock = threading.Lock()
_resume_open = 0
_resume_began = 0.0


def _join_resume_wait() -> float:
    """Count one more open restored partition; return when the wait began."""
    global _resume_open, _resume_began  # noqa: PLW0603
    with _resume_wait_lock:
        if _resume_open == 0:
            _resume_began = time.monotonic()
        _resume_open += 1
        return _resume_began


def _leave_resume_wait() -> None:
    """Count one restored partition closed."""
    global _resume_open  # noqa: PLW0603
    with _resume_wait_lock:
        _resume_open -= 1


def _sleep_until(deadline: float) -> None:
    """Sleep until the monotonic clock reads `deadline`, if it does not already."""
    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)


def _client_config(brokers: List[str], add_config: Dict[str, str]) -> Dict[str, str]:
    """The settings of a client, built as `KafkaSink` builds its producer.

    The brokers come first, so `add_config` can override `bootstrap.servers`.

    """
    return {"bootstrap.servers": ",".join(brokers), **add_config}


def _reader_config(brokers: List[str], add_config: Dict[str, str]) -> Dict:
    """Configuration of a consumer that reads the topic and commits nothing."""
    return {
        **_READER_DEFAULTS,
        **_client_config(brokers, add_config),
        "group.id": f"eos-{uuid.uuid4()}",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "auto.offset.reset": "earliest",
    }


@dataclass
class _Clients:
    """A producer and a reader, shared by the partitions of one thread."""

    producer: Producer
    reader: Consumer
    users: int = 0
    # When the closes that share this producer must have flushed by. The
    # first close sets it; see `_EosPartition._close_budget`.
    close_deadline: Optional[float] = None


_ClientsKey = Tuple[int, str, Tuple[Tuple[str, str], ...]]
# The client sets in use, one per worker thread, brokers and settings. A
# worker runs every partition it hosts on its own thread, so one set serves
# them all, and each delivery callback runs on the thread that polls the
# set. The last partition to release the set closes its reader.
_clients: Dict[_ClientsKey, _Clients] = {}
_clients_lock = threading.Lock()


def _acquire_clients(
    brokers: List[str], add_config: Dict[str, str]
) -> Tuple[_ClientsKey, _Clients]:
    """This thread's client set for `brokers` and `add_config`, with one more user."""
    key: _ClientsKey = (
        threading.get_ident(),
        ",".join(brokers),
        tuple(sorted(add_config.items())),
    )
    with _clients_lock:
        clients = _clients.get(key)
        if clients is None:
            producer = Producer(
                {**_client_config(brokers, add_config), **_PINNED_PRODUCER}
            )
            _start_auth_callbacks(producer)
            reader = Consumer(_reader_config(brokers, add_config))
            _start_auth_callbacks(reader)
            clients = _Clients(producer, reader)
            _clients[key] = clients
        clients.users += 1
    return key, clients


def _release_clients(key: _ClientsKey) -> None:
    """Drop one user of a client set. The last user closes its reader.

    The producer is not closed: its queue is empty once every publish has
    flushed, and it is freed with the last reference.

    """
    with _clients_lock:
        clients = _clients[key]
        clients.users -= 1
        if clients.users > 0:
            return
        del _clients[key]
    try:
        clients.reader.close()
    except Exception:
        _logger.warning("KafkaEosSink could not close its reader", exc_info=True)


def _read_records(
    consumer, topic: str, partition: int, start: int, end: int
) -> Iterator[_Eos]:
    """The EOS headers of the messages at offsets `start` up to `end`, in order.

    `end` is not included. The scan is bounded by offsets, not by message
    counts. A compacted topic has gaps in its offsets, and retention can
    move the low watermark past `start` after it was read, so a count
    would wait for messages that will never arrive. The scan ends at the
    message at `end - 1`, or at the end of the partition when that message
    has been removed. The timeout is for a stall: a read that keeps
    receiving records is not timed out. Messages of another partition,
    which the reader can still hand out after a reassignment, are not
    records of this one.

    """
    if start >= end:
        return
    consumer.assign([TopicPartition(topic, partition, start)])
    last = None
    progress = time.monotonic()
    try:
        while last is None or last < end - 1:
            for msg in consumer.consume(_TAIL_BATCH, _READ_POLL_S):
                error = msg.error()
                if error is not None and error.code() == KafkaError._PARTITION_EOF:
                    return
                if error is not None:
                    err = f"could not read Kafka topic `{topic}`: {error}"
                    raise RuntimeError(err)
                if msg.topic() != topic or msg.partition() != partition:
                    continue
                last = msg.offset()
                progress = time.monotonic()
                if last < end:
                    yield _eos_headers(topic, msg)
            if time.monotonic() - progress > _TAIL_TIMEOUT_S:
                err = f"timed out reading the end of Kafka topic `{topic}`"
                raise RuntimeError(err)
    finally:
        # A watermark query on an assigned consumer waits behind its pending
        # fetch. Unassigning after each read keeps the build's watermark
        # queries from waiting behind the last partition's fetch.
        consumer.unassign()


def _check_partition_count(store: str, topic: str, restored: int, count: int) -> None:
    """Refuse to resume a store whose topic has changed its partition count.

    Keys are placed by the count, so a store that wrote with one count
    cannot resume with another: its keys would move between partitions.

    """
    if restored == count:
        return
    old = f"{restored} partition{'' if restored == 1 else 's'}"
    new = f"{count} partition{'' if count == 1 else 's'}"
    err = (
        f"KafkaEosSink eos-store {store} wrote `{topic}` with {old}, but "
        f"`{topic}` has {new} now. Keys would move between partitions, so this "
        "store refuses to resume. Restore the partition count, or use a fresh "
        "topic and recovery store."
    )
    raise RuntimeError(err)


def _watermarks(reader: Consumer, topic: str, partition: int) -> Tuple[int, int]:
    """The low and high watermarks of one partition of a topic already listed.

    On a multi-broker cluster the first watermark query of a topic created a
    moment ago can fail with `UNKNOWN_TOPIC_OR_PART`, because the partition's
    leader has not applied the creation yet. That error is retried for up to
    `_TAIL_TIMEOUT_S`, like the wait for leaders. Any other error raises at
    once. A topic that does not exist is refused earlier, by `_partition_count`.

    """
    deadline = time.monotonic() + _TAIL_TIMEOUT_S
    while True:
        try:
            return reader.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=_TAIL_TIMEOUT_S
            )
        except KafkaException as ex:
            code = ex.args[0].code() if ex.args else None
            if code != KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise
            if time.monotonic() >= deadline:
                raise
        time.sleep(_LEADER_POLL_S)


def _partition_count(client, brokers: str, topic: str) -> int:
    """The number of partitions of `topic`, once each one has a leader.

    `client` is an `AdminClient` or a `Consumer`. A partition that was just
    added can be listed before it has a leader, so the count waits for the
    leaders, up to `_TAIL_TIMEOUT_S`, and then counts what is listed.

    """
    deadline = time.monotonic() + _TAIL_TIMEOUT_S
    while True:
        metadata = _topic_metadata(client, brokers, topic)
        if metadata is None or metadata.error is not None or not metadata.partitions:
            err = f"Kafka topic `{topic}` is not available for KafkaEosSink"
            raise RuntimeError(err)
        led = all(part.leader >= 0 for part in metadata.partitions.values())
        if led or time.monotonic() >= deadline:
            return len(metadata.partitions)
        time.sleep(_LEADER_POLL_S)


def _parse_part(topic: str, for_part: str) -> int:
    """The partition index of a part key from `list_parts`.

    The index is zero-padded, so the keys sort in partition order.

    """
    name, sep, index = for_part.partition("#")
    if name != topic or not sep:
        err = f"KafkaEosSink writes to `{topic}`, not {for_part!r}"
        raise ValueError(err)
    return int(index)


def _check_state(state: object, topic: str, partition: int) -> None:
    """Refuse a restored state unless it is a state of the supported version.

    The version is the first element. A state from before the version was
    added has none, and its fields cannot be told apart from those of a
    later format, so it is refused by its shape and not read.

    """
    fields = state if isinstance(state, tuple) else ()
    version = fields[0] if fields else None
    # A bool is an int, but `True` is not a version.
    versioned = isinstance(version, int) and not isinstance(version, bool)
    if versioned and version == _STATE_VERSION and len(fields) == _STATE_FIELDS:
        return
    found = f"version {version}" if versioned else "no version"
    if isinstance(state, tuple):
        found += f" and {len(fields)} fields"
    err = (
        f"KafkaEosSink cannot resume partition {partition} of `{topic}`: its "
        f"recovery state has {found}, but this bytewax reads only version "
        f"{_STATE_VERSION} states, which have {_STATE_FIELDS} fields. The state "
        "was written by a different bytewax version, so it is refused rather "
        "than guessed at. Resume with the bytewax version that wrote it."
    )
    raise RuntimeError(err)


class _EosPartition(StatefulSinkPartition[KafkaSinkMessage, _State]):
    # `close` runs once the execution has ended, when its outcome is known.
    _close_after_run = True

    def __init__(
        self,
        brokers: List[str],
        topic: str,
        add_config: Dict[str, str],
        resume_state: Optional[_State],
        resume_grace: Optional[timedelta] = None,
        partition: int = 0,
    ):
        self._topic = topic
        self._partition = partition
        self._store_id = uuid.uuid4().hex
        self._counter = 0
        self._state_end = 0
        restored: _Windows = []
        restored_count: Optional[int] = None
        if resume_state is not None:
            _check_state(resume_state, topic, partition)
            (
                _version,
                self._store_id,
                self._counter,
                restored,
                self._state_end,
                restored_count,
            ) = resume_state

        # Parsed before any client is made: a value that does not parse must
        # not leave clients that no partition owns.
        crash_after = int(os.environ[_CRASH_ENV]) if _CRASH_ENV in os.environ else None
        # The clients are shared by the partitions of this thread. The reader
        # serves the build's reads and the reads of the restore check.
        key, clients = _acquire_clients(brokers, add_config)
        self._key: Optional[_ClientsKey] = key
        self._clients = clients
        self._producer = clients.producer
        self._reader = clients.reader
        self._resume_waiting = False
        self._count = 0
        self._delivery_errors: List[str] = []
        self._crash_after = crash_after
        self._produced = 0
        self._failed = False

        self._buf: List[KafkaSinkMessage] = []
        # The windows of the state, oldest first. A window is in the state
        # from its close until every one of its messages is confirmed.
        self._windows: List[_Window] = []

        try:
            if resume_grace is not None and resume_state is not None:
                # A previous execution may have handed records to the network
                # just before it died. Waiting lets them land, or expire,
                # before the topic is read below.
                began = _join_resume_wait()
                self._resume_waiting = True
                _sleep_until(began + resume_grace.total_seconds())
            servers = _client_config(brokers, add_config)["bootstrap.servers"]
            self._count = _partition_count(self._reader, servers, topic)
            if restored_count is not None:
                _check_partition_count(
                    self._store_id, topic, restored_count, self._count
                )
            low, high = _watermarks(self._reader, topic, partition)
            if resume_state is None:
                if high > low:
                    err = (
                        f"Kafka topic `{topic}` already holds {high - low} "
                        f"message(s) at offsets {low} to {high}, but this recovery "
                        "store has no KafkaEosSink state for this step, so it did "
                        "not write them. Use a fresh topic, or the recovery store "
                        "that wrote it."
                    )
                    raise RuntimeError(err)
                self._state_end = high
            else:
                if self._state_end > high:
                    err = (
                        f"Kafka topic `{topic}` ends at offset {high}, before the "
                        f"offset {self._state_end} that eos-store {self._store_id} "
                        "recorded. The topic was recreated, or it is not the topic "
                        "this store wrote. Use a fresh topic, or the store that "
                        "wrote it."
                    )
                    raise RuntimeError(err)
                self._restore(restored, low, high)
        except BaseException:
            self._release()
            raise

    def _restore(self, restored: _Windows, low: int, high: int) -> None:
        """Check the topic against the restored state, then produce what it lacks.

        The windows of the state are one run of messages, and the first of
        them starts at `_state_end`. The topic holds a prefix of that run,
        of length `high - _state_end`, so the rest is produced. Every record
        still in the topic is checked against its place in the run before
        anything is produced, so a topic with other records, or with the
        same ones out of order, is refused.

        """
        total = sum(len(messages) for _number, messages in restored)
        self._check_records(restored, max(low, self._state_end), high)
        present = high - self._state_end
        if present > total:
            err = (
                f"Kafka topic `{self._topic}` holds {present} message(s) after "
                f"offset {self._state_end}, but eos-store {self._store_id} "
                f"recorded only {total}. The topic is ahead of the store, as after "
                "restoring an older backup, or it holds messages this store did "
                "not write. Use a fresh topic, or the store that wrote it."
            )
            raise RuntimeError(err)
        skip = present
        for number, messages in restored:
            window = _Window(number, messages)
            here = min(skip, len(messages))
            skip -= here
            self._publish(window, here)
            self._windows.append(window)

    def _check_records(self, restored: _Windows, start: int, high: int) -> None:
        """Refuse a record whose headers are not those of its place in the run.

        A record at offset `o` must be the message at place `o - _state_end`
        of the run of restored messages, with the same window and seq, and
        the same store.

        """
        places = [
            (number, seq)
            for number, messages in restored
            for seq in range(len(messages))
        ]
        for rec in _read_records(
            self._reader, self._topic, self._partition, start, high
        ):
            if rec.store != self._store_id:
                err = (
                    f"Kafka topic `{self._topic}` holds window {rec.window} written by "
                    f"eos-store {rec.store}, but this store is eos-store "
                    f"{self._store_id}. Another KafkaEosSink store wrote the topic. "
                    "Use a fresh topic, or the store that wrote it."
                )
                raise RuntimeError(err)
            place = rec.offset - self._state_end
            if place >= len(places) or rec.window >= self._counter:
                err = (
                    f"Kafka topic `{self._topic}` holds a window {rec.window} message "
                    f"at offset {rec.offset}, which the recovery state does not list. "
                    "The topic is ahead of the store, as after restoring an older "
                    "backup. Restore the store that wrote the topic, or use a fresh "
                    "topic."
                )
                raise RuntimeError(err)
            number, seq = places[place]
            if (rec.window, rec.seq) != (number, seq):
                err = (
                    f"Kafka topic `{self._topic}` has a hole at seq {seq} in window "
                    f"{number}: offset {rec.offset} holds window {rec.window} seq "
                    f"{rec.seq}, where the recovery state puts window {number} seq "
                    f"{seq}. Messages of that window are missing or out of order, so "
                    "the topic cannot be resumed."
                )
                raise RuntimeError(err)

    def write_batch(self, values: List[KafkaSinkMessage]) -> None:
        for msg in values:
            if msg.topic not in (None, self._topic):
                msg_err = f"KafkaEosSink writes to `{self._topic}`, not {msg.topic!r}"
                raise ValueError(msg_err)
            if msg.partition is not None:
                msg_err = "KafkaEosSink chooses the partition; leave it unset"
                raise ValueError(msg_err)
            self._buf.append(msg)

    def snapshot(self) -> _State:
        self._guard(self._snapshot)
        return (
            _STATE_VERSION,
            self._store_id,
            self._counter,
            [(window.number, window.messages) for window in self._windows],
            self._state_end,
            self._count,
        )

    def _snapshot(self) -> None:
        self._harvest()
        self._retire()
        # The engine takes this snapshot for a later boundary than the
        # previous one only after every recovery write for the earlier
        # boundaries is durable. So the window the previous snapshot closed
        # is durable now, and it can be produced.
        for window in self._windows:
            if not window.produced:
                self._publish(window)
        self._windows.append(_Window(self._counter, self._buf))
        self._buf = []
        self._counter += 1

    def close(self) -> None:
        """Publish and flush what the outcome of the execution allows.

        A process that holds a recovery partition lock may publish. The
        locks of the execution keep a successor out until every worker has
        returned, so a completed execution publishes its pending windows
        here, once its recovery writes are durable. A process with no lock
        cannot keep a successor out, so it publishes nothing and leaves its
        windows in the state for the next execution.

        A completed execution publishes its pending windows and then waits
        for every delivery. A failure raises, so the run fails visibly. An
        incomplete one flushes the messages it already produced, so they
        reach the topic before the process moves on. That is best effort:
        the next execution publishes whatever did not arrive.

        """
        try:
            if self._failed:
                return
            _recovering, completed, holds_lock = execution_state()
            if not holds_lock:
                if completed and self._windows:
                    _logger.warning(
                        "KafkaEosSink leaves %d window(s) of `%s` to the next "
                        "execution: this process holds no recovery partition lock, "
                        "and a successor could start while it publishes. Run with at "
                        "least as many recovery partitions as processes.",
                        len(self._windows),
                        self._topic,
                    )
                return
            if completed:
                self._guard(self._finish)
                return
            try:
                self._guard(self._flush_at_close)
            except Exception:
                _logger.error(
                    "KafkaEosSink could not flush windows to `%s` on close; "
                    "the next execution on the same recovery store will "
                    "publish any that did not arrive",
                    self._topic,
                    exc_info=True,
                )
        finally:
            self._release()

    def _release(self) -> None:
        """Release this partition's client set and resume wait, once."""
        if self._resume_waiting:
            self._resume_waiting = False
            _leave_resume_wait()
        if self._key is not None:
            key, self._key = self._key, None
            _release_clients(key)

    def _guard(self, action) -> None:
        """Run `action`; after any failure, never publish again."""
        if self._failed:
            msg = "KafkaEosSink already failed; not publishing again"
            raise RuntimeError(msg)
        try:
            action()
        except BaseException:
            self._failed = True
            raise

    def _finish(self) -> None:
        """Publish the pending windows of a completed execution, then wait for them.

        The delivery reports are harvested first, so a failure already known
        stops the publication before anything new is produced.

        """
        self._harvest()
        self._publish_pending()
        self._flush_at_close()

    def _publish_pending(self) -> None:
        """Produce every window the state holds that has not been produced."""
        for window in self._windows:
            if not window.produced:
                self._publish(window)

    def _harvest(self) -> None:
        """Take the delivery reports that have arrived, and fail on an error.

        The reports reach this partition only through a poll. The producer is
        shared with the other partitions of this thread, so a poll also takes
        their reports, and each partition raises for its own failures.

        """
        self._producer.poll(0)
        if self._delivery_errors:
            self._raise_delivery_failure(self._unconfirmed())

    def _retire(self) -> None:
        """Drop the oldest windows whose messages are all confirmed.

        `_state_end` moves past each window dropped, by its length: the
        messages of the run are contiguous in the topic, so the next window
        starts right after it. A failed delivery raises in `_harvest` before
        this runs, so a window with an unconfirmed message never leaves the
        state here.

        """
        while (
            self._windows
            and self._windows[0].produced
            and self._windows[0].unconfirmed == 0
        ):
            window = self._windows.pop(0)
            self._state_end += len(window.messages)

    def _unconfirmed(self) -> int:
        """How many of this partition's messages still await a delivery report."""
        return sum(window.unconfirmed for window in self._windows)

    def _publish(self, window: _Window, skip: int = 0) -> None:
        """Produce one window's messages from `skip` on, unless present.

        `skip` counts messages the topic already holds. A window published
        whole has `skip` 0. The produce calls do not wait for the broker, so
        the window stays in the state until its delivery reports arrive.

        """
        messages = window.messages
        if skip == len(messages):
            window.produced = True
            return
        for seq in range(skip, len(messages)):
            self._produce(messages[seq], window, seq)
        window.produced = True

    def _produce(self, msg: KafkaSinkMessage, window: _Window, seq: int) -> None:
        headers = list(msg.headers) + [
            (WINDOW_HEADER, str(window.number).encode()),
            (SEQ_HEADER, str(seq).encode()),
            (STORE_HEADER, self._store_id.encode()),
        ]
        deadline = time.monotonic() + _FLUSH_TIMEOUT_S
        while True:
            try:
                self._producer.produce(
                    topic=self._topic,
                    partition=self._partition,
                    value=msg.value,
                    key=msg.key,
                    headers=headers,
                    timestamp=msg.timestamp,
                    on_delivery=partial(self._on_delivery, window),
                )
                break
            except BufferError:
                # Messages leave the queue only when they are delivered, and
                # with no message timeout a queue that stays full means the
                # broker is not taking them. The run fails rather than wait.
                if time.monotonic() > deadline:
                    err = (
                        f"KafkaEosSink could not queue a message to `{self._topic}` "
                        f"for {_FLUSH_TIMEOUT_S:g} s: the producer queue stayed full, "
                        "so the broker took no messages"
                    )
                    raise RuntimeError(err) from None
                self._producer.poll(0.1)
        # Counted once the produce call has returned, so a report for this
        # message cannot arrive before it is counted.
        window.unconfirmed += 1
        self._producer.poll(0)
        self._produced += 1
        # Test-only hook: die after `BYTEWAX_EOS_CRASH_AFTER` messages of
        # publishing. The produced messages are flushed first, so they
        # are in the topic when the crash happens.
        if self._crash_after is not None and self._produced == self._crash_after:
            self._flush()
            msg_err = f"{_CRASH_ENV}={self._crash_after}: crashed mid-publish"
            raise RuntimeError(msg_err)

    def _on_delivery(self, window: _Window, err, _msg) -> None:
        window.unconfirmed -= 1
        if err is not None:
            # The error raises at the next harvest, which runs before any
            # window can leave the state.
            self._delivery_errors.append(str(err))

    def _flush_at_close(self) -> None:
        self._flush(self._close_budget())

    def _close_budget(self) -> float:
        """Seconds this close may wait for deliveries.

        The partitions of a thread share one producer. The first close of the
        execution starts a deadline on it, and each later close waits only for
        the rest of that deadline. A producer that cannot deliver therefore
        costs one `_FLUSH_TIMEOUT_S` in all, not one per partition.

        """
        clients = self._clients
        now = time.monotonic()
        if clients.close_deadline is None:
            clients.close_deadline = now + _FLUSH_TIMEOUT_S
        return max(0.0, clients.close_deadline - now)

    def _flush(self, timeout: float = _FLUSH_TIMEOUT_S) -> None:
        remaining = self._producer.flush(timeout)
        if remaining or self._delivery_errors:
            self._raise_delivery_failure(remaining)

    def _raise_delivery_failure(self, remaining: int) -> None:
        """Raise for a failed delivery.

        Nothing is purged. The producer is shared with the other partitions
        of the thread, and a purge would fail their queued messages too. A
        gapless producer stops after a failed batch, and the state holds
        whatever did not land, so the next execution resolves it.

        """
        errors = self._delivery_errors
        self._delivery_errors = []
        msg = (
            f"KafkaEosSink could not deliver to `{self._topic}`: "
            f"{remaining} message(s) unacknowledged; errors: {errors}"
        )
        raise RuntimeError(msg)


class KafkaEosSink(FixedPartitionedSink[KafkaSinkMessage, _State]):
    """Exactly-once output to a Kafka topic, one sink partition per partition.

    Items must be `(routing_key, KafkaSinkMessage)` tuples. The routing
    key chooses the partition: an item goes to partition
    `crc32(routing_key bytes) % partitions`, the placement of librdkafka's
    `consistent` partitioner. The Kafka message key comes from the
    {py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage` and does not
    choose the partition.

    Each result reaches the topic exactly once, per partition, across
    crashes, drains and resumes, without broker transactions. Each message
    carries `eos-window`, `eos-seq`, and `eos-store` headers that place it,
    so a consumer can deduplicate by (partition, `eos-window`, `eos-seq`).
    Requires a recovery config. See the module documentation for the
    protocol and its limitations.

    """

    def __init__(
        self,
        brokers: Iterable[str],
        topic: str,
        add_config: Optional[Dict[str, str]] = None,
        resume_grace: Optional[timedelta] = None,
    ):
        """Init.

        :arg brokers: List of `host:port` strings of Kafka brokers.

        :arg topic: Topic to produce to. Each of its partitions is written
            by one sink partition, placed by the routing key.

        :arg add_config: Any additional configuration properties. See
            [the `rdkafka`
            documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
            for options. `enable.idempotence` and `enable.gapless.guarantee`
            must stay `true`, and `message.timeout.ms` must stay `0`, or the
            constructor raises. Other settings can be overridden.

        :arg resume_grace: Optional. A restored partition waits this long
            before it reads the topic, so that a record a crashed execution
            already handed to the network can land or expire first. The
            wait is paid once per execution in this process, not once per
            partition. The default is no wait.

        """
        if isinstance(brokers, str):
            msg = "brokers must be an iterable and not a string"
            raise TypeError(msg)
        self._brokers = list(brokers)
        self._topic = topic
        self._add_config = {} if add_config is None else dict(add_config)
        _check_producer_config(self._add_config)
        self._resume_grace = resume_grace

    def list_parts(self) -> List[str]:
        """One part per partition of the topic. See ABC docstring."""
        _require_recovery()
        config = _client_config(self._brokers, self._add_config)
        client = AdminClient(config)
        _start_auth_callbacks(client)
        # Once per listing, not per partition: each execution lists the
        # parts, so a topic that became compacted is refused next time.
        _check_not_compacted(client, self._topic)
        count = _partition_count(client, config["bootstrap.servers"], self._topic)
        return [f"{self._topic}#{partition:06d}" for partition in range(count)]

    def part_fn(self, item_key: str) -> int:
        """The CRC-32 of the routing key's bytes, before wrapping.

        The engine wraps this value into the sorted parts with `% count`, which
        gives librdkafka's `consistent` partition for the key's bytes. See the
        class documentation.

        """
        return zlib.crc32(item_key.encode("utf-8"))

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[_State]
    ) -> _EosPartition:
        """See ABC docstring."""
        _require_recovery()
        return _EosPartition(
            self._brokers,
            self._topic,
            self._add_config,
            resume_state,
            self._resume_grace,
            _parse_part(self._topic, for_part),
        )
