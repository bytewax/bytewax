"""Fresh-start witness for a Kafka source, pinned as a red test.

Needs a broker, as `test_kafka.py` does: set `TEST_KAFKA_BROKER`.

A recovery store that loses its progress restarts a `KafkaSource` from
the beginning of its topic, while the consumer group the store was
reading for still holds committed offsets that record that work as
done. The store cannot tell that apart from a genuinely new store, so
it starts over without an error.

The API pinned here is a PROPOSAL and does not exist yet:
`KafkaSource(..., witness_group="<group>")`. It would commit the resume
offsets to that consumer group at each durable epoch boundary, and on a
fresh start (no resume state) it would refuse to run while the group
has committed offsets, naming the group and those offsets.

The proposed commits would have been made by the first execution. The
test therefore writes the group's committed offset itself, to the
durable boundary read from the store, which is the state the proposal
would have left behind.

"""

import os
import pickle
import sqlite3
import subprocess
import sys
import time
import uuid
from concurrent.futures import wait
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

from bytewax.connectors.kafka import KafkaSource
from bytewax.recovery import init_db_dir
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pytest import fixture, mark

pytestmark = mark.skipif(
    not os.environ.get("TEST_KAFKA_BROKER"),
    reason="Set `TEST_KAFKA_BROKER` env var (non-empty) to run",
)
BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost")
config = {"bootstrap.servers": BROKER}
N = 200
KILL_AFTER = 30

# The first execution consumes part of the topic: items are slowed, and
# polled in small batches so that epochs, and so durable offsets, advance
# while the topic is still only partly consumed.
_WORKLOAD = r"""
import sys
import time
from datetime import timedelta

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSource
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.recovery import RecoveryConfig
from bytewax.testing import run_main

store, out, broker, topic = sys.argv[1:5]
extra = {}
if len(sys.argv) > 5:
    extra["witness_group"] = sys.argv[5]


class _FilePartition(StatelessSinkPartition):
    def __init__(self, path):
        self._file = open(path, "a")

    def write_batch(self, items):
        for item in items:
            self._file.write(f"{item}\n")
        self._file.flush()

    def close(self):
        self._file.close()


class _FileSink(DynamicSink):
    def build(self, step_id, worker_index, worker_count):
        return _FilePartition(out)


def _slow(msg):
    time.sleep(0.01)
    return int(msg.value)


flow = Dataflow("kafka_witness")
msgs = op.input(
    "inp", flow, KafkaSource([broker], [topic], batch_size=5, **extra)
)
vals = op.map("value", msgs, _slow)
op.output("out", vals, _FileSink())
run_main(
    flow,
    epoch_interval=timedelta(seconds=0.05),
    recovery_config=RecoveryConfig(store),
)
"""


class WitnessUnsupported(AssertionError):
    """A fresh start ran over a consumer group's committed offsets."""


def _wait_for_leader(client: AdminClient, topic: str) -> None:
    # Partition leader election is asynchronous after create_topics, and
    # a producer can hang on delivery until it settles.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        meta = client.list_topics(topic, timeout=5.0)
        found = meta.topics.get(topic)
        if (
            found is not None
            and found.error is None
            and found.partitions
            and all(p.leader >= 0 for p in found.partitions.values())
        ):
            return
        time.sleep(0.1)
    msg = f"topic {topic!r} partitions never got a leader"
    raise RuntimeError(msg)


@fixture
def witness_topic() -> Iterator[str]:
    """A one-partition topic holding the items 0..N-1."""
    client = AdminClient(config)
    topic = f"pytest_witness_{uuid.uuid4()}"
    for fut in client.create_topics(
        [NewTopic(topic, 1)], operation_timeout=5.0
    ).values():
        fut.result()
    _wait_for_leader(client, topic)
    producer = Producer({**config, "enable.idempotence": "true"})
    for i in range(N):
        producer.produce(topic, str(i).encode())
    producer.flush()
    yield topic
    wait(client.delete_topics([topic], operation_timeout=5.0).values())


def _spawn(
    store: Path, out: Path, log: Path, topic: str, witness: Optional[str] = None
) -> subprocess.Popen:
    args = [sys.executable, "-c", _WORKLOAD, str(store), str(out), BROKER, topic]
    if witness is not None:
        args.append(witness)
    # The child keeps its own copy of the file descriptor after `with`.
    with log.open("w") as err:
        return subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=err, cwd=str(store.parent)
        )


def _count_lines(out: Path) -> int:
    if not out.exists():
        return 0
    return len(out.read_text().splitlines())


def _count(db: Path, sql: str) -> int:
    """Read-only count, which also sees rows still in the WAL."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        (value,) = con.execute(sql).fetchone()
    finally:
        con.close()
    return int(value)


def _durable_offset(part: Path, state_key: str) -> int:
    """The source's offset at the last durable epoch boundary of `part`.

    A snapshot taken at epoch `e` is durable once the partition has
    committed epoch `e`, so the latest snapshot at or below the commit
    epoch is where a resume would start.

    """
    con = sqlite3.connect(f"file:{part}?mode=ro", uri=True)
    try:
        commit = con.execute(
            "SELECT commit_epoch FROM commits WHERE part_index = 0"
        ).fetchone()
        assert commit is not None, f"{part} has no commit row"
        row = con.execute(
            "SELECT ser_change FROM snaps WHERE state_key = ? AND snap_epoch <= ? "
            "AND ser_change IS NOT NULL ORDER BY snap_epoch DESC LIMIT 1",
            (state_key, commit[0]),
        ).fetchone()
    finally:
        con.close()
    assert row is not None, f"no durable snapshot for {state_key!r} in {part}"
    return int(pickle.loads(row[0]))


@mark.xfail(
    raises=WitnessUnsupported,
    strict=True,
    reason="KafkaSource has no witness_group: a store that lost its WAL restarts "
    "from the first message while its consumer group keeps committed offsets",
)
def test_kafka_source_fresh_start_witness(tmp_path: Path, witness_topic: str) -> None:
    """A fresh start must refuse to run over a group's committed offsets.

    The first execution is killed partway through the topic and loses its
    WAL, so its store holds nothing. The group the source was reading for
    has committed offsets at the durable boundary. A fresh start with the
    witness should refuse, naming the group and those offsets. Without
    it, the source starts over from the first message.

    """
    topic = witness_topic
    store = tmp_path / "store"
    store.mkdir()
    init_db_dir(store, 1)
    out = tmp_path / "out.txt"
    proc = _spawn(store, out, tmp_path / "first.err", topic)
    try:
        deadline = time.monotonic() + 120
        while _count_lines(out) < KILL_AFTER:
            if proc.poll() is not None:
                stderr = (tmp_path / "first.err").read_text()
                msg = f"execution exited {proc.returncode} early:\n{stderr}"
                raise AssertionError(msg)
            if time.monotonic() > deadline:
                msg = f"execution emitted fewer than {KILL_AFTER} items"
                raise TimeoutError(msg)
            time.sleep(0.02)
        proc.kill()
        proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    assert proc.returncode != 0, "the killed execution exited cleanly"

    (part,) = sorted(store.glob("part-*.sqlite3"))
    state_key = f"0-{topic}"
    durable = _durable_offset(part, state_key)
    assert 0 < durable < N, f"no durable progress partway through the topic: {durable}"
    wal = part.with_name(part.name + "-wal")
    shm = part.with_name(part.name + "-shm")
    assert _count(part, "SELECT count(*) FROM snaps") > 0, "no progress in the WAL"
    wal.unlink()
    shm.unlink(missing_ok=True)
    assert (
        _count(part, "SELECT count(*) FROM exs") == 0
    ), "the main database file holds progress of its own"

    group = f"bf-gap-pins-{uuid.uuid4()}"
    consumer = Consumer({**config, "group.id": group, "enable.auto.commit": False})
    try:
        consumer.commit(offsets=[TopicPartition(topic, 0, durable)], asynchronous=False)
        (committed,) = consumer.committed([TopicPartition(topic, 0)], timeout=10)
    finally:
        consumer.close()
    assert committed.offset == durable, f"group committed {committed.offset}"

    # The proposed keyword is not in the signature yet, so it is passed as
    # a mapping. Once the API exists, this is a plain keyword argument.
    proposed: Dict[str, Any] = {"witness_group": group}
    try:
        KafkaSource([BROKER], [topic], **proposed)
    except TypeError as ex:
        if "witness_group" not in str(ex):
            raise
        msg = (
            f"KafkaSource has no witness_group, so a fresh start after the WAL "
            f"was lost ignores group {group!r}, which has committed offset "
            f"{durable} on {topic!r}: {ex}"
        )
        raise WitnessUnsupported(msg) from ex

    # The desired behavior, reached only once the proposed API exists.
    before = _count_lines(out)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _WORKLOAD,
            str(store),
            str(out),
            BROKER,
            topic,
            group,
        ],
        capture_output=True,
        timeout=120,
        check=False,
    )
    stderr = result.stderr.decode()
    assert (
        result.returncode != 0
    ), f"a fresh start ran over the committed offsets:\n{stderr}"
    assert group in stderr, f"the refusal does not name the group:\n{stderr}"
    assert str(durable) in stderr, f"the refusal does not name the offset:\n{stderr}"
    assert _count_lines(out) == before, "the refused start emitted output"
