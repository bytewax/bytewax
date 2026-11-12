"""Kafka connectivity fault gates 1-10.

Each gate runs a bytewax dataflow in a subprocess against a 3-broker
KRaft cluster whose client traffic goes through Toxiproxy. The brokers
are reachable only through the proxies named kafka1 to kafka3. Run the
suite with scripts/kafka-faults/run.sh, which brings the cluster up.

Invariants:

- Z: every input message is in the output at least once.
- D: duplicate output messages are confined to the snapshot window.
  Gates that expect no replay assert zero duplicates.
- L: the dataflow terminates within a bound and does not exit silently.

A gate that main violates is marked
``xfail(raises=KafkaFaultGap, strict=True)``. ``KafkaFaultGap`` is raised
only by ``invariant``, and only after the preconditions have passed.
Preconditions are plain asserts: a fault that never fired fails the test
for real instead of passing it.

"""

import functools
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from bytewax.recovery import init_db_dir
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from pytest import fixture, mark

ENABLED = os.environ.get("BYTEWAX_KAFKA_FAULTS") == "1"
BOOTSTRAP = os.environ.get(
    "BYTEWAX_KAFKA_FAULTS_BOOTSTRAP",
    "localhost:39101,localhost:39102,localhost:39103",
)
TOXIPROXY_API = os.environ.get(
    "BYTEWAX_KAFKA_FAULTS_TOXIPROXY", "http://localhost:39000"
)
PROXY = {1: "kafka1", 2: "kafka2", 3: "kafka3"}
CONTAINER = {1: "bwkf-kafka1", 2: "bwkf-kafka2", 3: "bwkf-kafka3"}
PARTITIONS = 3
RUN_BOUND_S = 180.0
"""Longest a dataflow may run before it counts as hung (L)."""
MSG_MAX_BYTES = 5_242_880
"""Producer-side limit used where a message must exceed the broker's default."""

pytestmark = mark.skipif(
    not ENABLED,
    reason="set BYTEWAX_KAFKA_FAULTS=1 (see scripts/kafka-faults/run.sh)",
)

DRIVER = r"""
import json
import os
import sys
import threading
import time
from datetime import timedelta

import bytewax.operators as op
from bytewax.connectors.kafka import (
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    KafkaSourceMessage,
)
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.recovery import RecoveryConfig
from bytewax.testing import run_main

spec = json.loads(sys.argv[1])
progress = open(spec["progress"], "a", buffering=1)
hold = spec["hold"]
writes = 0
# The test process owns this driver. If the test process dies without
# running its fixtures (SIGKILL, SIGTERM, a hard timeout), the driver is
# re-parented and exits, so it cannot outlive the test.
parent = os.getppid()


def exit_with_parent():
    while os.getppid() == parent:
        time.sleep(0.2)
    os._exit(1)


threading.Thread(target=exit_with_parent, daemon=True).start()


def log(ev, **fields):
    progress.write(json.dumps({"ev": ev, "t": time.time(), **fields}) + "\n")


# Pause before the chosen write until the test creates the release file.
# The sink has no produce request in flight at that point: the previous
# write returned only after its flush.
def pause_if_held(items):
    global writes
    writes += 1
    if hold is None or writes != hold["call"]:
        return
    log("hold", n=len(items))
    while not os.path.exists(hold["release"]):
        time.sleep(0.05)


class _TimedPart(StatelessSinkPartition):
    def __init__(self, inner):
        self._inner = inner

    def write_batch(self, items):
        pause_if_held(items)
        log("start", n=len(items))
        try:
            self._inner.write_batch(items)
        finally:
            log("end")

    def close(self):
        self._inner.close()


class _TimedSink(DynamicSink):
    def __init__(self, inner):
        self._inner = inner

    def build(self, step_id, worker_index, worker_count):
        part = self._inner.build(step_id, worker_index, worker_count)
        return _TimedPart(part)


def to_sink(msg):
    if not isinstance(msg, KafkaSourceMessage):
        raise RuntimeError(f"Kafka source error: {msg.err}")
    return KafkaSinkMessage(msg.key, msg.value)


flow = Dataflow("kf_faults")
inp = op.input(
    "in",
    flow,
    KafkaSource(
        spec["brokers"],
        [spec["in_topic"]],
        tail=False,
        batch_size=spec["batch_size"],
        add_config=spec["source_config"],
    ),
)
msgs = op.map("to_sink", inp, to_sink)
sink = KafkaSink(spec["brokers"], spec["out_topic"], add_config=spec["sink_config"])
op.output("out", msgs, _TimedSink(sink))

recovery = RecoveryConfig(spec["recovery_dir"]) if spec["recovery_dir"] else None
run_main(
    flow,
    epoch_interval=timedelta(seconds=spec["epoch_s"]),
    recovery_config=recovery,
)
log("done")
"""
"""Dataflow run in a subprocess. Logs when each sink batch starts and ends."""


class KafkaFaultGap(AssertionError):
    """Main violates a gate invariant. Raised only by `invariant`."""


def invariant(ok: bool, msg: str) -> None:
    """Check a gate invariant. Failing it means the gate is red."""
    if not ok:
        raise KafkaFaultGap(msg)


def wait_for(pred, timeout_s: float, what: str, interval: float = 0.25) -> None:
    """Poll `pred` until it is true. A timeout is a real failure."""
    deadline = time.monotonic() + timeout_s
    while not pred():
        if time.monotonic() > deadline:
            msg = f"timed out after {timeout_s:g} s waiting for {what}"
            raise AssertionError(msg)
        time.sleep(interval)


class Toxiproxy:
    """Minimal client for the Toxiproxy HTTP API."""

    def __init__(self, api: str) -> None:
        self._api = api

    def call(self, method: str, path: str, body: Optional[dict] = None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(
            self._api + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
        return json.loads(raw) if raw else None

    def toxic_names(self, broker: int) -> set:
        proxy = self.call("GET", f"/proxies/{PROXY[broker]}")
        return {t["name"] for t in proxy["toxics"]}


class FlowRun:
    """One dataflow subprocess, with its progress log and peak RSS."""

    def __init__(self, proc: subprocess.Popen, workdir: Path, files: list) -> None:
        self.proc = proc
        self.workdir = workdir
        self._files = files
        self.peak_rss_kb = 0
        self._sampler = threading.Thread(target=self._sample_rss, daemon=True)
        self._sampler.start()

    def _sample_rss(self) -> None:
        while self.proc.poll() is None:
            ps = subprocess.run(
                ["ps", "-o", "rss=", "-p", str(self.proc.pid)],
                capture_output=True,
                text=True,
                check=False,
            )
            if ps.stdout.strip():
                self.peak_rss_kb = max(self.peak_rss_kb, int(ps.stdout))
            time.sleep(0.2)

    def alive(self) -> bool:
        return self.proc.poll() is None

    def wait(self, timeout_s: float) -> Optional[int]:
        """Exit status, or None if still running after `timeout_s`."""
        try:
            return self.proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            return None

    def kill(self) -> None:
        """Kill the dataflow if it still runs, then reap it and close its logs."""
        if self.alive():
            self.proc.kill()
        self.proc.wait()
        for f in self._files:
            f.close()

    def stderr_text(self) -> str:
        return (self.workdir / "stderr.log").read_text(errors="replace")

    def stderr_tail(self) -> str:
        return self.stderr_text()[-1500:]

    def _events(self) -> List[dict]:
        path = self.workdir / "progress.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def write_spans(self) -> List[Tuple[float, Optional[float]]]:
        """(start, end) of each sink batch; end is None while in flight."""
        spans: List[Tuple[float, Optional[float]]] = []
        start: Optional[float] = None
        for ev in self._events():
            if ev["ev"] == "start":
                start = ev["t"]
            elif ev["ev"] == "end" and start is not None:
                spans.append((start, ev["t"]))
                start = None
        if start is not None:
            spans.append((start, None))
        return spans

    def items_received(self) -> int:
        """Items the sink was handed; equals the input count unless the
        source replayed messages."""
        return sum(ev["n"] for ev in self._events() if ev["ev"] == "start")

    def writes_done(self) -> int:
        return sum(1 for _, end in self.write_spans() if end is not None)

    def longest_write_s(self) -> float:
        now = time.time()
        return (
            max(
                ((end if end is not None else now) - start)
                for start, end in self.write_spans()
            )
            if self.write_spans()
            else 0.0
        )

    def max_gap_between_writes_s(self) -> float:
        starts = [s for s, _ in self.write_spans()]
        return max((b - a for a, b in zip(starts, starts[1:])), default=0.0)

    def held_items(self) -> Optional[int]:
        """Items in the sink write the driver paused before, once paused."""
        for ev in self._events():
            if ev["ev"] == "hold":
                return ev["n"]
        return None

    def release(self) -> None:
        (self.workdir / "release").touch()


def payloads(count: int, size: int) -> List[bytes]:
    return [f"v{i:06d}".encode().ljust(size, b"x") for i in range(count)]


@functools.lru_cache(maxsize=None)
def admin() -> AdminClient:
    """One shared client: futures fail if their client is garbage collected."""
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def describe(topic: str) -> Dict[int, Tuple[int, List[int], List[int]]]:
    """Per partition: (leader, replicas, isr)."""
    meta = admin().list_topics(topic, timeout=15)
    parts = meta.topics[topic].partitions
    return {p: (m.leader, list(m.replicas), list(m.isrs)) for p, m in parts.items()}


def committed_offsets(topic: str, via: int) -> int:
    """Sum over the partitions of `topic` of the latest offset, read by a
    Kafka CLI inside broker `via` on its internal listener.

    This path bypasses Toxiproxy. The latest offset that a read_uncommitted
    consumer sees is the high watermark, so a record counts here once the
    brokers have committed it.
    """
    out = subprocess.run(
        [
            "docker",
            "exec",
            CONTAINER[via],
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            f"kafka{via}:9092",
            "--topic",
            topic,
            "--time",
            "-1",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    # Each line is topic:partition:offset.
    return sum(
        int(line.rsplit(":", 1)[1])
        for line in out.stdout.splitlines()
        if line.count(":") == 2
    )


def _produce_when_known(
    producer: Producer,
    topic: str,
    key: bytes,
    value: bytes,
    on_delivery,
    deadline: float,
) -> None:
    """`produce()`, waiting out a topic the producer does not know yet."""
    while True:
        try:
            producer.produce(topic, value, key, on_delivery=on_delivery)
            return
        except KafkaException as ex:
            code = ex.args[0].code()
            if code not in (KafkaError._UNKNOWN_TOPIC, KafkaError._UNKNOWN_PARTITION):
                raise
            assert time.monotonic() < deadline, f"{topic} never became producible"
            producer.poll(0.5)


def produce_items(
    topic: str, values: Sequence[bytes], *, config: Optional[dict] = None
) -> List[Tuple[bytes, bytes]]:
    """Produce `values` with unique keys, spread over partitions by hash.

    A topic created moments ago can be refused with `_UNKNOWN_PARTITION`
    while the broker answering the producer's metadata request has not yet
    applied it. Such messages were never written, so they are retried
    until they are accepted. Any other error fails the setup.
    """
    producer = Producer({"bootstrap.servers": BOOTSTRAP, **(config or {})})
    items = [(f"k{i:06d}".encode(), value) for i, value in enumerate(values)]
    values_by_key = dict(items)
    pending = [key for key, _ in items]
    refused: List[bytes] = []
    errors: list = []

    def on_delivery(err, msg):
        if err is None:
            return
        if err.code() == KafkaError._UNKNOWN_PARTITION:
            refused.append(msg.key())
        else:
            errors.append(err)

    deadline = time.monotonic() + 60
    while True:
        refused.clear()
        errors.clear()
        for key in pending:
            _produce_when_known(
                producer, topic, key, values_by_key[key], on_delivery, deadline
            )
            producer.poll(0)
        left = producer.flush(60)
        assert not errors, f"input produce failed: {errors[:3]}"
        assert left == 0, f"input produce did not flush: {left} left"
        if not refused:
            return items
        assert time.monotonic() < deadline, f"{topic} never became producible"
        time.sleep(0.5)
        pending = list(refused)


Record = Tuple[int, int, bytes, bytes]
"""(partition, offset, key, value) of one output message."""


def read_output(topic: str) -> List[Record]:
    """Read every message in `topic` up to its current high watermark."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"kf-read-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        parts = sorted(describe(topic))
        low: Dict[int, int] = {}
        high: Dict[int, int] = {}
        for p in parts:
            low[p], high[p] = consumer.get_watermark_offsets(
                TopicPartition(topic, p), timeout=15
            )
        consumer.assign([TopicPartition(topic, p, low[p]) for p in parts])
        done = {p: high[p] <= low[p] for p in parts}
        records: List[Record] = []
        deadline = time.monotonic() + 90
        while not all(done.values()):
            if time.monotonic() > deadline:
                msg = f"timed out reading output of {topic!r}"
                raise AssertionError(msg)
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise AssertionError(str(message.error()))
            p = message.partition()
            records.append((p, message.offset(), message.key(), message.value()))
            if message.offset() + 1 >= high[p]:
                done[p] = True
        return records
    finally:
        consumer.close()


def check_delivery(
    items: List[Tuple[bytes, bytes]],
    records: List[Record],
    max_dups: int,
    detail: str = "",
) -> None:
    """Z: every input is in the output. D: duplicates stay within `max_dups`."""
    expected = {k for k, _ in items}
    got = [r[2] for r in records]
    missing = expected - set(got)
    invariant(
        not missing,
        f"Z violated: {len(missing)} of {len(expected)} input messages never "
        f"reached the output, e.g. {sorted(missing)[:3]!r}",
    )
    dups = len(got) - len(set(got))
    invariant(
        dups <= max_dups,
        f"D violated: {dups} duplicate output messages, at most {max_dups} "
        f"allowed{detail}",
    )


def free_closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Env:
    """Per-test handle on the cluster, Toxiproxy, topics, and dataflows."""

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.toxi = Toxiproxy(TOXIPROXY_API)
        self.recovery_dir = tmp / "recovery"
        self.recovery_dir.mkdir()
        init_db_dir(self.recovery_dir, 1)
        self._driver = tmp / "driver.py"
        self._driver.write_text(DRIVER)
        self._topics: List[str] = []
        self._runs: List[FlowRun] = []
        self._stopped: List[int] = []
        self._flows = 0

    def topic(
        self,
        label: str,
        *,
        rf: int = 3,
        assignment: Optional[List[List[int]]] = None,
        config: Optional[dict] = None,
    ) -> str:
        name = f"kf-{label}-{uuid.uuid4().hex[:10]}"
        if assignment is not None:
            spec = NewTopic(name, len(assignment), -1, replica_assignment=assignment)
            count = len(assignment)
        else:
            spec = NewTopic(name, PARTITIONS, rf)
            count = PARTITIONS
        spec.config = config or {}
        for fut in admin().create_topics([spec], operation_timeout=30).values():
            fut.result(timeout=60)
        self._topics.append(name)
        wait_for(
            lambda: len(describe(name)) == count
            and all(leader >= 0 for leader, _, _ in describe(name).values()),
            60,
            f"partitions of {name} to get leaders",
        )
        return name

    def alter_topic(self, topic: str, settings: Dict[str, str]) -> None:
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic, set_config=settings)
        for fut in admin().alter_configs([resource]).values():
            fut.result(timeout=30)

    @contextmanager
    def toxic(
        self,
        brokers: Sequence[int],
        kind: str,
        attributes: dict,
        *,
        streams: Sequence[str] = ("upstream", "downstream"),
    ) -> Iterator[None]:
        """Apply a toxic to each broker's proxy, asserting it is active."""
        added: List[Tuple[int, str]] = []
        try:
            for broker in brokers:
                for stream in streams:
                    name = f"{kind}-{stream}"
                    self.toxi.call(
                        "POST",
                        f"/proxies/{PROXY[broker]}/toxics",
                        {
                            "name": name,
                            "type": kind,
                            "stream": stream,
                            "toxicity": 1.0,
                            "attributes": attributes,
                        },
                    )
                    added.append((broker, name))
            for broker in brokers:
                wanted = {n for b, n in added if b == broker}
                active = self.toxi.toxic_names(broker)
                assert wanted <= active, f"toxics {wanted - active} not active"
            yield
        finally:
            for broker, name in added:
                try:
                    self.toxi.call("DELETE", f"/proxies/{PROXY[broker]}/toxics/{name}")
                except urllib.error.HTTPError as ex:
                    if ex.code != 404:
                        raise

    def stop_broker(self, broker: int) -> None:
        subprocess.run(
            ["docker", "stop", CONTAINER[broker]],
            check=True,
            capture_output=True,
            text=True,
        )
        self._stopped.append(broker)

    def kill_broker(self, broker: int) -> None:
        subprocess.run(
            ["docker", "kill", CONTAINER[broker]],
            check=True,
            capture_output=True,
            text=True,
        )
        self._stopped.append(broker)

    def start_broker(self, broker: int) -> None:
        subprocess.run(
            ["docker", "start", CONTAINER[broker]],
            check=True,
            capture_output=True,
            text=True,
        )
        if broker in self._stopped:
            self._stopped.remove(broker)
        wait_cluster_registered()

    def start_flow(
        self,
        *,
        in_topic: str,
        out_topic: str,
        sink_config: Optional[dict] = None,
        source_config: Optional[dict] = None,
        batch_size: int = 100,
        epoch_s: float = 1.0,
        brokers: Optional[str] = None,
        hold_call: Optional[int] = None,
    ) -> FlowRun:
        self._flows += 1
        workdir = self.tmp / f"run{self._flows}"
        workdir.mkdir()
        spec = {
            "brokers": (brokers or BOOTSTRAP).split(","),
            "in_topic": in_topic,
            "out_topic": out_topic,
            "sink_config": sink_config or {},
            "source_config": source_config or {},
            "batch_size": batch_size,
            "epoch_s": epoch_s,
            "recovery_dir": str(self.recovery_dir),
            "progress": str(workdir / "progress.jsonl"),
            "hold": None,
        }
        if hold_call is not None:
            spec["hold"] = {
                "call": hold_call,
                "release": str(workdir / "release"),
            }
        stdout = (workdir / "stdout.log").open("wb")
        stderr = (workdir / "stderr.log").open("wb")
        proc = subprocess.Popen(
            [sys.executable, str(self._driver), json.dumps(spec)],
            cwd=str(workdir),
            stdout=stdout,
            stderr=stderr,
        )
        run = FlowRun(proc, workdir, [stdout, stderr])
        self._runs.append(run)
        return run

    def kill_flows(self) -> None:
        """Kill and reap every dataflow this test started."""
        for run in self._runs:
            run.kill()

    def close(self) -> None:
        self.kill_flows()
        for broker in list(self._stopped):
            self.start_broker(broker)
        self.toxi.call("POST", "/reset")
        wait_cluster_registered()
        if self._topics:
            for fut in (
                admin().delete_topics(self._topics, operation_timeout=30).values()
            ):
                try:
                    fut.result(timeout=60)
                except Exception:  # noqa: BLE001,S110 - best-effort cleanup
                    pass


@fixture
def env(tmp_path) -> Iterator[Env]:
    handle = Env(tmp_path)
    try:
        yield handle
    finally:
        # Runs on failure and on KeyboardInterrupt. Each dataflow is killed
        # and reaped before the cluster is reset.
        handle.kill_flows()
        handle.close()


def wait_cluster_registered() -> None:
    """Wait until all three brokers are in cluster metadata."""

    def registered() -> bool:
        try:
            return sorted(admin().list_topics(timeout=5).brokers) == [1, 2, 3]
        except Exception:  # noqa: BLE001 - brokers may still be starting
            return False

    wait_for(registered, 120, "all three brokers to be registered")


@fixture(autouse=True)
def _cluster_up() -> None:
    """Fail real if the cluster is not three brokers reachable via proxies."""
    if ENABLED:
        wait_cluster_registered()


SILENCE_S = 15.0
"""Broker silence injected by gate 1. Well under its delivery timeout."""


def test_gate01_silent_broker_within_delivery_timeout(env: Env) -> None:
    # Broker 1 leads partition 0 of the output topic and nothing on the
    # input topic: the source never fetches from it, while the sink's
    # writes for output partition 0 must go through it.
    inp = env.topic("in", assignment=[[2, 3, 1], [3, 1, 2]])
    out = env.topic("out", assignment=[[1, 2, 3], [2, 3, 1], [3, 1, 2]])
    items = produce_items(inp, payloads(900, 1000))

    # The flow is short, so the silence starts before it: any sink batch
    # holding a message for output partition 0 is stuck in flush() until
    # the toxic is removed.
    with env.toxic([1], "timeout", {"timeout": 0}):
        run = env.start_flow(
            in_topic=inp,
            out_topic=out,
            sink_config={"delivery.timeout.ms": "120000"},
            batch_size=300,
        )
        exited = run.wait(SILENCE_S)
        # Preconditions: the silence reached the sink. Either a batch
        # was held for most of the window, or the flow failed in it.
        fault_seen = (exited is not None and exited != 0) or (
            run.longest_write_s() >= SILENCE_S / 2
        )
        assert fault_seen, "broker 1 silence did not stall or fail the sink"

    rc = run.wait(RUN_BOUND_S)
    invariant(rc is not None, "L violated: dataflow still running long after recovery")
    invariant(
        rc == 0,
        f"exception during a {SILENCE_S:g} s silence (< delivery.timeout): "
        f"exit {rc}: {run.stderr_tail()}",
    )
    check_delivery(items, read_output(out), max_dups=0)


@mark.xfail(
    raises=KafkaFaultGap,
    strict=True,
    reason="KafkaSink ignores delivery reports: flush() returns after "
    "delivery.timeout.ms and the flow exits 0 with the batches dropped",
)
def test_gate02_broker_silent_beyond_delivery_timeout(env: Env) -> None:
    # Each output partition has its only replica on broker 1, so stopping
    # broker 1 leaves the sink with no leader for any of them.
    out = env.topic("out", assignment=[[1], [1], [1]])
    inp = env.topic("in")
    items = produce_items(inp, payloads(300, 200))
    env.stop_broker(1)
    wait_for(
        lambda: all(leader == -1 for leader, _, _ in describe(out).values()),
        90,
        "the output partitions to lose their leader",
    )

    run = env.start_flow(
        in_topic=inp,
        out_topic=out,
        sink_config={"delivery.timeout.ms": "10000"},
        batch_size=100,
    )
    rc = run.wait(RUN_BOUND_S)
    # Precondition: the sink waited out the delivery timeout (or failed).
    fault_seen = (rc not in (None, 0)) or run.longest_write_s() >= 5.0
    assert fault_seen, "sink never blocked on the stopped broker"

    invariant(rc is not None, "L violated: dataflow still running after 180 s")
    invariant(
        rc != 0,
        "exit 0 although the output partitions had no leader past "
        "delivery.timeout.ms: undelivered batches were dropped silently",
    )

    # Recovery: broker back, same recovery store, resume.
    env.start_broker(1)
    wait_for(
        lambda: all(leader >= 0 for leader, _, _ in describe(out).values()),
        180,
        "broker 1 to lead its partitions again",
    )
    run2 = env.start_flow(
        in_topic=inp,
        out_topic=out,
        sink_config={"delivery.timeout.ms": "10000"},
        batch_size=100,
    )
    rc2 = run2.wait(RUN_BOUND_S)
    invariant(rc2 == 0, f"resume after broker restart exited {rc2}")
    # Nothing reached the output before the failure, so there is no
    # replay window and any duplicate is a bug.
    check_delivery(items, read_output(out), max_dups=0)


RESET_WINDOW_S = 4.0
RESET_AFTER_MS = 300


@mark.xfail(
    raises=KafkaFaultGap,
    strict=True,
    reason="no idempotence by default: a produce retried after a TCP reset "
    "is written again, so duplicates reach the output",
)
def test_gate03_tcp_reset_mid_batch(env: Env) -> None:
    inp = env.topic("in")
    out = env.topic("out")
    items = produce_items(inp, payloads(2000, 500))

    # Latency keeps the flow busy. Once it is writing on established
    # connections, each connection is reset 300 ms after the reset toxic
    # is added, so resets land while produce requests are in flight.
    # A request can reach the broker, lose its response, and be sent
    # again.
    with env.toxic([1, 2, 3], "latency", {"latency": 200, "jitter": 0}):
        run = env.start_flow(in_topic=inp, out_topic=out, batch_size=50)
        wait_for(lambda: run.writes_done() >= 2, 60, "the dataflow to start writing")
        with env.toxic(
            [1, 2, 3],
            "reset_peer",
            {"timeout": RESET_AFTER_MS},
            streams=("downstream",),
        ):
            exited = run.wait(RESET_WINDOW_S)
            # Precondition: librdkafka saw established connections drop.
            dropped = run.stderr_text().count("in state UP")
            assert exited is None, "flow finished before the resets were applied"
            assert dropped > 0, "no established connection was reset"
        rc = run.wait(RUN_BOUND_S)

    invariant(rc is not None, "L violated: dataflow still running after 180 s")
    invariant(rc == 0, f"TCP resets surfaced as a dataflow failure: exit {rc}")
    received = run.items_received()
    cause = (
        "; no replay at the source, so the extra writes are producer retries"
        if received == len(items)
        else ""
    )
    check_delivery(
        items,
        read_output(out),
        max_dups=0,
        detail=f"; the sink was handed {received} items for {len(items)} inputs{cause}",
    )


LATENCY = {"latency": 500, "jitter": 200}
RSS_BOUND_KB = 400 * 1024


def _round_trip_s() -> float:
    start = time.monotonic()
    admin().list_topics(timeout=15)
    return time.monotonic() - start


def test_gate04_high_latency_jitter(env: Env) -> None:
    inp = env.topic("in")
    out = env.topic("out")
    items = produce_items(inp, payloads(600, 500))

    with env.toxic([1, 2, 3], "latency", LATENCY):
        # Precondition: metadata round trips now pay the latency. Each
        # direction is at least 300 ms with 200 ms jitter, so the minimum
        # of three round trips is at least 600 ms when the toxic applies.
        rtt = min(_round_trip_s() for _ in range(3))
        assert rtt >= 0.5, f"latency not applied: fastest round trip {rtt:.2f} s"
        run = env.start_flow(in_topic=inp, out_topic=out, batch_size=100)
        rc = run.wait(RUN_BOUND_S)

    invariant(rc is not None, "L violated: dataflow still running after 180 s")
    invariant(rc == 0, f"latency with sane configs crashed the flow: exit {rc}")
    gap_s = run.max_gap_between_writes_s()
    print(
        f"[gate04] round trip {rtt:.2f} s, max gap between sink batches "
        f"{gap_s:.1f} s, peak RSS {run.peak_rss_kb / 1024:.0f} MiB"
    )
    invariant(gap_s <= 30.0, f"epochs stalled: {gap_s:.1f} s between sink batches")
    invariant(
        run.peak_rss_kb <= RSS_BOUND_KB,
        f"peak RSS {run.peak_rss_kb / 1024:.0f} MiB above the bound",
    )
    check_delivery(items, read_output(out), max_dups=0)


BANDWIDTH_KBPS = 128


def test_gate05_narrow_bandwidth(env: Env) -> None:
    inp = env.topic("in")
    out = env.topic("out")
    # Every partition of the probe topic leads on broker 1, so the probe
    # measures one link. The bandwidth toxic limits each broker link.
    probe = env.topic("probe", assignment=[[1], [1], [1]])
    items = produce_items(inp, payloads(1500, 4000))

    with env.toxic([1, 2, 3], "bandwidth", {"rate": BANDWIDTH_KBPS}):
        # Precondition: 256 KiB over one 128 KiB/s link takes about 2 s.
        start = time.monotonic()
        produce_items(probe, payloads(64, 4000))
        elapsed = time.monotonic() - start
        assert elapsed >= 1.0, f"bandwidth not applied: probe took {elapsed:.2f} s"
        run = env.start_flow(in_topic=inp, out_topic=out, batch_size=100)
        rc = run.wait(RUN_BOUND_S)

    invariant(rc is not None, "L violated: dataflow still running after 180 s")
    invariant(
        rc == 0,
        f"narrow bandwidth surfaced as a dataflow failure: exit {rc}: "
        f"{run.stderr_tail()}",
    )
    print(
        f"[gate05] probe {elapsed:.2f} s over one link, peak RSS "
        f"{run.peak_rss_kb / 1024:.0f} MiB"
    )
    invariant(
        run.peak_rss_kb <= RSS_BOUND_KB,
        f"peak RSS {run.peak_rss_kb / 1024:.0f} MiB above the bound",
    )
    check_delivery(items, read_output(out), max_dups=0)


def _probe_oversized(topic: str) -> Optional[int]:
    """Produce one message above the broker limit; return the error code."""
    producer = Producer(
        {"bootstrap.servers": BOOTSTRAP, "message.max.bytes": str(MSG_MAX_BYTES)}
    )
    result: dict = {}

    def on_delivery(err, _msg):
        result["err"] = err

    producer.produce(topic, b"x" * 1_500_000, b"probe", on_delivery=on_delivery)
    producer.flush(30)
    err = result.get("err")
    return None if err is None else err.code()


@mark.xfail(
    raises=KafkaFaultGap,
    strict=True,
    reason="a message the broker rejects as too large is dropped silently: "
    "the flow exits 0 with 49 of 50 messages delivered",
)
def test_gate06_oversized_message(env: Env) -> None:
    inp = env.topic("in", config={"max.message.bytes": str(MSG_MAX_BYTES)})
    out = env.topic("out")  # broker default message.max.bytes (~1 MB)
    values = payloads(50, 200)
    values[25] = b"x" * 1_500_000
    items = produce_items(inp, values, config={"message.max.bytes": str(MSG_MAX_BYTES)})
    # Precondition: the broker really refuses the oversized message.
    code = _probe_oversized(out)
    assert code == KafkaError.MSG_SIZE_TOO_LARGE, f"broker accepted it: {code}"

    sink = {"message.max.bytes": str(MSG_MAX_BYTES)}
    run = env.start_flow(in_topic=inp, out_topic=out, sink_config=sink)
    rc = run.wait(RUN_BOUND_S)
    invariant(rc is not None, "L violated: dataflow still running after 180 s")
    delivered = len(read_output(out))
    invariant(
        rc != 0,
        "exit 0 although the broker rejected a message as too large: "
        f"{delivered} of {len(items)} messages delivered, the rest dropped silently",
    )

    # Recovery: raise the broker limit and resume.
    before = read_output(out)
    env.alter_topic(out, {"max.message.bytes": str(MSG_MAX_BYTES)})
    run2 = env.start_flow(in_topic=inp, out_topic=out, sink_config=sink)
    rc2 = run2.wait(RUN_BOUND_S)
    invariant(rc2 == 0, f"resume after raising the limit exited {rc2}")
    check_delivery(items, read_output(out), max_dups=len(before))


@mark.xfail(
    raises=KafkaFaultGap,
    strict=True,
    reason="NOT_ENOUGH_REPLICAS refusals are dropped silently: the flow exits 0 "
    "with no messages delivered",
)
def test_gate07_not_enough_replicas(env: Env) -> None:
    out = env.topic("out", config={"min.insync.replicas": "3"})
    inp = env.topic("in")
    items = produce_items(inp, payloads(300, 200))
    env.stop_broker(3)
    wait_for(
        lambda: all(len(isr) < 3 for _, _, isr in describe(out).values()),
        120,
        "the ISR of the output partitions to shrink below 3",
    )

    run = env.start_flow(
        in_topic=inp,
        out_topic=out,
        sink_config={"delivery.timeout.ms": "15000"},
        batch_size=100,
    )
    rc = run.wait(RUN_BOUND_S)
    # Precondition: acks=all writes were refused (or timed out) at the sink.
    fault_seen = (rc not in (None, 0)) or run.longest_write_s() >= 10.0
    assert fault_seen, "sink never waited on NOT_ENOUGH_REPLICAS"

    invariant(rc is not None, "L violated: dataflow still running after 180 s")
    delivered = len(read_output(out))
    invariant(
        rc != 0,
        "exit 0 although the output partitions could not reach min.insync.replicas: "
        f"{delivered} of {len(items)} messages delivered, the rest dropped silently",
    )

    env.start_broker(3)
    wait_for(
        lambda: all(len(isr) == 3 for _, _, isr in describe(out).values()),
        180,
        "the ISR to recover to 3",
    )
    run2 = env.start_flow(
        in_topic=inp,
        out_topic=out,
        sink_config={"delivery.timeout.ms": "15000"},
        batch_size=100,
    )
    rc2 = run2.wait(RUN_BOUND_S)
    invariant(rc2 == 0, f"resume after ISR recovery exited {rc2}")
    check_delivery(items, read_output(out), max_dups=0)


HOLD_CALL = 3
"""Sink write that the gate 8 construction pauses before, while nothing is in flight."""
LEADER = 1
"""Broker that leads every output partition in gates 8 and its guard."""


def _lost_ack_then_failover(
    env: Env, sink_config: Dict[str, str]
) -> Tuple[List[Tuple[bytes, bytes]], str, FlowRun, int, Optional[int]]:
    """Commit a sink batch on the leader, lose its acknowledgement, then kill
    the leader. Returns (inputs, output topic, flow run, batch size, exit
    status of the flow or None if it was still running).

    Every output partition is led by LEADER, so every message of the held
    batch goes through the one client link that the cut affects. The sink
    is paused before a batch, so no produce request is in flight when the
    downstream direction of that link is cut. The batch is then released.
    Its request still reaches LEADER over the open upstream direction, and
    LEADER appends it and replicates it over the unproxied inter-broker
    listeners (acks=all, RF 3). The acknowledgement cannot reach the client
    while the cut holds. The commit is checked on the internal listener
    before LEADER is killed, and the client retries the batch against the
    new leaders.
    """
    out = env.topic("out", assignment=[[LEADER, 2, 3]] * PARTITIONS)
    inp = env.topic("in", assignment=[[2, 3, 1], [3, 2, 1], [2, 3, 1]])
    items = produce_items(inp, payloads(2000, 1000))

    run = env.start_flow(
        in_topic=inp,
        out_topic=out,
        sink_config=sink_config,
        batch_size=50,
        hold_call=HOLD_CALL,
    )
    wait_for(
        lambda: run.held_items() is not None or not run.alive(),
        90,
        "the sink to pause before a batch",
        interval=0.5,
    )
    held = run.held_items()
    # Precondition: the sink is paused with work to do, and nothing is in
    # flight. Earlier writes returned only after their flush.
    assert held is not None and run.alive(), "flow ended before the sink paused"
    assert held > 0, "the paused write holds no items"
    before = committed_offsets(out, via=2)

    # The cut stays in place until the flow has finished, so the held
    # acknowledgement cannot be released while the client retries.
    with env.toxic([LEADER], "timeout", {"timeout": 0}, streams=("downstream",)):
        run.release()
        wait_for(
            lambda: committed_offsets(out, via=2) >= before + held,
            90,
            f"broker {LEADER} to commit the {held} released messages",
            interval=0.5,
        )
        after = committed_offsets(out, via=2)
        # Precondition: the write is still unacknowledged. Its span has
        # started and has not ended, because flush() still waits for it.
        spans = run.write_spans()
        assert (
            run.alive() and spans and spans[-1][1] is None
        ), "sink write was acknowledged although its acknowledgement was cut"
        print(
            f"[gate08] {held} messages in the held write; committed offsets "
            f"{before} -> {after} (internal listener) before the kill"
        )
        env.kill_broker(LEADER)
        rc = run.wait(RUN_BOUND_S)
    return items, out, run, held, rc


@mark.xfail(
    raises=KafkaFaultGap,
    strict=True,
    reason="no idempotence by default: the batch the leader committed without "
    "acknowledging is sent again to the new leader and written twice",
)
def test_gate08_partition_leader_failover(env: Env) -> None:
    items, out, _, held, rc = _lost_ack_then_failover(env, sink_config={})
    invariant(rc is not None, "L violated: dataflow still running after 180 s")
    invariant(rc == 0, f"leader failover surfaced as a dataflow failure: exit {rc}")
    check_delivery(
        items,
        read_output(out),
        max_dups=0,
        detail=f"; broker {LEADER} committed {held} messages whose acknowledgement "
        "never reached the client, and the client sent them to the new leader",
    )


def test_gate08_guard_idempotent_producer_no_duplicates(env: Env) -> None:
    items, out, run, _, rc = _lost_ack_then_failover(
        env, sink_config={"enable.idempotence": "true"}
    )
    invariant(rc is not None, "L violated: dataflow still running after 180 s")
    invariant(rc == 0, f"leader failover surfaced as a dataflow failure: exit {rc}")
    check_delivery(items, read_output(out), max_dups=0)


def test_gate09_consumer_process_sigkill_resume(env: Env) -> None:
    inp = env.topic("in")
    out = env.topic("out")
    items = produce_items(inp, payloads(2000, 200))

    # Latency keeps the flow in progress long enough to SIGKILL it.
    with env.toxic([1, 2, 3], "latency", {"latency": 200, "jitter": 0}):
        run = env.start_flow(in_topic=inp, out_topic=out, batch_size=50, epoch_s=0.2)
        partial: List[Record] = []
        deadline = time.monotonic() + 120
        while len(partial) < 300 and run.alive():
            if time.monotonic() > deadline:
                msg = "timed out waiting for the dataflow to write 300 messages"
                raise AssertionError(msg)
            time.sleep(1.0)
            partial = read_output(out)
        # Precondition: SIGKILL lands mid-flow, after some output and
        # before the end.
        assert run.alive(), "flow finished before the SIGKILL"
        assert 0 < len(partial) < len(items), f"{len(partial)} written, not partial"
        run.kill()

        run2 = env.start_flow(in_topic=inp, out_topic=out, batch_size=50, epoch_s=0.2)
        rc2 = run2.wait(RUN_BOUND_S)

    invariant(rc2 is not None, "L violated: resumed dataflow still running")
    invariant(rc2 == 0, f"resume after SIGKILL exited {rc2}: {run2.stderr_tail()}")
    check_delivery(items, read_output(out), max_dups=len(partial))


def test_gate10_bootstrap_unreachable(env: Env) -> None:
    port = free_closed_port()
    with socket.socket() as s:
        # Precondition: nothing listens on the bootstrap address.
        assert s.connect_ex(("127.0.0.1", port)) != 0, "port unexpectedly open"

    run = env.start_flow(
        in_topic="kf-no-such-in",
        out_topic="kf-no-such-out",
        brokers=f"localhost:{port}",
    )
    rc = run.wait(90.0)
    invariant(rc is not None, "L violated: no startup error within 90 s")
    invariant(rc != 0, "dataflow started with an unreachable bootstrap server")
    invariant(
        f"localhost:{port}" in run.stderr_text(),
        "startup error does not name the bootstrap address",
    )
