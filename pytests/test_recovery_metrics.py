import json
import os
import socket
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.request
from collections import Counter

import pytest
from prometheus_client.parser import text_string_to_metric_families

FLOW = """
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bytewax.operators as op
from bytewax._bytewax import cli_main
from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource

directory = Path(sys.argv[1])
workers = int(sys.argv[2])

class HoldPartition(StatefulSourcePartition):
    def next_batch(self):
        if (directory / "stop").exists():
            raise StopIteration
        return []

    def next_awake(self):
        return datetime.now(timezone.utc) + timedelta(milliseconds=10)

    def snapshot(self):
        return None

class HoldSource(FixedPartitionedSource):
    def list_parts(self):
        return ["hold"]

    def build_part(self, step_id, part_key, resume_state):
        return HoldPartition()

def accumulate(state, value):
    state = (state or 0) + value
    return state, state

flow = Dataflow("metrics_test")
up = op.input("input", flow, TestingSource(
    [(str(i % 4), i) for i in range(100)], batch_size=10))
up = op.stateful_map("sum", up, accumulate)
up = op.map_value("encode", up, json.dumps)
op.output("file", up, FileSink(directory / "output.jsonl"))
hold = op.input("hold", flow, HoldSource())
op.output("hold_out", hold, TestingSink([]))
recovery = directory / "recovery"
recovery.mkdir()
init_db_dir(recovery, 1)
cli_main(flow, workers_per_process=workers, epoch_interval=timedelta(0),
         recovery_config=RecoveryConfig(recovery))
"""


@pytest.mark.parametrize("workers", [1, 2])
def test_recovery_histogram_has_unique_series(tmp_path, workers):
    # The provider and HTTP server are process-global. Isolate each run and
    # keep a separate source alive while scraping the completed finite branch.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {k: v for k, v in os.environ.items() if not k.startswith("BYTEWAX_")}
    env.update(
        BYTEWAX_DATAFLOW_API_ENABLED="true",
        BYTEWAX_DATAFLOW_API_PORT=str(port),
        BYTEWAX_DATAFLOW_API_CACHE_PATH=str(tmp_path),
    )
    output = tmp_path / "output.jsonl"
    samples = []
    with (tmp_path / "process.log").open("w+") as log:
        process = subprocess.Popen(
            [sys.executable, "-c", textwrap.dedent(FLOW), str(tmp_path), str(workers)],
            env=env,
            stdout=log,
            stderr=log,
        )
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    log.seek(0)
                    pytest.fail(log.read())
                if output.exists() and len(output.read_text().splitlines()) == 100:
                    try:
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{port}/metrics", timeout=1
                        ) as response:
                            text = response.read().decode()
                    except (urllib.error.URLError, TimeoutError):
                        continue
                    samples = [
                        sample
                        for family in text_string_to_metric_families(text)
                        for sample in family.samples
                    ]
                    eof = sum(
                        s.value
                        for s in samples
                        if s.name
                        == "bytewax_stateful_batch_on_eof_duration_seconds_count"
                        and s.labels.get("step_id")
                        == "metrics_test.sum.stateful_flat_map.stateful.stateful_batch"
                    )
                    writes = sum(
                        s.value
                        for s in samples
                        if s.name == "bytewax_partd_write_duration_seconds_count"
                    )
                    # Do not scrape just the initial execution-metadata write:
                    # wait for all keys' EOF and repeated recovery writes too.
                    if eof == 4 and writes > 10:
                        break
                time.sleep(0.01)
            else:
                pytest.fail("recovery metrics did not become ready")
        finally:
            (tmp_path / "stop").touch()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        log.seek(0)
        assert process.returncode == 0, log.read()

    histogram = [
        s for s in samples if s.name.startswith("bytewax_partd_write_duration_seconds_")
    ]
    assert histogram
    identities = Counter((s.name, tuple(sorted(s.labels.items()))) for s in histogram)
    duplicates = {
        identity: count for identity, count in identities.items() if count > 1
    }
    assert not duplicates, duplicates
    for sample in histogram:
        expected_labels = {"part_id", "worker_id", "otel_scope_name"}
        if sample.name.endswith("_bucket"):
            expected_labels.add("le")
        assert sample.labels.keys() == expected_labels

    sums, expected = [0] * 4, []
    for value in range(100):
        sums[value % 4] += value
        expected.append(sums[value % 4])
    assert sorted(
        json.loads(line) for line in output.read_text().splitlines()
    ) == sorted(expected)
