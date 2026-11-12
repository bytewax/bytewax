# Kafka connectivity fault gates

Gates 1-10 for the Kafka connectors (`bytewax.connectors.kafka`). Each
gate runs a dataflow in a subprocess against a 3-broker KRaft cluster
whose client traffic goes through Toxiproxy.

## Running

```bash
scripts/kafka-faults/run.sh             # all gates
scripts/kafka-faults/run.sh --runxfail  # show the failure behind each red gate
```

The script needs Docker with compose v2, and a bytewax build plus pytest in
the active environment. It starts three Kafka brokers and
`ghcr.io/shopify/toxiproxy:2.12.0`, waits for the brokers and the Toxiproxy
API, runs `pytests/connectors/test_kafka_faults.py`, and tears everything
down on exit.

The broker image is set by `BWKF_KAFKA_IMAGE` and defaults to
`apache/kafka:4.0.0`. To run the same gates against another KRaft image,
for example in CI:

```bash
BWKF_KAFKA_IMAGE=apache/kafka:4.3.1 scripts/kafka-faults/run.sh
```

The compose file runs brokers that combine the broker and controller roles,
so the image must support KRaft. ZooKeeper-mode 3.9 is out of scope for this
compose file.

Host ports: 39000 (Toxiproxy API), 39101-39103 (client listeners, one per
broker). Nothing else is published. Ports 9092, 19092 and 29092 are never
used, because other brokers on the same machine use them.

## Layout

- `docker-compose.yml`: three combined broker and controller nodes. Each
  CLIENT listener is advertised as its Toxiproxy port on localhost. The
  INTERNAL and CONTROLLER listeners are not proxied.
- `toxiproxy.json`: proxies `kafka1` to `kafka3`, loaded at start.
- `run.sh`: bring-up with readiness checks, pytest, and a trap that always
  runs `compose down -v`.
- `pytests/connectors/test_kafka_faults.py`: the gates.

## Gates

Each test asserts the expected behavior from the gate spec. Red gates are
marked `xfail(raises=KafkaFaultGap, strict=True)`. `KafkaFaultGap` is raised
only by the invariant check, after the preconditions have passed. The
preconditions check that the toxic is active and that the client observed
the fault: a stall, an error, or a reset.

| # | Injection | Expected behavior | Status on main |
|---|-----------|-------------------|----------------|
| 1 | Toxiproxy `timeout` on broker 1 for 15 s, applied before the flow starts; broker 1 leads output partition 0 (delivery.timeout 120 s) | No exception. The sink's write is held for the window, then Z=100%, D=0 | green (guard) |
| 2 | `docker stop` of the only replica of every output partition, delivery.timeout 10 s | Exception. After restart and resume, Z=100%, D=0 | red |
| 3 | Toxiproxy `reset_peer` (300 ms timeout) added mid-flow, on top of 200 ms latency | Retries absorb the resets: Z=100%, D=0 | red |
| 4 | Toxiproxy `latency` 500 ms, jitter 200 ms | Flow completes, no sink batch gap over 30 s, bounded RSS, Z=100%, D=0 | green (guard) |
| 5 | Toxiproxy `bandwidth` 128 KiB/s on every broker link, 6 MB input | Flow waits, no `BufferError`, bounded RSS, Z=100%, D=0 | green (guard) |
| 6 | 1.5 MB message, producer limit 5 MiB, broker default `message.max.bytes` | Immediate failure. After raising the topic limit and resuming, Z=100% | red |
| 7 | `docker stop` of broker 3, output topic `min.insync.replicas=3` | Exception. After ISR recovery and resume, Z=100%, D=0 | red |
| 8 | Toxiproxy `timeout` 0 on the downstream direction of broker 1's client link, applied while the sink is paused before a write. The write is released and broker 1 commits it, but the acknowledgement never arrives. Broker 1 is killed once the internal listener shows the commit. Broker 1 leads every output partition | Flow exits 0, Z=100%, D=0 | red |
| 8 guard | As 8, with `enable.idempotence=true` passed to the sink | Flow exits 0, Z=100%, D=0 | green (guard) |
| 9 | SIGKILL of the consumer process mid-flow, under 200 ms latency, resume from recovery | Z=100% on the output | green (guard) |
| 10 | Bootstrap address with nothing listening | Exit non-zero within 90 s, error names the address | green (guard) |

Invariants: Z is every input message reaching the output at least once. D is
the number of duplicate output messages, which must be zero unless the gate
replays (gates 2, 6, 9). L is the dataflow terminating within a bound.

## Red gates on main

- Gate 2: `KafkaSink` ignores delivery reports. When the leader is gone,
  `flush()` returns after `delivery.timeout.ms` and the flow exits 0 with
  the batches dropped.
- Gate 3: the sink producer is not idempotent by default. A produce request
  that is retried after a reset is written again. In the reproduction the
  sink was handed 2000 items for 2000 inputs and the output held 150
  duplicates, so the duplicates come from producer retries.
- Gate 8: the same missing idempotence, with the lost acknowledgement caused by
  the cut. Broker 1 commits the paused write, the client never reads the
  acknowledgement, and the client sends the write again to the new leader.
  In the reproduction, broker 1 committed 150 messages and the output held
  150 duplicates.
- Gate 6: a message the broker rejects as too large is dropped silently. The
  flow exits 0 with 49 of 50 messages delivered.
- Gate 7: `NOT_ENOUGH_REPLICAS` refusals are dropped silently. The flow exits
  0 with 0 of 300 messages delivered.

## Notes

- Each dataflow is generated by the test into its temporary directory and
  run with `sys.executable`. Waits are bounded, and a dataflow still running
  at its bound counts as an L violation.
- Gate 8 pauses the sink before its third write (the driver's `hold`), so no
  produce request is in flight when the cut lands. The write is then released.
  The cut is applied before the release, not after the request is sent. A
  broker answers within milliseconds, so no observation point can place the
  cut between the request and its response deterministically. The request
  still reaches the leader over the open upstream direction. The commit is
  read with `kafka-get-offsets.sh` inside a broker container, on the internal
  listener, which bypasses Toxiproxy. Only after the commit does the test kill
  the leader. The cut stays up until the flow has finished, so the held
  acknowledgement cannot be released during the retries.
- Topics created moments before a produce can refuse it with
  `_UNKNOWN_TOPIC` or `_UNKNOWN_PARTITION` while brokers catch up. The test
  setup retries only those refusals. Other errors fail the setup.
- The sink uses main's defaults. The only overrides are the ones a gate
  needs: `delivery.timeout.ms` (gates 1, 2, 7), `message.max.bytes`
  (gate 6), and `enable.idempotence=true` (gate 8 guard). The queue-full path
  (`BufferError`) is not reached in gates 1 and 5, because the sink flushes
  every batch and the default queue holds 100000 messages.
- The three recovery assertions of gates 2, 6 and 7 are not exercised on
  main, because the gap fires first. They were checked with a scratch copy
  that neutralises the gap check and gives the resumed run a fresh recovery
  store, so the assertions run against a real restart.
