#!/usr/bin/env bash
# Download, start, stop Apache Kafka 3.8.0 (ZK-coordinated) for CI.
# One script runs on linux native (x86_64 or aarch64), macos arm64,
# macos-13 Intel, and Git Bash on windows-latest. Inside qemu-armv7
# (via uraimo/run-on-arch-action) it also runs unchanged. A JDK is
# pre-installed on every GitHub-hosted runner image.

set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-3.8.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_DIR="${KAFKA_DIR:-$PWD/.kafka}"
KAFKA_LOG_DIRS="${KAFKA_LOG_DIRS:-$PWD/.kafka-logs}"

ensure_kafka() {
  if [[ -d "$KAFKA_DIR/bin" ]]; then return; fi
  local tgz="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${tgz}" -o "/tmp/${tgz}"
  mkdir -p "$(dirname "$KAFKA_DIR")"
  tar -xzf "/tmp/${tgz}" -C "$(dirname "$KAFKA_DIR")"
  mv "$(dirname "$KAFKA_DIR")/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "$KAFKA_DIR"
}

start() {
  ensure_kafka
  rm -rf "$KAFKA_LOG_DIRS"
  mkdir -p "$KAFKA_LOG_DIRS/zk" "$KAFKA_LOG_DIRS/data"

  cat > "$KAFKA_DIR/config/zk-test.properties" <<EOF
dataDir=$KAFKA_LOG_DIRS/zk
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF

  nohup "$KAFKA_DIR/bin/zookeeper-server-start.sh" \
    "$KAFKA_DIR/config/zk-test.properties" \
    </dev/null > "$KAFKA_LOG_DIRS/zk.log" 2>&1 &
  echo $! > "$KAFKA_LOG_DIRS/zk.pid"
  disown 2>/dev/null || true

  # Wait up to 60s for ZK to accept TCP connections on its client port.
  for _ in $(seq 1 60); do
    if (echo > /dev/tcp/localhost/2181) >/dev/null 2>&1; then break; fi
    sleep 1
  done

  local props="$KAFKA_DIR/config/server-test.properties"
  cp "$PWD/examples/utils/kafka-server.properties" "$props"
  # Portable in-place sed (BSD on macOS, GNU elsewhere). Two overrides:
  # - log.dirs to a runner-writable path
  # - zookeeper.connect from the docker-compose network alias to localhost
  sed -i.bak \
    -e "s|^log.dirs=.*|log.dirs=$KAFKA_LOG_DIRS/data|" \
    -e "s|^zookeeper.connect=.*|zookeeper.connect=localhost:2181|" \
    "$props" && rm -f "${props}.bak"

  nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$props" \
    </dev/null > "$KAFKA_LOG_DIRS/broker.log" 2>&1 &
  echo $! > "$KAFKA_LOG_DIRS/broker.pid"
  disown 2>/dev/null || true

  for _ in $(seq 1 90); do
    if "$KAFKA_DIR/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
      echo "Kafka broker ready on localhost:9092."
      return 0
    fi
    sleep 2
  done
  echo "Kafka broker failed to start; broker log:" >&2
  tail -200 "$KAFKA_LOG_DIRS/broker.log" >&2
  exit 1
}

stop() {
  for name in broker zk; do
    local pidfile="$KAFKA_LOG_DIRS/${name}.pid"
    if [[ -f "$pidfile" ]]; then
      kill "$(cat "$pidfile")" 2>/dev/null || true
      rm -f "$pidfile"
    fi
  done
}

case "${1:-}" in
  start) start ;;
  stop)  stop ;;
  *) echo "usage: $0 {start|stop}" >&2; exit 2 ;;
esac
