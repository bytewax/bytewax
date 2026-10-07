#!/usr/bin/env bash
# Download, start, stop Apache Kafka 4.3.1 in KRaft mode (single
# combined broker + controller node) for CI. One script runs on linux
# native, macOS arm64, macOS Intel, and Git Bash on windows-latest. A
# JDK 17+ is pre-installed on every GitHub-hosted runner image.

set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-4.3.1}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_DIR="${KAFKA_DIR:-$PWD/.kafka}"
KAFKA_LOG_DIRS="${KAFKA_LOG_DIRS:-$PWD/.kafka-logs}"

java_path() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -m "$1"
  else
    printf '%s\n' "$1"
  fi
}

ensure_kafka() {
  if [[ -d "$KAFKA_DIR/bin" ]]; then return; fi
  local tgz="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  # The CDN serves current releases quickly; archive.apache.org has
  # every release but is heavily throttled (~100 KB/s, so a fetch can
  # take most of a job's timeout). Try the CDN first and give up on it
  # if it stalls; only fall back to the archive for versions that have
  # left the CDN.
  local url min_speed
  for source in \
    "https://dlcdn.apache.org/kafka 102400" \
    "https://archive.apache.org/dist/kafka 10240"; do
    read -r url min_speed <<< "$source"
    url="${url}/${KAFKA_VERSION}/${tgz}"
    echo "Downloading ${url}"
    if curl -fsSL --retry 3 --retry-delay 5 --connect-timeout 30 \
        --speed-limit "$min_speed" --speed-time 60 "$url" -o "/tmp/${tgz}"; then
      break
    fi
    rm -f "/tmp/${tgz}"
  done
  if [[ ! -s "/tmp/${tgz}" ]]; then
    echo "Failed to download Apache Kafka ${KAFKA_VERSION}" >&2
    exit 1
  fi
  mkdir -p "$(dirname "$KAFKA_DIR")"
  tar -xzf "/tmp/${tgz}" -C "$(dirname "$KAFKA_DIR")"
  mv "$(dirname "$KAFKA_DIR")/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "$KAFKA_DIR"
}

start() {
  ensure_kafka
  rm -rf "$KAFKA_LOG_DIRS"
  mkdir -p "$KAFKA_LOG_DIRS/data"

  # Explicit Java-style paths so log4j2 finds its config under Git Bash
  # on Windows; tools log quietly, the server uses its own config below.
  export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=$(java_path "$KAFKA_DIR/config/tools-log4j2.yaml")"
  local props="$KAFKA_DIR/config/server-test.properties"
  cp "$PWD/examples/utils/kafka-server.properties" "$props"
  # Portable in-place sed (BSD on macOS, GNU elsewhere): point log.dirs
  # at a runner-writable path.
  sed -i.bak \
    -e "s|^log.dirs=.*|log.dirs=$(java_path "$KAFKA_LOG_DIRS/data")|" \
    "$props" && rm -f "${props}.bak"

  # KRaft storage must be formatted with a cluster ID before first
  # start; `--standalone` bootstraps this node as the sole controller.
  local kafka_props cluster_id
  kafka_props="$(java_path "$props")"
  cluster_id="$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_DIR/bin/kafka-storage.sh" format --standalone \
    -t "$cluster_id" -c "$kafka_props" > "$KAFKA_LOG_DIRS/format.log" 2>&1 || {
    echo "Kafka storage format failed:" >&2
    cat "$KAFKA_LOG_DIRS/format.log" >&2
    exit 1
  }

  KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=$(java_path "$KAFKA_DIR/config/log4j2.yaml")" \
    nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$kafka_props" \
    </dev/null > "$KAFKA_LOG_DIRS/broker.log" 2>&1 &
  echo $! > "$KAFKA_LOG_DIRS/broker.pid"
  disown 2>/dev/null || true

  for _ in $(seq 1 90); do
    if "$KAFKA_DIR/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
      echo "Kafka ${KAFKA_VERSION} (KRaft) broker ready on localhost:9092."
      return 0
    fi
    sleep 2
  done
  echo "Kafka broker failed to start; broker log:" >&2
  tail -200 "$KAFKA_LOG_DIRS/broker.log" >&2
  exit 1
}

stop() {
  local pidfile="$KAFKA_LOG_DIRS/broker.pid"
  if [[ -f "$pidfile" ]]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
  fi
}

case "${1:-}" in
  start) start ;;
  stop)  stop ;;
  *) echo "usage: $0 {start|stop}" >&2; exit 2 ;;
esac
