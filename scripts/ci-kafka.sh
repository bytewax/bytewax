#!/usr/bin/env bash
# Download, start, stop Apache Kafka for CI: by default a single
# combined broker + controller node in KRaft mode, or with
# KAFKA_MODE=zookeeper a broker with a standalone ZooKeeper (Kafka 3.x
# and older; 3.9 is the last line with ZooKeeper support). One script runs
# on linux native, macOS arm64, macOS Intel, and Git Bash on
# windows-latest. A JDK 17+ is pre-installed on every GitHub-hosted
# runner image.

set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-4.3.1}"
KAFKA_MODE="${KAFKA_MODE:-kraft}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
ZK_VERSION="${ZK_VERSION:-3.9.6}"
KAFKA_DIR="${KAFKA_DIR:-$PWD/.kafka}"
ZK_DIR="${ZK_DIR:-$PWD/.zookeeper}"
KAFKA_LOG_DIRS="${KAFKA_LOG_DIRS:-$PWD/.kafka-logs}"

java_path() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -m "$1"
  else
    printf '%s\n' "$1"
  fi
}

# log4j2 wants a URI for its config file: a bare Windows path like
# `D:/a/...` is read as a URL with scheme `D:` and rejected, leaving
# the broker without its logging config.
log4j_uri() {
  local p
  p="$(java_path "$1")"
  case "$p" in
    /*) printf 'file://%s\n' "$p" ;;
    *) printf 'file:///%s\n' "$p" ;;
  esac
}

java_bin() {
  if [[ -n "${JAVA_HOME:-}" ]]; then
    printf '%s\n' "$JAVA_HOME/bin/java"
  else
    printf 'java\n'
  fi
}

# Download an Apache release file, given its path under the release
# root, to `$2`.
download() {
  local path="$1" out="$2"
  # The CDN serves current releases quickly; archive.apache.org has
  # every release but is heavily throttled (~100 KB/s, so a fetch can
  # take most of a job's timeout). Try the CDN first and give up on it
  # if it stalls; only fall back to the archive for versions that have
  # left the CDN.
  local url min_speed
  for source in \
    "https://dlcdn.apache.org 102400" \
    "https://archive.apache.org/dist 10240"; do
    read -r url min_speed <<< "$source"
    url="${url}/${path}"
    echo "Downloading ${url}"
    if curl -fsSL --retry 3 --retry-delay 5 --connect-timeout 30 \
        --speed-limit "$min_speed" --speed-time 60 "$url" -o "$out"; then
      return 0
    fi
    rm -f "$out"
  done
  echo "Failed to download ${path}" >&2
  exit 1
}

# Unpack `$1`, whose top-level directory is `$2`, to `$3`.
unpack() {
  local tgz="$1" top="$2" dest="$3"
  rm -rf "$dest"
  mkdir -p "$(dirname "$dest")"
  tar -xzf "$tgz" -C "$(dirname "$dest")"
  mv "$(dirname "$dest")/$top" "$dest"
}

ensure_kafka() {
  # Check the version, so a cached download of another version is
  # replaced.
  if [[ -f "$KAFKA_DIR/libs/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.jar" ]]; then
    return
  fi
  local tgz="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  download "kafka/${KAFKA_VERSION}/${tgz}" "/tmp/${tgz}"
  unpack "/tmp/${tgz}" "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "$KAFKA_DIR"
}

ensure_zookeeper() {
  if [[ -f "$ZK_DIR/lib/zookeeper-${ZK_VERSION}.jar" ]]; then
    return
  fi
  local tgz="apache-zookeeper-${ZK_VERSION}-bin.tar.gz"
  download "zookeeper/zookeeper-${ZK_VERSION}/${tgz}" "/tmp/${tgz}"
  unpack "/tmp/${tgz}" "apache-zookeeper-${ZK_VERSION}-bin" "$ZK_DIR"
}

# Java options pointing Kafka's logging at its `tools` or `server`
# config. Kafka 4 uses log4j2 with YAML configs; Kafka 3 uses log4j 1
# with properties files.
log4j_opts() {
  local kind="$1"
  if [[ -f "$KAFKA_DIR/config/log4j2.yaml" ]]; then
    local file="log4j2.yaml"
    [[ "$kind" == tools ]] && file="tools-log4j2.yaml"
    printf -- '-Dlog4j2.configurationFile=%s\n' "$(log4j_uri "$KAFKA_DIR/config/$file")"
  else
    local file="log4j.properties"
    [[ "$kind" == tools ]] && file="tools-log4j.properties"
    printf -- '-Dlog4j.configuration=%s\n' "$(log4j_uri "$KAFKA_DIR/config/$file")"
  fi
}

port_open() {
  (echo > "/dev/tcp/localhost/$1") >/dev/null 2>&1
}

wait_for_port() {
  local port="$1" seconds="$2"
  for _ in $(seq 1 "$seconds"); do
    if port_open "$port"; then return 0; fi
    sleep 1
  done
  return 1
}

# Fail if something else already listens on a port we need; otherwise
# the readiness check below would talk to it instead of our server.
require_free_port() {
  if port_open "$1"; then
    echo "Port $1 is already in use; is another Kafka or ZooKeeper running?" >&2
    exit 1
  fi
}

start_zookeeper() {
  ensure_zookeeper
  require_free_port 2181
  mkdir -p "$KAFKA_LOG_DIRS/zk"
  local cfg="$KAFKA_LOG_DIRS/zoo.cfg"
  cat > "$cfg" <<EOF
dataDir=$(java_path "$KAFKA_LOG_DIRS/zk")
clientPort=2181
tickTime=2000
maxClientCnxns=0
admin.enableServer=false
4lw.commands.whitelist=ruok,srvr
EOF

  # Run the server class directly rather than through zkServer.sh, so
  # the same command works under Git Bash on Windows.
  nohup "$(java_bin)" -Xmx256m \
    -Dlogback.configurationFile="$(java_path "$ZK_DIR/conf/logback.xml")" \
    -cp "$(java_path "$ZK_DIR")/lib/*" \
    org.apache.zookeeper.server.quorum.QuorumPeerMain "$(java_path "$cfg")" \
    </dev/null > "$KAFKA_LOG_DIRS/zk.log" 2>&1 &
  echo $! > "$KAFKA_LOG_DIRS/zk.pid"
  disown 2>/dev/null || true

  if ! wait_for_port 2181 60; then
    echo "ZooKeeper failed to start; zk log:" >&2
    tail -200 "$KAFKA_LOG_DIRS/zk.log" >&2
    exit 1
  fi
  echo "ZooKeeper ${ZK_VERSION} ready on localhost:2181."
}

# KRaft storage must be formatted with a cluster ID before first
# start; `--standalone` bootstraps this node as the sole controller.
format_kraft_storage() {
  local kafka_props="$1" cluster_id
  cluster_id="$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_DIR/bin/kafka-storage.sh" format --standalone \
    -t "$cluster_id" -c "$kafka_props" > "$KAFKA_LOG_DIRS/format.log" 2>&1 || {
    echo "Kafka storage format failed:" >&2
    cat "$KAFKA_LOG_DIRS/format.log" >&2
    exit 1
  }
}

start() {
  local props_src
  case "$KAFKA_MODE" in
    kraft) props_src="kafka-server.properties" ;;
    zookeeper)
      if (( ${KAFKA_VERSION%%.*} >= 4 )); then
        echo "KAFKA_MODE=zookeeper needs Kafka 3.x or older, not ${KAFKA_VERSION}" >&2
        exit 2
      fi
      props_src="kafka-server-zookeeper.properties"
      ;;
    *)
      echo "unknown KAFKA_MODE ${KAFKA_MODE}; use kraft or zookeeper" >&2
      exit 2
      ;;
  esac

  ensure_kafka
  require_free_port 9092
  rm -rf "$KAFKA_LOG_DIRS"
  mkdir -p "$KAFKA_LOG_DIRS/data"

  # Explicit Java-style paths so log4j finds its config under Git Bash
  # on Windows; tools log quietly, the server uses its own config below.
  KAFKA_LOG4J_OPTS="$(log4j_opts tools)"
  export KAFKA_LOG4J_OPTS
  local props="$KAFKA_DIR/config/server-test.properties"
  cp "$PWD/examples/utils/${props_src}" "$props"
  # Portable in-place sed (BSD on macOS, GNU elsewhere): point log.dirs
  # at a runner-writable path.
  sed -i.bak \
    -e "s|^log.dirs=.*|log.dirs=$(java_path "$KAFKA_LOG_DIRS/data")|" \
    "$props" && rm -f "${props}.bak"
  local kafka_props
  kafka_props="$(java_path "$props")"

  if [[ "$KAFKA_MODE" == zookeeper ]]; then
    start_zookeeper
  else
    format_kraft_storage "$kafka_props"
  fi

  KAFKA_LOG4J_OPTS="$(log4j_opts server)" \
    nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$kafka_props" \
    </dev/null > "$KAFKA_LOG_DIRS/broker.log" 2>&1 &
  local broker_pid=$!
  echo "$broker_pid" > "$KAFKA_LOG_DIRS/broker.pid"
  disown 2>/dev/null || true

  for _ in $(seq 1 90); do
    if ! kill -0 "$broker_pid" 2>/dev/null; then
      break
    fi
    # Only trust the port once our broker logged that it started; a
    # broker that failed to bind would otherwise pass the check below
    # through whatever else listens there.
    if grep -qE '\[KafkaServer id=[0-9]+\] started|Kafka Server started' \
        "$KAFKA_LOG_DIRS/broker.log" &&
        "$KAFKA_DIR/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
      echo "Kafka ${KAFKA_VERSION} (${KAFKA_MODE}) broker ready on localhost:9092."
      return 0
    fi
    sleep 2
  done
  echo "Kafka broker failed to start; broker log:" >&2
  tail -200 "$KAFKA_LOG_DIRS/broker.log" >&2
  exit 1
}

stop() {
  # Broker first, and wait for it to exit, so it can still reach
  # ZooKeeper while shutting down.
  local name pidfile pid
  for name in broker zk; do
    pidfile="$KAFKA_LOG_DIRS/${name}.pid"
    if [[ -f "$pidfile" ]]; then
      pid="$(cat "$pidfile")"
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 60); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
      done
      rm -f "$pidfile"
    fi
  done
}

case "${1:-}" in
  start) start ;;
  stop)  stop ;;
  *) echo "usage: $0 {start|stop}" >&2; exit 2 ;;
esac
