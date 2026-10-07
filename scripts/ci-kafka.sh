#!/usr/bin/env bash
# Download, start, stop Apache Kafka 3.9.2 coordinated by a standalone
# Apache ZooKeeper 3.9.6 for CI. One script runs on linux native,
# macOS arm64, macOS Intel, and Git Bash on windows-latest. A JDK is
# pre-installed on every GitHub-hosted runner image.

set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-3.9.2}"
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

java_bin() {
  if [[ -n "${JAVA_HOME:-}" ]]; then
    printf '%s\n' "$JAVA_HOME/bin/java"
  else
    printf 'java\n'
  fi
}

# Download a tarball and unpack its single top-level directory to
# `dest`. Skipped if `dest` is already populated (e.g. CI cache hit).
ensure_dist() {
  local url="$1" top="$2" dest="$3"
  if [[ -d "$dest/bin" ]]; then return; fi
  local tgz
  tgz="/tmp/$(basename "$url")"
  curl -fsSL "$url" -o "$tgz"
  mkdir -p "$(dirname "$dest")"
  tar -xzf "$tgz" -C "$(dirname "$dest")"
  mv "$(dirname "$dest")/$top" "$dest"
}

wait_for_port() {
  local port="$1" seconds="$2"
  for _ in $(seq 1 "$seconds"); do
    if (echo > "/dev/tcp/localhost/$port") >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  return 1
}

start_zookeeper() {
  ensure_dist \
    "https://archive.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz" \
    "apache-zookeeper-${ZK_VERSION}-bin" \
    "$ZK_DIR"

  local cfg="$KAFKA_LOG_DIRS/zoo.cfg"
  cat > "$cfg" <<EOF
dataDir=$(java_path "$KAFKA_LOG_DIRS/zk")
clientPort=2181
tickTime=2000
maxClientCnxns=0
admin.enableServer=false
4lw.commands.whitelist=ruok,srvr
EOF

  # Launch the server class directly instead of `zkServer.sh` /
  # `zkServer.cmd` so the same invocation works under Git Bash on
  # Windows. `$!` is then the JVM itself, which `stop` can kill.
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

start_kafka() {
  ensure_dist \
    "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" \
    "$KAFKA_DIR"

  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:///$(java_path "$KAFKA_DIR/config/log4j.properties")"
  local props="$KAFKA_DIR/config/server-test.properties"
  cp "$PWD/examples/utils/kafka-server.properties" "$props"
  # Portable in-place sed (BSD on macOS, GNU elsewhere). Two overrides:
  # - log.dirs to a runner-writable path
  # - zookeeper.connect from the docker-compose network alias to localhost
  sed -i.bak \
    -e "s|^log.dirs=.*|log.dirs=$(java_path "$KAFKA_LOG_DIRS/data")|" \
    -e "s|^zookeeper.connect=.*|zookeeper.connect=localhost:2181|" \
    "$props" && rm -f "${props}.bak"

  nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$(java_path "$props")" \
    </dev/null > "$KAFKA_LOG_DIRS/broker.log" 2>&1 &
  echo $! > "$KAFKA_LOG_DIRS/broker.pid"
  disown 2>/dev/null || true

  for _ in $(seq 1 90); do
    if "$KAFKA_DIR/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
      echo "Kafka ${KAFKA_VERSION} broker ready on localhost:9092."
      return 0
    fi
    sleep 2
  done
  echo "Kafka broker failed to start; broker log:" >&2
  tail -200 "$KAFKA_LOG_DIRS/broker.log" >&2
  exit 1
}

start() {
  rm -rf "$KAFKA_LOG_DIRS"
  mkdir -p "$KAFKA_LOG_DIRS/zk" "$KAFKA_LOG_DIRS/data"
  start_zookeeper
  start_kafka
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
