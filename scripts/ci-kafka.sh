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

is_windows() {
  case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) return 0 ;; *) return 1 ;; esac
}

# Launch a JVM-backed Kafka script in the background, redirecting
# output to a log file. On Linux/macOS, bash's nohup+disown is
# sufficient. On Windows Git Bash, bash's job control is not enough
# to keep the JVM alive across step boundaries — the broker dies
# silently when "Start Kafka"'s shell exits — so we shell out to
# cmd.exe `start /B` (a true Windows-side detach) and use the .bat
# variants under bin/windows/, which translate paths correctly for
# Java (the .sh scripts feed Java a /d/a/... path that fails to
# resolve on Windows).
spawn_kafka() {
  local sh_script="$1" arg_path="$2" log_path="$3" pidfile="$4"
  if is_windows; then
    local bat_name; bat_name="$(basename "${sh_script%.sh}").bat"
    local bat_win; bat_win="$(cygpath -w "$KAFKA_DIR/bin/windows/$bat_name")"
    local arg_win; arg_win="$(cygpath -w "$arg_path")"
    local log_win; log_win="$(cygpath -w "$log_path")"
    cmd.exe //c "start /B \"\" \"$bat_win\" \"$arg_win\" > \"$log_win\" 2>&1"
    # cmd.exe `start` doesn't return a usable PID. Drop a marker file
    # so stop() knows we're on the windows path.
    echo windows > "$pidfile"
  else
    nohup "$sh_script" "$arg_path" </dev/null > "$log_path" 2>&1 &
    echo $! > "$pidfile"
    disown 2>/dev/null || true
  fi
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

  spawn_kafka "$KAFKA_DIR/bin/zookeeper-server-start.sh" \
    "$KAFKA_DIR/config/zk-test.properties" \
    "$KAFKA_LOG_DIRS/zk.log" \
    "$KAFKA_LOG_DIRS/zk.pid"

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

  spawn_kafka "$KAFKA_DIR/bin/kafka-server-start.sh" "$props" \
    "$KAFKA_LOG_DIRS/broker.log" "$KAFKA_LOG_DIRS/broker.pid"

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
  if is_windows; then
    # cmd.exe `start /B` doesn't give us a PID, so kill any java.exe.
    # Fine in CI where we're the only Java workload on the runner.
    taskkill //F //IM java.exe 2>/dev/null || true
    rm -f "$KAFKA_LOG_DIRS/broker.pid" "$KAFKA_LOG_DIRS/zk.pid"
    return
  fi
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
