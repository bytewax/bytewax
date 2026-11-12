#!/usr/bin/env bash
# Run the Kafka connectivity fault gates against a 3-broker KRaft
# cluster behind Toxiproxy. Usage (from the repo root):
#
#   scripts/kafka-faults/run.sh [extra pytest args, e.g. --runxfail]
#
# Brings the compose stack up, waits for the brokers and the Toxiproxy
# API, runs pytests/connectors/test_kafka_faults.py, and always tears
# the stack down on exit.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_DIR="$ROOT/scripts/kafka-faults"
COMPOSE=(docker compose -p bwkfaults -f "$COMPOSE_DIR/docker-compose.yml")
TOXIPROXY_API="http://localhost:39000"
# Ports published by compose. All must be free before we start.
PORTS=(39000 39101 39102 39103)

export BYTEWAX_KAFKA_FAULTS=1
export BYTEWAX_KAFKA_FAULTS_BOOTSTRAP="localhost:39101,localhost:39102,localhost:39103"
export BYTEWAX_KAFKA_FAULTS_TOXIPROXY="$TOXIPROXY_API"

cleanup() {
  "${COMPOSE[@]}" down -v --remove-orphans --timeout 20 >/dev/null 2>&1 || true
}
trap cleanup EXIT

for port in "${PORTS[@]}"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "port $port is already in use; refusing to start" >&2
    exit 2
  fi
done

echo "images: $("${COMPOSE[@]}" config --images | sort -u | tr '\n' ' ')"
"${COMPOSE[@]}" up -d --force-recreate

# Every broker must have registered with the KRaft controllers and be
# unfenced. The check runs inside a container on the INTERNAL listener:
# a client that connects there gets the INTERNAL advertised addresses,
# which resolve inside the containers. The CLIENT addresses (localhost
# ports) do not. A fenced broker is missing from the metadata, so it has
# no API-versions block. The "-> (" suffix marks a block without an error.
deadline=$((SECONDS + 180))
until
  ready="$(docker exec bwkf-kafka1 /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server kafka1:9092 2>/dev/null | grep -c ' -> ($' || true)"
  [[ "$ready" -ge 3 ]]
do
  if (( SECONDS > deadline )); then
    echo "Kafka cluster did not become ready in time" >&2
    "${COMPOSE[@]}" logs --tail 80 >&2 || true
    exit 1
  fi
  sleep 2
done
echo "kafka cluster ready: 3 brokers registered and unfenced"

deadline=$((SECONDS + 60))
until curl -fsS "$TOXIPROXY_API/proxies" 2>/dev/null | grep -q '"kafka3"'; do
  if (( SECONDS > deadline )); then
    echo "Toxiproxy API did not become ready in time" >&2
    "${COMPOSE[@]}" logs --tail 80 toxiproxy >&2 || true
    exit 1
  fi
  sleep 1
done
echo "toxiproxy ready at $TOXIPROXY_API"

cd "$ROOT"
pytest "$ROOT/pytests/connectors/test_kafka_faults.py" "$@"
