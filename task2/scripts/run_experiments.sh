#!/usr/bin/env bash
# Run all 8 Kafka throughput/latency experiments sequentially, then generate report.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TASK2_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$TASK2_DIR/docker-compose.yml"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# Experiment matrix: "NUM_PRODUCERS NUM_PARTITIONS NUM_CONSUMERS"
EXPERIMENTS=(
    "1 1  1"
    "1 1  2"
    "1 2  2"
    "1 5  5"
    "1 10 1"
    "1 10 5"
    "1 10 10"
    "2 10 10"
)

: "${NUM_MESSAGES:=200}"
: "${FAKER_SEED:=42}"

# Build images once before the experiment loop (include two-producers profile for exp08)
log "Building images..."
docker compose -f "$COMPOSE_FILE" --profile two-producers build producer producer2 consumer aggregator

IDX=1
for EXP in "${EXPERIMENTS[@]}"; do
    read -r NP NPART NC <<< "$EXP"
    LABEL=$(printf "exp%02d_%dp_%dpart_%dc" "$IDX" "$NP" "$NPART" "$NC")

    log "=== $LABEL  (producers=$NP  partitions=$NPART  consumers=$NC) ==="

    # Create per-experiment log directory and write metadata
    mkdir -p "$TASK2_DIR/logs/$LABEL"
    printf '{"exp_label":"%s","num_producers":%d,"num_partitions":%d,"num_consumers":%d}\n' \
        "$LABEL" "$NP" "$NPART" "$NC" \
        > "$TASK2_DIR/logs/$LABEL/meta.json"

    # Tear down any previous stack (volumes removed to reset Redpanda state)
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true

    # Export vars for docker compose interpolation
    export NUM_PRODUCERS="$NP"
    export NUM_PARTITIONS="$NPART"
    export NUM_MESSAGES
    export FAKER_SEED
    export EXP_LABEL="$LABEL"

    # Start services in the background.
    # Experiment 08 uses two explicitly separate producer services (producer + producer2)
    # so each gets a unique PRODUCER_ID env var. docker compose --scale gives all
    # replicas the same env vars, making it unsuitable for the 2-producer split.
    if [ "$NP" -eq 2 ]; then
        docker compose -f "$COMPOSE_FILE" --profile two-producers up -d \
            --no-build \
            --scale consumer="$NC" \
            redpanda producer producer2 consumer
    else
        docker compose -f "$COMPOSE_FILE" up -d \
            --no-build \
            --scale consumer="$NC" \
            redpanda producer consumer
    fi

    # Wait for all producer containers to finish
    PRODUCER_IDS=$(docker compose -f "$COMPOSE_FILE" ps -q producer 2>/dev/null || true)
    if [ "$NP" -eq 2 ]; then
        PRODUCER2_IDS=$(docker compose -f "$COMPOSE_FILE" --profile two-producers ps -q producer2 2>/dev/null || true)
        PRODUCER_IDS="$PRODUCER_IDS $PRODUCER2_IDS"
    fi
    if [ -n "$(echo "$PRODUCER_IDS" | tr -d ' ')" ]; then
        # shellcheck disable=SC2086
        docker wait $PRODUCER_IDS > /dev/null
        log "All producers done"
    fi

    # Wait for all consumer containers to finish
    CONSUMER_IDS=$(docker compose -f "$COMPOSE_FILE" ps -q consumer 2>/dev/null || true)
    if [ -n "$CONSUMER_IDS" ]; then
        # shellcheck disable=SC2086
        docker wait $CONSUMER_IDS > /dev/null
        log "All consumers done"
    fi

    log "=== $LABEL complete ==="
    IDX=$((IDX + 1))
done

log "All experiments done. Running aggregator..."

docker compose -f "$COMPOSE_FILE" --profile aggregate run --rm \
    -e LOG_DIR=/logs \
    -e REPORT_DIR=/reports \
    aggregator

log "Reports saved to $TASK2_DIR/reports/"
log "  throughput.png"
log "  latency.png"
log "  summary.csv"
