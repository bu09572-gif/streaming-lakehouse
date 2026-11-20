#!/usr/bin/env bash
# Full check: produce half the changes, stream, validate; produce the rest, start the
# stream and kill it partway, restart it, validate again.
#   ./run_demo.sh 2        # February (73,513 changes)
#   ./run_demo.sh          # whole year (1,000,898 changes)
set -euo pipefail
cd "$(dirname "$0")"
MONTH=${1:-}
M=${MONTH:+--month $MONTH}
PY=${PYTHON:-python}
LAKE=${LAKE_DIR:-../../../data/lakehouse}
KAFKA_BIN=${KAFKA_BIN:-}

if [ -n "$KAFKA_BIN" ]; then
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "${KAFKA_BOOTSTRAP:-localhost:9092}" \
    --delete --topic "${KAFKA_TOPIC:-flight-events}" 2>/dev/null || true
  sleep 3
fi
rm -rf "$LAKE"

$PY producer.py $M --part 1/2
$PY stream.py
$PY validate.py $M --upto 1/2

$PY producer.py $M --part 2/2
MAX_OFFSETS_PER_BATCH=${CRASH_BATCH:-20000} $PY stream.py &
PID=$!
sleep "${CRASH_AFTER:-15}"
kill -9 $PID 2>/dev/null || true
pkill -9 -f "SparkSubmit.*pyspark-shell" 2>/dev/null || true
sleep 2
echo "killed the stream; restarting"
$PY stream.py
$PY validate.py $M --upto 2/2
