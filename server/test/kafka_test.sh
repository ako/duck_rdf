#!/usr/bin/env bash
# The Kafka endpoint through the Apache Kafka Java client (3.9, as in the Mendix runtime): produce with
# the client's defaults (idempotent, acks=all) and gzip/zstd, then check the Iceberg tables shaped like
# WarpStream Tableflow's, a restart, and a crash before a commit. Needs Java and Maven; checks
# librdkafka too when Python's confluent-kafka is installed.
# Usage: server/test/kafka_test.sh
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
BIN=${BIN:-$HERE/../../build/server/extension/rdf/duckgraphlake}
WORK=$(mktemp -d)
DATA="$WORK/data"
mkdir -p "$DATA"
(cd "$HERE/kafka" && mvn -q dependency:copy-dependencies -DoutputDirectory="$WORK/lib" && javac -d "$WORK" -cp "$WORK/lib/*" KafkaProduceTest.java) || exit 1
start() {
  "$BIN" "$DATA" -t "$HERE/kafka/tableflow.yaml" --pg-port 0 --sql-port 19494 --http-port 19495 --kafka-port 19092 "$@" >>"$WORK/server.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 60); do curl -s -o /dev/null http://127.0.0.1:19495/ && break; sleep 1; done
}
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$WORK"' EXIT
status=0
start
java -cp "$WORK/lib/*:$WORK" KafkaProduceTest 127.0.0.1:19092 1000 2>/dev/null | grep -E "^(orders|mendix|snappy)"
sleep 3
python3 "$HERE/kafka/check.py" first || status=1
kill $PID; wait $PID 2>/dev/null
start
java -cp "$WORK/lib/*:$WORK" KafkaProduceTest 127.0.0.1:19092 10 2>/dev/null | grep -E "^orders"
sleep 3
python3 "$HERE/kafka/check.py" restarted || status=1
# A crash before the commit: flush once a minute, produce, kill -9, restart.
kill $PID; wait $PID 2>/dev/null
printf '[kafka]\nflush_ms = 60000\n' >"$WORK/slow.toml"
start -c "$WORK/slow.toml"
java -cp "$WORK/lib/*:$WORK" KafkaProduceTest 127.0.0.1:19092 18 2>/dev/null | grep -E "^orders"
kill -9 $PID; wait $PID 2>/dev/null
start
sleep 3
python3 "$HERE/kafka/check.py" crashed || status=1
if python3 -c "import confluent_kafka" 2>/dev/null; then
  python3 - <<'PY' || status=1
from confluent_kafka import Producer
ok = True
for codec in ["none", "gzip", "zstd"]:
    p = Producer({"bootstrap.servers": "127.0.0.1:19092", "enable.idempotence": True, "compression.type": codec})
    results = []
    for i in range(20):
        p.produce("orders", value='{"id": %d}' % i, on_delivery=lambda err, msg: results.append(err))
    p.flush(15)
    good = len(results) == 20 and not any(results)
    ok = ok and good
    print("PASS" if good else "FAIL", "librdkafka produces with compression", codec, results[:1])
raise SystemExit(0 if ok else 1)
PY
fi
[ $status = 0 ] && echo "0 failure(s)" || { echo "failures; server log:"; tail -20 "$WORK/server.log"; }
exit $status
