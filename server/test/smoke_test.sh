#!/usr/bin/env bash
# Run DuckGraphLake's server smoke-test clients (SQL over Quack, SPARQL, MCP) against the binary.
# Usage: DUCKGRAPHLAKE=<path to a DuckGraphLake checkout> server/test/smoke_test.sh
set -uo pipefail
BIN=${BIN:-$(cd "$(dirname "$0")/../.." && pwd)/build/server/extension/rdf/duckgraphlake}
TEST=${DUCKGRAPHLAKE:?set DUCKGRAPHLAKE to a DuckGraphLake checkout}/server/test
LOG=$(mktemp)
cd "$TEST"
python3 make_data.py fixture >/dev/null
"$BIN" fixture/data -c fixture/config/server.toml -m "$TEST/fixture/config/mapping/*.ttl" \
  -o "$TEST/fixture/config/ontology/*.ttl" --pg-port 0 --sql-port 19494 --http-port 19495 \
  --token smoke-test-token --require-token >"$LOG" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -f "$LOG"' EXIT
for _ in $(seq 1 60); do curl -s -o /dev/null http://127.0.0.1:19495/ && break; sleep 1; done
cat "$LOG"
status=0
python3 client.py 127.0.0.1:19494 smoke-test-token || status=1
# The SPARQL client takes a CA file for https; any one will do over http.
CA=/etc/ssl/certs/ca-certificates.crt
[ -f "$CA" ] || CA=/etc/ssl/cert.pem   # macOS
python3 sparql_client.py http://127.0.0.1:19495/sparql smoke-test-token "$CA" || status=1
python3 mcp_client.py http://127.0.0.1:19495/mcp smoke-test-token || status=1
# The web UI: its page, and its catalog API (classes from the mapping, sources from discovery).
curl -sf http://127.0.0.1:19495/ | grep -q '<title>duckgraphlake</title>' && echo "PASS web UI page" || { echo "FAIL web UI page"; status=1; }
curl -sf -H 'Authorization: Bearer smoke-test-token' http://127.0.0.1:19495/api/catalog | python3 -c '
import json, sys
c = json.load(sys.stdin)
classes = {x["name"] for x in c["classes"]}
sources = {x["name"] for x in c["sources"]}
ok = {"Employee", "Party"} <= classes and {"emp", "payment", "orders"} <= sources
print("PASS" if ok else "FAIL", "web UI catalog", sorted(classes), sorted(sources))
sys.exit(0 if ok else 1)' || status=1
exit $status
