#!/usr/bin/env bash
# The PostgreSQL endpoint through the PostgreSQL JDBC driver, as Mendix's external database
# connector uses it: metadata, parameterized queries (server-side prepared, binary transfer),
# typed parameters, transactions with a fetch size, errors.
# Usage: DUCKGRAPHLAKE=<path to a DuckGraphLake checkout> server/test/pg_test.sh   (needs Java and Maven)
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
BIN=${BIN:-$HERE/../../build/server/extension/rdf/duckgraphlake}
FIX=${DUCKGRAPHLAKE:?set DUCKGRAPHLAKE to a DuckGraphLake checkout}/server/test/fixture
WORK=$(mktemp -d)
(cd "$FIX/.." && python3 make_data.py fixture >/dev/null)
(cd "$HERE/pgjdbc" && mvn -q dependency:copy-dependencies -DoutputDirectory="$WORK/lib" && javac -d "$WORK" -cp "$WORK/lib/*" PgJdbcTest.java) || exit 1
"$BIN" "$FIX/data" -m "$FIX/config/mapping/*.ttl" -o "$FIX/config/ontology/*.ttl" --pg-port 15433 --sql-port 19494 \
  --http-port 19495 --token pg-test-token >"$WORK/server.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$WORK"' EXIT
for _ in $(seq 1 60); do curl -s -o /dev/null http://127.0.0.1:19495/ && break; sleep 1; done
java -cp "$WORK/lib/*:$WORK" PgJdbcTest 15433 pg-test-token
