#!/usr/bin/env bash
# The OData endpoint through Apache Olingo's OData v4 client (Java, as Mendix's runtime): the
# metadata model, $filter/$orderby/$select/$count/$top/$skip, navigation filters, keys,
# navigation, $expand.
# Usage: DUCKGRAPHLAKE=<path to a DuckGraphLake checkout> server/test/odata_test.sh   (needs Java and Maven)
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
BIN=${BIN:-$HERE/../../build/server/extension/rdf/duckgraphlake}
FIX=${DUCKGRAPHLAKE:?set DUCKGRAPHLAKE to a DuckGraphLake checkout}/server/test/fixture
WORK=$(mktemp -d)
(cd "$FIX/.." && python3 make_data.py fixture >/dev/null)
(cd "$HERE/odata" && mvn -q dependency:copy-dependencies -DoutputDirectory="$WORK/lib" && javac -d "$WORK" -cp "$WORK/lib/*" ODataTest.java) || exit 1
"$BIN" "$FIX/data" -m "$FIX/config/mapping/*.ttl" -o "$FIX/config/ontology/*.ttl" --pg-port 0 --sql-port 19494 \
  --http-port 19495 >"$WORK/server.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$WORK"' EXIT
for _ in $(seq 1 60); do curl -s -o /dev/null http://127.0.0.1:19495/ && break; sleep 1; done
java -cp "$WORK/lib/*:$WORK" ODataTest http://127.0.0.1:19495/odata
