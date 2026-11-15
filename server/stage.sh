#!/usr/bin/env bash
# Put the extensions the image needs into dist/:
#   - rdf.duckdb_extension, built from this repository against DuckDB 2.0.0
#     (run `OVERRIDE_GIT_DESCRIBE=v2.0.0 GEN=ninja make release` first)
#   - the official core extensions (quack, iceberg and its dependencies, sqlite),
#     installed with the local Python duckdb==2.0.0 and copied from its cache
set -euo pipefail
cd "$(dirname "$0")"
RDF=${RDF_EXTENSION:-../build/release/extension/rdf/rdf.duckdb_extension}
PLATFORM=${DUCKDB_PLATFORM:-linux_amd64}

[ -f "$RDF" ] || { echo "no rdf extension at $RDF; build it first" >&2; exit 1; }
rm -rf dist && mkdir -p "dist/extensions/v2.0.0/$PLATFORM"
cp "$RDF" dist/rdf.duckdb_extension

python3 - "$PLATFORM" <<'EOF'
import duckdb, os, shutil, sys
platform = sys.argv[1]
c = duckdb.connect()
assert c.sql("SELECT version()").fetchall()[0][0] == 'v2.0.0', "needs Python duckdb==2.0.0"
for ext in ("quack", "iceberg", "avro", "sqlite_scanner"):
    c.execute(f"INSTALL {ext}")
rows = c.sql("SELECT extension_name, install_path FROM duckdb_extensions() WHERE installed AND install_path LIKE '%.duckdb_extension'").fetchall()
target = f"dist/extensions/v2.0.0/{platform}"
for name, path in rows:
    shutil.copy(path, target)
    print("staged", name)
EOF
python3 - <<'EOF'
import duckdb
c = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
c.execute("LOAD 'dist/rdf.duckdb_extension'")
print("rdf extension loads in stock DuckDB", c.sql("SELECT version()").fetchall()[0][0])
EOF
