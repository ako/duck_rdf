# duckgraphlake: a folder of data files as SQL, SPARQL and MCP endpoints

One executable for development and testing: point it at a folder of Parquet, CSV, JSON,
Iceberg, SQLite and DuckDB files, and an R2RML/YARRRML mapping, and it serves

| Endpoint | Where | For |
| --- | --- | --- |
| SQL | `quack:127.0.0.1:9494` (DuckDB's Quack protocol) | DuckDB CLI, Python, JDBC (e.g. Mendix's external database connector) |
| SPARQL | `http://127.0.0.1:9495/sparql` (SPARQL 1.1 Protocol) | RDF tools, REST calls from apps, the query page at `/` |
| MCP | `http://127.0.0.1:9495/mcp` (streamable HTTP, JSON responses) | agents |

It is DuckDB 2.0.0 with the rdf extension, Quack, SQLite, Parquet and JSON compiled in. Iceberg
is DuckDB's own extension, installed from DuckDB's repository the first time a folder holds an
Iceberg table. It is the same server as the Docker image in DuckGraphLake (`server/`), without
TLS: it is meant for a developer's machine.

## Run

```bash
duckgraphlake ./data --mapping 'mapping/*.ttl' --ontology 'ontology/*.ttl'
```

What it finds in `./data`:

| Entry | Becomes |
| --- | --- |
| `name.parquet`, `name.csv`, `name.tsv`, `name.json`, `name.jsonl` (also `.gz`, `.zst`) | view `name` |
| a folder of Parquet, CSV or JSON files (any depth) | view named after the folder |
| a folder with `metadata/` (an Iceberg table) | view named after the folder, newest snapshot |
| `name.sqlite`, `name.sqlite3`, `name.db` | attached database `name`, each table also as a plain view |
| `name.duckdb` | attached database `name`, each table also as a plain view |

Options (`duckgraphlake --help`): `--config server.toml` (the Docker image's format, see
DuckGraphLake's `server/server.example.toml`; flags override it), `--host`, `--http-port`,
`--sql-port`, `--token` (or `DUCKGRAPHLAKE_TOKEN`), `--no-lock-down`.

On a loopback address the token defaults to `duckgraphlake` and SPARQL and MCP need no token.
On any other address a token is required, and SPARQL and MCP require it too (as
`Authorization: Bearer <token>`) unless the config file says `anonymous = true`.

By default SQL clients can read only the data, mapping, ontology and config folders and cannot
change settings (`lock_down`).

## Connect

SQL from DuckDB:

```sql
LOAD quack;
ATTACH 'quack:127.0.0.1:9494' AS lake (TOKEN 'duckgraphlake');
USE lake;
SELECT * FROM emp;
SELECT * FROM sparql('SELECT ?n WHERE { ?p <http://example.com/ns#name> ?n }');
```

SQL from JDBC (DuckDB's driver, `org.duckdb:duckdb_jdbc` 2.0.0.x): the URL is
`jdbc:duckdb:quack:127.0.0.1:9494`. The driver reads the token from a DuckDB secret, created once
on the client machine, for example with the DuckDB CLI:

```sql
CREATE PERSISTENT SECRET lake (TYPE quack, TOKEN 'duckgraphlake');
```

SPARQL:

```bash
curl --data-urlencode 'query=SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 5' http://127.0.0.1:9495/sparql
```

MCP: add `http://127.0.0.1:9495/mcp` as a streamable HTTP server in the agent's configuration.
Tools: `list_classes`, `describe_class`, `find_entity`, `describe_entity`, `sparql`, plus one per
`[[mcp.templates]]` entry in the config file.

## Build

```bash
GEN=ninja EXT_FLAGS="-DRDF_BUILD_SERVER=ON" \
  EXTRA_EXTENSION_CONFIGS="$PWD/server/extension_config.cmake" make release
./build/release/duckgraphlake --help
```

The same prerequisites as the extension (see the root README). `server/extension_config.cmake`
adds Quack and SQLite (fetched from their DuckDB repositories at the commits DuckDB 2.0.0 pins);
the server itself fetches cpp-httplib and toml++.
