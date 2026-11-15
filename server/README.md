# DuckDB + rdf server image

One container that serves mounted data files over DuckDB's Quack protocol, with
SPARQL built in. A client needs only stock DuckDB 2.0.0; the rdf extension runs
in the server.

```
/data   (read-only mount)   Parquet, Iceberg, DuckDB and SQLite files
/config (read-only mount)   server.toml, R2RML/YARRRML mappings, ontology
        │
        ▼
  entrypoint.py ── DuckDB 2.0.0 + rdf, views over /data, SPARQL defaults,
        │          file access limited to /data and /config
        ▼
  Quack on localhost:9494 ── nginx TLS on :9443 ── clients
```

## Run

```bash
docker run -d --name rdf -e QUACK_TOKEN=change-me -p 127.0.0.1:9443:9443 \
  -v /path/to/data:/data:ro -v /path/to/config:/config:ro duck-rdf-server
```

Connect from any DuckDB 2.0.0 (CLI, Python, …):

```sql
LOAD quack;
ATTACH 'quack:127.0.0.1:9443' AS srv (TOKEN 'change-me', SSL_FINGERPRINT '<from the container log>');
CONNECT srv;

SELECT * FROM emp;                                -- plain SQL over the mounted files
SELECT * FROM sparql('SELECT ?n ?s WHERE { ?p <http://example.com/ns#name> ?n ;
                                             <http://example.com/ns#salary> ?s }');
```

`sparql(query)` is a macro over `execute_sparql(query, <the configured mapping>)`.
`execute_sparql` itself works too, with an explicit mapping path under `/config`.

## What gets served

With no configuration, everything under `/data` is exposed:

| Mounted | Becomes |
|---|---|
| `name.parquet`, or a directory of Parquet files | view `name` |
| a directory with `metadata/` (an Iceberg table) | view `name` (newest snapshot) |
| `name.sqlite`, `.sqlite3`, `.db` | attached database `name`, read-only |
| `name.duckdb` | attached database `name`, read-only |

Tables in attached databases also get a plain view of the same name, so a
mapping can say `emp` rather than `hr.emp`. `[[views]]` in `server.toml` adds
hand-written views. See `server.example.toml` for every option.

## Security

- **Token:** Quack requires the token (`QUACK_TOKEN`) on every connection.
- **TLS:** the container serves TLS on 9443 through nginx; Quack itself only listens on the container's localhost, as DuckDB recommends. Mount `/config/tls/cert.pem` and `key.pem` for a real certificate. Without them a self-signed one is generated, and its SHA-256 fingerprint is printed in the log for the client's `SSL_FINGERPRINT`.
- **Lock-down:** after start-up, DuckDB can read only `/data` and `/config` (`allowed_directories` with `enable_external_access = false`), and `lock_configuration` freezes all settings, so a client cannot widen access. Mount both directories read-only.

## Build

The image bakes in every extension, so a running container downloads nothing.

```bash
# 1. Build the rdf extension against DuckDB 2.0.0 (repository root)
OVERRIDE_GIT_DESCRIBE=v2.0.0 GEN=ninja make release
# 2. Stage it and the core extensions it needs into server/dist/
server/stage.sh
# 3. Build the image
docker build -t duck-rdf-server server/
```

`test/smoke_test.sh` builds the image, starts it on a small fixture (one file of
each kind) and checks SQL, SPARQL, reasoning, typed results and the lock-down
from a plain DuckDB client.
