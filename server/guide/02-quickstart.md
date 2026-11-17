# quickstart: install, run, connect

## Install

duckgraphlake is a single executable with everything compiled in (DuckDB, the rdf extension,
Quack, SQLite, Parquet, JSON, httpfs). Put it on the PATH:

```bash
chmod +x duckgraphlake && mv duckgraphlake /usr/local/bin/
```

On macOS, a downloaded binary is quarantined; clear that once:

```bash
xattr -d com.apple.quarantine /usr/local/bin/duckgraphlake
```

The first Kafka topic installs DuckDB's iceberg extension, which needs internet access once
(it is then cached in ~/.duckdb).

## Run

```bash
mkdir -p lake && duckgraphlake ./lake
```

It serves the folder until stopped (Ctrl-C), logs what it found and every endpoint, and writes
nothing into the folder until data arrives. Everything it writes goes to `warpstream/` (topic
tables) and `.duckgraphlake/` (its buffer, mappings and views).

Useful options (all of them: `duckgraphlake --help`; a config file: `duckgraphlake help config`):

- `-m GLOB` an R2RML (.ttl) or YARRRML (.yml) mapping of your own (more in `help graph`);
- `-o GLOB` an ontology (class hierarchy, inverses, transitivity);
- `-i FILE` a SQL script run at start-up and again whenever it changes;
- `-c FILE` a server.toml;
- `--host 0.0.0.0 --token SECRET` to serve other machines (a token is then required).

## Check it

```bash
curl -s {{http}}/api/catalog
psql "host={{host}} port={{pg_port}} user=dev password={{token}} dbname=lake" -c "SELECT 42"
```

## Connect an app

- Mendix: `duckgraphlake help mendix`.
- Any Kafka producer: `duckgraphlake help kafka`.
- An agent (Claude Code, Cursor, ...): add `{{http}}/mcp` as a streamable HTTP MCP server, e.g.
  `claude mcp add --transport http duckgraphlake {{http}}/mcp`. Then `help agents`.
