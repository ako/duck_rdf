# duckgraphlake: a folder of data files as SQL, SPARQL and MCP endpoints

One executable for development and testing: point it at a folder of Parquet, CSV, JSON,
Iceberg, SQLite and DuckDB files, and an R2RML/YARRRML mapping, and it serves

| Endpoint | Where | For |
| --- | --- | --- |
| SQL | `postgresql://127.0.0.1:5433` (PostgreSQL wire protocol, DuckDB's SQL) | Mendix's external database connector, psql, any PostgreSQL driver |
| SQL | `quack:127.0.0.1:9494` (DuckDB's Quack protocol) | DuckDB CLI, Python, DuckDB's JDBC driver |
| SPARQL | `http://127.0.0.1:9495/sparql` (SPARQL 1.1 Protocol) | RDF tools, REST calls from apps, the query page at `/` |
| OData | `http://127.0.0.1:9495/odata` (OData v4, read-only) | Mendix external entities, any OData v4 client |
| MCP | `http://127.0.0.1:9495/mcp` (streamable HTTP, JSON responses) | agents |

It is DuckDB 2.0.0 with the rdf extension, Quack, httpfs, SQLite, Parquet and JSON compiled in. Iceberg
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

Options (`duckgraphlake --help`): `--init setup.sql` (a SQL script run after discovery, e.g. views
the mapping reads), `--config server.toml` (the Docker image's format, see
DuckGraphLake's `server/server.example.toml`; flags override it), `--host`, `--http-port`,
`--pg-port` (0 turns it off), `--sql-port`, `--token` (or `DUCKGRAPHLAKE_TOKEN`),
`--require-token`, `--no-lock-down`.

On a loopback address the token defaults to `duckgraphlake` and SPARQL and MCP need no token.
On any other address a token is required, and SPARQL and MCP require it too (as
`Authorization: Bearer <token>`) unless the config file says `anonymous = true`.

By default SQL clients can read only the data, mapping, ontology and config folders and cannot
change settings (`lock_down`).

## Web UI

`http://127.0.0.1:9495/` is a small catalog and query page, compiled into the binary (no CDN, so
it works offline):

- **Catalog.** The classes of the graph, with their labels and comments from the ontology, super-
  and subclasses, properties with their types, relationships, the number of instances and a sample.
  It also lists the tables and views the folder produced, with their columns and first rows.
- **Query.** SPARQL (through `/sparql`) and SQL, with results as a table.
- **Connect.** Every endpoint's address, to copy, and the server's settings.

Its data comes from `/api/catalog`, `/api/table` and `/api/sql`, plus `/odata` and `/sparql`; they
need the token wherever SPARQL does, and the page asks for it then. YASGUI is still at `/yasgui`.

## Connect

SQL from a PostgreSQL client, such as Mendix's external database connector (PostgreSQL) or psql:
host `127.0.0.1`, port `5433`, any database and user name, the token as password, no SSL.

```
jdbc:postgresql://127.0.0.1:5433/lake?sslmode=disable
psql -h 127.0.0.1 -p 5433 -U dev lake
```

The SQL is DuckDB's, which is close to PostgreSQL's; `sparql('...')` works here too, also with
the query as a parameter (`SELECT * FROM sparql(?)`). What the endpoint covers: the startup
handshake (password = token; SSL is refused, so set `sslmode=disable` or `prefer`), simple and
extended queries with parameters, text and binary formats for the common types, cursors with a
fetch size, cancel requests, and the session commands drivers send (`SET`, `SHOW`, `BEGIN`, ...).
The PostgreSQL JDBC driver's metadata calls (`getTables`, `getColumns`, `getPrimaryKeys`,
`getTypeInfo`, foreign keys) are answered from DuckDB's catalog with PostgreSQL types; they list
the server's own views and tables. psql's backslash commands (`\dt`) do not work yet.

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

OData: `http://127.0.0.1:9495/odata`, like Graph Studio's OData APIs: one entity set per class
of the graph, not per table. The model comes from the mapping and the ontology (the extension's
`rdf_mapping_schema`), so it needs no scan of the data:

- **Entity sets and keys.** Each class (asserted by the mapping or implied by the ontology) is an
  entity set and type, named after the class's local name. The key `ID` is the resource's IRI:
  `GET /odata/Employee('http://data.example.com/person/10')`.
- **Properties.** Each literal property of the class's instances is a typed property (Edm types from
  the XSD datatypes: `xsd:integer` -> `Edm.Int64`, `xsd:decimal` -> `Edm.Decimal`, `xsd:date` ->
  `Edm.Date`, ...). A subclass inherits its superclasses' properties.
- **Navigation properties.** Each reference to another class is a navigation property. When a
  reference can point to several classes, its target is the class that covers them all, or else
  the most specific one.
- **Queries.** `$filter` (comparisons, `and`/`or`/`not`, `null`, string functions, `year`/`month`/`day`,
  and one level of navigation such as `worksFor/name eq 'Acme'`), `$select`, `$orderby`, `$top`,
  `$skip`, `$count`, `$expand` (with a nested `$select`), the `/$count` segment, and navigation
  (`Employee('...')/worksFor`).
- **Execution.** Each request becomes a SPARQL query through the same translator as `/sparql`. Pages
  hold at most 1000 entities (`[odata] max_page_size`), with an `@odata.nextLink` when a request
  without `$top` has more.
- **Read-only.** The `Capabilities` annotations in `$metadata` say so.

If a property has several values for one entity, the entity shows one of them.

MCP: add `http://127.0.0.1:9495/mcp` as a streamable HTTP server in the agent's configuration.
Tools: `list_classes`, `describe_class`, `find_entity`, `describe_entity`, `sparql`, plus one per
`[[mcp.templates]]` entry in the config file.

## Build

```bash
server/build.sh          # build/server/extension/rdf/duckgraphlake
```

The same prerequisites as the extension (see the root README). `server/extension_config.cmake`
compiles in httpfs, Quack and SQLite (fetched from their DuckDB repositories at the commits
DuckDB 2.0.0 pins); the server itself fetches cpp-httplib and toml++. A first build takes
about an hour on 4 cores.

Tests (both need a DuckGraphLake checkout, for its fixture):

- `DUCKGRAPHLAKE=<checkout> server/test/smoke_test.sh` runs the Docker image's SQL (Quack),
  SPARQL and MCP test clients against the binary.
- `DUCKGRAPHLAKE=<checkout> server/test/odata_test.sh` runs `server/test/odata/ODataTest.java`
  through Apache Olingo's OData v4 client: the metadata model, queries, keys, navigation and
  `$expand`. Needs Java and Maven.
- `DUCKGRAPHLAKE=<checkout> server/test/pg_test.sh` runs `server/test/pgjdbc/PgJdbcTest.java`
  through the PostgreSQL JDBC driver: metadata, parameterized queries (repeated, so the driver
  switches to server-side prepared statements and binary transfer), typed parameters,
  transactions with a fetch size, errors. Needs Java and Maven.

## Known limitations

- **No query parameters over Quack.** Quack (DuckDB 2.0.0) does not forward bound parameters:
  `SELECT ... WHERE id = ?` fails with "Values were not provided for the following parameters".
  Use the PostgreSQL endpoint for parameterized queries.
- No TLS, no hot reload of the mapping or the data folder yet: restart after changing them.
