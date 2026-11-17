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
| Kafka | `127.0.0.1:9092` (Kafka protocol, producers) | Mendix Business Events and entity CDC: topics become Iceberg tables |
| Iceberg | `http://127.0.0.1:9495/iceberg` (Iceberg REST catalog) | DuckDB, PyIceberg, Spark: read and write the folder's Iceberg tables |

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

## Kafka in, Iceberg out (a local WarpStream + Tableflow)

In production, the Mendix runtime publishes Business Events and entity-level CDC events (from its
outbox) to a WarpStream agent, and WarpStream Tableflow writes the topics to Iceberg tables in the
data lake. Locally, `duckgraphlake` stands in for both: point the app's Kafka setting at it (for
Business Events, the `ServerUrl` constant: `localhost:9092`) and each topic becomes an Iceberg table in
the data folder, queryable over SQL, SPARQL (with a mapping), OData and the web UI within a second.
For entity CDC, set the configuration's Kafka bootstrap servers (the `Kafka.BootstrapServers` runtime
setting) to the same address.

```bash
duckgraphlake ./data --tableflow tableflow.yaml
```

- **The protocol: producers only.**
  - **Requests:** ApiVersions, Metadata (topics are created on first use), InitProducerId (idempotent
    producers, the Java client's default), CreateTopics and Produce.
  - **Record batches:** format v2, with no compression, gzip or zstd. snappy and lz4 are refused with
    `UNSUPPORTED_COMPRESSION_TYPE`.
  - **Not offered:** consuming (Fetch, consumer groups) and transactions.
  - **Clients tested:** the Apache Kafka Java client 3.9 and librdkafka 2.x.
- **Tables shaped like Tableflow's.** `--tableflow` reads a WarpStream Tableflow YAML file. For each of
  its `tables`, the subset it uses is:
  - `source_cluster_name`, `source_topic` and `name`;
  - `source_format: json` and `schema_mode: inline`, with an `input_schema` (JSON Schema);
  - `field_type_remapping` and `skip_raw_record_values`.

  Other settings (`partitioning_scheme`, `transforms`, `dlq_mode`, ...) are reported at start-up and
  not applied. A topic's table:
  - **Name:** `<cluster>__<topic>`, or the configured `name`.
  - **Location:** `<data>/warpstream/_tableflow/<table>/`, in the Iceberg catalog namespace `tableflow`.
  - **Columns:** one for each property of the JSON Schema, parsed from the record's JSON value:
    - `integer` → BIGINT, `number` → DOUBLE, `boolean` → BOOLEAN;
    - `string` → VARCHAR, or DATE, TIMESTAMPTZ, TIME or UUID by `format`;
    - objects → STRUCT (MAP with `additionalProperties`), arrays → LIST.
  - **Metadata:** a `warpstream` STRUCT(partition, offset, key, value, timestamp, headers). `headers`
    is a MAP(VARCHAR, VARCHAR) of the record's headers: for a Mendix business event,
    `warpstream.headers['ce_type']` is the event's type (its message name) and `ce_subject` its
    subject. A repeated header keeps its last value; a value that is not UTF-8 is NULL. Tableflow's
    struct has no `headers` field; tables written before it was added keep their shape.
  - **Unconfigured topics:** only the `warpstream` struct.
  - **Unparseable records:** a record that is not JSON, or not of the schema's shape, keeps NULL
    fields and its raw `warpstream.value`. That's like Tableflow's `dlq_mode: keep`; its default stops
    instead.
- **Change events get a column per field.** A topic of change events (the Debezium envelope as Kafka
  Connect's JSON converter writes it, schema embedded: `{"schema": ..., "payload": {"before", "after",
  "op", "ts_ms", "source"}}`) that the Tableflow file does not configure is unwrapped. Mendix's entity
  CDC publishes exactly this, one topic per entity (`cdc.<app>.<entity>.<major revision>.<space>`), so
  each entity gets a table such as `local__cdc.TestApp.Order.1.local`:
  - **Columns:** the entity's fields from the record's own schema: `id` (the Mendix object id), the
    attributes, and each association as a list of object ids. `int*` → TINYINT..BIGINT, `float*` →
    FLOAT/DOUBLE, `boolean`, `string` → VARCHAR, Decimal → DECIMAL(p, s), Timestamp → TIMESTAMP, Date
    → DATE, arrays → LIST, structs → STRUCT. A delete's row holds the values from `before`.
  - **`cdc`:** STRUCT(op, ts, source_ts, tx_id, sequence, snapshot, connector, version); `op` is `c`, `u`,
    `d`, or `r` for a snapshot read.
  - **New attributes:** a later minor revision of a CDC service that adds an attribute adds a column.
  - **`<table>_current`:** a view with the latest row per record key (the object id), deleted objects
    left out: the entity's current state.
  - `[kafka] cdc = false` (or `--no-kafka-cdc`) keeps change events as raw values only.
- **Durable, then committed.**
  - **Acknowledgement:** a produce is acknowledged once its records are in a buffer file
    (`<data>/.duckgraphlake/kafka.duckdb`).
  - **Commit:** every `[kafka] flush_ms` (1000), each topic's buffered records are committed to its
    table as one Iceberg snapshot.
  - **Crashes and restarts:** a crash loses nothing that was acknowledged, and offsets carry on after a
    restart.
  - **The data folder:** nothing is written to it until the first topic.
- **The tables are plain Iceberg.**
  - **Writing:** a second, internal DuckDB commits them through the server's own Iceberg REST catalog
    (`/iceberg`), which DuckDB, PyIceberg and Spark can also use to read and write the folder:
    `ATTACH '' AS lake (TYPE iceberg, ENDPOINT 'http://127.0.0.1:9495/iceberg', TOKEN 'duckgraphlake')`.
  - **Reading without the catalog:** each commit also updates `metadata/version-hint.text`, so
    `iceberg_scan('<folder>')` reads the newest snapshot.

Settings (`[kafka]` in the config file): `port` (9092; `--kafka-port 0` turns it off),
`advertised_host`, `cluster` (`local`), `partitions` (1), `flush_ms` (1000), `tableflow`, `cdc` (true).

Not verified against WarpStream itself: the `warpstream` struct's field types (from WarpStream's
documentation), what Tableflow does with JSON properties outside the schema (here: ignored), and how
Mendix's own Iceberg bridge lays out CDC tables (the unwrapped shape above is this server's own). The
CDC message format was checked against a Mendix 11.16.1 runtime.

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

On macOS (Apple Silicon or Intel), Homebrew is enough: `brew install cmake ninja openssl@3` (libxml2 and
curl come with macOS), then
`EXTRA_CMAKE_FLAGS="-DOPENSSL_ROOT_DIR=$(brew --prefix openssl@3)" server/build.sh`. If CMake's
compiler check fails with `ld: tapi error: malformed file ... unknown architecture`, Xcode and the
Command Line Tools are at different versions; build with one of them throughout, e.g.
`DEVELOPER_DIR=/Library/Developer/CommandLineTools`.

Tests (both need a DuckGraphLake checkout, for its fixture):

- `DUCKGRAPHLAKE=<checkout> server/test/smoke_test.sh` runs the Docker image's SQL (Quack),
  SPARQL and MCP test clients against the binary.
- `DUCKGRAPHLAKE=<checkout> server/test/odata_test.sh` runs `server/test/odata/ODataTest.java`
  through Apache Olingo's OData v4 client: the metadata model, queries, keys, navigation and
  `$expand`. Needs Java and Maven.
- `server/test/kafka_test.sh` produces with the Kafka Java client (and librdkafka, when Python's
  `confluent-kafka` is installed) and checks the tables, change events and headers, a restart and a crash. It needs Java and
  Maven, but no DuckGraphLake checkout.
- `DUCKGRAPHLAKE=<checkout> server/test/pg_test.sh` runs `server/test/pgjdbc/PgJdbcTest.java`
  through the PostgreSQL JDBC driver: metadata, parameterized queries (repeated, so the driver
  switches to server-side prepared statements and binary transfer), typed parameters,
  transactions with a fetch size, errors. Needs Java and Maven.

## Known limitations

- **No query parameters over Quack.** Quack (DuckDB 2.0.0) does not forward bound parameters:
  `SELECT ... WHERE id = ?` fails with "Values were not provided for the following parameters".
  Use the PostgreSQL endpoint for parameterized queries.
- No TLS, no hot reload of the mapping or the data folder yet: restart after changing them.
