# config: every setting, as a server.toml

Command-line flags override the file: `duckgraphlake -c server.toml [DATA_DIR]`. Relative paths in
the file are relative to the file. This is the complete file with the defaults; save it with
`duckgraphlake help config > server.toml` and keep the lines you change.

```toml
[server]
host = "127.0.0.1"   # --host; 0.0.0.0 serves other machines and then needs a token
http_port = 9495     # --http-port: SPARQL /sparql, OData /odata, MCP /mcp, Iceberg /iceberg, UI /
sql_port = 9494      # --sql-port: DuckDB's Quack protocol
pg_port = 5433       # --pg-port: PostgreSQL wire protocol (0 turns it off)
# token = "..."      # --token or DUCKGRAPHLAKE_TOKEN; default on loopback: "duckgraphlake"
lock_down = true     # SQL clients may not read files outside the data folder or change settings

[duckdb]
# threads = 4
# memory_limit = "4GB"
# temp_directory = "/tmp/duckgraphlake"

[data]
directory = "."              # DATA_DIR on the command line
auto_discover = true         # a view per data file/folder, attach SQLite/DuckDB files
expose_attached_tables = true  # tables of attached databases also under their plain names
# init_sql = "views.sql"     # -i: run at start-up, and again when it changes

# Views by hand (they win over discovered ones of the same name):
# [[views]]
# name = "big_orders"
# sql = "SELECT * FROM orders WHERE amount > 1000"

[rdf]
# mapping = "mapping/*.ttl"  # -m: R2RML (.ttl) or YARRRML (.yml); reloaded when it changes
# ontology = "ontology.ttl"  # -o: rdfs:subClassOf, domain/range, owl:inverseOf, transitive, symmetric
# unique_keys = "orders(id); customer(id)"  # keys the database cannot declare (faster SPARQL)
typed_results = true         # SPARQL results keep their SQL types
detect_keys = true           # find unique, non-null template columns at start-up

[sparql]
enabled = true
# anonymous = true           # default: anonymous only on a loopback address
timeout_seconds = 60
max_rows = 100000
cors_origin = "*"

[odata]
enabled = true
# anonymous = true
max_page_size = 1000

[mcp]
enabled = true
# anonymous = true
max_rows = 1000
allow_changes = true         # MCP tools may define views and set mappings (kept in the data folder)
# Reviewed SPARQL queries as tools of their own:
# [[mcp.templates]]
# name = "orders_of_customer"
# description = "The orders of one customer"
# query = "SELECT ?o WHERE { ?o <http://example.com/customer> {{customer}} }"
# params = [ { name = "customer", type = "iri", description = "the customer's IRI" } ]

[iceberg]
enabled = true               # the Iceberg REST catalog at /iceberg (the Kafka endpoint needs it)

[kafka]
port = 9092                  # --kafka-port (0 turns it off)
# advertised_host = "..."    # the host producers are told to connect to (default: host)
cluster = "local"            # topic tables are named <cluster>__<topic>
partitions = 1               # for topics created on first use
flush_ms = 1000              # how often buffered records are committed to Iceberg
# tableflow = "tableflow.yaml"  # -t: WarpStream Tableflow table names and JSON schemas per topic
cdc = true                   # --no-kafka-cdc: change events (Debezium/Mendix CDC) get a column per field
cdc_mapping = true           # and each app's CDC entities a generated mapping (graph, OData)
```
