# data: what the folder becomes

At start-up each entry of the data folder ({{data}}) becomes something SQL can read, named after
the file or folder (lowercase letters, digits and _):

| Entry                                   | Becomes                                         |
|-----------------------------------------|-------------------------------------------------|
| `x.parquet`, `x.csv`, `x.tsv`, `x.json` | view `x`                                        |
| a folder of one kind of those files     | view over all of them (`**`, union by name)     |
| an Iceberg table folder (`metadata/`)   | view reading its newest snapshot                |
| a folder of Iceberg namespaces          | a view per table (e.g. `warpstream/_tableflow`) |
| `x.sqlite`, `x.db` (SQLite)             | attached read-only as `x`, tables also as views |
| `x.duckdb`                              | attached read-only as `x`, tables also as views |

Files added later are not picked up until a restart; topics are (see `help kafka`).

What the server writes:

- `warpstream/_tableflow/<cluster>__<topic>/`: one Iceberg table per Kafka topic.
- `.duckgraphlake/kafka.duckdb`: records received but not yet committed.
- `.duckgraphlake/mappings/`: mappings agents set over MCP (`set_mapping`).
- `.duckgraphlake/views/`: views agents defined over MCP, created again at start-up.

Nothing is written until a topic arrives or an agent defines something: a folder of files can be
served read-only.

Copy or commit the folder to share a data set: it is plain Parquet and Iceberg.

Inspect it: `SELECT * FROM duckdb_views()`, `DESCRIBE <name>`, or the MCP tools `list_tables` and
`describe_table`.
