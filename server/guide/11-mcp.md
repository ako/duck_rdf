# mcp: tools for agents

`{{http}}/mcp`, Model Context Protocol over streamable HTTP (JSON responses). Claude Code:
`claude mcp add --transport http duckgraphlake {{http}}/mcp`.

Discovery and SQL (always there):

| Tool             | What                                                               |
|------------------|--------------------------------------------------------------------|
| `guide`          | these help topics                                                  |
| `list_tables`    | every table and view, with what it is (topic, app, entity)         |
| `describe_table` | columns, types, row count, sample rows, origin and graph class     |
| `sql`            | a read-only SQL query (DuckDB dialect)                             |
| `list_topics`    | Kafka topics: records, last offset and time, event types, apps     |

Changing the model (kept in the data folder; `[mcp] allow_changes`):

| Tool             | What                                                               |
|------------------|--------------------------------------------------------------------|
| `define_view`    | create or replace a view (schema.name), kept across restarts       |
| `drop_view`      | drop one of those                                                  |
| `get_mapping`    | the effective mapping files and their text                         |
| `set_mapping`    | add or replace a mapping (R2RML Turtle or YARRRML), checked first  |
| `remove_mapping` | remove one                                                         |

The graph (when there is a mapping, e.g. from CDC topics):

| Tool              | What                                                              |
|-------------------|-------------------------------------------------------------------|
| `list_classes`    | classes with instance counts                                      |
| `describe_class`  | a class's properties, with examples                               |
| `find_entity`     | entities by text                                                  |
| `describe_entity` | everything about one IRI                                          |
| `sparql`          | a SPARQL query                                                    |

The help topics are also MCP resources: `guide://<topic>`.
