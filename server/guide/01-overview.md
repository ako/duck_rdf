# overview: what duckgraphlake is and where to start

duckgraphlake is one binary that serves a folder of data to apps and agents during development: a
local stand-in for a data lake with a knowledge graph on top. Point it at a folder, and:

- every Parquet, CSV or JSON file (or folder of them), Iceberg table, SQLite or DuckDB file in it
  becomes a SQL table or view;
- apps stream events into it over the Kafka protocol (Mendix Business Events and entity CDC, or any
  Kafka producer); each topic becomes an Iceberg table in the folder within a second, and CDC
  topics also become one readable view per entity with its current state;
- the data is readable as SQL (PostgreSQL protocol, DuckDB's Quack), as a graph (SPARQL), as OData
  v4 (Mendix external entities) and over MCP (for agents);
- the graph is described by an R2RML or YARRRML mapping: one is generated for every app's CDC
  entities, and you or an agent can add your own while the server runs.

Endpoints on this machine (defaults):

| What | Where |
|---|---|
| PostgreSQL | {{pg_url}} |
| Quack SQL | quack:{{host}}:{{sql_port}} |
| SPARQL | {{http}}/sparql |
| OData v4 | {{http}}/odata |
| MCP | {{http}}/mcp (streamable HTTP) |
| Kafka | {{kafka}} (producers) |
| Iceberg | {{http}}/iceberg (REST catalog) |
| Web UI | {{http}}/ |
| This guide | {{http}}/api/guide, or `duckgraphlake help` |

The token (the PostgreSQL password, the Quack token, and the bearer token where one is needed) is
{{token}}.

Start with `duckgraphlake help quickstart`. A coding agent should read `duckgraphlake help agents`.
