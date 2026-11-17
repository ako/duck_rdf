# sql: the PostgreSQL and Quack endpoints

The server's database is DuckDB: SQL is DuckDB's dialect (https://duckdb.org/docs/sql/introduction).

- PostgreSQL protocol: `{{pg_url}}`, any user, password {{token}}. psql, JDBC (Mendix's external
  database connector, DataGrip, DBeaver), psycopg, ... Bound parameters work.
- Quack (DuckDB clients): `LOAD quack; ATTACH 'quack:{{host}}:{{sql_port}}' AS lake (TOKEN '...'); USE lake;`
- HTTP: `POST {{http}}/api/sql` with the SQL as the body (JSON rows, at most 1000).
- MCP: the `sql` tool (read-only).

What is there: `SELECT * FROM duckdb_views()`, `SELECT * FROM duckdb_tables()`, `DESCRIBE <name>`;
per-app schemas come from CDC topics (`help cdc`, `help events`).

The graph from SQL: `SELECT * FROM sparql('SELECT ?s WHERE { ?s a <...> }')` runs SPARQL, and
`sparql_sql('...')` shows the SQL it becomes.

Views of your own: create them over SQL (they last until a restart), in the `-i` script (run again
when it changes), or with the MCP tool `define_view` (kept in the data folder).

With lock-down (the default) SQL clients cannot reach files outside the data folder, use the
network, or change settings.
