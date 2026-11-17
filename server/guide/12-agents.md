# agents: how a coding agent should use this server

You are building apps that write to this lake (events, CDC) and read from it (SQL, OData, graph).

1. Orient: MCP `guide` (or `duckgraphlake help`), then `list_tables` and `list_topics`.
2. Wire an app's output: `help mendix` (Business Events constants, `Kafka.BootstrapServers`) or
   `help kafka`. Produce something, then check it arrived: `list_topics`, `describe_table`.
3. Read what an app produced: `describe_table` on `<app>.<entity>` (current state) or
   `<app>.<entity>_history`; `sql` for anything else. Business events: `<app>.<event type>`.
4. Shape data for consumers: `define_view` for a SQL view another app or agent should read (it
   lasts across restarts); `set_mapping` to put tables on the graph and in OData. Generated CDC
   mappings already cover every app's entities: extend them rather than repeat them.
5. Read it back the way the consuming app will: PostgreSQL (`{{pg_url}}`), OData
   (`{{http}}/odata/<App>_<Entity>`), SPARQL or MCP.

Conventions and pitfalls:

- Quote names with dots or keywords: `"{{cluster}}__cdc.TestApp.Order.1.local"`, `testapp."order"`.
- Mendix object ids (`id`) are BIGINT; associations are lists of ids (`<entity>__<assoc>` views).
- CDC sends data present before the app first ran against this folder only once per entity
  revision (`help cdc`, Snapshots).
- Writes from apps go through Kafka; SQL clients cannot write files (lock-down).
- Everything you define is in `.duckgraphlake/` of the data folder: commit or copy it with the data.
