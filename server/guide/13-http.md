# http: the HTTP API

Base: `{{http}}`. Where a token is needed: `Authorization: Bearer <token>`.

| Request                         | What                                                     |
|---------------------------------|----------------------------------------------------------|
| `GET /`                         | the web UI: catalog, tables, SQL, SPARQL                 |
| `GET /api/guide`                | the help topics (JSON); `/api/guide/<topic>` one (Markdown) |
| `GET /api/catalog`              | server, endpoints, tables with columns, graph classes    |
| `GET /api/topics`               | Kafka topics and their state                             |
| `GET /api/table?name=&limit=`   | the first rows of a table or view                        |
| `POST /api/sql`                 | run SQL (the body); JSON rows, at most 1000              |
| `GET/POST /sparql`              | SPARQL 1.1 Protocol                                      |
| `GET /odata/...`                | OData v4                                                 |
| `POST /mcp`                     | MCP (JSON-RPC)                                           |
| `/iceberg/v1/...`               | Iceberg REST catalog over the data folder                |
