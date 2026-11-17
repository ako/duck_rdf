# cdc: entity change events as tables, views and a graph

A topic of change events (the Debezium envelope with the schema embedded, which is what Mendix entity
CDC publishes) is unwrapped: its table gets a column per field of the entity, typed from the
messages' own schema (decimals, timestamps and dates decoded), plus a `cdc` struct
(op c/u/d/r, ts, source_ts, tx_id, sequence, snapshot, connector, version). Attributes added by a
later revision become new columns.

For each Mendix CDC topic `cdc.<App>.<Entity>.<rev>.<space>` the server keeps, in a schema named
after the app (lowercase):

| View                          | What                                                         |
|-------------------------------|--------------------------------------------------------------|
| `<app>.<entity>`              | the current state: one row per object, deleted ones left out |
| `<app>.<entity>_history`      | every change: change_op, changed_at, tx_id, snapshot, fields, kafka_offset |
| `<app>.<entity>__<assoc>`     | one row per association link: id, target                     |

`id` is the Mendix object id. An association is a list of the ids it points to.

```sql
SELECT o."OrderId", c."CustomerName"
FROM testapp."order" o JOIN testapp.order__order_customer l ON l.id = o.id
JOIN testapp.customer c ON c.id = l.target;
```

(`order` is an SQL keyword: quote it.)

The raw tables stay too: `"{{cluster}}__cdc.<App>.<Entity>.<rev>.<space>"` and its `_current` view.

## The generated graph

Every app's CDC entities are also mapped to the graph (a generated mapping, `90-cdc-<app>.ttl`;
MCP `get_mapping` shows it):

- class `<http://duckgraphlake.local/<App>/<App>_<Entity>>`, which is also the OData entity set;
- each attribute a property `<http://duckgraphlake.local/<App>/<Entity>#<Attribute>>`, typed;
- each association a link to the target entity's IRI
  `<http://data.duckgraphlake.local/<App>/<entity>/<id>>`, an OData navigation property.

So OData, SPARQL and the MCP graph tools see every app's entities without a hand-written mapping.
Turn it off with `[kafka] cdc_mapping = false`; extend it with a mapping of your own (`help graph`).

## Snapshots

When it starts, the runtime sends the current data of each entity (op `r`) only if it has not
already published that entity's revision. Data created before the app first ran against this
folder can therefore be missing: start the app with an empty database, or a fresh data folder.
