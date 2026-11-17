# kafka: produce events from any client

The Kafka endpoint at {{kafka}} accepts producers (the Java client 3.x/4.x, librdkafka, ...):

- topics are created on first use (or with CreateTopics);
- record batches with no compression, gzip or zstd (not snappy or lz4);
- idempotent producers and transactions-capable clients work; consuming (Fetch) does not.

Each topic is written to an Iceberg table `{{cluster}}__<topic>` (in
`warpstream/_tableflow/`), shaped like WarpStream Tableflow's, and is queryable as a view of the
same name within about a second:

| Column       | What                                                                          |
|--------------|-------------------------------------------------------------------------------|
| (fields)     | a column per field, for a CDC topic (`help cdc`) or a Tableflow-configured one |
| `cdc`        | change metadata, for a CDC topic                                              |
| `warpstream` | STRUCT(partition, offset, key BLOB, value BLOB, timestamp, headers MAP)        |

```sql
SELECT decode(warpstream.value) AS json, warpstream.headers['ce_type'] AS type
FROM "{{cluster}}__orders" ORDER BY warpstream."offset";
```

Topic names contain dots: quote them (`"{{cluster}}__cdc.TestApp.Order.1.local"`).

JSON values of a plain topic can get typed columns from a WarpStream Tableflow file
(`--tableflow tableflow.yaml`: `tables:` with `source_topic`, `input_schema` as JSON Schema).

A produce is acknowledged once the records are in a local buffer; they are committed to Iceberg
every flush interval. Nothing acknowledged is lost on a crash, and offsets carry on after a restart.

Check what arrived: MCP `list_topics`, or

```sql
SELECT count(*), max(warpstream.timestamp) FROM "{{cluster}}__orders";
```
