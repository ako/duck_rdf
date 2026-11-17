# events: business events as tables

A Mendix business event arrives as a CloudEvents message: the value is the event entity's
attributes as JSON, the headers carry the metadata (`ce_type` the message name, `ce_source`
/<App>/<Service>/, `ce_subject`, `ce_id`, `ce_time`).

The topic table (`"{{cluster}}__businessevents.<channel id>.local"`) keeps them in
`warpstream.value` and `warpstream.headers`. For each app and event type the server also keeps a
view `<app>.<event type>` (lowercase) with event_id, event_time, subject, the payload's fields as
columns, and kafka_offset:

```sql
SELECT * FROM testapp.orderplaceevent ORDER BY event_time;
```

Several event types on one topic (a shared `ChannelName`) get a view each.
