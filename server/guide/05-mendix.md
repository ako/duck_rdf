# mendix: connect a Mendix app

Run the app (Studio Pro, or `mxcli run --local`) on the same machine as the server.

## Business events (Business Events marketplace module)

Constants of the BusinessEvents module, for the configuration you run with:

| Constant                         | Value            |
|----------------------------------|------------------|
| BusinessEvents.ServerUrl         | {{kafka}}        |
| BusinessEvents.EventBrokerSpace  | local            |
| (leave UserName, Password, Truststore*, Authn* empty) |  |

Topics are then `businessevents.<channel id>.local`. Setting `BusinessEvents.ChannelName` instead
sends every message of the app to that one topic.

The module must be started: the app's after-startup microflow calls the Java action
`BusinessEvents.StartupBusinessEvents` (and the before-shutdown microflow
`BusinessEvents.BSd_ShutdownSubscribers`). Events are published with the "Publish business event"
activity (`BusinessEvents.PublishBusinessEvent_V2`) on an entity that specializes
`BusinessEvents.PublishedBusinessEvent`, configured in a published business event service.

What arrives: `help events`.

## Entity CDC (Change Data Capture, Studio Pro 11.12+)

A published CDC service document lists the entities and attributes. Point the runtime at the server
with the runtime setting:

| Setting                 | Value      |
|-------------------------|------------|
| Kafka.BootstrapServers  | {{kafka}}  |

(App > Settings > Configurations > the configuration > Custom; or with mxcli:
`mxcli run --local --runtime-setting Kafka.BootstrapServers={{kafka}}`.)

Topics are `cdc.<app>.<entity>.<major revision>.local`, one per entity. What arrives: `help cdc`.

## With mxcli

```bash
mxcli setup mxbuild -p App.mpr && mxcli setup mxruntime -p App.mpr
mxcli run --local --detach --db-type hsqldb -p App.mpr \
  --constant BusinessEvents.ServerUrl={{kafka}} --constant BusinessEvents.EventBrokerSpace=local \
  --runtime-setting Kafka.BootstrapServers={{kafka}}
```

## Reading data back into a Mendix app

- External database connector: PostgreSQL, `{{pg_url}}`, any user, password {{token}}. The CDC
  views are tables there (`testapp."order"`, ...).
- OData (external entities, read-only): `{{http}}/odata`, `$metadata` describes it. Every app's
  CDC entities are entity sets `<App>_<Entity>` with their associations as navigation properties.
- REST/SPARQL: `{{http}}/sparql`.
