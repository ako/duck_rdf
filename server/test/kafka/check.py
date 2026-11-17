"""Checks the Kafka test's tables through the server's SQL API: usage: check.py <phase>"""
import json
import sys
import urllib.request

failures = 0


def sql(query):
    req = urllib.request.Request("http://127.0.0.1:19495/api/sql", data=query.encode(), method="POST")
    with urllib.request.urlopen(req) as r:
        return json.load(r)["rows"]


def get(url):
    with urllib.request.urlopen(url) as r:
        return r.read().decode()


def mcp(method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request("http://127.0.0.1:19495/mcp", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.load(r)["result"]


def check(label, ok, detail):
    global failures
    print("PASS" if ok else "FAIL", label, detail)
    failures += 0 if ok else 1


phase = sys.argv[1]
orders = sql('SELECT count(*), count(DISTINCT warpstream."offset"), min(warpstream."offset"), max(warpstream."offset"), '
             'count(id) FROM "local__orders"')[0]
if phase == "first":
    check("every acknowledged record is in the table, offsets 0..1001 without gaps", orders == [1002, 1002, 0, 1001, 1000], orders)
    # placed is a TIMESTAMPTZ, which ::VARCHAR shows in the server's time zone: compare it in UTC.
    row = sql('SELECT id, customer, amount, (placed AT TIME ZONE \'UTC\')::VARCHAR, lines[1].qty, warpstream."partition", '
              'decode(warpstream."key") FROM "local__orders" WHERE warpstream."offset" = 7')[0]
    check("JSON fields as typed columns (integer, string, number, date-time, nested list)",
          row == [7, "c0", 8.75, "2026-11-17 10:00:07", 2, 0, "order-7"], row)
    bad = sql('SELECT id, customer, decode(warpstream."value") FROM "local__orders" WHERE warpstream."offset" IN (1000, 1001) '
              'ORDER BY warpstream."offset"')
    check("a record that is not JSON, or not of the schema's shape, is kept with its raw value",
          bad == [[None, None, "this is not json"], [None, "x", '{"id":"not-a-number","customer":"x"}']], bad)
    events = sql('SELECT count(*), max(warpstream."offset"), any_value(decode(warpstream."value")) LIKE \'%Sales.Order%\' '
                 'FROM "local__mendix.events"')[0]
    check("an unconfigured topic is created on first use and keeps the warpstream struct", events == [10, 9, True], events)
    cols = [r[0] for r in sql('SELECT column_name FROM (DESCRIBE "local__orders")')]
    check("the table has the schema's columns and the warpstream struct",
          cols == ["id", "customer", "amount", "placed", "lines", "warpstream"], cols)
    headers = sql('SELECT warpstream.headers[\'ce_type\'], warpstream.headers[\'ce_specversion\'] FROM "local__mendix.events" LIMIT 1')
    check("record headers are kept in warpstream.headers", headers == [["EntityChanged", "1.0"]], headers)
    cdc = sql('SELECT id, "OrderId", "CustomerName", "Price"::VARCHAR, "Order_Customer", "PlacedOn"::VARCHAR, "Note", cdc.op, '
              'cdc.tx_id, cdc.connector FROM "local__cdc.TestApp.Order.1.local" ORDER BY warpstream."offset"')
    check("change events: a column per field (decimal, list, timestamp), the deleted row from before, a cdc struct",
          cdc == [[11, 101, "Ada", "9.95000000", "[7]", "2026-11-17 18:31:22.45", None, "c", "tx-1", "mendix_event_broker_cdc"],
                  [12, 102, "Bob", "20.00000000", "[7]", "2026-11-17 18:31:22.45", None, "c", "tx-1", "mendix_event_broker_cdc"],
                  [11, 101, "Ada", "12.50000000", "[7]", "2026-11-17 18:31:22.45", None, "u", "tx-1", "mendix_event_broker_cdc"],
                  [12, 102, "Bob", "20.00000000", "[7]", "2026-11-17 18:31:22.45", None, "d", "tx-1", "mendix_event_broker_cdc"],
                  [13, 103, "Cy", "-0.11106000", "[7]", "2026-11-17 18:31:22.45", "rush", "c", "tx-1", "mendix_event_broker_cdc"]], cdc)
    app = sql('SELECT id, "Price"::VARCHAR FROM testapp."order" ORDER BY id')
    check("a schema per app: testapp.\"order\" is the current state", app == [[11, "12.50000000"], [13, "-0.11106000"]], app)
    hist = sql('SELECT change_op, kafka_offset FROM testapp.order_history ORDER BY kafka_offset')
    check("testapp.order_history has every change", [r[0] for r in hist] == ["c", "c", "u", "d", "c"], hist)
    ev = sql('SELECT entity, change, n FROM testapp.entitychanged ORDER BY n LIMIT 2')
    check("business events: a view per app and event type, payload fields as columns",
          ev == [["Sales.Order", "update", 0], ["Sales.Order", "update", 1]], ev)
    count = get("http://127.0.0.1:19495/odata/TestApp_Order/$count")
    check("the generated mapping puts CDC entities in OData (<App>_<Entity>)", count.strip() == "2", count)
    topics = json.loads(get("http://127.0.0.1:19495/api/topics"))["topics"]
    cdc_topic = [t for t in topics if t["topic"] == "cdc.TestApp.Order.1.local"]
    check("/api/topics reports records and what the topic became",
          cdc_topic and cdc_topic[0]["records"] == 5 and cdc_topic[0]["kind"] == "cdc"
          and cdc_topic[0]["views"]["current"] == "testapp.order", cdc_topic)
    tools = [t["name"] for t in mcp("tools/list", {})["tools"]]
    check("MCP: discovery, SQL and graph tools", all(t in tools for t in
          ["guide", "list_tables", "describe_table", "sql", "list_topics", "define_view", "set_mapping",
           "list_classes", "sparql"]), tools)
    described = mcp("tools/call", {"name": "describe_table", "arguments": {"name": "testapp.order"}})["structuredContent"]
    check("MCP describe_table: origin and OData entity set",
          described["origin"]["entity"] == "Order" and described["origin"]["odata_entity_set"] == "TestApp_Order"
          and described["rows"] == 2, described.get("origin"))
    refused = mcp("tools/call", {"name": "sql", "arguments": {"query": "DROP VIEW testapp.order_history"}})
    check("MCP sql is read-only", refused.get("isError") is True, refused)
    guide = get("http://127.0.0.1:19495/api/guide/mendix")
    check("the help topics are served with this server's addresses", "127.0.0.1:19092" in guide, guide[:80])
    current = sql('SELECT id, "Price"::VARCHAR, "Note" FROM "local__cdc.TestApp.Order.1.local_current" ORDER BY id')
    check("the _current view has the latest row per key, without deleted ones",
          current == [[11, "12.50000000", None], [13, "-0.11106000", "rush"]], current)
elif phase == "restarted":
    check("offsets carry on after a restart (12 more records: 1002..1013; each run sends two that do not parse)", orders == [1014, 1014, 0, 1013, 1010], orders)
elif phase == "crashed":
    check("records acknowledged before a crash are committed after the restart", orders == [1034, 1034, 0, 1033, 1028], orders)
sys.exit(1 if failures else 0)
