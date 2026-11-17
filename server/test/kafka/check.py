"""Checks the Kafka test's tables through the server's SQL API: usage: check.py <phase>"""
import json
import sys
import urllib.request

failures = 0


def sql(query):
    req = urllib.request.Request("http://127.0.0.1:19495/api/sql", data=query.encode(), method="POST")
    with urllib.request.urlopen(req) as r:
        return json.load(r)["rows"]


def check(label, ok, detail):
    global failures
    print("PASS" if ok else "FAIL", label, detail)
    failures += 0 if ok else 1


phase = sys.argv[1]
orders = sql('SELECT count(*), count(DISTINCT warpstream."offset"), min(warpstream."offset"), max(warpstream."offset"), '
             'count(id) FROM "local__orders"')[0]
if phase == "first":
    check("every acknowledged record is in the table, offsets 0..1001 without gaps", orders == [1002, 1002, 0, 1001, 1000], orders)
    row = sql('SELECT id, customer, amount, placed::VARCHAR, lines[1].qty, warpstream."partition", decode(warpstream."key") '
              'FROM "local__orders" WHERE warpstream."offset" = 7')[0]
    check("JSON fields as typed columns (integer, string, number, date-time, nested list)",
          row == [7, "c0", 8.75, "2026-11-17 10:00:07+00", 2, 0, "order-7"], row)
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
elif phase == "restarted":
    check("offsets carry on after a restart (12 more records: 1002..1013; each run sends two that do not parse)", orders == [1014, 1014, 0, 1013, 1010], orders)
elif phase == "crashed":
    check("records acknowledged before a crash are committed after the restart", orders == [1034, 1034, 0, 1033, 1028], orders)
sys.exit(1 if failures else 0)
