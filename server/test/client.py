"""Smoke test client: a plain DuckDB 2.0.0 (no rdf extension) connects over Quack.

Usage: client.py <host:port> <token> [ssl_fingerprint]
"""
import sys

import duckdb

hostport, token = sys.argv[1], sys.argv[2]
fingerprint = sys.argv[3] if len(sys.argv) > 3 else None
c = duckdb.connect()
c.execute("LOAD quack")
opts = f"TOKEN '{token}'" + (f", SSL_FINGERPRINT '{fingerprint}'" if fingerprint else "")
c.execute(f"ATTACH 'quack:{hostport}' AS srv ({opts})")
c.execute("CONNECT srv")

failures = 0


def check(label, sql, expect=None, error=None):
    """Pass when `expect(rows, types)` holds, or, with `error`, when the query fails with it."""
    global failures
    try:
        r = c.execute(sql)
        rows = r.fetchall()
        types = [d[1] for d in r.description]
        ok = error is None and (expect is None or expect(rows, types))
        print("PASS" if ok else "FAIL", label, types, rows[:4])
    except Exception as e:
        msg = str(e).splitlines()[0][:200]
        ok = error is not None and error in str(e)
        print("PASS" if ok else "FAIL", label, "-", msg)
    failures += 0 if ok else 1


check("SQLite table", "SELECT count(*) FROM emp", lambda r, t: r == [(3,)])
check("DuckDB file table", "SELECT name FROM company ORDER BY id", lambda r, t: r == [("Acme",), ("Globex",)])
check("Parquet file", "SELECT sum(amount) FROM payment", lambda r, t: str(r[0][0]) == "0.30")
check("join across SQLite and DuckDB files",
      "SELECT e.name, c.name FROM emp e JOIN company c ON e.company_id = c.id ORDER BY e.id",
      lambda r, t: r[0] == ("Ann", "Acme"))
check("SPARQL with the server's default mapping, typed",
      "SELECT * FROM sparql('PREFIX ex: <http://example.com/ns#> SELECT ?n ?s WHERE { ?p ex:name ?n ; ex:salary ?s } ORDER BY DESC(?s)')",
      lambda r, t: r[0] == ("Bob", 61000) and t[1] == "BIGINT")
check("SPARQL reasoning with the server's ontology",
      "SELECT count(*) FROM sparql('PREFIX ex: <http://example.com/ns#> SELECT ?x WHERE { ?x a ex:Party }')",
      lambda r, t: r == [(5,)])
check("SPARQL join across files",
      "SELECT * FROM sparql('PREFIX ex: <http://example.com/ns#> SELECT ?n ?c WHERE { ?p ex:name ?n ; ex:worksFor/ex:name ?c } ORDER BY ?n')",
      lambda r, t: r[0] == ("Ann", "Acme"))
check("SPARQL CONSTRUCT",
      "SELECT count(*) FROM sparql('PREFIX ex: <http://example.com/ns#> CONSTRUCT { ?c ex:employs ?p } WHERE { ?p ex:worksFor ?c }')",
      lambda r, t: r == [(3,)])
iceberg = c.execute("SELECT count(*) FROM duckdb_views() WHERE view_name = 'orders'").fetchall()[0][0]
if iceberg:
    check("Iceberg table", "SELECT count(*), sum(amount) FROM orders", lambda r, t: r == [(3, 36.75)])
    check("SPARQL over Iceberg + SQLite",
          "SELECT * FROM sparql('PREFIX ex: <http://example.com/ns#> SELECT ?n (SUM(?t) AS ?total) WHERE { ?o ex:orderedBy ?p ; ex:total ?t . ?p ex:name ?n } GROUP BY ?n ORDER BY ?n')",
          lambda r, t: r[0] == ("Ann", 29.5))
check("locked down: no reading outside /data and /config", "SELECT count(*) FROM read_csv('/etc/passwd')",
      error="Permission Error")
check("locked down: settings are frozen", "SET enable_external_access = true", error="locked")
print(f"{failures} failure(s)")
sys.exit(1 if failures else 0)
