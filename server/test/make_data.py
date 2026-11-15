"""Build a small /data and /config for the smoke test: one file of each kind.

  data/hr.sqlite          SQLite database with table emp
  data/crm.duckdb         DuckDB database with table company
  data/payment.parquet    a single Parquet file
  data/orders/            an Iceberg table (written with pyiceberg)
  config/server.toml      mapping + ontology + typed results
  config/mapping/hr.ttl   R2RML mapping over emp, company, payment, orders
  config/ontology/hr.ttl  class hierarchy
"""
import os
import shutil
import sqlite3
import sys

import duckdb

out = sys.argv[1] if len(sys.argv) > 1 else "test/fixture"
data, config = os.path.join(out, "data"), os.path.join(out, "config")
shutil.rmtree(out, ignore_errors=True)
for d in (data, os.path.join(config, "mapping"), os.path.join(config, "ontology")):
    os.makedirs(d)

s = sqlite3.connect(os.path.join(data, "hr.sqlite"))
s.execute("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, company_id INTEGER, salary INTEGER)")
s.executemany("INSERT INTO emp VALUES (?, ?, ?, ?)", [(10, "Ann", 1, 52000), (11, "Bob", 2, 61000), (12, "Cy", 1, 48000)])
s.commit()
s.close()

d = duckdb.connect(os.path.join(data, "crm.duckdb"))
d.execute("CREATE TABLE company AS SELECT * FROM (VALUES (1, 'Acme'), (2, 'Globex')) t(id, name)")
d.close()

d = duckdb.connect()
d.execute(f"COPY (SELECT * FROM (VALUES (1, 0.10::DECIMAL(10,2)), (2, 0.20::DECIMAL(10,2))) t(id, amount)) "
          f"TO '{os.path.join(data, 'payment.parquet')}'")

try:
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, DoubleType

    wh = os.path.abspath(os.path.join(out, "warehouse"))
    os.makedirs(wh)
    cat = SqlCatalog("t", uri=f"sqlite:///{wh}/catalog.db", warehouse=f"file://{wh}")
    cat.create_namespace("s")
    schema = Schema(NestedField(1, "oid", LongType(), required=True), NestedField(2, "cid", LongType()),
                    NestedField(3, "amount", DoubleType()))
    t = cat.create_table("s.orders", schema=schema)
    t.append(pa.table({"oid": [100, 101, 102], "cid": [10, 10, 11], "amount": [9.5, 20.0, 7.25]}).cast(schema.as_arrow()))
    # The table is copied to data/orders; its metadata still names the warehouse path,
    # which the server's iceberg view handles with allow_moved_paths.
    src = os.path.dirname(os.path.dirname(t.metadata_location.replace("file://", "")))
    shutil.copytree(src, os.path.join(data, "orders"))
    shutil.rmtree(wh)
    iceberg = True
except ImportError:
    iceberg = False
    print("pyiceberg not installed: no Iceberg table in the fixture")

open(os.path.join(config, "mapping", "hr.ttl"), "w").write("""@prefix rr:  <http://www.w3.org/ns/r2rml#> .
@prefix ex:  <http://example.com/ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<#Emp> rr:logicalTable [ rr:tableName "emp" ] ;
  rr:subjectMap [ rr:template "http://data.example.com/person/{id}" ; rr:class ex:Employee ] ;
  rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:salary ; rr:objectMap [ rr:column "salary" ; rr:datatype xsd:integer ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:worksFor ; rr:objectMap [ rr:template "http://data.example.com/company/{company_id}" ] ] .
<#Company> rr:logicalTable [ rr:tableName "company" ] ;
  rr:subjectMap [ rr:template "http://data.example.com/company/{id}" ; rr:class ex:Company ] ;
  rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
<#Payment> rr:logicalTable [ rr:tableName "payment" ] ;
  rr:subjectMap [ rr:template "http://data.example.com/payment/{id}" ; rr:class ex:Payment ] ;
  rr:predicateObjectMap [ rr:predicate ex:amount ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:decimal ] ] .
""" + ("""<#Order> rr:logicalTable [ rr:tableName "orders" ] ;
  rr:subjectMap [ rr:template "http://data.example.com/order/{oid}" ; rr:class ex:Order ] ;
  rr:predicateObjectMap [ rr:predicate ex:orderedBy ; rr:objectMap [ rr:template "http://data.example.com/person/{cid}" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:total ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:double ] ] .
""" if iceberg else ""))

open(os.path.join(config, "ontology", "hr.ttl"), "w").write("""@prefix ex:   <http://example.com/ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
ex:Employee rdfs:subClassOf ex:Person .
ex:Person   rdfs:subClassOf ex:Party .
ex:Company  rdfs:subClassOf ex:Party .
""")

open(os.path.join(config, "server.toml"), "w").write("""[rdf]
mapping = "/config/mapping/*.ttl"
ontology = "/config/ontology/*.ttl"
typed_results = true
""")
print("fixture in", out, "(iceberg)" if iceberg else "")
