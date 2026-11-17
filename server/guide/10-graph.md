# graph: mappings, SPARQL, ontology and OData

The graph is a view of the SQL tables through an R2RML or YARRRML mapping: nothing is copied.
The effective mapping combines (MCP `get_mapping` lists the files):

| File                    | From                                                                |
|-------------------------|---------------------------------------------------------------------|
| `00-config-*`           | the files of `-m` / `[rdf] mapping`                                 |
| `50-agent-<name>.ttl`   | set over MCP (`set_mapping`), kept in `.duckgraphlake/mappings/`    |
| `90-cdc-<app>.ttl`      | generated from the app's CDC entities (`help cdc`)                  |

All are reloaded when they change, without a restart: SPARQL, OData and the MCP graph tools follow.

A minimal R2RML triples map:

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.com/ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<#Order> rr:logicalTable [ rr:tableName "orders" ] ;   # or rr:sqlQuery "SELECT ..."
  rr:subjectMap [ rr:template "http://example.com/order/{id}" ; rr:class ex:Order ] ;
  rr:predicateObjectMap [ rr:predicate ex:amount ;
      rr:objectMap [ rr:column "amount" ; rr:datatype xsd:decimal ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:customer ;
      rr:objectMap [ rr:template "http://example.com/customer/{customer_id}" ] ] .
```

YARRRML (.yml) works too. A schema-qualified table: `rr:tableName "\"testapp\".\"order\""`.

- SPARQL 1.1 SELECT/ASK/CONSTRUCT: `{{http}}/sparql` (GET or POST `query=`), YASGUI at `{{http}}/yasgui`.
- OData v4: `{{http}}/odata`: an entity set per class (its local name), a typed property per
  literal predicate (from rr:datatype), a navigation property per link to another class; `$filter`,
  `$select`, `$expand`, `$orderby`, `$top`, `$skip`, `$count`. Read-only.
- Ontology (`-o`): rdfs:subClassOf/subPropertyOf, rdfs:domain/range, owl:inverseOf,
  owl:SymmetricProperty, owl:TransitiveProperty are applied to queries; labels and comments show in
  MCP and the UI.

Check a mapping before using it: MCP `set_mapping` refuses one that does not load, with the reason.
