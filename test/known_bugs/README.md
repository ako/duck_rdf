# Known bugs

Each `.test` file here asserts the **correct** behaviour for a bug that is not
fixed yet, so it fails today. These files are not part of the normal suite
(`test/sql`), which keeps that suite green.

Check their status with:

```bash
scripts/check_known_bugs.sh
```

It runs each file and reports `still failing` or `FIXED`. It exits non-zero
when a file passes: move that file to `test/sql/` in the same change that fixes
the bug, so the fix stays covered.

| File | Problem |
|---|---|
| `sum_integer.test` | Integer `SUM` returns `231000.0` once the mapping also contains a generic triple map (predicate from a column), e.g. ontology triples |
| `sum_decimal.test` | `SUM` over `xsd:decimal` is done in floating point: `0.10 + 0.20` gives `0.30000000000000004` |
| `yarrrml_type_iri.test` | YARRRML `o: {value: $(col), type: iri}` is ignored for a plain column; joins on it silently return nothing |
| `unknown_vocabulary.test` | A misspelled class or property returns zero rows instead of an error |
| `iri_pushdown.test` | A constant IRI is compared as text (`CAST(col AS VARCHAR) = '1'`), so it cannot be pushed into Postgres or Parquet/Iceberg scans |
| `numeric_pushdown.test` | A numeric `FILTER` goes through text and `DOUBLE`, so it cannot be pushed down either |
