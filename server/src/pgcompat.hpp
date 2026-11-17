// PostgreSQL catalog compatibility for the wire-protocol endpoint.
//
// PostgreSQL drivers implement metadata calls (JDBC's getTables, getColumns, getPrimaryKeys,
// getTypeInfo, ...) as queries over pg_catalog. DuckDB's pg_catalog is an approximation: its type
// OIDs are not PostgreSQL's and it lacks regclass, _pg_expandarray and array_upper. So the
// queries the PostgreSQL JDBC driver sends are recognised here and answered from DuckDB's own
// catalog, in the columns the driver reads and with PostgreSQL type OIDs, over the objects
// clients see: the views and tables of the server's own database.
#pragma once

#include <optional>
#include <string>

namespace dgl {

// The DuckDB SQL to run in place of a known driver catalog query, or nullopt.
std::optional<std::string> RewriteCatalogQuery(const std::string &sql);

// A SQL CASE expression mapping a duckdb_columns().data_type text to its PostgreSQL type OID.
std::string PgTypeOidCase(const std::string &data_type_expr);

} // namespace dgl
