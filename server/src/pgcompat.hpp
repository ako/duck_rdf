// PostgreSQL catalog compatibility for the wire-protocol endpoint.
//
// PostgreSQL drivers implement metadata calls (JDBC's getTables, getColumns, getPrimaryKeys,
// getTypeInfo, ...) as queries over pg_catalog. DuckDB's pg_catalog is an approximation: its type
// OIDs are not PostgreSQL's and it lacks regclass, _pg_expandarray and array_upper. So the
// queries the PostgreSQL JDBC driver sends are recognised here and answered from DuckDB's own
// catalog, in the columns the driver reads and with PostgreSQL type OIDs, over the objects
// clients see: the views and tables of the server's own database.
#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace dgl {

// The DuckDB SQL to run in place of a known driver catalog query, or nullopt.
std::optional<std::string> RewriteCatalogQuery(const std::string &sql);

// sql with the parts of pg_catalog DuckDB lacks, or answers differently, filled in, for clients
// such as DataGrip: current_database() is the database the client connected to (DuckDB's is
// "memory"), pg_database lists only that one, pg_backend_pid() is the session's process id,
// pg_get_userbyid() is current_user, age() of a transaction id is 0, xmin is 0, pg_stat_ssl and
// pg_stat_activity have this session (without SSL), pg_roles and pg_user have one role, pg_timezone_names and
// pg_timezone_abbrevs are DuckDB's, and pg_locks, pg_shdescription and pg_auth_members are empty.
// pg_catalog-qualified function calls are made unqualified.
std::string ReplaceCatalogGaps(const std::string &sql, const std::string &database, int32_t pid);

// A SQL CASE expression mapping a duckdb_columns().data_type text to its PostgreSQL type OID.
std::string PgTypeOidCase(const std::string &data_type_expr);

} // namespace dgl
