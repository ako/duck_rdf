#include "pgcompat.hpp"

#include <regex>
#include <vector>

namespace dgl {

namespace {

// The value of the first `<prefix> '<literal>'` in sql, unescaped, or nullopt.
std::optional<std::string> Literal(const std::string &sql, const std::string &prefix) {
	std::regex pattern(prefix + R"(\s*'((?:[^']|'')*)')", std::regex::icase);
	std::smatch m;
	if (!std::regex_search(sql, m, pattern)) {
		return std::nullopt;
	}
	std::string value;
	auto raw = m[1].str();
	for (size_t i = 0; i < raw.size(); i++) {
		value.push_back(raw[i]);
		if (raw[i] == '\'' && i + 1 < raw.size() && raw[i + 1] == '\'') {
			i++;
		}
	}
	return value;
}

std::string Quote(const std::string &text) {
	std::string out = "'";
	for (char c : text) {
		out += c == '\'' ? std::string("''") : std::string(1, c);
	}
	return out + "'";
}

// A LIKE condition with PostgreSQL's default escape character, or TRUE without a pattern.
std::string Like(const std::string &column, const std::optional<std::string> &pattern) {
	if (!pattern) {
		return "TRUE";
	}
	return column + " LIKE " + Quote(*pattern) + " ESCAPE '\\'";
}

std::string Equals(const std::string &column, const std::optional<std::string> &value) {
	return value ? column + " = " + Quote(*value) : "TRUE";
}

bool Contains(const std::string &sql, const char *fragment) {
	return sql.find(fragment) != std::string::npos;
}

// The server's own objects: what discovery and the config file created. Attached databases'
// tables appear through their views, so they are not listed twice.
const char *OWN_DATABASE = "database_name = current_database()";

std::string TablesQuery(const std::string &sql) {
	auto name = Literal(sql, R"(c\.relname\s+LIKE)");
	auto schema = Literal(sql, R"(n\.nspname\s+LIKE)");
	bool filtered = Contains(sql, "(false");
	bool tables = !filtered || Contains(sql, "c.relkind = 'r' AND n.nspname !~ '^pg_'");
	bool views = !filtered || Contains(sql, "c.relkind = 'v' AND n.nspname <> 'pg_catalog'");
	std::vector<std::string> parts;
	if (tables) {
		parts.push_back("SELECT schema_name AS s, table_name AS n, 'TABLE' AS t, comment AS r FROM duckdb_tables() "
		                "WHERE NOT internal AND " +
		                std::string(OWN_DATABASE));
	}
	if (views) {
		parts.push_back("SELECT schema_name, view_name, 'VIEW', comment FROM duckdb_views() WHERE NOT internal AND "
		                "NOT temporary AND " +
		                std::string(OWN_DATABASE));
	}
	if (parts.empty()) {
		parts.push_back("SELECT NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR WHERE FALSE");
	}
	std::string from;
	for (auto &p : parts) {
		from += (from.empty() ? "" : " UNION ALL ") + p;
	}
	return "SELECT NULL::VARCHAR AS \"TABLE_CAT\", s AS \"TABLE_SCHEM\", n AS \"TABLE_NAME\", t AS \"TABLE_TYPE\", "
	       "r AS \"REMARKS\", ''::VARCHAR AS \"TYPE_CAT\", ''::VARCHAR AS \"TYPE_SCHEM\", ''::VARCHAR AS "
	       "\"TYPE_NAME\", "
	       "''::VARCHAR AS \"SELF_REFERENCING_COL_NAME\", ''::VARCHAR AS \"REF_GENERATION\" FROM (" +
	       from + ") WHERE " + Like("n", name) + " AND " + Like("s", schema) +
	       " ORDER BY \"TABLE_TYPE\", \"TABLE_SCHEM\", \"TABLE_NAME\"";
}

std::string ColumnsQuery(const std::string &sql) {
	auto table = Literal(sql, R"(c\.relname\s+LIKE)");
	auto schema = Literal(sql, R"(n\.nspname\s+LIKE)");
	auto column = Literal(sql, R"(attname\s+LIKE)");
	return "SELECT schema_name AS nspname, table_name AS relname, column_name AS attname, " +
	       PgTypeOidCase("data_type") +
	       " AS atttypid, NOT is_nullable AS attnotnull, "
	       "CASE WHEN data_type LIKE 'DECIMAL%' THEN ((numeric_precision << 16) | numeric_scale) + 4 ELSE -1 "
	       "END::INTEGER "
	       "AS atttypmod, -1::SMALLINT AS attlen, -1::INTEGER AS typtypmod, column_index::INTEGER AS attnum, "
	       "NULL::VARCHAR AS attidentity, NULL::VARCHAR AS attgenerated, column_default AS adsrc, comment AS "
	       "description, "
	       "0::INTEGER AS typbasetype, 'b' AS typtype FROM duckdb_columns() WHERE NOT internal AND " +
	       std::string(OWN_DATABASE) + " AND " + Like("table_name", table) + " AND " + Like("schema_name", schema) +
	       " AND " + Like("column_name", column) + " ORDER BY nspname, relname, attnum";
}

// Primary keys: the server's own tables', and for a view over an attached table, that table's.
std::string PrimaryKeysQuery(const std::string &sql) {
	auto table = Literal(sql, R"(ct\.relname\s*=)");
	auto schema = Literal(sql, R"(n\.nspname\s*=)");
	return "SELECT DISTINCT NULL::VARCHAR AS \"TABLE_CAT\", 'main' AS \"TABLE_SCHEM\", table_name AS \"TABLE_NAME\", "
	       "col AS \"COLUMN_NAME\", seq::SMALLINT AS \"KEY_SEQ\", table_name || '_pkey' AS \"PK_NAME\" FROM ("
	       "SELECT table_name, unnest(constraint_column_names) AS col, generate_subscripts(constraint_column_names, 1) "
	       "AS seq FROM duckdb_constraints() WHERE constraint_type = 'PRIMARY KEY') k WHERE " +
	       Equals("table_name", table) + " AND " + Equals("'main'", schema) +
	       " AND (table_name IN (SELECT table_name FROM duckdb_tables() WHERE " + OWN_DATABASE +
	       ") OR table_name IN (SELECT view_name FROM duckdb_views() WHERE " + OWN_DATABASE +
	       ")) ORDER BY \"TABLE_NAME\", \"PK_NAME\", \"KEY_SEQ\"";
}

const char *FOREIGN_KEYS_EMPTY =
    "SELECT NULL::VARCHAR AS \"PKTABLE_CAT\", NULL::VARCHAR AS \"PKTABLE_SCHEM\", NULL::VARCHAR AS \"PKTABLE_NAME\", "
    "NULL::VARCHAR AS \"PKCOLUMN_NAME\", NULL::VARCHAR AS \"FKTABLE_CAT\", NULL::VARCHAR AS \"FKTABLE_SCHEM\", "
    "NULL::VARCHAR AS \"FKTABLE_NAME\", NULL::VARCHAR AS \"FKCOLUMN_NAME\", NULL::SMALLINT AS \"KEY_SEQ\", "
    "NULL::SMALLINT AS \"UPDATE_RULE\", NULL::SMALLINT AS \"DELETE_RULE\", NULL::VARCHAR AS \"FK_NAME\", "
    "NULL::VARCHAR AS \"PK_NAME\", NULL::SMALLINT AS \"DEFERRABILITY\" WHERE FALSE";

// The PostgreSQL types this endpoint produces, as pg_type rows.
const char *PG_TYPES = "(VALUES ('bool', 16), ('bytea', 17), ('int8', 20), ('int2', 21), ('int4', 23), ('text', 25), "
                       "('float4', 700), ('float8', 701), ('varchar', 1043), ('date', 1082), ('time', 1083), "
                       "('timestamp', 1114), ('timestamptz', 1184), ('interval', 1186), ('numeric', 1700), "
                       "('uuid', 2950)) AS t(typname, oid)";

} // namespace

std::string PgTypeOidCase(const std::string &e) {
	return "(CASE WHEN " + e + " = 'BOOLEAN' THEN 16 WHEN " + e + " IN ('TINYINT', 'SMALLINT', 'UTINYINT') THEN 21 " +
	       "WHEN " + e + " IN ('INTEGER', 'USMALLINT') THEN 23 WHEN " + e + " IN ('BIGINT', 'UINTEGER') THEN 20 " +
	       "WHEN " + e + " = 'FLOAT' THEN 700 WHEN " + e + " = 'DOUBLE' THEN 701 WHEN " + e + " LIKE 'DECIMAL%' OR " +
	       e + " IN ('HUGEINT', 'UHUGEINT', 'UBIGINT') THEN 1700 WHEN " + e + " = 'DATE' THEN 1082 WHEN " + e +
	       " = 'TIME' THEN 1083 WHEN " + e + " = 'TIMESTAMP WITH TIME ZONE' THEN 1184 WHEN " + e +
	       " LIKE 'TIMESTAMP%' THEN 1114 WHEN " + e + " = 'INTERVAL' THEN 1186 WHEN " + e + " = 'BLOB' THEN 17 WHEN " +
	       e + " = 'UUID' THEN 2950 WHEN " + e + " = 'VARCHAR' THEN 1043 ELSE 25 END)::INTEGER";
}

std::optional<std::string> RewriteCatalogQuery(const std::string &sql) {
	// DatabaseMetaData.getTables
	if (Contains(sql, "AS TABLE_TYPE") && Contains(sql, "FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c")) {
		return TablesQuery(sql);
	}
	// DatabaseMetaData.getColumns
	if (Contains(sql, "a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull")) {
		return ColumnsQuery(sql);
	}
	// DatabaseMetaData.getPrimaryKeys
	if (Contains(sql, "_pg_expandarray(i.indkey)") && Contains(sql, "i.indisprimary")) {
		return PrimaryKeysQuery(sql);
	}
	// DatabaseMetaData.getImportedKeys / getExportedKeys / getCrossReference: no foreign keys
	if (Contains(sql, "AS PKTABLE_CAT") && Contains(sql, "pg_catalog.pg_constraint")) {
		return std::string(FOREIGN_KEYS_EMPTY);
	}
	// TypeInfoCache: the type list (getTypeInfo) and type lookups by name or OID
	if (Contains(sql, "SELECT t.typname,t.oid FROM pg_catalog.pg_type t")) {
		return std::string("SELECT typname, oid FROM ") + PG_TYPES;
	}
	if (Contains(sql, "'pg_catalog.array_in'::regproc")) {
		return std::string("SELECT false AS is_array, 'b' AS typtype, typname, oid FROM ") + PG_TYPES +
		       " ORDER BY oid DESC";
	}
	return std::nullopt;
}

} // namespace dgl
