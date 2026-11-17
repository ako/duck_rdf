#include "pgcompat.hpp"

#include <cctype>
#include <regex>
#include <set>
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

namespace {

// Replaces each match of pattern in sql by replacement(match), literally.
template <class F>
std::string ReplaceEach(const std::string &sql, const std::regex &pattern, F replacement) {
	std::string out;
	size_t last = 0;
	for (auto it = std::sregex_iterator(sql.begin(), sql.end(), pattern); it != std::sregex_iterator(); ++it) {
		auto start = static_cast<size_t>(it->position());
		auto end = start + static_cast<size_t>(it->length());
		out += sql.substr(last, start - last) + replacement(*it, end);
		last = end;
	}
	return out + sql.substr(last);
}

// Whether a FROM item ending at `end` in sql is followed by an alias of its own.
bool HasAlias(const std::string &sql, size_t end) {
	static const std::regex next(R"(^\s+([A-Za-z_][A-Za-z_0-9]*|"))");
	std::smatch m;
	auto rest = sql.substr(end);
	if (!std::regex_search(rest, m, next)) {
		return false;
	}
	auto word = m[1].str();
	for (auto &c : word) {
		c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
	}
	static const std::set<std::string> keywords = {
	    "where", "join",  "left",   "right", "inner",     "outer",  "full",   "cross", "on",     "order",
	    "group", "limit", "offset", "union", "intersect", "except", "having", "window", "natural", "using"};
	return keywords.count(word) == 0;
}

} // namespace

std::string ReplaceCatalogGaps(const std::string &sql, const std::string &database, int32_t pid) {
	// Cheap test first: almost every query has none of these.
	auto lower = sql;
	for (auto &c : lower) {
		c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
	}
	static const char *markers[] = {"pg_catalog", "current_database", "pg_backend_pid", "pg_locks", "pg_database",
	                                "pg_stat_",   "pg_shdescription", "pg_get_userbyid", "age",      "xmin",
	                                "pg_roles",   "pg_auth_members",  "pg_timezone_",    "pg_is_in_recovery",
	                                "pg_tablespace_location"};
	bool any = false;
	for (auto marker : markers) {
		any = any || lower.find(marker) != std::string::npos;
	}
	if (!any) {
		return sql;
	}
	auto db = Quote(database);
	auto session_pid = std::to_string(pid);
	// Functions.
	static const std::regex current_database(R"(\b(?:pg_catalog\s*\.\s*)?current_database\s*\(\s*\))",
	                                         std::regex::icase);
	static const std::regex backend_pid(R"(\b(?:pg_catalog\s*\.\s*)?pg_backend_pid\s*\(\s*\))", std::regex::icase);
	static const std::regex userbyid(R"(\b(?:pg_catalog\s*\.\s*)?pg_get_userbyid\s*\([^()]*\))", std::regex::icase);
	// age() of a transaction id: DuckDB's age() takes timestamps only. No transaction id is ever old.
	static const std::regex xid_age(R"re(\b(?:pg_catalog\s*\.\s*)?age\s*\(\s*[A-Za-z_0-9."]*(?:xid|transactionid|xmin)"?\s*\))re",
	                                std::regex::icase);
	// DuckDB registers some PostgreSQL functions only unqualified (pg_catalog.txid_current() is read
	// as a column pg_catalog), and resolves all of them that way.
	static const std::regex qualified_call(R"(\bpg_catalog\s*\.\s*([A-Za-z_][A-Za-z_0-9]*)\s*\()", std::regex::icase);
	auto out = ReplaceEach(sql, qualified_call, [](const std::smatch &m, size_t) { return m[1].str() + "("; });
	out = ReplaceEach(out, current_database, [&](const std::smatch &, size_t) { return db; });
	out = ReplaceEach(out, backend_pid, [&](const std::smatch &, size_t) { return session_pid; });
	out = ReplaceEach(out, userbyid, [](const std::smatch &, size_t) { return std::string("current_user"); });
	// not a bare 0: ORDER BY 0 is a position
	out = ReplaceEach(out, xid_age, [](const std::smatch &, size_t) { return std::string("0::INTEGER"); });
	// xmin, the row version clients such as DataGrip use to see what changed: DuckDB's catalog
	// tables have none, so every row is at version 0.
	static const std::regex xmin(R"(\b[A-Za-z_][A-Za-z_0-9]*\s*\.\s*xmin\b)", std::regex::icase);
	out = ReplaceEach(out, xmin, [](const std::smatch &, size_t) { return std::string("0::BIGINT"); });
	static const std::regex in_recovery(R"(\bpg_is_in_recovery\s*\(\s*\))", std::regex::icase);
	out = ReplaceEach(out, in_recovery, [](const std::smatch &, size_t) { return std::string("false"); });
	static const std::regex tablespace_location(R"(\bpg_tablespace_location\s*\([^()]*\))", std::regex::icase);
	out = ReplaceEach(out, tablespace_location, [](const std::smatch &, size_t) { return std::string("NULL::VARCHAR"); });
	// Catalog views DuckDB lacks, or (pg_database) lists DuckDB's own databases in.
	struct View {
		const char *name;
		std::string query;
	};
	const View views[] = {
	    // The database the client connected to, and no other.
	    {"pg_database",
	     "(SELECT 1::INTEGER AS oid, " + db + " AS datname, 10::INTEGER AS datdba, 6::INTEGER AS encoding, "
	     "'c' AS datlocprovider, false AS datistemplate, true AS datallowconn, -1::INTEGER AS datconnlimit, "
	     "0::BIGINT AS datfrozenxid, 0::BIGINT AS datminmxid, 1663::INTEGER AS dattablespace, 'C' AS datcollate, "
	     "'C' AS datctype, NULL::VARCHAR AS daticulocale, NULL::VARCHAR AS daticurules, NULL::VARCHAR AS "
	     "datcollversion, NULL::VARCHAR[] AS datacl)"},
	    // No locks: DuckDB's concurrency control is optimistic.
	    {"pg_locks",
	     "(SELECT NULL::VARCHAR AS locktype, NULL::INTEGER AS database, NULL::INTEGER AS relation, "
	     "NULL::INTEGER AS page, NULL::SMALLINT AS tuple, NULL::VARCHAR AS virtualxid, NULL::BIGINT AS transactionid, "
	     "NULL::INTEGER AS classid, NULL::INTEGER AS objid, NULL::SMALLINT AS objsubid, "
	     "NULL::VARCHAR AS virtualtransaction, NULL::INTEGER AS pid, NULL::VARCHAR AS mode, NULL::BOOLEAN AS granted, "
	     "NULL::BOOLEAN AS fastpath, NULL::TIMESTAMPTZ AS waitstart WHERE false)"},
	    // This session, without SSL (the endpoint refuses it).
	    {"pg_stat_ssl",
	     "(SELECT " + session_pid + "::INTEGER AS pid, false AS ssl, NULL::VARCHAR AS version, NULL::VARCHAR AS cipher, "
	     "NULL::INTEGER AS bits, NULL::VARCHAR AS client_dn, NULL::DECIMAL(38,0) AS client_serial, "
	     "NULL::VARCHAR AS issuer_dn)"},
	// One role: the user DuckDB reports as current_user.
	    {"pg_roles",
	     "(SELECT 10::INTEGER AS oid, current_user AS rolname, true AS rolsuper, true AS rolinherit, "
	     "false AS rolcreaterole, false AS rolcreatedb, true AS rolcanlogin, false AS rolreplication, "
	     "-1::INTEGER AS rolconnlimit, '********' AS rolpassword, NULL::TIMESTAMPTZ AS rolvaliduntil, "
	     "false AS rolbypassrls, NULL::VARCHAR[] AS rolconfig)"},
	    {"pg_auth_members",
	     "(SELECT NULL::INTEGER AS oid, NULL::INTEGER AS roleid, NULL::INTEGER AS member, NULL::INTEGER AS grantor, "
	     "NULL::BOOLEAN AS admin_option, NULL::BOOLEAN AS inherit_option, NULL::BOOLEAN AS set_option WHERE false)"},
	    // DuckDB's own time zone list.
	    {"pg_timezone_names", "(SELECT name, abbrev, utc_offset, is_dst FROM pg_timezone_names())"},
	    {"pg_timezone_abbrevs", "(SELECT DISTINCT abbrev, utc_offset, is_dst FROM pg_timezone_names())"},
	    // This session only.
	    {"pg_stat_activity",
	     "(SELECT 1::INTEGER AS datid, " + db + " AS datname, " + session_pid + "::INTEGER AS pid, "
	     "NULL::INTEGER AS leader_pid, 10::INTEGER AS usesysid, current_user AS usename, "
	     "NULL::VARCHAR AS application_name, '127.0.0.1' AS client_addr, "
	     "NULL::VARCHAR AS client_hostname, NULL::INTEGER AS client_port, NULL::TIMESTAMPTZ AS backend_start, "
	     "NULL::TIMESTAMPTZ AS xact_start, NULL::TIMESTAMPTZ AS query_start, NULL::TIMESTAMPTZ AS state_change, "
	     "NULL::VARCHAR AS wait_event_type, NULL::VARCHAR AS wait_event, 'active' AS state, "
	     "NULL::BIGINT AS backend_xid, NULL::BIGINT AS backend_xmin, NULL::BIGINT AS query_id, '' AS query, "
	     "'client backend' AS backend_type)"},
	    // No comments on databases or roles.
	    {"pg_shdescription",
	     "(SELECT NULL::INTEGER AS objoid, NULL::INTEGER AS classoid, NULL::VARCHAR AS description WHERE false)"},
	};
	for (auto &view : views) {
		if (lower.find(view.name) == std::string::npos) {
			continue;
		}
		std::regex pattern(std::string(R"(\b(?:pg_catalog\s*\.\s*)?)") + view.name + R"(\b(?!\s*[.(]))", std::regex::icase);
		out = ReplaceEach(out, pattern, [&](const std::smatch &, size_t end) {
			return HasAlias(out, end) ? view.query : view.query + " AS " + view.name;
		});
	}
	return out;
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
