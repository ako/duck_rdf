#include "pgcompat.hpp"

#include <cctype>
#include <regex>
#include <map>
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

namespace {

// An empty table with the given columns ("name TYPE, ..."; a bare name is a BIGINT, as DuckDB's
// catalog oids are) plus xmin.
std::string EmptyTable(const std::string &columns) {
	std::string select;
	size_t pos = 0;
	while (pos < columns.size()) {
		auto comma = columns.find(',', pos);
		auto column = columns.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
		pos = comma == std::string::npos ? columns.size() : comma + 1;
		auto first = column.find_first_not_of(' ');
		column = column.substr(first, column.find_last_not_of(' ') - first + 1);
		auto space = column.find(' ');
		auto name = column.substr(0, space);
		auto type = space == std::string::npos ? std::string("BIGINT") : column.substr(space + 1);
		select += "NULL::" + type + " AS " + name + ", ";
	}
	return "(SELECT " + select + "0::BIGINT AS xmin WHERE false)";
}

// PostgreSQL's fixed oids of its catalog tables, for 'pg_class'::regclass and the like.
const std::map<std::string, int> CATALOG_OIDS = {
    {"pg_aggregate", 2600}, {"pg_am", 2601},          {"pg_amop", 2602},      {"pg_amproc", 2603},
    {"pg_attrdef", 2604},   {"pg_attribute", 1249},   {"pg_authid", 1260},    {"pg_cast", 2605},
    {"pg_class", 1259},     {"pg_collation", 3456},   {"pg_constraint", 2606}, {"pg_database", 1262},
    {"pg_depend", 2608},    {"pg_description", 2609}, {"pg_enum", 3501},      {"pg_event_trigger", 3466},
    {"pg_extension", 3079}, {"pg_foreign_data_wrapper", 2328}, {"pg_foreign_server", 1417},
    {"pg_index", 2610},     {"pg_inherits", 2611},    {"pg_language", 2612},  {"pg_namespace", 2615},
    {"pg_opclass", 2616},   {"pg_operator", 2617},    {"pg_opfamily", 2753},  {"pg_policy", 3256},
    {"pg_proc", 1255},      {"pg_rewrite", 2618},     {"pg_sequence", 2224},  {"pg_tablespace", 1213},
    {"pg_trigger", 2620},   {"pg_type", 1247},        {"pg_user_mapping", 1418}};

struct FunctionGap {
	std::regex pattern;
	std::string replacement; // with $1 for the first argument; see the loop below
};

} // namespace

std::string ReplaceCatalogGaps(const std::string &sql, const std::string &database, int32_t pid) {
	// Cheap test first: almost every query has none of these.
	auto lower = sql;
	for (auto &c : lower) {
		c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
	}
	static const char *markers[] = {"pg_", "current_database", "xmin", "age", "quote_ident", "::reg"};
	bool any = false;
	for (auto marker : markers) {
		any = any || lower.find(marker) != std::string::npos;
	}
	if (!any) {
		return sql;
	}
	auto db = Quote(database);
	auto session_pid = std::to_string(pid);
	auto icase = std::regex::icase;

	// DuckDB registers some PostgreSQL functions only unqualified (pg_catalog.txid_current() is read
	// as a column pg_catalog), and resolves all of them that way.
	static const std::regex qualified_call(R"(\bpg_catalog\s*\.\s*([A-Za-z_][A-Za-z_0-9]*)\s*\()", icase);
	auto out = ReplaceEach(sql, qualified_call, [](const std::smatch &m, size_t) { return m[1].str() + "("; });

	// 'pg_class'::regclass: PostgreSQL's oid of that catalog. Other reg* casts (of columns of catalogs
	// that are empty here), and casts to PostgreSQL's identifier type name, become text.
	static const std::regex regclass_literal(R"('(?:pg_catalog\.)?([A-Za-z_0-9]+)'\s*::\s*regclass\b)", icase);
	out = ReplaceEach(out, regclass_literal, [](const std::smatch &m, size_t) {
		auto it = CATALOG_OIDS.find(m[1].str());
		return std::to_string(it == CATALOG_OIDS.end() ? 0 : it->second) + "::BIGINT";
	});
	static const std::regex reg_cast(R"(::\s*(?:reg(?:class|proc|procedure|oper|operator|type)|name)\b)", icase);
	out = ReplaceEach(out, reg_cast, [](const std::smatch &, size_t) { return std::string("::VARCHAR"); });

	// Functions DuckDB lacks or answers differently. $1 is the (parenthesis-free) first argument.
	static const std::vector<FunctionGap> functions = {
	    {std::regex(R"(\bcurrent_database\s*\(\s*\))", icase), "@DB"},
	    {std::regex(R"(\bpg_backend_pid\s*\(\s*\))", icase), "@PID"},
	    {std::regex(R"(\bpg_get_userbyid\s*\([^()]*\))", icase), "current_user"},
	    // age() of a transaction id: DuckDB's age() takes timestamps only. No transaction id is ever
	    // old. Not a bare 0: ORDER BY 0 is a position.
	    {std::regex(R"re(\bage\s*\(\s*[A-Za-z_0-9."]*(?:xid|transactionid|xmin)"?\s*\))re", icase), "0::INTEGER"},
	    {std::regex(R"(\bpg_is_in_recovery\s*\(\s*\))", icase), "false"},
	    {std::regex(R"(\bpg_tablespace_location\s*\([^()]*\))", icase), "NULL::VARCHAR"},
	    {std::regex(R"(\bquote_ident\s*\(([^()]*)\))", icase), "('\"' || replace($1, '\"', '\"\"') || '\"')"},
	    // DuckDB's pg_get_viewdef gives the whole CREATE VIEW statement; PostgreSQL's only the query.
	    {std::regex(R"(\bpg_get_viewdef\s*\(([^(),]*)(?:,\s*(?:true|false)\s*)?\))", icase),
	     "regexp_replace(regexp_replace(pg_get_viewdef($1), '(?is)^\\s*create\\s+(or\\s+replace\\s+)?(temp\\w*\\s+)?"
	     "view\\s+.+?\\s+as\\s+', ''), '\\s*;\\s*$', '')"},
	    // Definitions of objects this server never has (functions, rules, triggers, indexes, partitions).
	    {std::regex(R"(\bpg_get_(?:function_arguments|function_result|function_sqlbody|ruledef|triggerdef|indexdef|partkeydef)\s*\([^()]*\))",
	                icase),
	     "NULL::VARCHAR"},
	    // Also used as a table function (cross join pg_indexam_has_property(...) alias): a one-row
	    // subquery works there and as an expression.
	    {std::regex(R"(\bpg_indexam_has_property\s*\([^()]*\))", icase), "(SELECT false AS pg_indexam_has_property)"},
	    {std::regex(R"(\bpg_available_extension_versions\s*\(\s*\))", icase),
	     EmptyTable("name VARCHAR, version VARCHAR, installed BOOLEAN, superuser BOOLEAN, trusted BOOLEAN, "
	                "relocatable BOOLEAN, schema VARCHAR, requires VARCHAR[], comment VARCHAR")},
	    // encode(bytes, 'escape'): DuckDB's encode takes text only.
	    {std::regex(R"(\bencode\s*\(([^(),]*),\s*'escape'\s*\))", icase), "CAST($1 AS VARCHAR)"},
	};
	for (auto &gap : functions) {
		out = ReplaceEach(out, gap.pattern, [&](const std::smatch &m, size_t) {
			if (gap.replacement == "@DB") {
				return db;
			}
			if (gap.replacement == "@PID") {
				return session_pid;
			}
			auto text = gap.replacement;
			auto at = text.find("$1");
			while (at != std::string::npos && m.size() > 1) {
				text.replace(at, 2, m[1].str());
				at = text.find("$1", at + m[1].length());
			}
			return text;
		});
	}

	// Catalog tables. DuckDB's own get an xmin (the row version clients such as DataGrip read to see
	// what changed: every row is at version 0); the rest are filled in.
	static const char *with_xmin[] = {"pg_class",       "pg_namespace", "pg_attribute", "pg_type",
	                                  "pg_description", "pg_attrdef",   "pg_tablespace", "pg_am",
	                                  "pg_enum",        "pg_depend",    "pg_sequence"};
	std::vector<std::pair<std::string, std::string>> catalogs;
	for (auto name : with_xmin) {
		catalogs.emplace_back(name, std::string("(SELECT *, 0::BIGINT AS xmin FROM pg_catalog.") + name + ")");
	}
	// The database the client connected to, and no other.
	catalogs.emplace_back(
	    "pg_database",
	    "(SELECT 1::BIGINT AS oid, " + db + " AS datname, 10::BIGINT AS datdba, 6::INTEGER AS encoding, "
	    "'c' AS datlocprovider, false AS datistemplate, true AS datallowconn, -1::INTEGER AS datconnlimit, "
	    "0::BIGINT AS datfrozenxid, 0::BIGINT AS datminmxid, 1663::BIGINT AS dattablespace, 'C' AS datcollate, "
	    "'C' AS datctype, NULL::VARCHAR AS daticulocale, NULL::VARCHAR AS daticurules, NULL::VARCHAR AS "
	    "datcollversion, NULL::VARCHAR[] AS datacl, 0::BIGINT AS xmin)");
	// One role: the user DuckDB reports as current_user.
	catalogs.emplace_back(
	    "pg_roles", "(SELECT 10::BIGINT AS oid, current_user AS rolname, true AS rolsuper, true AS rolinherit, "
	                "false AS rolcreaterole, false AS rolcreatedb, true AS rolcanlogin, false AS rolreplication, "
	                "-1::INTEGER AS rolconnlimit, '********' AS rolpassword, NULL::TIMESTAMPTZ AS rolvaliduntil, "
	                "false AS rolbypassrls, NULL::VARCHAR[] AS rolconfig, 0::BIGINT AS xmin)");
	catalogs.emplace_back("pg_user",
	                      "(SELECT current_user AS usename, 10::BIGINT AS usesysid, false AS usecreatedb, true AS usesuper, "
	                      "false AS userepl, false AS usebypassrls, '********' AS passwd, NULL::TIMESTAMPTZ AS valuntil, "
	                      "NULL::VARCHAR[] AS useconfig)");
	// This session, without SSL (the endpoint refuses it).
	catalogs.emplace_back("pg_stat_ssl",
	                      "(SELECT " + session_pid +
	                          "::INTEGER AS pid, false AS ssl, NULL::VARCHAR AS version, NULL::VARCHAR AS cipher, "
	                          "NULL::INTEGER AS bits, NULL::VARCHAR AS client_dn, NULL::DECIMAL(38,0) AS client_serial, "
	                          "NULL::VARCHAR AS issuer_dn)");
	// This session only.
	catalogs.emplace_back(
	    "pg_stat_activity",
	    "(SELECT 1::BIGINT AS datid, " + db + " AS datname, " + session_pid +
	        "::INTEGER AS pid, NULL::INTEGER AS leader_pid, 10::BIGINT AS usesysid, current_user AS usename, "
	        "NULL::VARCHAR AS application_name, '127.0.0.1' AS client_addr, NULL::VARCHAR AS client_hostname, "
	        "NULL::INTEGER AS client_port, NULL::TIMESTAMPTZ AS backend_start, NULL::TIMESTAMPTZ AS xact_start, "
	        "NULL::TIMESTAMPTZ AS query_start, NULL::TIMESTAMPTZ AS state_change, NULL::VARCHAR AS wait_event_type, "
	        "NULL::VARCHAR AS wait_event, 'active' AS state, NULL::BIGINT AS backend_xid, NULL::BIGINT AS backend_xmin, "
	        "NULL::BIGINT AS query_id, '' AS query, 'client backend' AS backend_type)");
	// DuckDB's own time zone list.
	catalogs.emplace_back("pg_timezone_names", "(SELECT name, abbrev, utc_offset, is_dst FROM pg_timezone_names())");
	catalogs.emplace_back("pg_timezone_abbrevs", "(SELECT DISTINCT abbrev, utc_offset, is_dst FROM pg_timezone_names())");
	// Empty here, with PostgreSQL 16's columns: locks (DuckDB's concurrency control is optimistic),
	// shared comments and role memberships, and every kind of object the server never has. The
	// server's schema holds views only, so it has no indexes or constraints, and it defines no
	// functions (DuckDB's pg_proc would list its internal macros).
	static const std::pair<const char *, const char *> empty[] = {
	    {"pg_locks", "locktype VARCHAR, database, relation, page INTEGER, tuple SMALLINT, virtualxid VARCHAR, "
	                 "transactionid, classid, objid, objsubid SMALLINT, virtualtransaction VARCHAR, pid INTEGER, "
	                 "mode VARCHAR, granted BOOLEAN, fastpath BOOLEAN, waitstart TIMESTAMPTZ"},
	    {"pg_shdescription", "objoid, classoid, description VARCHAR"},
	    {"pg_auth_members", "oid, roleid, member, grantor, admin_option BOOLEAN, inherit_option BOOLEAN, "
	                        "set_option BOOLEAN"},
	    {"pg_index", "indexrelid, indrelid, indnatts SMALLINT, indnkeyatts SMALLINT, indisunique BOOLEAN, "
	                 "indnullsnotdistinct BOOLEAN, indisprimary BOOLEAN, indisexclusion BOOLEAN, indimmediate BOOLEAN, "
	                 "indisclustered BOOLEAN, indisvalid BOOLEAN, indcheckxmin BOOLEAN, indisready BOOLEAN, "
	                 "indislive BOOLEAN, indisreplident BOOLEAN, indkey SMALLINT[], indcollation BIGINT[], "
	                 "indclass BIGINT[], indoption SMALLINT[], indexprs VARCHAR, indpred VARCHAR"},
	    {"pg_constraint", "oid, conname VARCHAR, connamespace, contype VARCHAR, condeferrable BOOLEAN, "
	                      "condeferred BOOLEAN, convalidated BOOLEAN, conrelid, contypid, conindid, conparentid, "
	                      "confrelid, confupdtype VARCHAR, confdeltype VARCHAR, confmatchtype VARCHAR, "
	                      "conislocal BOOLEAN, coninhcount SMALLINT, connoinherit BOOLEAN, conkey SMALLINT[], "
	                      "confkey SMALLINT[], conpfeqop BIGINT[], conppeqop BIGINT[], conffeqop BIGINT[], "
	                      "confdelsetcols SMALLINT[], conexclop BIGINT[], conbin VARCHAR"},
	    {"pg_proc", "oid, proname VARCHAR, pronamespace, proowner, prolang, procost DOUBLE, prorows DOUBLE, "
	                "provariadic, prosupport, prokind VARCHAR, prosecdef BOOLEAN, proleakproof BOOLEAN, "
	                "proisstrict BOOLEAN, proretset BOOLEAN, provolatile VARCHAR, proparallel VARCHAR, "
	                "pronargs SMALLINT, pronargdefaults SMALLINT, prorettype, proargtypes BIGINT[], "
	                "proallargtypes BIGINT[], proargmodes VARCHAR[], proargnames VARCHAR[], proargdefaults VARCHAR, "
	                "protrftypes BIGINT[], prosrc VARCHAR, probin VARCHAR, prosqlbody VARCHAR, proconfig VARCHAR[], "
	                "proacl VARCHAR[]"},
	    {"pg_aggregate", "aggfnoid, aggkind VARCHAR, aggnumdirectargs SMALLINT, aggtransfn, aggfinalfn, aggcombinefn, "
	                     "aggserialfn, aggdeserialfn, aggmtransfn, aggminvtransfn, aggmfinalfn, aggfinalextra BOOLEAN, "
	                     "aggmfinalextra BOOLEAN, aggfinalmodify VARCHAR, aggmfinalmodify VARCHAR, aggsortop, "
	                     "aggtranstype, aggtransspace INTEGER, aggmtranstype, aggmtransspace INTEGER, "
	                     "agginitval VARCHAR, aggminitval VARCHAR"},
	    {"pg_collation", "oid, collname VARCHAR, collnamespace, collowner, collprovider VARCHAR, "
	                     "collisdeterministic BOOLEAN, collencoding INTEGER, collcollate VARCHAR, collctype VARCHAR, "
	                     "colliculocale VARCHAR, collicurules VARCHAR, collversion VARCHAR"},
	    {"pg_inherits", "inhrelid, inhparent, inhseqno INTEGER, inhdetachpending BOOLEAN"},
	    {"pg_language", "oid, lanname VARCHAR, lanowner, lanispl BOOLEAN, lanpltrusted BOOLEAN, lanplcallfoid, "
	                    "laninline, lanvalidator, lanacl VARCHAR[]"},
	    {"pg_event_trigger", "oid, evtname VARCHAR, evtevent VARCHAR, evtowner, evtfoid, evtenabled VARCHAR, "
	                         "evttags VARCHAR[]"},
	    {"pg_trigger", "oid, tgrelid, tgparentid, tgname VARCHAR, tgfoid, tgtype SMALLINT, tgenabled VARCHAR, "
	                   "tgisinternal BOOLEAN, tgconstrrelid, tgconstrindid, tgconstraint, tgdeferrable BOOLEAN, "
	                   "tginitdeferred BOOLEAN, tgnargs SMALLINT, tgattr SMALLINT[], tgargs BLOB, tgqual VARCHAR, "
	                   "tgoldtable VARCHAR, tgnewtable VARCHAR"},
	    {"pg_rewrite", "oid, rulename VARCHAR, ev_class, ev_type VARCHAR, ev_enabled VARCHAR, is_instead BOOLEAN, "
	                   "ev_qual VARCHAR, ev_action VARCHAR"},
	    {"pg_operator", "oid, oprname VARCHAR, oprnamespace, oprowner, oprkind VARCHAR, oprcanmerge BOOLEAN, "
	                    "oprcanhash BOOLEAN, oprleft, oprright, oprresult, oprcom, oprnegate, oprcode, oprrest, oprjoin"},
	    {"pg_opclass", "oid, opcmethod, opcname VARCHAR, opcnamespace, opcowner, opcfamily, opcintype, "
	                   "opcdefault BOOLEAN, opckeytype"},
	    {"pg_opfamily", "oid, opfmethod, opfname VARCHAR, opfnamespace, opfowner"},
	    {"pg_amop", "oid, amopfamily, amoplefttype, amoprighttype, amopstrategy SMALLINT, amoppurpose VARCHAR, "
	                "amopopr, amopmethod, amopsortfamily"},
	    {"pg_amproc", "oid, amprocfamily, amproclefttype, amprocrighttype, amprocnum SMALLINT, amproc"},
	    {"pg_cast", "oid, castsource, casttarget, castfunc, castcontext VARCHAR, castmethod VARCHAR"},
	    {"pg_policy", "oid, polname VARCHAR, polrelid, polcmd VARCHAR, polpermissive BOOLEAN, polroles BIGINT[], "
	                  "polqual VARCHAR, polwithcheck VARCHAR"},
	    {"pg_extension", "oid, extname VARCHAR, extowner, extnamespace, extrelocatable BOOLEAN, extversion VARCHAR, "
	                     "extconfig BIGINT[], extcondition VARCHAR[]"},
	    {"pg_available_extension_versions", "name VARCHAR, version VARCHAR, installed BOOLEAN, superuser BOOLEAN, "
	                                        "trusted BOOLEAN, relocatable BOOLEAN, schema VARCHAR, requires VARCHAR[], "
	                                        "comment VARCHAR"},
	    {"pg_available_extensions", "name VARCHAR, default_version VARCHAR, installed_version VARCHAR, comment VARCHAR"},
	    {"pg_foreign_table", "ftrelid, ftserver, ftoptions VARCHAR[]"},
	    {"pg_foreign_data_wrapper", "oid, fdwname VARCHAR, fdwowner, fdwhandler, fdwvalidator, fdwacl VARCHAR[], "
	                                "fdwoptions VARCHAR[]"},
	    {"pg_foreign_server", "oid, srvname VARCHAR, srvowner, srvfdw, srvtype VARCHAR, srvversion VARCHAR, "
	                          "srvacl VARCHAR[], srvoptions VARCHAR[]"},
	    {"pg_user_mapping", "oid, umuser, umserver, umoptions VARCHAR[]"},
	    {"pg_user_mappings", "umid, srvid, srvname VARCHAR, umuser, usename VARCHAR, umoptions VARCHAR[]"},
	};
	for (auto &table : empty) {
		catalogs.emplace_back(table.first, EmptyTable(table.second));
	}
	for (auto &catalog : catalogs) {
		auto &name = catalog.first;
		if (lower.find(name) == std::string::npos) {
			continue;
		}
		// A table reference only: not a function (pg_timezone_names()) or a qualifier (pg_class.oid).
		std::regex pattern(R"(\b(?:pg_catalog\s*\.\s*)?)" + name + R"(\b(?!\s*[.(]))", icase);
		auto input = out;
		out = ReplaceEach(input, pattern, [&](const std::smatch &, size_t end) {
			return HasAlias(input, end) ? catalog.second : catalog.second + " AS " + name;
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
	// TypeInfoCache: one type by name ($1)
	if (Contains(sql, "WHERE typname = $1") && Contains(sql, "array_upper(current_schemas(false), 1)")) {
		return std::string("SELECT oid, typname FROM ") + PG_TYPES + " WHERE typname = $1 LIMIT 1";
	}
	// The length of the name type (getClientInfoProperties, getVersionColumns)
	if (Contains(sql, "SELECT t.typlen FROM pg_catalog.pg_type t") && Contains(sql, "t.typname='name'")) {
		return std::string("SELECT 64::SMALLINT AS typlen");
	}
	// getMaxIndexKeys, which getImportedKeys/getExportedKeys read first: PostgreSQL's default
	if (Contains(sql, "FROM pg_catalog.pg_settings WHERE name='max_index_keys'")) {
		return std::string("SELECT '32' AS setting");
	}
	// DatabaseMetaData.getIndexInfo: clients see views, which have no indexes
	if (Contains(sql, "AS NON_UNIQUE") && Contains(sql, "_pg_expandarray(i.indkey)")) {
		return std::string(
		    "SELECT NULL::VARCHAR AS TABLE_CAT, NULL::VARCHAR AS TABLE_SCHEM, NULL::VARCHAR AS TABLE_NAME, "
		    "NULL::BOOLEAN AS NON_UNIQUE, NULL::VARCHAR AS INDEX_QUALIFIER, NULL::VARCHAR AS INDEX_NAME, "
		    "NULL::INTEGER AS TYPE, NULL::INTEGER AS ORDINAL_POSITION, NULL::VARCHAR AS COLUMN_NAME, "
		    "NULL::VARCHAR AS ASC_OR_DESC, NULL::DOUBLE AS CARDINALITY, NULL::INTEGER AS PAGES, "
		    "NULL::VARCHAR AS FILTER_CONDITION WHERE false");
	}
	// DatabaseMetaData.getFunctions: the server defines none
	if (Contains(sql, "AS FUNCTION_TYPE") && Contains(sql, "pg_get_function_result")) {
		return std::string("SELECT NULL::VARCHAR AS FUNCTION_CAT, NULL::VARCHAR AS FUNCTION_SCHEM, "
		                   "NULL::VARCHAR AS FUNCTION_NAME, NULL::VARCHAR AS REMARKS, NULL::SMALLINT AS FUNCTION_TYPE, "
		                   "NULL::VARCHAR AS SPECIFIC_NAME WHERE false");
	}
	if (Contains(sql, "'pg_catalog.array_in'::regproc")) {
		return std::string("SELECT false AS is_array, 'b' AS typtype, typname, oid FROM ") + PG_TYPES +
		       " ORDER BY oid DESC";
	}
	return std::nullopt;
}

} // namespace dgl
