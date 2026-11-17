#include "lakemodel.hpp"

#include "log.hpp"
#include "setup.hpp"
#include "sparql.hpp"

#include "duckdb/parser/parser.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <cerrno>
#include <csignal>
#include <unistd.h>

namespace dgl {

namespace fs = std::filesystem;

namespace {

const char *GENERATED_PREFIX = "90-cdc-";
const char *CONFIGURED_PREFIX = "00-config-";
const char *AGENT_PREFIX = "50-agent-";

std::vector<std::vector<std::string>> Rows(duckdb::Connection &con, const std::string &sql) {
	std::vector<std::vector<std::string>> out;
	auto result = con.Query(sql);
	if (result->HasError()) {
		return out;
	}
	while (auto chunk = result->Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
			std::vector<std::string> row;
			for (duckdb::idx_t c = 0; c < chunk->ColumnCount(); c++) {
				auto v = chunk->GetValue(c, r);
				row.push_back(v.IsNull() ? "" : v.ToString());
			}
			out.push_back(row);
		}
	}
	return out;
}

std::vector<std::string> Split(const std::string &text, char sep) {
	std::vector<std::string> out;
	std::string part;
	std::istringstream in(text);
	while (std::getline(in, part, sep)) {
		out.push_back(part);
	}
	return out;
}

bool AllDigits(const std::string &s) {
	return !s.empty() &&
	       std::all_of(s.begin(), s.end(), [](char c) { return std::isdigit(static_cast<unsigned char>(c)); });
}

std::string ReadText(const std::string &path) {
	std::ifstream in(path, std::ios::binary);
	std::stringstream text;
	text << in.rdbuf();
	return text.str();
}

// A Turtle "..." string.
std::string TurtleString(const std::string &s) {
	std::string out = "\"";
	for (char c : s) {
		if (c == '"' || c == '\\') {
			out += '\\';
		}
		out += c;
	}
	return out + "\"";
}

// An IRI path segment: letters, digits and _ - . kept, the rest percent-encoded.
std::string IriSegment(const std::string &s) {
	static const char *hex = "0123456789ABCDEF";
	std::string out;
	for (unsigned char c : s) {
		if (std::isalnum(c) || c == '_' || c == '-' || c == '.') {
			out += static_cast<char>(c);
		} else {
			out += '%';
			out += hex[c >> 4];
			out += hex[c & 15];
		}
	}
	return out;
}

// The XSD datatype R2RML should give a column of this DuckDB type, or "" for a plain string.
std::string XsdFor(const std::string &type) {
	static const std::string XSD = "http://www.w3.org/2001/XMLSchema#";
	if (type == "BIGINT" || type == "HUGEINT" || type == "UBIGINT" || type == "UINTEGER") {
		return XSD + "long";
	}
	if (type == "INTEGER" || type == "USMALLINT") {
		return XSD + "int";
	}
	if (type == "SMALLINT" || type == "UTINYINT") {
		return XSD + "short";
	}
	if (type == "TINYINT") {
		return XSD + "byte";
	}
	if (type.rfind("DECIMAL", 0) == 0) {
		return XSD + "decimal";
	}
	if (type == "DOUBLE") {
		return XSD + "double";
	}
	if (type == "FLOAT") {
		return XSD + "float";
	}
	if (type == "BOOLEAN") {
		return XSD + "boolean";
	}
	if (type == "DATE") {
		return XSD + "date";
	}
	if (type.rfind("TIMESTAMP", 0) == 0) {
		return XSD + "dateTime";
	}
	return "";
}

bool IsIdList(const std::string &type) {
	return type == "BIGINT[]" || type == "INTEGER[]" || type == "UBIGINT[]";
}

std::string Qualified(const std::string &schema, const std::string &name) {
	return SqlIdent(schema) + "." + SqlIdent(name);
}

} // namespace

std::string PlainName(const std::string &name) {
	std::string out;
	for (unsigned char c : name) {
		if (std::isalnum(c)) {
			out += static_cast<char>(std::tolower(c));
		} else if (!out.empty() && out.back() != '_') {
			out += '_';
		}
	}
	while (!out.empty() && out.back() == '_') {
		out.pop_back();
	}
	if (out.empty() || std::isdigit(static_cast<unsigned char>(out[0]))) {
		out = "t_" + out;
	}
	return out;
}

LakeModel::LakeModel(duckdb::DuckDB &db, const Config &config) : db_(db), config_(config) {
	state_dir_ = (fs::path(config.data_dir) / ".duckgraphlake").string();
	agent_mapping_dir_ = (fs::path(state_dir_) / "mappings").string();
	views_dir_ = (fs::path(state_dir_) / "views").string();
	std::error_code ec;
	auto base = fs::temp_directory_path(ec);
	// Mapping folders of earlier runs that did not stop cleanly (their process is gone).
	for (auto &entry : fs::directory_iterator(base, ec)) {
		auto name = entry.path().filename().string();
		if (name.rfind("duckgraphlake-", 0) != 0 || name.find("-mapping") == std::string::npos) {
			continue;
		}
		auto pid = std::atoll(name.c_str() + 14);
		if (pid > 0 && ::kill(static_cast<pid_t>(pid), 0) != 0 && errno == ESRCH) {
			fs::remove_all(entry.path(), ec);
		}
	}
	for (int i = 0;; i++) {
		auto dir = base / ("duckgraphlake-" + std::to_string(static_cast<long long>(::getpid())) +
		                   (i ? "-" + std::to_string(i) : std::string()) + "-mapping");
		if (fs::create_directory(dir, ec)) {
			mapping_dir_ = dir.string();
			break;
		}
		if (i > 100) {
			throw std::runtime_error("cannot create a mapping folder in " + base.string() + ": " + ec.message());
		}
	}
}

LakeModel::~LakeModel() {
	std::error_code ec;
	fs::remove_all(mapping_dir_, ec);
	fs::remove_all(mapping_dir_ + "-check", ec);
}

std::string LakeModel::MappingGlob() const {
	return (fs::path(mapping_dir_) / "*").string();
}

bool LakeModel::HasMapping() const {
	return !MappingFiles().empty();
}

std::vector<MappingFile> LakeModel::MappingFiles() const {
	std::vector<MappingFile> files;
	std::error_code ec;
	if (!fs::is_directory(mapping_dir_, ec)) {
		return files;
	}
	for (auto &entry : fs::directory_iterator(mapping_dir_, ec)) {
		auto name = entry.path().filename().string();
		MappingFile f;
		f.name = name;
		f.path = entry.path().string();
		if (name.rfind(CONFIGURED_PREFIX, 0) == 0) {
			f.origin = "configured";
			if (fs::is_symlink(entry.path(), ec)) {
				f.path = fs::read_symlink(entry.path(), ec).string();
			}
		} else if (name.rfind(AGENT_PREFIX, 0) == 0) {
			f.origin = "agent";
			if (fs::is_symlink(entry.path(), ec)) {
				f.path = fs::read_symlink(entry.path(), ec).string();
			}
		} else if (name.rfind(GENERATED_PREFIX, 0) == 0) {
			f.origin = "generated";
		} else {
			f.origin = "other";
		}
		if (fs::exists(entry.path(), ec)) { // a link to a file that is gone does not count
			files.push_back(f);
		}
	}
	std::sort(files.begin(), files.end(), [](const MappingFile &a, const MappingFile &b) { return a.name < b.name; });
	return files;
}

std::string LakeModel::MappingOf(const std::string &name) const {
	for (auto &f : MappingFiles()) {
		if (f.name == name) {
			return ReadText((fs::path(mapping_dir_) / f.name).string());
		}
	}
	throw std::runtime_error("no mapping file " + name + "; get_mapping lists them");
}

bool LakeModel::WriteIfChanged(const std::string &path, const std::string &text) {
	std::error_code ec;
	if (fs::exists(path, ec) && ReadText(path) == text) {
		return false;
	}
	auto tmp = path + ".tmp";
	{
		std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
		out << text;
		if (!out) {
			throw std::runtime_error("cannot write " + tmp);
		}
	}
	fs::rename(tmp, path);
	return true;
}

void LakeModel::LinkConfiguredMappings() {
	std::error_code ec;
	std::set<std::string> wanted;
	if (!config_.mapping.empty()) {
		duckdb::Connection con(db_);
		int i = 0;
		for (auto &file : GlobFiles(con, config_.mapping)) {
			auto target = fs::absolute(file).lexically_normal();
			char index[8];
			snprintf(index, sizeof(index), "%02d-", i++);
			auto link = fs::path(mapping_dir_) / (std::string(CONFIGURED_PREFIX) + index + target.filename().string());
			wanted.insert(link.filename().string());
			if (fs::is_symlink(link, ec) && fs::read_symlink(link, ec) == target) {
				continue;
			}
			fs::remove(link, ec);
			fs::create_symlink(target, link, ec);
			if (ec) {
				throw std::runtime_error("cannot link " + link.string() + " to " + target.string() + ": " +
				                         ec.message());
			}
		}
	}
	// Agents' mappings, kept in the data folder.
	if (fs::is_directory(agent_mapping_dir_, ec)) {
		for (auto &entry : fs::directory_iterator(agent_mapping_dir_, ec)) {
			auto target = entry.path();
			auto link = fs::path(mapping_dir_) / (std::string(AGENT_PREFIX) + target.filename().string());
			wanted.insert(link.filename().string());
			if (fs::is_symlink(link, ec) && fs::read_symlink(link, ec) == target) {
				continue;
			}
			fs::remove(link, ec);
			fs::create_symlink(target, link, ec);
		}
	}
	for (auto &entry : fs::directory_iterator(mapping_dir_, ec)) {
		auto name = entry.path().filename().string();
		if ((name.rfind(CONFIGURED_PREFIX, 0) == 0 || name.rfind(AGENT_PREFIX, 0) == 0) && !wanted.count(name)) {
			fs::remove(entry.path(), ec);
		}
	}
}

bool LakeModel::Refresh() {
	std::lock_guard<std::mutex> lock(mutex_);
	std::error_code ec;
	fs::create_directories(mapping_dir_, ec);
	LinkConfiguredMappings();

	duckdb::Connection con(db_);
	// Topic tables: the views over Kafka topics (they have the warpstream struct), named
	// <cluster>__<topic>.
	std::vector<TopicTable> topics;
	for (auto &row : Rows(con, "SELECT c.table_name, bool_or(c.column_name = 'cdc') FROM duckdb_columns() c JOIN "
	                           "duckdb_views() v ON v.view_name = c.table_name AND v.schema_name = c.schema_name "
	                           "WHERE c.schema_name = 'main' AND NOT v.internal GROUP BY 1 HAVING bool_or("
	                           "c.column_name = 'warpstream') ORDER BY 1")) {
		auto &name = row[0];
		auto sep = name.find("__");
		if (sep == std::string::npos) {
			continue;
		}
		TopicTable t;
		t.table = name;
		t.topic = name.substr(sep + 2);
		auto parts = Split(t.topic, '.');
		if (row[1] == "true") {
			t.kind = "cdc";
			t.current = name + "_current";
			// Mendix: cdc.<app>.<entity>.<major revision>.<space>, perhaps after a prefix.
			auto at = std::find(parts.begin(), parts.end(), "cdc");
			if (at != parts.end() && parts.end() - at >= 4 && AllDigits(*(at + 3))) {
				t.app = *(at + 1);
				t.entity = *(at + 2);
				t.revision = *(at + 3);
			} else if (parts.size() >= 2) { // Debezium: <server>.<schema>.<table>
				t.app = parts.front();
				t.entity = parts.back();
			} else {
				t.app = "cdc";
				t.entity = t.topic;
			}
		} else {
			t.kind = "raw"; // "events" once BuildAppViews finds CloudEvents headers (ce_type, ce_source)
		}
		topics.push_back(t);
	}
	BuildAppViews(con, topics);

	// A generated mapping per app with CDC entities; files of apps that went away are removed.
	std::map<std::string, std::vector<TopicTable>> by_app;
	for (auto &t : topics) {
		if (t.kind == "cdc" && config_.kafka_cdc_mapping) {
			by_app[t.app].push_back(t);
		}
	}
	std::set<std::string> wanted;
	for (auto &app : by_app) {
		auto name = std::string(GENERATED_PREFIX) + PlainName(app.first) + ".ttl";
		wanted.insert(name);
		auto text = GenerateMapping(con, app.first, app.second);
		if (text.empty()) {
			fs::remove(fs::path(mapping_dir_) / name, ec);
		} else {
			WriteIfChanged((fs::path(mapping_dir_) / name).string(), text);
		}
	}
	for (auto &entry : fs::directory_iterator(mapping_dir_, ec)) {
		auto name = entry.path().filename().string();
		if (name.rfind(GENERATED_PREFIX, 0) == 0 && !wanted.count(name)) {
			fs::remove(entry.path(), ec);
		}
	}
	topics_ = topics;

	// Changed since the last Refresh (also by SetMapping, RemoveMapping or an edited file)?
	std::string stamp;
	for (auto &f : MappingFiles()) {
		auto p = fs::path(mapping_dir_) / f.name;
		stamp += f.name + ":" + std::to_string(fs::file_size(p, ec)) + ":" +
		         std::to_string(fs::last_write_time(p, ec).time_since_epoch().count()) + ";";
	}
	bool changed = stamp != mapping_stamp_;
	mapping_stamp_ = stamp;
	return changed;
}

void LakeModel::BuildAppViews(duckdb::Connection &con, std::vector<TopicTable> &topics) {
	auto create = [&](const std::string &schema, const std::string &name, const std::string &sql) {
		auto key = schema + "." + name;
		if (view_sql_.count(key) && view_sql_[key] == sql) {
			return true; // unchanged: leave the catalog (and the SPARQL cache) alone
		}
		try {
			Exec(con, "CREATE SCHEMA IF NOT EXISTS " + SqlIdent(schema));
			Exec(con, "CREATE OR REPLACE VIEW " + Qualified(schema, name) + " AS " + sql);
			view_sql_[key] = sql;
			return true;
		} catch (std::exception &ex) {
			Log("model: no view " + key + ": " + ex.what());
			return false;
		}
	};
	for (auto &t : topics) {
		if (t.kind == "cdc") {
			auto schema = PlainName(t.app);
			auto entity = PlainName(t.entity);
			if (create(schema, entity, "SELECT * FROM " + SqlIdent(t.current))) {
				t.view = schema + "." + entity;
			}
			if (create(schema, entity + "_history",
			           "SELECT cdc.op AS change_op, cdc.ts AS changed_at, cdc.tx_id AS tx_id, cdc.snapshot AS "
			           "snapshot, * EXCLUDE (cdc, warpstream), warpstream.\"offset\" AS kafka_offset FROM " +
			               SqlIdent(t.table))) {
				t.history = schema + "." + entity + "_history";
			}
			// Associations (lists of object ids): one row per link, for joins and the mapping.
			for (auto &col : Rows(con, "SELECT column_name, column_type FROM (DESCRIBE " + SqlIdent(t.current) + ")")) {
				if (IsIdList(col[1])) {
					create(schema, entity + "__" + PlainName(col[0]),
					       "SELECT id, unnest(" + SqlIdent(col[0]) + ") AS target FROM " + SqlIdent(t.current));
				}
			}
		} else {
			// Events with CloudEvents headers (Mendix business events): the app is the first segment of
			// ce_source (/TestApp/OrdersEventService/), the event type ce_type. A view per type, its JSON
			// payload's fields as columns.
			auto types = Rows(con, "SELECT warpstream.headers['ce_source'], warpstream.headers['ce_type'], "
			                       "json_group_structure(TRY_CAST(TRY(decode(warpstream.\"value\")) AS JSON)) FROM " +
			                           SqlIdent(t.table) +
			                           " WHERE warpstream.headers['ce_type'] IS NOT NULL GROUP BY ALL ORDER BY 1, 2");
			for (auto &row : types) {
				auto segments = Split(row[0], '/');
				std::string app;
				for (auto &s : segments) {
					if (!s.empty()) {
						app = s;
						break;
					}
				}
				if (app.empty() || row[1].empty()) {
					continue;
				}
				t.app = app;
				auto schema = PlainName(app);
				auto name = PlainName(row[1]);
				std::string fields;
				// JSON integers come out of json_group_structure as UBIGINT: signed is what clients expect.
				for (size_t at = row[2].find("\"UBIGINT\""); at != std::string::npos;
				     at = row[2].find("\"UBIGINT\"", at)) {
					row[2].replace(at, 9, "\"BIGINT\"");
				}
				if (!row[2].empty() && row[2][0] == '{') {
					fields = "unnest(json_transform(TRY(decode(warpstream.\"value\")), " + SqlLiteral(row[2]) + ")), ";
				} else {
					fields = "TRY_CAST(TRY(decode(warpstream.\"value\")) AS JSON) AS payload, ";
				}
				auto sql = "SELECT warpstream.headers['ce_id'] AS event_id, TRY_CAST(warpstream.headers['ce_time'] AS "
				           "TIMESTAMPTZ) AS event_time, warpstream.headers['ce_subject'] AS subject, " +
				           fields + "warpstream.\"offset\" AS kafka_offset FROM " + SqlIdent(t.table) +
				           " WHERE warpstream.headers['ce_type'] = " + SqlLiteral(row[1]) +
				           " AND warpstream.headers['ce_source'] = " + SqlLiteral(row[0]);
				if (create(schema, name, sql)) {
					t.event_views.push_back(schema + "." + name);
					t.kind = "events";
				}
			}
		}
	}
}

std::string LakeModel::GenerateMapping(duckdb::Connection &con, const std::string &app,
                                       const std::vector<TopicTable> &topics) {
	// IRIs: the app's own namespace, so two apps with an Order entity do not meet. Classes are named
	// <App>_<Entity>, which is also the OData entity set name.
	auto ns = "http://duckgraphlake.local/" + IriSegment(app) + "/";
	auto data = "http://data.duckgraphlake.local/" + IriSegment(app) + "/";
	auto schema = PlainName(app);
	struct Entity {
		const TopicTable *topic;
		std::string view, subject_template;
		std::vector<std::vector<std::string>> columns; // name, type
		std::string type_code;                         // Mendix object ids: entity type in the bits above 48
	};
	std::vector<Entity> entities;
	for (auto &t : topics) {
		if (t.view.empty()) {
			continue;
		}
		Entity e;
		e.topic = &t;
		e.view = PlainName(t.entity);
		e.subject_template = data + IriSegment(PlainName(t.entity)) + "/{id}";
		e.columns = Rows(con, "SELECT column_name, column_type FROM (DESCRIBE " + Qualified(schema, e.view) + ")");
		bool has_id = false;
		for (auto &c : e.columns) {
			has_id = has_id || c[0] == "id";
		}
		if (!has_id) {
			continue;
		}
		auto code = Rows(con, "SELECT any_value(id >> 48) FROM " + Qualified(schema, e.view));
		e.type_code = code.empty() ? "" : code[0][0];
		entities.push_back(e);
	}
	if (entities.empty()) {
		return "";
	}
	std::ostringstream ttl;
	ttl << "# Generated by duckgraphlake from the CDC topics of " << app
	    << ". Do not edit: it is rewritten as the topics change.\n"
	    << "# Extend or override it with a mapping of your own (set_mapping over MCP, or --mapping).\n"
	    << "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n\n";
	for (auto &e : entities) {
		auto cls = ns + IriSegment(app) + "_" + IriSegment(e.topic->entity);
		auto props = ns + IriSegment(e.topic->entity) + "#";
		// Associations (lists of object ids) are joined in through their link views, one row per link,
		// so each entity is one triples map with one class: every attribute and association of it is
		// then a property of that class (OData: a navigation property). The target entity is the one
		// whose object ids carry the same type code, or else the one the association is named after
		// (Order_Customer -> Customer).
		struct Link {
			std::string column, alias, object_template;
		};
		std::vector<Link> links;
		std::string joins, extra;
		for (auto &c : e.columns) {
			if (!IsIdList(c[1])) {
				continue;
			}
			auto link_view = e.view + "__" + PlainName(c[0]);
			auto code = Rows(con, "SELECT any_value(target >> 48) FROM " + Qualified(schema, link_view));
			const Entity *target = nullptr;
			if (!code.empty() && !code[0][0].empty()) {
				for (auto &other : entities) {
					if (other.type_code == code[0][0]) {
						target = &other;
					}
				}
			}
			if (!target) {
				auto under = c[0].find('_');
				auto named = under == std::string::npos ? c[0] : c[0].substr(under + 1);
				for (auto &other : entities) {
					if (PlainName(other.topic->entity) == PlainName(named)) {
						target = &other;
					}
				}
			}
			if (!target) {
				continue;
			}
			Link link;
			link.column = c[0];
			link.alias = "link_" + std::to_string(links.size() + 1);
			link.object_template = target->subject_template;
			link.object_template.replace(link.object_template.find("{id}"), 4, "{" + link.alias + "}");
			joins += " LEFT JOIN " + Qualified(schema, link_view) + " AS l" + std::to_string(links.size() + 1) +
			         " ON l" + std::to_string(links.size() + 1) + ".id = e.id";
			extra += ", l" + std::to_string(links.size() + 1) + ".target AS " + link.alias;
			links.push_back(link);
		}
		auto query = "SELECT e.*" + extra + " FROM " + Qualified(schema, e.view) + " AS e" + joins;
		ttl << "<#" << IriSegment(e.topic->entity) << "> rr:logicalTable [ rr:sqlQuery " << TurtleString(query)
		    << " ] ;\n"
		    << "  rr:subjectMap [ rr:template " << TurtleString(e.subject_template) << " ; rr:class <" << cls << "> ]";
		for (auto &c : e.columns) {
			if (IsIdList(c[1]) || c[1].rfind("STRUCT", 0) == 0 || c[1] == "JSON" || c[1].find("MAP(") == 0 ||
			    c[1].back() == ']') {
				continue;
			}
			auto xsd = XsdFor(c[1]);
			ttl << " ;\n  rr:predicateObjectMap [ rr:predicate <" << props << IriSegment(c[0])
			    << "> ; rr:objectMap [ rr:column " << TurtleString(c[0]);
			if (!xsd.empty()) {
				ttl << " ; rr:datatype <" << xsd << ">";
			}
			ttl << " ] ]";
		}
		for (auto &link : links) {
			ttl << " ;\n  rr:predicateObjectMap [ rr:predicate <" << props << IriSegment(link.column)
			    << "> ; rr:objectMap [ rr:template " << TurtleString(link.object_template) << " ] ]";
		}
		ttl << " .\n\n";
	}
	return ttl.str();
}

std::vector<TopicTable> LakeModel::Topics() const {
	std::lock_guard<std::mutex> lock(mutex_);
	return topics_;
}

std::optional<TopicTable> LakeModel::Find(const std::string &name) const {
	std::lock_guard<std::mutex> lock(mutex_);
	auto plain = name;
	plain.erase(std::remove(plain.begin(), plain.end(), '"'), plain.end());
	for (auto &t : topics_) {
		if (t.table == plain || t.current == plain || t.view == plain || t.history == plain ||
		    std::find(t.event_views.begin(), t.event_views.end(), plain) != t.event_views.end()) {
			return t;
		}
	}
	return std::nullopt;
}

namespace {

// schema.name or name (schema main), each part [A-Za-z0-9_]+.
std::pair<std::string, std::string> ViewName(const std::string &name) {
	auto dot = name.find('.');
	auto schema = dot == std::string::npos ? std::string("main") : name.substr(0, dot);
	auto view = dot == std::string::npos ? name : name.substr(dot + 1);
	auto ok = [](const std::string &s) {
		return !s.empty() && std::all_of(s.begin(), s.end(), [](char c) {
			return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
		});
	};
	if (!ok(schema) || !ok(view)) {
		throw std::runtime_error("a view name is name or schema.name, with letters, digits and _ only");
	}
	return {schema, view};
}

std::string OnlySelect(const std::string &sql) {
	auto parser = duckdb::Parser::GetBuiltinParser();
	try {
		parser.ParseQuery(sql);
	} catch (std::exception &ex) {
		throw std::runtime_error(duckdb::ErrorData(ex).RawMessage());
	}
	if (parser.statements.size() != 1 || parser.statements[0]->type != duckdb::StatementType::SELECT_STATEMENT) {
		throw std::runtime_error("expected one SELECT statement");
	}
	auto text = sql;
	while (!text.empty() && (std::isspace(static_cast<unsigned char>(text.back())) || text.back() == ';')) {
		text.pop_back();
	}
	return text;
}

} // namespace

void LakeModel::DefineView(const std::string &name, const std::string &select, const std::string &description) {
	auto parts = ViewName(name);
	auto sql = OnlySelect(select);
	duckdb::Connection con(db_);
	if (parts.first != "main") {
		Exec(con, "CREATE SCHEMA IF NOT EXISTS " + SqlIdent(parts.first));
	}
	Exec(con, "CREATE OR REPLACE VIEW " + Qualified(parts.first, parts.second) + " AS " + sql);
	if (!description.empty()) {
		Exec(con, "COMMENT ON VIEW " + Qualified(parts.first, parts.second) + " IS " + SqlLiteral(description));
	}
	std::error_code ec;
	fs::create_directories(views_dir_, ec);
	std::ostringstream file;
	if (!description.empty()) {
		file << "-- " << description << "\n";
	}
	file << sql << "\n";
	WriteIfChanged((fs::path(views_dir_) / (parts.first + "." + parts.second + ".sql")).string(), file.str());
}

void LakeModel::DropView(const std::string &name) {
	auto parts = ViewName(name);
	auto path = fs::path(views_dir_) / (parts.first + "." + parts.second + ".sql");
	std::error_code ec;
	if (!fs::exists(path, ec)) {
		throw std::runtime_error("no agent view " + name + " (only views made with define_view can be dropped)");
	}
	duckdb::Connection con(db_);
	Exec(con, "DROP VIEW IF EXISTS " + Qualified(parts.first, parts.second));
	fs::remove(path, ec);
}

std::vector<std::pair<std::string, std::string>> LakeModel::AgentViews() const {
	std::vector<std::pair<std::string, std::string>> out;
	std::error_code ec;
	if (!fs::is_directory(views_dir_, ec)) {
		return out;
	}
	for (auto &entry : fs::directory_iterator(views_dir_, ec)) {
		auto file = entry.path().filename().string();
		if (file.size() > 4 && file.substr(file.size() - 4) == ".sql") {
			out.emplace_back(file.substr(0, file.size() - 4), ReadText(entry.path().string()));
		}
	}
	std::sort(out.begin(), out.end());
	return out;
}

std::vector<std::string> LakeModel::RestoreViews() {
	auto views = AgentViews();
	std::vector<std::string> problems;
	duckdb::Connection con(db_);
	// Views can depend on each other: repeat while some succeed.
	for (bool progress = true; progress && !views.empty();) {
		progress = false;
		problems.clear();
		std::vector<std::pair<std::string, std::string>> failed;
		for (auto &v : views) {
			auto parts = ViewName(v.first);
			try {
				if (parts.first != "main") {
					Exec(con, "CREATE SCHEMA IF NOT EXISTS " + SqlIdent(parts.first));
				}
				Exec(con, "CREATE OR REPLACE VIEW " + Qualified(parts.first, parts.second) + " AS " + v.second);
				progress = true;
			} catch (std::exception &ex) {
				failed.push_back(v);
				problems.push_back(v.first + ": " + ex.what());
			}
		}
		views = failed;
	}
	return problems;
}

void LakeModel::SetMapping(const std::string &name, const std::string &text, const std::string &format) {
	auto plain = PlainName(name);
	std::string ext;
	if (format.empty() || format == "turtle" || format == "ttl" || format == "r2rml") {
		ext = ".ttl";
	} else if (format == "yarrrml" || format == "yaml" || format == "yml") {
		ext = ".yml";
	} else {
		throw std::runtime_error("format is turtle (R2RML) or yarrrml");
	}
	std::error_code ec;
	auto tmp_dir = fs::path(mapping_dir_ + "-check");
	fs::create_directories(tmp_dir, ec);
	auto tmp = (tmp_dir / (plain + ext)).string();
	{
		std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
		out << text;
	}
	duckdb::Connection con(db_);
	// Parses the mapping and checks it can be translated against: the translator's own error.
	auto check = con.Query("SELECT sparql_to_sql('ASK { ?s ?p ?o }', " + SqlLiteral(tmp) + ")");
	if (check->HasError()) {
		fs::remove(tmp, ec);
		throw std::runtime_error("the mapping does not load: " + check->GetError());
	}
	std::lock_guard<std::mutex> lock(mutex_);
	fs::create_directories(agent_mapping_dir_, ec);
	for (auto &old : {".ttl", ".yml"}) {
		fs::remove(fs::path(agent_mapping_dir_) / (plain + old), ec);
	}
	fs::copy_file(tmp, fs::path(agent_mapping_dir_) / (plain + ext), fs::copy_options::overwrite_existing, ec);
	if (ec) {
		throw std::runtime_error("cannot write the mapping into " + agent_mapping_dir_ + ": " + ec.message());
	}
	fs::remove(tmp, ec);
}

void LakeModel::RemoveMapping(const std::string &name) {
	auto plain = PlainName(name);
	std::lock_guard<std::mutex> lock(mutex_);
	std::error_code ec;
	bool removed = false;
	for (auto &ext : {".ttl", ".yml"}) {
		removed = fs::remove(fs::path(agent_mapping_dir_) / (plain + ext), ec) || removed;
	}
	if (!removed) {
		throw std::runtime_error("no agent mapping " + name + " (configured and generated mappings stay)");
	}
}

std::string LakeModel::WatchStamp() const {
	std::string stamp;
	std::error_code ec;
	auto add = [&](const std::string &path) {
		stamp += path + ":" + std::to_string(fs::file_size(path, ec)) + ":" +
		         std::to_string(fs::last_write_time(path, ec).time_since_epoch().count()) + ";";
	};
	if (!config_.mapping.empty()) {
		duckdb::Connection con(db_);
		for (auto &f : GlobFiles(con, config_.mapping)) {
			add(f);
		}
	}
	if (!config_.init_sql.empty()) {
		add(config_.init_sql);
	}
	return stamp;
}

Json TopicReport(duckdb::DuckDB &db, const LakeModel &model, const std::vector<TopicState> &topics) {
	duckdb::Connection con(db);
	std::set<std::string> views;
	for (auto &row : Rows(con, "SELECT view_name FROM duckdb_views() WHERE schema_name = 'main'")) {
		views.insert(row[0]);
	}
	Json out = Json::Array();
	for (auto &t : topics) {
		Json item = Json::Object();
		item.Set("topic", t.topic);
		item.Set("table", t.table);
		item.Set("partitions", t.partitions);
		item.Set("uncommitted", t.buffered);
		auto info = model.Find(t.table);
		if (views.count(t.table)) {
			auto cdc = info && info->kind == "cdc";
			auto stats =
			    Rows(con, "SELECT count(*), max(warpstream.\"offset\"), max(warpstream.\"timestamp\")::VARCHAR" +
			                  std::string(cdc ? ", count(*) FILTER (WHERE cdc IS NULL)" : "") + " FROM " +
			                  SqlIdent(t.table));
			if (!stats.empty()) {
				item.Set("records", std::stoll(stats[0][0]));
				item.Set("last_offset", stats[0][1].empty() ? Json() : Json(std::stoll(stats[0][1])));
				item.Set("last_record", stats[0][2].empty() ? Json() : Json(stats[0][2]));
				if (cdc) {
					item.Set("not_change_events", std::stoll(stats[0][3]));
				}
			}
		} else {
			item.Set("records", 0);
			item.Set("note", "nothing committed yet");
		}
		if (info) {
			item.Set("kind", info->kind);
			if (!info->app.empty()) {
				item.Set("app", info->app);
			}
			if (info->kind == "cdc") {
				item.Set("entity", info->entity);
				if (!info->revision.empty()) {
					item.Set("revision", info->revision);
				}
				Json readable = Json::Object();
				readable.Set("current", info->view.empty() ? Json(info->current) : Json(info->view));
				if (!info->history.empty()) {
					readable.Set("history", info->history);
				}
				item.Set("views", readable);
			}
			if (info->kind == "events") {
				item.Set("views", Json::ArrayOf(info->event_views));
			}
		}
		out.Push(item);
	}
	return out;
}

} // namespace dgl
