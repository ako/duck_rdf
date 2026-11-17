#include "api.hpp"

#include "sparql.hpp"

#include "duckdb/main/query_result_stream.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>

namespace dgl {

namespace {

const std::string RDFS = "http://www.w3.org/2000/01/rdf-schema#";
const std::string SKOS = "http://www.w3.org/2004/02/skos/core#";

// Interrupts a connection's query after a timeout, unless it finished first.
class Deadline {
public:
	Deadline(duckdb::Connection &con, double seconds) {
		if (seconds > 0) {
			thread_ = std::thread([this, &con, seconds]() {
				std::unique_lock<std::mutex> lock(mutex_);
				auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(int64_t(seconds * 1000));
				if (!cv_.wait_until(lock, until, [this]() { return done_; })) {
					con.Interrupt();
				}
			});
		}
	}
	~Deadline() {
		{
			std::lock_guard<std::mutex> lock(mutex_);
			done_ = true;
		}
		cv_.notify_all();
		if (thread_.joinable()) {
			thread_.join();
		}
	}

private:
	std::mutex mutex_;
	std::condition_variable cv_;
	bool done_ = false;
	std::thread thread_;
};

Json CellValue(const duckdb::Value &v) {
	if (v.IsNull()) {
		return Json();
	}
	switch (v.type().id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		return Json(v.GetValue<bool>());
	case duckdb::LogicalTypeId::TINYINT:
	case duckdb::LogicalTypeId::SMALLINT:
	case duckdb::LogicalTypeId::INTEGER:
	case duckdb::LogicalTypeId::BIGINT:
	case duckdb::LogicalTypeId::UTINYINT:
	case duckdb::LogicalTypeId::USMALLINT:
	case duckdb::LogicalTypeId::UINTEGER:
		return Json(v.GetValue<int64_t>());
	case duckdb::LogicalTypeId::FLOAT:
	case duckdb::LogicalTypeId::DOUBLE:
		return Json(v.GetValue<double>());
	default:
		return Json(v.ToString()); // decimals keep their exact digits as text
	}
}

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

} // namespace

void Api::Build(const std::vector<Exposed> &exposed, const ODataService *odata,
                const std::vector<std::string> &ontology_files, const std::vector<ApiEndpoint> &endpoints) {
	duckdb::Connection con(db_);
	Json catalog = Json::Object();

	Json server = Json::Object();
	server.Set("name", "duckgraphlake");
	server.Set("data_dir", config_.data_dir);
	server.Set("mapping", config_.mapping);
	server.Set("ontology", config_.ontology);
	server.Set("duckdb", duckdb::DuckDB::LibraryVersion());
	catalog.Set("server", server);

	Json eps = Json::Array();
	for (auto &e : endpoints) {
		Json ep = Json::Object();
		ep.Set("name", e.name);
		ep.Set("url", e.url);
		ep.Set("detail", e.detail);
		eps.Push(ep);
	}
	catalog.Set("endpoints", eps);

	// Data sources: the tables and views of the server's own database (what discovery, the config
	// file and the init script created), with what each was made from.
	std::map<std::string, const Exposed *> by_name;
	for (auto &e : exposed) {
		by_name[e.name] = &e;
	}
	std::map<std::string, Json> columns;
	for (auto &row : Rows(con, "SELECT table_name, column_name, data_type FROM duckdb_columns() WHERE database_name = "
	                           "current_database() AND NOT internal ORDER BY table_name, column_index")) {
		if (!columns.count(row[0])) {
			columns[row[0]] = Json::Array();
		}
		Json col = Json::Object();
		col.Set("name", row[1]);
		col.Set("type", row[2]);
		columns[row[0]].Push(col);
	}
	Json sources = Json::Array();
	own_objects_.clear();
	for (auto &row : Rows(con, "SELECT table_name, 'table' FROM duckdb_tables() WHERE database_name = "
	                           "current_database() AND NOT internal UNION ALL SELECT view_name, 'view' FROM "
	                           "duckdb_views() WHERE database_name = current_database() AND NOT internal AND NOT "
	                           "temporary ORDER BY 1")) {
		own_objects_.push_back(row[0]);
		Json source = Json::Object();
		source.Set("name", row[0]);
		auto it = by_name.find(row[0]);
		source.Set("kind", it != by_name.end() ? it->second->kind : row[1]);
		source.Set("source", it != by_name.end() ? Json(it->second->source) : Json());
		source.Set("columns", columns.count(row[0]) ? columns[row[0]] : Json::Array());
		sources.Push(source);
	}
	catalog.Set("sources", sources);

	// Labels, comments and the class hierarchy from the ontology.
	std::map<std::string, std::string> labels, comments;
	std::map<std::string, std::set<std::string>> supers;
	if (!ontology_files.empty()) {
		std::string list;
		for (auto &f : ontology_files) {
			list += (list.empty() ? "" : ", ") + SqlLiteral(f);
		}
		for (auto &row :
		     Rows(con, "SELECT subject, predicate, object FROM read_rdf([" + list +
		                   "], prefix_expansion = true) WHERE predicate IN (" + SqlLiteral(RDFS + "label") + ", " +
		                   SqlLiteral(SKOS + "prefLabel") + ", " + SqlLiteral(RDFS + "comment") + ", " +
		                   SqlLiteral(SKOS + "definition") + ", " + SqlLiteral(RDFS + "subClassOf") + ")")) {
			if (row[1] == RDFS + "subClassOf") {
				if (row[2].rfind("_:", 0) != 0) {
					supers[row[0]].insert(row[2]);
				}
			} else if (row[1] == RDFS + "label" || row[1] == SKOS + "prefLabel") {
				labels.emplace(row[0], row[2]);
			} else {
				comments.emplace(row[0], row[2]);
			}
		}
	}
	auto annotate = [&](Json &obj, const std::string &iri) {
		if (labels.count(iri)) {
			obj.Set("label", labels[iri]);
		}
		if (comments.count(iri)) {
			obj.Set("comment", comments[iri]);
		}
	};

	Json classes = Json::Array();
	if (odata) {
		std::map<std::string, std::string> name_of;
		for (auto &set : odata->Sets()) {
			name_of[set.class_iri] = set.name;
		}
		for (auto &set : odata->Sets()) {
			Json cls = Json::Object();
			cls.Set("name", set.name);
			cls.Set("iri", set.class_iri);
			annotate(cls, set.class_iri);
			Json super_names = Json::Array(), sub_names = Json::Array();
			for (auto &s : supers[set.class_iri]) {
				if (name_of.count(s)) {
					super_names.Push(name_of[s]);
				}
			}
			for (auto &entry : supers) {
				if (entry.second.count(set.class_iri) && name_of.count(entry.first)) {
					sub_names.Push(name_of[entry.first]);
				}
			}
			cls.Set("superclasses", super_names);
			cls.Set("subclasses", sub_names);
			Json props = Json::Array();
			for (auto &p : set.properties) {
				Json prop = Json::Object();
				prop.Set("name", p.name);
				prop.Set("predicate", p.predicate);
				prop.Set("type", p.iri_valued ? "IRI" : p.edm_type.substr(4)); // Edm.Int64 -> Int64
				annotate(prop, p.predicate);
				props.Push(prop);
			}
			cls.Set("properties", props);
			Json navs = Json::Array();
			for (auto &n : set.navigations) {
				Json nav = Json::Object();
				nav.Set("name", n.name);
				nav.Set("predicate", n.predicate);
				nav.Set("target", n.target);
				annotate(nav, n.predicate);
				navs.Push(nav);
			}
			cls.Set("relationships", navs);
			classes.Push(cls);
		}
	}
	catalog.Set("classes", classes);
	catalog_ = catalog;
}

Json Api::QueryToJson(duckdb::Connection &con, const std::string &sql, int64_t max_rows) const {
	Deadline deadline(con, timeout_);
	auto started = std::chrono::steady_clock::now();
	auto result = con.Submit(sql);
	if (result->HasError()) {
		throw std::runtime_error(result->GetErrorObject().RawMessage());
	}
	duckdb::QueryResultStream<> stream(std::move(result));
	Json out = Json::Object();
	Json cols = Json::Array();
	auto &names = stream.GetNames();
	auto &types = stream.GetTypes();
	for (size_t i = 0; i < names.size(); i++) {
		Json col = Json::Object();
		col.Set("name", names[i].GetIdentifierName());
		col.Set("type", types[i].ToString());
		cols.Push(col);
	}
	out.Set("columns", cols);
	Json rows = Json::Array();
	bool truncated = false;
	int64_t count = 0;
	while (auto chunk = stream.Fetch()) {
		for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
			if (count == max_rows) {
				truncated = true;
				break;
			}
			Json row = Json::Array();
			for (duckdb::idx_t c = 0; c < chunk->ColumnCount(); c++) {
				row.Push(CellValue(chunk->GetValue(c, r)));
			}
			rows.Push(row);
			count++;
		}
		if (truncated) {
			break;
		}
	}
	if (stream.HasError()) {
		throw std::runtime_error(stream.GetErrorObject().RawMessage());
	}
	out.Set("rows", rows);
	out.Set("truncated", truncated);
	out.Set(
	    "ms",
	    static_cast<int64_t>(
	        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count()));
	return out;
}

Json Api::TablePreview(const std::string &name, int64_t limit) const {
	if (std::find(own_objects_.begin(), own_objects_.end(), name) == own_objects_.end()) {
		throw std::runtime_error("no table or view named " + name);
	}
	duckdb::Connection con(db_);
	limit = std::max<int64_t>(1, std::min<int64_t>(limit, 1000));
	return QueryToJson(con, "SELECT * FROM " + SqlIdent(name) + " LIMIT " + std::to_string(limit), limit);
}

Json Api::RunSql(const std::string &sql, int64_t max_rows) const {
	duckdb::Connection con(db_);
	return QueryToJson(con, sql, max_rows);
}

} // namespace dgl
