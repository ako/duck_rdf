// The JSON API behind the web UI (server/ui/index.html):
//
//   GET  /api/catalog              the server, its endpoints, the data sources with their columns,
//                                  and the classes of the graph (as OData exposes them) with labels
//                                  and comments from the ontology
//   GET  /api/table?name=&limit=   the first rows of one of the server's tables or views
//   POST /api/sql                  run SQL (the body), at most 1000 rows
//
// Entity data comes from /odata and SPARQL from /sparql; the UI calls those directly.
#pragma once

#include "config.hpp"
#include "json.hpp"
#include "odata.hpp"
#include "setup.hpp"

#include "duckdb.hpp"

#include <string>
#include <vector>

namespace dgl {

struct ApiEndpoint {
	std::string name, url, detail;
};

class Api {
public:
	Api(duckdb::DuckDB &db, const Config &config, double timeout_seconds)
	    : db_(db), config_(config), timeout_(timeout_seconds) {
	}

	// Builds the catalog once; the data folder does not change while the server runs.
	void Build(const std::vector<Exposed> &exposed, const ODataService *odata,
	           const std::vector<std::string> &ontology_files, const std::vector<ApiEndpoint> &endpoints);

	const Json &Catalog() const {
		return catalog_;
	}
	// Throws std::runtime_error with DuckDB's message.
	Json TablePreview(const std::string &name, int64_t limit) const;
	Json RunSql(const std::string &sql, int64_t max_rows) const;

private:
	Json QueryToJson(duckdb::Connection &con, const std::string &sql, int64_t max_rows) const;

	duckdb::DuckDB &db_;
	const Config &config_;
	double timeout_;
	Json catalog_;
	std::vector<std::string> own_objects_; // tables and views a preview may read
};

} // namespace dgl
