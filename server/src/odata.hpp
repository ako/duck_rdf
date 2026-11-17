// OData v4 over the graph, the way Graph Studio exposes graph data to Mendix external entities:
// each class becomes an entity set, each literal property of its instances a typed property,
// each reference to another class a navigation property. The model comes from the mapping
// (rdf_mapping_schema, with the ontology), so it needs no scan of the data. Requests become
// SPARQL queries, run through the same translator as /sparql.
//
//   GET /odata                         service document
//   GET /odata/$metadata               CSDL (XML)
//   GET /odata/Set                     entities: $filter, $select, $orderby, $top, $skip, $count, $expand
//   GET /odata/Set/$count              the number of entities (with $filter)
//   GET /odata/Set('<iri>')            one entity by key (its IRI)
//   GET /odata/Set('<iri>')/nav        the entity a navigation property points to
//
// $filter supports eq, ne, gt, ge, lt, le, and, or, not, parentheses, null, string, number,
// boolean, date and date-time literals, and contains, startswith, endswith, tolower, toupper,
// length, trim, concat, year, month, day. Read-only: other methods get 405.
#pragma once

#include <mutex>
#include <shared_mutex>

#include "duckdb.hpp"
#include "json.hpp"
#include "sparql.hpp"

#include <map>
#include <string>
#include <vector>

namespace dgl {

struct ODataProperty {
	std::string name;      // OData name
	std::string predicate; // IRI
	std::string edm_type;  // Edm.String, Edm.Int64, ...
	bool iri_valued = false;
};

struct ODataNavigation {
	std::string name;
	std::string predicate;
	std::string target; // entity type / set name
};

struct ODataEntitySet {
	std::string name;      // entity set and entity type name
	std::string class_iri; // the class
	std::string comment;
	std::vector<ODataProperty> properties;
	std::vector<ODataNavigation> navigations;
};

struct ODataRequest {
	std::string path;                              // after /odata, decoded
	std::multimap<std::string, std::string> query; // query parameters, decoded
	std::string base_url;                          // http://host:port/odata
};

struct ODataResponse {
	int status = 200;
	std::string content_type = "application/json;odata.metadata=minimal;charset=utf-8";
	std::string body;
};

class ODataService {
public:
	ODataService(duckdb::DuckDB &db, const SparqlExecutor &executor, int64_t max_page);

	// Reads the model from the mapping, and the class hierarchy from the ontology (to pick the most
	// specific target of a reference); throws std::runtime_error.
	void Load(const std::string &mapping, const std::vector<std::string> &ontology_files);
	ODataResponse Handle(const ODataRequest &request) const;

	// Empties the model (no mapping).
	void Clear();
	std::vector<ODataEntitySet> Sets() const {
		std::shared_lock<std::shared_mutex> lock(mutex_);
		return sets_;
	}

private:
	const ODataEntitySet *FindSet(const std::string &name) const;
	std::string Metadata() const;
	ODataResponse Collection(const ODataEntitySet &set, const ODataRequest &request, const std::string &key,
	                         bool count_only) const;
	ODataResponse Navigate(const ODataEntitySet &set, const std::string &key, const std::string &nav,
	                       const ODataRequest &request) const;
	// Entities of `set` with the given IRIs (for $expand and navigation), keyed by IRI.
	std::map<std::string, Json> FetchByIds(const ODataEntitySet &set, const std::vector<std::string> &ids,
	                                       const std::vector<std::string> &select) const;

	duckdb::DuckDB &db_;
	const SparqlExecutor &executor_;
	int64_t max_page_;
	mutable std::shared_mutex mutex_; // Load replaces sets_ while requests read it
	std::vector<ODataEntitySet> sets_;
};

} // namespace dgl
