// MCP: the graph as tools for agents, over streamable HTTP (JSON responses, stateless).
//
//     list_classes      the classes in the data, with labels and instance counts
//     describe_class    a class's place in the ontology and the properties its instances have
//     find_entity       entities whose text values contain a search string, with their classes
//     describe_entity   everything stated about one IRI, outgoing and incoming
//     sparql            any read-only SPARQL SELECT, ASK or CONSTRUCT query
//     <templates>       one tool per reviewed query template in the config file
//
// Values from the agent are never pasted into query text unchecked: they are escaped as
// SPARQL literals or validated as IRIs first. The ontology files are the resource
// ontology://turtle.
#pragma once

#include "config.hpp"
#include "json.hpp"
#include "sparql.hpp"

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace dgl {

class McpServer {
public:
	McpServer(duckdb::DuckDB &db, const SparqlExecutor &executor, const Config &config,
	          std::vector<std::string> ontology_files);

	// One JSON-RPC message; nullopt for a notification (no reply).
	std::optional<Json> Handle(const Json &message);

private:
	struct ToolError : std::runtime_error {
		using std::runtime_error::runtime_error;
	};
	struct OntologyEntry {
		std::vector<std::string> labels, comments, superclasses;
	};

	Json ToolList() const;
	Json CallTool(const std::string &name, const Json &args);

	Json Run(const std::string &query, int64_t limit);
	const std::map<std::string, OntologyEntry> &Ontology();
	const std::vector<std::pair<std::string, int64_t>> &ClassCounts();
	int64_t InstancesOf(const std::string &cls);
	std::string ResolveClass(const std::string &name);
	std::map<std::string, Json> ClassesOf(const std::vector<std::string> &entities);

	Json ListClasses();
	Json DescribeClass(const std::string &cls);
	Json FindEntity(const std::string &text, const std::string &cls, int64_t limit);
	Json DescribeEntity(const std::string &entity, int64_t limit);
	Json Sparql(const std::string &query, int64_t limit);
	Json Template(const QueryTemplate &tmpl, const Json &args);

	duckdb::DuckDB &db_;
	const SparqlExecutor &executor_;
	const Config &config_;
	std::vector<std::string> ontology_files_;

	std::mutex mutex_;
	bool ontology_loaded_ = false;
	std::map<std::string, OntologyEntry> ontology_;
	bool counts_loaded_ = false;
	std::vector<std::pair<std::string, int64_t>> counts_;
	std::map<std::string, Json> class_cache_;
};

} // namespace dgl
