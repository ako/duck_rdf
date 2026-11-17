// MCP: the server as tools for agents, over streamable HTTP (JSON responses, stateless).
//
// Always:
//     guide             the help topics (also resources guide://<topic>)
//     list_tables       every table and view, with what it was made from (topic, app, entity)
//     describe_table    a table's columns, row count, sample rows, origin and graph class
//     sql               a read-only SQL query
//     list_topics       the Kafka topics and what arrived on them
// With [mcp] allow_changes:
//     define_view, drop_view, get_mapping, set_mapping, remove_mapping
// When there is a mapping (one is generated for CDC topics):
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

#include "api.hpp"
#include "config.hpp"
#include "json.hpp"
#include "lakemodel.hpp"
#include "odata.hpp"
#include "sparql.hpp"

#include <functional>

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace dgl {

// The rest of the server, as MCP needs it; the pointers are set before requests are served.
struct McpContext {
	LakeModel *model = nullptr;
	Api *api = nullptr;
	const ODataService *odata = nullptr;
	std::function<std::vector<TopicState>()> topics; // empty without the Kafka endpoint
	std::function<void(const std::string &)> reload; // after a change to views or mappings
	std::map<std::string, std::string> guide_values; // for the help topics' placeholders
};

class McpServer {
public:
	McpServer(duckdb::DuckDB &db, const SparqlExecutor &executor, const Config &config,
	          std::vector<std::string> ontology_files, McpContext &context);

	// Forgets what it learnt about the graph (the mapping changed).
	void Invalidate();

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

	bool HasGraph() const;
	std::string Instructions() const;
	std::string TableRef(const std::string &name) const; // SQL for a table name, or throws
	Json Guide(const std::string &topic);
	Json ListTables(const std::string &schema);
	Json DescribeTable(const std::string &name, int64_t sample);
	Json Sql(const std::string &query, int64_t limit);
	Json ListTopics();
	Json DefineView(const std::string &name, const std::string &sql, const std::string &description);
	Json DropView(const std::string &name);
	Json GetMapping(const std::string &name);
	Json SetMapping(const std::string &name, const std::string &text, const std::string &format);
	Json RemoveMapping(const std::string &name);
	Json GraphSummary() const; // the OData entity sets after a change

	duckdb::DuckDB &db_;
	const SparqlExecutor &executor_;
	const Config &config_;
	std::vector<std::string> ontology_files_;
	McpContext &context_;

	std::mutex mutex_;
	bool ontology_loaded_ = false;
	std::map<std::string, OntologyEntry> ontology_;
	bool counts_loaded_ = false;
	std::vector<std::pair<std::string, int64_t>> counts_;
	std::map<std::string, Json> class_cache_;
};

} // namespace dgl
