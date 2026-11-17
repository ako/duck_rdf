// Server configuration: the same TOML file as the Docker image (server.example.toml),
// with command-line flags on top, and defaults suited to a developer's machine.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dgl {

struct TemplateParam {
	std::string name;
	std::string type = "string"; // string, iri, integer, decimal, boolean
	std::string description;
};

struct QueryTemplate {
	std::string name;
	std::string description;
	std::string query;
	int64_t limit = 100;
	std::vector<TemplateParam> params;
};

struct ViewDef {
	std::string name;
	std::string sql;
};

struct Config {
	std::string config_file; // empty: none
	std::string config_dir;  // directory of config_file, for relative paths

	// [server]
	std::string host = "127.0.0.1";
	int http_port = 9495; // SPARQL at /sparql, MCP at /mcp, a query page at /
	int sql_port = 9494;  // DuckDB's Quack protocol
	int pg_port = 5433;   // PostgreSQL wire protocol (0: off)
	std::string token;
	bool lock_down = true;

	// [duckdb]
	std::string threads, memory_limit, temp_directory;

	// [data]
	std::string data_dir = ".";
	bool auto_discover = true;
	bool expose_attached_tables = true;
	std::vector<ViewDef> views;
	std::string init_sql; // a SQL file run after discovery (views over the discovered data)

	// [rdf]
	std::string mapping, ontology, unique_keys;
	bool typed_results = true;
	bool detect_keys = true;

	// [sparql]
	bool sparql_enabled = true;
	int sparql_anonymous = -1; // -1: anonymous only when listening on a loopback address
	double timeout_seconds = 60;
	int64_t sparql_max_rows = 100000;
	std::string cors_origin = "*";

	// [odata]
	bool odata_enabled = true;
	int odata_anonymous = -1;
	int64_t odata_max_page = 1000;

	// [iceberg]: a REST catalog over the data folder at /iceberg
	bool iceberg_enabled = true;

	// [kafka]: a Kafka-compatible endpoint for producers, writing each topic to an Iceberg table
	// shaped like WarpStream Tableflow's
	int kafka_port = 9092;             // 0: off
	std::string kafka_advertised_host; // default: host
	std::string kafka_cluster = "local";
	int kafka_partitions = 1;
	int kafka_flush_ms = 1000;
	std::string tableflow_config;  // a Tableflow YAML file
	bool kafka_cdc = true;         // change events get a column per field, and a <table>_current view
	bool kafka_cdc_mapping = true; // and each app's CDC entities a generated mapping (graph, OData, MCP)

	// [mcp]
	bool mcp_enabled = true;
	int mcp_anonymous = -1;
	int64_t mcp_max_rows = 1000;
	bool mcp_allow_changes = true; // define_view, drop_view, set_mapping, remove_mapping
	std::vector<QueryTemplate> templates;

	bool Loopback() const;
	bool SparqlAnonymous() const {
		return sparql_anonymous < 0 ? Loopback() : sparql_anonymous == 1;
	}
	bool ODataAnonymous() const {
		return odata_anonymous < 0 ? Loopback() : odata_anonymous == 1;
	}
	bool McpAnonymous() const {
		return mcp_anonymous < 0 ? Loopback() : mcp_anonymous == 1;
	}
};

// Reads the TOML file into config (throws std::runtime_error on a bad file).
void LoadConfigFile(const std::string &path, Config &config);

// A path from the config file, resolved against its directory when relative.
std::string ResolvePath(const Config &config, const std::string &path);

} // namespace dgl
