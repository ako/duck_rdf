// duckgraphlake: point it at a folder of data files and get, from one process,
//
//   SQL     over the PostgreSQL wire protocol (psql, the PostgreSQL JDBC driver: Mendix's external database
//           connector) and over DuckDB's Quack protocol (DuckDB CLI, Python, DuckDB's JDBC driver)
//   SPARQL  a SPARQL 1.1 Protocol endpoint at http://host:port/sparql, over an R2RML/YARRRML mapping
//   OData   OData v4 over the graph at http://host:port/odata: one entity set per class (Mendix external entities)
//   MCP     an MCP server for agents at http://host:port/mcp
//
// The same configuration file as the Docker image (server.example.toml) works here; flags override it.
#include "api.hpp"
#include "config.hpp"
#include "iceberg_catalog.hpp"
#include "kafka.hpp"
#include "kafka_lake.hpp"
#include "json.hpp"
#include "log.hpp"
#include "mcp.hpp"
#include "odata.hpp"
#include "pgwire.hpp"
#include "setup.hpp"
#include "sparql.hpp"
#include "ui_page.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "httplib.h"

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <iostream>

namespace fs = std::filesystem;
using namespace dgl; // NOLINT

namespace {

const char *VERSION = "0.1.0";

httplib::Server *g_http = nullptr;

void Stop(int) {
	if (g_http) {
		g_http->stop();
	}
}

void Usage() {
	std::cout << R"(duckgraphlake )" << VERSION << R"( - a folder of data files as SQL, SPARQL and MCP endpoints

Usage: duckgraphlake [options] [DATA_DIR]

  DATA_DIR               folder to serve (default: the current one). Each Parquet, CSV or JSON
                         file becomes a view, as does each folder of them and each Iceberg table;
                         each SQLite or DuckDB file is attached, its tables also as views.
  -c, --config FILE      a server.toml (the Docker image's format); flags override it
  -m, --mapping GLOB     R2RML (.ttl) or YARRRML (.yml) mapping file(s) for SPARQL and MCP
  -o, --ontology GLOB    ontology file(s): class and property hierarchies, inverses, transitivity
  -i, --init FILE        a SQL script run after discovery, e.g. views over the discovered data
      --host HOST        address to listen on (default 127.0.0.1)
      --http-port PORT   SPARQL (/sparql), OData (/odata), MCP (/mcp) and a query page (/) (default 9495)
      --pg-port PORT     PostgreSQL wire protocol (default 5433; 0 turns it off)
      --sql-port PORT    DuckDB Quack protocol (default 9494)
      --kafka-port PORT  Kafka-compatible endpoint for producers (default 9092; 0 turns it off): each
                         topic is written to an Iceberg table under warpstream/_tableflow/
  -t, --tableflow FILE   a WarpStream Tableflow YAML file: table names and JSON schemas per topic
      --token TOKEN      access token (or DUCKGRAPHLAKE_TOKEN); required unless on a loopback address
      --require-token    SPARQL and MCP also need the token on a loopback address
      --no-lock-down     leave file access and settings open to SQL clients
  -h, --help             this text
  -v, --version          the version
)";
}

std::string NeedValue(int &i, int argc, char **argv) {
	if (i + 1 >= argc) {
		throw std::runtime_error(std::string(argv[i]) + " needs a value");
	}
	return argv[++i];
}

void SetCors(const Config &config, httplib::Response &res) {
	if (!config.cors_origin.empty()) {
		res.set_header("Access-Control-Allow-Origin", config.cors_origin);
		res.set_header("Vary", "Origin");
	}
}

bool Authorized(const httplib::Request &req, const std::string &token) {
	auto header = req.get_header_value("Authorization");
	if (header.rfind("Bearer ", 0) != 0) {
		return false;
	}
	auto given = header.substr(7);
	while (!given.empty() && given.back() == ' ') {
		given.pop_back();
	}
	// Constant time in the token's length.
	unsigned char diff = given.size() == token.size() ? 0 : 1;
	for (size_t i = 0; i < token.size(); i++) {
		diff |= static_cast<unsigned char>(token[i] ^ (i < given.size() ? given[i] : 0));
	}
	return diff == 0;
}

void Text(httplib::Response &res, int status, const std::string &text) {
	res.status = status;
	res.set_content(text + "\n", "text/plain; charset=utf-8");
}

void HandleSparql(const Config &config, const SparqlExecutor &executor, const std::string &token,
                  const httplib::Request &req, httplib::Response &res) {
	SetCors(config, res);
	if (!config.SparqlAnonymous() && !Authorized(req, token)) {
		res.set_header("WWW-Authenticate", "Bearer realm=\"sparql\"");
		return Text(res, 401, "send Authorization: Bearer <token>");
	}
	std::string query;
	if (req.method == "POST") {
		auto ctype = req.get_header_value("Content-Type");
		ctype = ctype.substr(0, ctype.find(';'));
		while (!ctype.empty() && ctype.back() == ' ') {
			ctype.pop_back();
		}
		if (ctype == "application/sparql-update" || req.has_param("update")) {
			return Text(res, 400, "SPARQL Update is not supported: the graph is read-only");
		}
		if (ctype == "application/sparql-query") {
			query = req.body;
		} else if (ctype != "application/x-www-form-urlencoded") {
			return Text(res, 415, "send application/sparql-query or application/x-www-form-urlencoded");
		}
	}
	if (req.has_param("update")) {
		return Text(res, 400, "SPARQL Update is not supported: the graph is read-only");
	}
	if (query.empty()) {
		if (req.get_param_value_count("query") != 1) {
			return Text(res, 400, "give exactly one query parameter");
		}
		query = req.get_param_value("query");
	}
	if (query.find_first_not_of(" \t\r\n") == std::string::npos) {
		return Text(res, 400, "give exactly one query parameter");
	}
	if (req.has_param("default-graph-uri") || req.has_param("named-graph-uri")) {
		return Text(res, 400, "default-graph-uri and named-graph-uri are not supported; use FROM / FROM NAMED");
	}
	SparqlResult result;
	try {
		result = executor.Execute(query, static_cast<size_t>(config.sparql_max_rows));
	} catch (SparqlError &e) {
		return Text(res, e.status, e.what());
	}
	if (result.truncated) {
		return Text(res, 413,
		            "the result has more than " + std::to_string(config.sparql_max_rows) +
		                " rows; add a LIMIT or narrow the query");
	}
	static const std::vector<std::pair<std::string, std::string>> select_types {
	    {"application/sparql-results+json", "json"},
	    {"application/json", "json"},
	    {"application/sparql-results+xml", "xml"},
	    {"application/xml", "xml"},
	    {"text/csv", "csv"},
	    {"text/tab-separated-values", "tsv"}};
	static const std::vector<std::string> graph_types {"text/turtle", "application/n-triples", "text/plain",
	                                                   "application/rdf+xml"};
	std::vector<std::string> select_offered;
	for (auto &t : select_types) {
		select_offered.push_back(t.first);
	}
	auto accept = req.get_header_value("Accept");
	if (result.kind == SparqlResult::Kind::CONSTRUCT) {
		auto ctype = Negotiate(accept, graph_types, "text/turtle");
		if (ctype.empty() && !Negotiate(accept, select_offered, "").empty()) {
			ctype = "text/turtle"; // the client asked only for result formats, which graphs have none of
		}
		if (ctype.empty()) {
			return Text(res, 406, "CONSTRUCT results: text/turtle, application/n-triples, application/rdf+xml");
		}
		try {
			auto body = ctype == "application/rdf+xml" ? WriteRdfXml(result) : WriteNTriples(result);
			res.set_content(body, ctype + "; charset=utf-8");
		} catch (SparqlError &e) {
			return Text(res, e.status, e.what());
		}
		return;
	}
	auto ctype = Negotiate(accept, select_offered, "application/sparql-results+json");
	if (ctype.empty()) {
		return Text(res, 406,
		            "results: application/sparql-results+json, application/sparql-results+xml, text/csv, "
		            "text/tab-separated-values");
	}
	std::string format;
	for (auto &t : select_types) {
		if (t.first == ctype) {
			format = t.second;
		}
	}
	auto body = result.kind == SparqlResult::Kind::ASK ? WriteAsk(format, result.boolean) : WriteSelect(format, result);
	res.set_content(body, ctype + "; charset=utf-8");
}

void HandleMcp(const Config &config, McpServer &mcp, const std::string &token, const httplib::Request &req,
               httplib::Response &res) {
	if (!config.McpAnonymous() && !Authorized(req, token)) {
		res.set_header("WWW-Authenticate", "Bearer realm=\"mcp\"");
		return Text(res, 401, "send Authorization: Bearer <token>");
	}
	Json message;
	try {
		message = Json::Parse(req.body);
	} catch (std::exception &ex) {
		Json reply = Json::Object();
		reply.Set("jsonrpc", "2.0");
		reply.Set("id", Json());
		Json error = Json::Object();
		error.Set("code", -32700);
		error.Set("message", ex.what());
		reply.Set("error", error);
		res.status = 400;
		res.set_content(reply.Dump(), "application/json");
		return;
	}
	if (message.IsArray()) { // a batch (older protocol versions)
		Json replies = Json::Array();
		for (auto &item : message.Items()) {
			if (auto reply = mcp.Handle(item)) {
				replies.Push(*reply);
			}
		}
		if (replies.Size() == 0) {
			res.status = 202;
			return;
		}
		res.set_content(replies.Dump(), "application/json");
		return;
	}
	auto reply = mcp.Handle(message);
	if (!reply) {
		res.status = 202;
		return;
	}
	res.set_content(reply->Dump(), "application/json");
}

std::string YasguiPage(const Config &config) {
	// YASGUI from a CDN, pointed at this server's endpoint.
	return R"(<!doctype html>
<html><head><meta charset="utf-8"><title>duckgraphlake</title>
<link href="https://cdn.jsdelivr.net/npm/@zazuko/yasgui@4.5.0/build/yasgui.min.css" rel="stylesheet">
<style>body{font-family:system-ui,sans-serif;margin:0} header{padding:8px 16px;background:#222;color:#eee;font-size:14px}
header code{color:#fc6}</style></head>
<body><header>duckgraphlake: SPARQL <code>/sparql</code> &middot; MCP <code>/mcp</code> &middot; SQL on port <code>)" +
	       std::to_string(config.sql_port) + R"(</code> (Quack)</header>
<div id="yasgui"></div>
<script src="https://cdn.jsdelivr.net/npm/@zazuko/yasgui@4.5.0/build/yasgui.min.js"></script>
<script>new Yasgui(document.getElementById("yasgui"), {requestConfig: {endpoint: location.origin + "/sparql"},
copyEndpointOnNewTab: true});</script>
</body></html>
)";
}

} // namespace

int main(int argc, char **argv) {
	Config config;
	std::string data_dir_arg, mapping_arg, ontology_arg, host_arg, token_arg, init_arg;
	int http_port = 0, sql_port = 0, pg_port = -1, kafka_port = -1;
	std::string tableflow_arg;
	bool no_lock_down = false, require_token = false;
	try {
		std::string config_file;
		for (int i = 1; i < argc; i++) {
			std::string a = argv[i];
			if (a == "-h" || a == "--help") {
				Usage();
				return 0;
			} else if (a == "-v" || a == "--version") {
				std::cout << "duckgraphlake " << VERSION << " (DuckDB " << duckdb::DuckDB::LibraryVersion() << ")"
				          << std::endl;
				return 0;
			} else if (a == "-c" || a == "--config") {
				config_file = NeedValue(i, argc, argv);
			} else if (a == "-m" || a == "--mapping") {
				mapping_arg = NeedValue(i, argc, argv);
			} else if (a == "-o" || a == "--ontology") {
				ontology_arg = NeedValue(i, argc, argv);
			} else if (a == "-i" || a == "--init") {
				init_arg = NeedValue(i, argc, argv);
			} else if (a == "--host") {
				host_arg = NeedValue(i, argc, argv);
			} else if (a == "--http-port") {
				http_port = std::stoi(NeedValue(i, argc, argv));
			} else if (a == "--pg-port") {
				pg_port = std::stoi(NeedValue(i, argc, argv));
			} else if (a == "--kafka-port") {
				kafka_port = std::stoi(NeedValue(i, argc, argv));
			} else if (a == "-t" || a == "--tableflow") {
				tableflow_arg = NeedValue(i, argc, argv);
			} else if (a == "--sql-port") {
				sql_port = std::stoi(NeedValue(i, argc, argv));
			} else if (a == "--token") {
				token_arg = NeedValue(i, argc, argv);
			} else if (a == "--require-token") {
				require_token = true;
			} else if (a == "--no-lock-down") {
				no_lock_down = true;
			} else if (!a.empty() && a[0] == '-') {
				throw std::runtime_error("unknown option " + a + " (see --help)");
			} else if (data_dir_arg.empty()) {
				data_dir_arg = a;
			} else {
				throw std::runtime_error("give one data folder (see --help)");
			}
		}
		if (!config_file.empty()) {
			LoadConfigFile(config_file, config);
		}
	} catch (std::exception &ex) {
		std::cerr << "duckgraphlake: " << ex.what() << std::endl;
		return 2;
	}
	if (!data_dir_arg.empty()) {
		config.data_dir = data_dir_arg;
	}
	if (!mapping_arg.empty()) {
		config.mapping = mapping_arg;
	}
	if (!ontology_arg.empty()) {
		config.ontology = ontology_arg;
	}
	if (!host_arg.empty()) {
		config.host = host_arg;
	}
	if (!init_arg.empty()) {
		config.init_sql = init_arg;
	}
	if (http_port) {
		config.http_port = http_port;
	}
	if (sql_port) {
		config.sql_port = sql_port;
	}
	if (pg_port >= 0) {
		config.pg_port = pg_port;
	}
	if (kafka_port >= 0) {
		config.kafka_port = kafka_port;
	}
	if (!tableflow_arg.empty()) {
		config.tableflow_config = tableflow_arg;
	}
	if (config.kafka_port && !config.iceberg_enabled) {
		std::cerr << "duckgraphlake: the Kafka endpoint writes through the Iceberg catalog; enable [iceberg]"
		          << std::endl;
		return 2;
	}
	if (no_lock_down) {
		config.lock_down = false;
	}
	if (require_token) {
		config.sparql_anonymous = 0;
		config.odata_anonymous = 0;
		config.mcp_anonymous = 0;
	}
	std::string token = token_arg;
	if (token.empty() && std::getenv("DUCKGRAPHLAKE_TOKEN")) {
		token = std::getenv("DUCKGRAPHLAKE_TOKEN");
	}
	if (token.empty()) {
		token = config.token;
	}
	if (token.empty()) {
		if (!config.Loopback()) {
			std::cerr << "duckgraphlake: set --token (or DUCKGRAPHLAKE_TOKEN) when listening on " << config.host
			          << std::endl;
			return 2;
		}
		token = "duckgraphlake"; // only reachable from this machine
	}
	if (token.size() < 4) {
		std::cerr << "duckgraphlake: the token must be at least 4 characters" << std::endl;
		return 2;
	}
	if (!fs::is_directory(config.data_dir)) {
		std::cerr << "duckgraphlake: " << config.data_dir << " is not a folder" << std::endl;
		return 2;
	}
	config.data_dir = fs::absolute(config.data_dir).lexically_normal().string();

	try {
		duckdb::DBConfig db_config;
		if (!config.threads.empty()) {
			db_config.SetOptionByName("threads", duckdb::Value(config.threads));
		}
		if (!config.memory_limit.empty()) {
			db_config.SetOptionByName("memory_limit", duckdb::Value(config.memory_limit));
		}
		if (!config.temp_directory.empty()) {
			db_config.SetOptionByName("temp_directory", duckdb::Value(config.temp_directory));
		}
		if (config.lock_down) {
			// Secrets in memory only: the stored-secrets directory is outside the allowed ones. Set before
			// the database opens: once anything uses the secret manager (the first INSTALL of iceberg goes
			// through httpfs) DuckDB refuses to change it.
			db_config.SetOptionByName("allow_persistent_secrets", duckdb::Value::BOOLEAN(false));
		}
		duckdb::DuckDB db(nullptr, &db_config);
		// The extensions compiled into this binary: rdf, Quack, SQLite, Parquet, JSON, ...
		duckdb::ExtensionHelper::LoadAllExtensions(db);
		duckdb::Connection con(db);

		Log("serving " + config.data_dir);
		auto exposed = Discover(con, config);
		for (auto &e : exposed) {
			auto kind = e.kind;
			kind.resize(8, ' ');
			Log("  " + kind + " " + e.name + "  <- " + e.source);
		}
		if (exposed.empty()) {
			Log("  (no data files found)");
		}
		if (!config.init_sql.empty()) {
			std::ifstream in(config.init_sql);
			if (!in) {
				throw std::runtime_error("cannot read " + config.init_sql);
			}
			std::stringstream sql;
			sql << in.rdbuf();
			Exec(con, sql.str());
			Log("ran " + config.init_sql);
		}
		ConfigureSparql(con, config);
		Log("mapping: " + (config.mapping.empty() ? std::string("(none: SPARQL and MCP are off)") : config.mapping));
		if (!config.mapping.empty() && GlobFiles(con, config.mapping).empty()) {
			throw std::runtime_error("no mapping files match " + config.mapping);
		}
		if (!config.mapping.empty() && config.detect_keys) {
			auto keys = DetectKeys(con, exposed, config.mapping);
			if (!config.unique_keys.empty()) {
				keys.push_back(config.unique_keys); // hand-written keys add to the detected ones
			}
			std::string joined;
			for (auto &k : keys) {
				joined += (joined.empty() ? "" : "; ") + k;
			}
			if (!joined.empty()) {
				Exec(con, "SET GLOBAL sparql_unique_keys = " + SqlLiteral(joined));
			}
			Log("keys: " + (joined.empty() ? std::string("(none found)") : joined));
		}
		auto ontology_files = GlobFiles(con, config.ontology);
		Log("ontology: " + (config.ontology.empty() ? std::string("(none)") : config.ontology));
		if (config.kafka_port) {
			// Topic tables are read through iceberg_scan views, created after lock-down as topics appear.
			Exec(con, "INSTALL iceberg");
			Exec(con, "LOAD iceberg");
		}
		if (config.lock_down) {
			LockDown(con, config);
		}

		auto quack = config.host + ":" + std::to_string(config.sql_port);
		Exec(con, "CALL quack_serve(" + SqlLiteral("quack:" + quack) + ", token = " + SqlLiteral(token) + ")");

		PgServer pg(db, token);
		if (config.pg_port) {
			pg.Start(config.host, config.pg_port);
		}

		SparqlExecutor executor(db, config.mapping, config.timeout_seconds);
		McpServer mcp(db, executor, config, ontology_files);
		ODataService odata(db, executor, config.odata_max_page);
		bool serve_odata = config.odata_enabled && !config.mapping.empty();
		if (serve_odata) {
			odata.Load(config.mapping, ontology_files);
		}
		httplib::Server http;
		g_http = &http;
		std::signal(SIGINT, Stop);
		std::signal(SIGTERM, Stop);
		std::signal(SIGPIPE, SIG_IGN); // a client closing its socket must not kill the server

		bool serve_sparql = config.sparql_enabled && !config.mapping.empty();
		bool serve_mcp_flag = config.mcp_enabled && !config.mapping.empty();
		auto base = "http://" + config.host + ":" + std::to_string(config.http_port);
		bool default_token = token == "duckgraphlake";
		std::vector<ApiEndpoint> endpoints;
		auto token_note = default_token ? std::string("the token 'duckgraphlake'") : std::string("the server's token");
		if (config.pg_port) {
			auto hostport = config.host + ":" + std::to_string(config.pg_port);
			endpoints.push_back({"PostgreSQL", "jdbc:postgresql://" + hostport + "/lake?sslmode=disable",
			                     "SQL (DuckDB's) for Mendix's external database connector, psql or any PostgreSQL "
			                     "driver. Any user name; the password is " +
			                         token_note + "."});
		}
		endpoints.push_back({"Quack", "quack:" + config.host + ":" + std::to_string(config.sql_port),
		                     "SQL for DuckDB clients: LOAD quack; ATTACH 'quack:" + config.host + ":" +
		                         std::to_string(config.sql_port) + "' AS lake (TOKEN '...'), with " + token_note +
		                         "."});
		if (serve_sparql) {
			endpoints.push_back(
			    {"SPARQL", base + "/sparql",
			     "SPARQL 1.1 Protocol: GET or POST a query; JSON, XML, CSV, TSV or Turtle results." +
			         std::string(config.SparqlAnonymous() ? "" : " Send Authorization: Bearer <token>.")});
		}
		if (config.odata_enabled && !config.mapping.empty()) {
			endpoints.push_back(
			    {"OData", base + "/odata",
			     "OData v4, one entity set per class: Mendix external entities. $metadata describes it." +
			         std::string(config.ODataAnonymous() ? "" : " Send Authorization: Bearer <token>.")});
		}
		if (serve_mcp_flag) {
			endpoints.push_back({"MCP", base + "/mcp",
			                     "Model Context Protocol over streamable HTTP, for agents." +
			                         std::string(config.McpAnonymous() ? "" : " Send Authorization: Bearer <token>.")});
		}
		IcebergCatalog catalog(config.data_dir);
		if (config.iceberg_enabled) {
			endpoints.push_back({"Iceberg", base + "/iceberg",
			                     "Iceberg REST catalog over the data folder: read and write its Iceberg tables from "
			                     "DuckDB (ATTACH '' AS lake (TYPE iceberg, ENDPOINT '" +
			                         base + "/iceberg', TOKEN '...')), PyIceberg or Spark."});
		}
		std::unique_ptr<KafkaLake> lake;
		std::unique_ptr<KafkaServer> kafka;
		auto advertised = config.kafka_advertised_host.empty() ? config.host : config.kafka_advertised_host;
		if (config.kafka_port) {
			endpoints.push_back(
			    {"Kafka", advertised + ":" + std::to_string(config.kafka_port),
			     "Kafka-compatible, for producers (Mendix Business Events: ServerUrl). Each topic "
			     "becomes an Iceberg table <cluster>__<topic> under warpstream/_tableflow/, shaped like "
			     "WarpStream Tableflow's; committed every " +
			         std::to_string(config.kafka_flush_ms) + " ms."});
		}
		Api api(db, config, config.timeout_seconds);
		api.Build(exposed, serve_odata ? &odata : nullptr, ontology_files, endpoints);
		std::mutex exposed_mutex;
		if (config.kafka_port) {
			KafkaLakeOptions options;
			options.data_dir = config.data_dir;
			options.catalog_url = "http://127.0.0.1:" + std::to_string(config.http_port) + "/iceberg";
			options.token = token;
			options.cluster = config.kafka_cluster;
			options.default_partitions = config.kafka_partitions;
			options.flush_ms = config.kafka_flush_ms;
			if (!config.tableflow_config.empty()) {
				options.tableflow = LoadTableflowConfig(config.tableflow_config);
				Log("tableflow: " + config.tableflow_config + " (" + std::to_string(options.tableflow.tables.size()) +
				    " table(s))");
				for (auto &note : options.tableflow.notes) {
					Log("  " + note);
				}
			}
			catalog.AddAlias("tableflow", "warpstream/_tableflow");
			lake =
			    std::make_unique<KafkaLake>(options, catalog, [&](const std::string &table, const std::string &folder) {
				    // A topic's first commit: a view over its table in the served database.
				    try {
					    duckdb::Connection view_con(db);
					    Exec(view_con, "CREATE OR REPLACE VIEW " + SqlIdent(table) + " AS " + IcebergView(folder));
					    std::lock_guard<std::mutex> lock(exposed_mutex);
					    exposed.push_back({"iceberg", table, folder});
					    api.Build(exposed, serve_odata ? &odata : nullptr, ontology_files, endpoints);
					    Log("kafka: table " + table + " is queryable");
				    } catch (std::exception &ex) {
					    Log("kafka: no view for " + table + ": " + duckdb::ErrorData(ex).RawMessage());
				    }
			    });
			lake->Start();
			kafka = std::make_unique<KafkaServer>(*lake, advertised, config.kafka_port);
			kafka->Start(config.host, config.kafka_port);
		}
		bool serve_mcp = config.mcp_enabled && !config.mapping.empty();
		if (serve_sparql) {
			auto handler = [&](const httplib::Request &req, httplib::Response &res) {
				HandleSparql(config, executor, token, req, res);
			};
			http.Get("/sparql", handler);
			http.Post("/sparql", handler);
			http.Options("/sparql", [&](const httplib::Request &, httplib::Response &res) {
				SetCors(config, res);
				res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
				res.set_header("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept");
				res.status = 204;
			});
		}
		if (config.iceberg_enabled) {
			auto iceberg_handler = [&](const httplib::Request &req, httplib::Response &res) {
				// Writes go to the data folder: the token is required wherever SPARQL needs it.
				if (!config.SparqlAnonymous() && !Authorized(req, token)) {
					res.status = 401;
					res.set_content("{\"error\":{\"message\":\"send Authorization: Bearer <token>\",\"type\":"
					                "\"NotAuthorizedException\",\"code\":401}}",
					                "application/json");
					return;
				}
				auto response = catalog.Handle(req.method, req.path.substr(std::string("/iceberg").size()), req.body);
				res.status = response.status;
				if (!response.body.empty()) {
					res.set_content(response.body, "application/json");
				}
			};
			http.Get(R"(/iceberg/.*)", iceberg_handler);
			http.Post(R"(/iceberg/.*)", iceberg_handler);
			http.Delete(R"(/iceberg/.*)", iceberg_handler);
		}
		if (serve_odata) {
			auto odata_handler = [&](const httplib::Request &req, httplib::Response &res) {
				SetCors(config, res);
				res.set_header("OData-Version", "4.0");
				if (!config.ODataAnonymous() && !Authorized(req, token)) {
					res.set_header("WWW-Authenticate", "Bearer realm=\"odata\"");
					return Text(res, 401, "send Authorization: Bearer <token>");
				}
				if (req.method != "GET" && req.method != "HEAD") {
					res.set_header("Allow", "GET");
					return Text(res, 405, "the OData service is read-only");
				}
				ODataRequest request;
				request.path = req.path.substr(std::string("/odata").size());
				for (auto &p : req.params) {
					request.query.emplace(p.first, p.second);
				}
				auto host = req.get_header_value("Host");
				request.base_url =
				    "http://" + (host.empty() ? config.host + ":" + std::to_string(config.http_port) : host) + "/odata";
				auto response = odata.Handle(request);
				res.status = response.status;
				if (response.status != 204) {
					res.set_content(response.body, response.content_type);
				}
			};
			http.Get(R"(/odata(/.*)?)", odata_handler);
			http.Post(R"(/odata(/.*)?)", odata_handler);
			http.Put(R"(/odata(/.*)?)", odata_handler);
			http.Patch(R"(/odata(/.*)?)", odata_handler);
			http.Delete(R"(/odata(/.*)?)", odata_handler);
		}
		if (serve_mcp) {
			http.Post("/mcp", [&](const httplib::Request &req, httplib::Response &res) {
				HandleMcp(config, mcp, token, req, res);
			});
			auto no_stream = [](const httplib::Request &, httplib::Response &res) {
				res.set_header("Allow", "POST");
				Text(res, 405, "this server answers POST requests with JSON; it has no event stream");
			};
			http.Get("/mcp", no_stream);
			http.Delete("/mcp", no_stream);
		}
		// The web UI and its API. The page itself is public; its data needs the token where SPARQL does.
		http.Get("/", [&](const httplib::Request &, httplib::Response &res) {
			res.set_content(UI_PAGE, "text/html; charset=utf-8");
		});
		if (serve_sparql) {
			http.Get("/yasgui", [&](const httplib::Request &, httplib::Response &res) {
				res.set_content(YasguiPage(config), "text/html; charset=utf-8");
			});
		}
		auto api_error = [](httplib::Response &res, int status, const std::string &message) {
			Json body = Json::Object();
			body.Set("error", message);
			res.status = status;
			res.set_content(body.Dump(), "application/json");
		};
		auto api_auth = [&](const httplib::Request &req, httplib::Response &res) {
			if (!config.SparqlAnonymous() && !Authorized(req, token)) {
				res.set_header("WWW-Authenticate", "Bearer realm=\"api\"");
				api_error(res, 401, "send Authorization: Bearer <token>");
				return false;
			}
			return true;
		};
		http.Get("/api/catalog", [&](const httplib::Request &req, httplib::Response &res) {
			if (api_auth(req, res)) {
				res.set_content(api.Catalog().Dump(), "application/json");
			}
		});
		http.Get("/api/table", [&](const httplib::Request &req, httplib::Response &res) {
			if (!api_auth(req, res)) {
				return;
			}
			try {
				auto limit = req.has_param("limit") ? std::stoll(req.get_param_value("limit")) : 50;
				res.set_content(api.TablePreview(req.get_param_value("name"), limit).Dump(), "application/json");
			} catch (std::exception &ex) {
				api_error(res, 400, ex.what());
			}
		});
		http.Post("/api/sql", [&](const httplib::Request &req, httplib::Response &res) {
			if (!api_auth(req, res)) {
				return;
			}
			try {
				res.set_content(api.RunSql(req.body, 1000).Dump(), "application/json");
			} catch (std::exception &ex) {
				api_error(res, 400, ex.what());
			}
		});
		http.set_error_handler([](const httplib::Request &, httplib::Response &res) {
			if (res.body.empty()) {
				Text(res, res.status, res.status == 404 ? "not found: the endpoints are /sparql and /mcp" : "error");
			}
		});
		if (!http.bind_to_port(config.host, config.http_port)) {
			throw std::runtime_error("cannot listen on " + config.host + ":" + std::to_string(config.http_port));
		}

		if (config.pg_port) {
			Log("SQL     postgresql://" + config.host + ":" + std::to_string(config.pg_port) +
			    "  (PostgreSQL protocol, DuckDB SQL; any user, password = the token" +
			    (default_token ? " 'duckgraphlake'" : "") + ")");
			Log("          JDBC:    jdbc:postgresql://" + config.host + ":" + std::to_string(config.pg_port) +
			    "/lake?sslmode=disable");
		}
		Log("SQL     quack:" + quack + "  (token " + (default_token ? "'duckgraphlake'" : "as given") + ")");
		Log("          DuckDB:  LOAD quack; ATTACH 'quack:" + quack + "' AS lake (TOKEN '...'); USE lake;");
		Log("          JDBC:    jdbc:duckdb:quack:" + quack +
		    "  after, once on that machine: CREATE PERSISTENT SECRET (TYPE quack, TOKEN '...')");
		if (serve_sparql) {
			Log("SPARQL  " + base + "/sparql" + (config.SparqlAnonymous() ? "" : "  (Authorization: Bearer <token>)") +
			    "; YASGUI at " + base + "/yasgui");
		}
		if (serve_odata) {
			Log("OData   " + base + "/odata" + (config.ODataAnonymous() ? "" : "  (Authorization: Bearer <token>)") +
			    " (" + std::to_string(odata.Sets().size()) + " entity sets)");
		}
		if (kafka) {
			Log("Kafka   " + advertised + ":" + std::to_string(config.kafka_port) +
			    "  (producers; topics -> Iceberg tables in warpstream/_tableflow/)");
		}
		if (config.iceberg_enabled) {
			Log("Iceberg " + base + "/iceberg  (REST catalog over the data folder)");
		}
		if (serve_mcp) {
			Log("MCP     " + base + "/mcp" + (config.McpAnonymous() ? "" : "  (Authorization: Bearer <token>)") + " (" +
			    std::to_string(config.templates.size()) + " template tool(s))");
		}
		Log("Web UI  " + base + "/  (catalog, queries, connection details)");
		http.listen_after_bind();
		Log("stopping");
		if (kafka) {
			kafka->Stop();
		}
		if (lake) {
			lake->Stop();
		}
	} catch (std::exception &ex) {
		duckdb::ErrorData error(ex);
		Log("error: " + error.RawMessage());
		return 1;
	}
	return 0;
}
