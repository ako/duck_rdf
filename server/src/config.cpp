#include "config.hpp"

#include <toml++/toml.hpp>

#include <filesystem>
#include <stdexcept>

namespace dgl {

namespace fs = std::filesystem;

bool Config::Loopback() const {
	return host == "127.0.0.1" || host == "localhost" || host == "::1";
}

std::string ResolvePath(const Config &config, const std::string &path) {
	if (path.empty() || config.config_dir.empty() || fs::path(path).is_absolute()) {
		return path;
	}
	return (fs::path(config.config_dir) / path).lexically_normal().string();
}

namespace {

template <class T>
void Read(const toml::table &table, const char *section, const char *key, T &target) {
	auto node = table[section][key];
	if (auto value = node.value<T>()) {
		target = *value;
	} else if (node) {
		throw std::runtime_error(std::string("[") + section + "] " + key + " has the wrong type");
	}
}

void ReadTri(const toml::table &table, const char *section, const char *key, int &target) {
	bool value = false;
	auto node = table[section][key];
	if (node) {
		Read(table, section, key, value);
		target = value ? 1 : 0;
	}
}

} // namespace

void LoadConfigFile(const std::string &path, Config &config) {
	toml::table table;
	try {
		table = toml::parse_file(path);
	} catch (const toml::parse_error &err) {
		throw std::runtime_error(path + ": " + std::string(err.description()));
	}
	config.config_file = path;
	config.config_dir = fs::absolute(path).parent_path().string();

	Read(table, "server", "host", config.host);
	if (auto p = table["server"]["http_port"].value<int64_t>()) {
		config.http_port = static_cast<int>(*p);
	}
	if (auto p = table["server"]["sql_port"].value<int64_t>()) {
		config.sql_port = static_cast<int>(*p);
	}
	Read(table, "server", "token", config.token);
	Read(table, "server", "lock_down", config.lock_down);

	if (auto t = table["duckdb"]["threads"]) {
		config.threads = t.is_integer() ? std::to_string(*t.value<int64_t>()) : t.value_or(std::string());
	}
	Read(table, "duckdb", "memory_limit", config.memory_limit);
	Read(table, "duckdb", "temp_directory", config.temp_directory);

	Read(table, "data", "directory", config.data_dir);
	config.data_dir = ResolvePath(config, config.data_dir);
	Read(table, "data", "auto_discover", config.auto_discover);
	Read(table, "data", "expose_attached_tables", config.expose_attached_tables);

	Read(table, "rdf", "mapping", config.mapping);
	config.mapping = ResolvePath(config, config.mapping);
	Read(table, "rdf", "ontology", config.ontology);
	config.ontology = ResolvePath(config, config.ontology);
	Read(table, "rdf", "unique_keys", config.unique_keys);
	Read(table, "rdf", "typed_results", config.typed_results);
	Read(table, "rdf", "detect_keys", config.detect_keys);

	Read(table, "sparql", "enabled", config.sparql_enabled);
	ReadTri(table, "sparql", "anonymous", config.sparql_anonymous);
	if (auto t = table["sparql"]["timeout_seconds"]) {
		config.timeout_seconds = t.is_integer() ? double(*t.value<int64_t>()) : t.value_or(60.0);
	}
	Read(table, "sparql", "max_rows", config.sparql_max_rows);
	Read(table, "sparql", "cors_origin", config.cors_origin);

	Read(table, "mcp", "enabled", config.mcp_enabled);
	ReadTri(table, "mcp", "anonymous", config.mcp_anonymous);
	Read(table, "mcp", "max_rows", config.mcp_max_rows);
	if (auto templates = table["mcp"]["templates"].as_array()) {
		for (auto &node : *templates) {
			auto t = node.as_table();
			if (!t) {
				throw std::runtime_error("[[mcp.templates]] entries must be tables");
			}
			QueryTemplate tmpl;
			tmpl.name = (*t)["name"].value_or(std::string());
			tmpl.description = (*t)["description"].value_or(std::string());
			tmpl.query = (*t)["query"].value_or(std::string());
			tmpl.limit = (*t)["limit"].value_or(int64_t(100));
			if (tmpl.name.empty() || tmpl.query.empty()) {
				throw std::runtime_error("every [[mcp.templates]] entry needs a name and a query");
			}
			if (auto params = (*t)["params"].as_array()) {
				for (auto &pnode : *params) {
					auto p = pnode.as_table();
					if (!p) {
						throw std::runtime_error("template " + tmpl.name + ": params must be tables");
					}
					TemplateParam param;
					param.name = (*p)["name"].value_or(std::string());
					param.type = (*p)["type"].value_or(std::string("string"));
					param.description = (*p)["description"].value_or(std::string());
					tmpl.params.push_back(param);
				}
			}
			config.templates.push_back(tmpl);
		}
	}
	if (auto views = table["views"].as_array()) {
		for (auto &node : *views) {
			auto v = node.as_table();
			if (!v) {
				throw std::runtime_error("[[views]] entries must be tables");
			}
			ViewDef view;
			view.name = (*v)["name"].value_or(std::string());
			view.sql = (*v)["sql"].value_or(std::string());
			if (view.name.empty() || view.sql.empty()) {
				throw std::runtime_error("every [[views]] entry needs a name and sql");
			}
			config.views.push_back(view);
		}
	}
}

} // namespace dgl
