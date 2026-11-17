// Preparing the database: views over the data folder, SPARQL defaults, keys, lock-down.
#pragma once

#include "config.hpp"
#include "duckdb.hpp"

#include <string>
#include <vector>

namespace dgl {

struct Exposed {
	std::string kind; // parquet, csv, json, iceberg, sqlite, duckdb, view, table
	std::string name;
	std::string source;
};

// Runs SQL, throwing std::runtime_error with DuckDB's message on failure.
void Exec(duckdb::Connection &con, const std::string &sql);
// The first column of every row, as text.
std::vector<std::string> Column(duckdb::Connection &con, const std::string &sql);

// Files matching a glob (DuckDB's glob()), sorted.
std::vector<std::string> GlobFiles(duckdb::Connection &con, const std::string &pattern);

// The SELECT a view over an Iceberg table folder uses.
std::string IcebergView(const std::string &folder);

// For a view over a table of change events (a cdc struct and a warpstream struct, as the Kafka
// endpoint writes them): creates <view>_current, the latest row per record key, deletes left out,
// and returns its name; otherwise returns "".
std::string ChangeEventCurrentView(duckdb::Connection &con, const std::string &view);

std::vector<Exposed> Discover(duckdb::Connection &con, const Config &config);
void ConfigureSparql(duckdb::Connection &con, const Config &config);
// Unique, non-null template columns of the exposed tables, as sparql_unique_keys entries.
std::vector<std::string> DetectKeys(duckdb::Connection &con, const std::vector<Exposed> &exposed,
                                    const std::string &mapping_glob);
void LockDown(duckdb::Connection &con, const Config &config);

} // namespace dgl
