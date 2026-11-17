// An Iceberg REST catalog over the data folder, served at /iceberg.
//
// Namespaces are folders under the data folder (one level; an alias can point a namespace at a
// deeper folder, e.g. "tableflow" -> warpstream/_tableflow), tables are folders inside them with
// Iceberg's metadata/ and data/. The catalog only keeps the metadata JSON: clients that write
// (DuckDB's iceberg extension, PyIceberg, Spark) write the Parquet data files and Avro manifests
// themselves and commit through it. Each commit writes the next vN.metadata.json and updates
// metadata/version-hint.text, so the tables are also readable without the catalog
// (iceberg_scan('<folder>')).
//
//   GET  /v1/config                                   GET/POST /v1/namespaces
//   GET  /v1/namespaces/{ns}                          GET/POST /v1/namespaces/{ns}/tables
//   GET  /v1/namespaces/{ns}/tables/{table}           POST     /v1/namespaces/{ns}/tables/{table}   (commit)
//   POST /v1/transactions/commit
#pragma once

#include "json.hpp"

#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace dgl {

struct CatalogResponse {
	int status = 200;
	std::string body; // JSON, or empty
};

class IcebergCatalog {
public:
	explicit IcebergCatalog(std::string warehouse);

	// A namespace stored in a folder other than <warehouse>/<namespace>.
	void AddAlias(const std::string &ns, const std::string &relative_folder);
	// Creates the namespace's folder if needed.
	void EnsureNamespace(const std::string &ns);

	// method: GET, HEAD, POST, DELETE; path: after the /iceberg prefix (starts with /v1/).
	CatalogResponse Handle(const std::string &method, const std::string &path, const std::string &body);

	std::string NamespaceFolder(const std::string &ns) const;
	std::string TableFolder(const std::string &ns, const std::string &table) const;
	std::vector<std::string> Namespaces() const;
	std::vector<std::string> Tables(const std::string &ns) const;

private:
	CatalogResponse CreateTable(const std::string &ns, const Json &request);
	CatalogResponse LoadTable(const std::string &ns, const std::string &table);
	CatalogResponse CommitTable(const std::string &ns, const std::string &table, const Json &request);
	// Applies a commit's requirements and updates to the table's current metadata and writes the
	// next version; throws CatalogError.
	Json Commit(const std::string &ns, const std::string &table, const Json &requirements, const Json &updates,
	            std::string &metadata_location);

	std::string warehouse_;
	std::map<std::string, std::string> aliases_;
	std::map<std::string, Json> staged_; // staged creates by "<ns>\x1f<table>", written on commit
	std::mutex mutex_;
};

} // namespace dgl
