#include "setup.hpp"

#include "log.hpp"
#include "sparql.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>

namespace dgl {

namespace fs = std::filesystem;

void Exec(duckdb::Connection &con, const std::string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error(result->GetError());
	}
}

std::vector<std::string> Column(duckdb::Connection &con, const std::string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error(result->GetError());
	}
	std::vector<std::string> out;
	while (auto chunk = result->Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
			auto v = chunk->GetValue(0, r);
			out.push_back(v.IsNull() ? std::string() : v.ToString());
		}
	}
	return out;
}

std::vector<std::string> GlobFiles(duckdb::Connection &con, const std::string &pattern) {
	if (pattern.empty()) {
		return {};
	}
	return Column(con, "SELECT file FROM glob(" + SqlLiteral(pattern) + ") ORDER BY file");
}

namespace {

// A safe SQL identifier from a file name: anything unusual becomes _.
std::string IdentName(const std::string &name) {
	std::string cleaned;
	for (char c : name) {
		cleaned.push_back(std::isalnum(static_cast<unsigned char>(c)) || c == '_' ? c : '_');
	}
	if (cleaned.empty() || std::isdigit(static_cast<unsigned char>(cleaned[0]))) {
		cleaned = "t_" + cleaned;
	}
	return cleaned;
}

std::string Lower(std::string s) {
	std::transform(s.begin(), s.end(), s.begin(), ::tolower);
	return s;
}

bool EndsWith(const std::string &s, const std::string &suffix) {
	return s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

// The file's format from its name, compression suffix included: "data.csv.gz" is csv.
std::string FileKind(const std::string &filename) {
	auto name = Lower(filename);
	for (auto &suffix : {std::string(".gz"), std::string(".zst")}) {
		if (EndsWith(name, suffix)) {
			name = name.substr(0, name.size() - suffix.size());
		}
	}
	if (EndsWith(name, ".parquet")) {
		return "parquet";
	}
	if (EndsWith(name, ".csv") || EndsWith(name, ".tsv")) {
		return "csv";
	}
	if (EndsWith(name, ".json") || EndsWith(name, ".jsonl") || EndsWith(name, ".ndjson")) {
		return "json";
	}
	if (EndsWith(name, ".sqlite") || EndsWith(name, ".sqlite3") || EndsWith(name, ".db")) {
		return "sqlite";
	}
	if (EndsWith(name, ".duckdb")) {
		return "duckdb";
	}
	return "";
}

std::string StemOf(const std::string &filename) {
	auto name = filename;
	for (auto &suffix : {std::string(".gz"), std::string(".zst")}) {
		if (EndsWith(Lower(name), suffix)) {
			name = name.substr(0, name.size() - suffix.size());
		}
	}
	auto dot = name.rfind('.');
	return dot == std::string::npos || dot == 0 ? name : name.substr(0, dot);
}

std::string Reader(const std::string &kind, const std::string &source, bool many) {
	auto src = SqlLiteral(source);
	if (kind == "parquet") {
		return "read_parquet(" + src + (many ? ", union_by_name = true" : "") + ")";
	}
	if (kind == "csv") {
		return "read_csv(" + src + (many ? ", union_by_name = true" : "") + ")";
	}
	return "read_json(" + src + (many ? ", union_by_name = true" : "") + ")";
}

// The one tabular format of a directory's files (recursively), or "" if none or mixed.
std::string DirectoryKind(const fs::path &dir) {
	std::set<std::string> kinds;
	std::error_code ec;
	for (auto it = fs::recursive_directory_iterator(dir, ec); !ec && it != fs::recursive_directory_iterator();
	     it.increment(ec)) {
		if (!it->is_regular_file()) {
			continue;
		}
		auto name = it->path().filename().string();
		if (!name.empty() && name[0] == '.') {
			continue;
		}
		auto kind = FileKind(name);
		if (kind == "parquet" || kind == "csv" || kind == "json") {
			kinds.insert(kind);
		}
	}
	return kinds.size() == 1 ? *kinds.begin() : "";
}

std::string NewestIcebergVersion(const fs::path &metadata) {
	if (fs::exists(metadata / "version-hint.text")) {
		return "";
	}
	std::vector<std::string> candidates;
	for (auto &entry : fs::directory_iterator(metadata)) {
		auto name = entry.path().filename().string();
		if (EndsWith(name, ".metadata.json")) {
			candidates.push_back(name.substr(0, name.size() - std::string(".metadata.json").size()));
		}
	}
	std::sort(candidates.begin(), candidates.end());
	if (candidates.empty()) {
		throw std::runtime_error("metadata/ has no *.metadata.json");
	}
	return candidates.back();
}

} // namespace

std::vector<Exposed> Discover(duckdb::Connection &con, const Config &config) {
	std::vector<Exposed> exposed;
	bool iceberg_loaded = false;
	fs::path data_dir(config.data_dir);
	if (config.auto_discover && fs::is_directory(data_dir)) {
		std::vector<fs::path> entries;
		for (auto &entry : fs::directory_iterator(data_dir)) {
			entries.push_back(entry.path());
		}
		std::sort(entries.begin(), entries.end());
		for (auto &path : entries) {
			auto entry = path.filename().string();
			if (entry.empty() || entry[0] == '.') {
				continue;
			}
			auto full = fs::absolute(path).lexically_normal().string();
			try {
				if (fs::is_directory(path) && fs::is_directory(path / "metadata")) {
					// An Iceberg table. Not every writer leaves a version-hint.text, and DuckDB will not
					// guess one, so name the newest metadata file. allow_moved_paths: a copied table's
					// absolute paths rarely match where it sits now.
					auto version = NewestIcebergVersion(path / "metadata");
					if (!iceberg_loaded) {
						// Iceberg is not compiled in (its AWS dependencies are large): DuckDB's signed
						// extension is installed from its repository on first use, then cached.
						// Its avro dependency too: iceberg would auto-install it from this build's local
						// extension repository (default_autoinstall_repository), which holds no avro.
						Exec(con, "INSTALL avro");
						Exec(con, "INSTALL iceberg");
						Exec(con, "LOAD iceberg");
						iceberg_loaded = true;
					}
					Exec(con, "CREATE VIEW " + SqlIdent(IdentName(entry)) + " AS SELECT * FROM iceberg_scan(" +
					              SqlLiteral(full) + (version.empty() ? "" : ", version = " + SqlLiteral(version)) +
					              ", allow_moved_paths = true)");
					exposed.push_back({"iceberg", IdentName(entry), full + (version.empty() ? "" : " @ " + version)});
				} else if (fs::is_directory(path)) {
					auto kind = DirectoryKind(path);
					if (kind.empty()) {
						continue;
					}
					std::string ext = kind == "parquet" ? "*.parquet" : kind == "csv" ? "*.[ct]sv*" : "*.*json*";
					auto pattern = (fs::path(full) / "**" / ext).string();
					Exec(con, "CREATE VIEW " + SqlIdent(IdentName(entry)) + " AS SELECT * FROM " +
					              Reader(kind, pattern, true));
					exposed.push_back({kind, IdentName(entry), pattern});
				} else {
					auto kind = FileKind(entry);
					auto name = IdentName(StemOf(entry));
					if (kind == "parquet" || kind == "csv" || kind == "json") {
						Exec(con, "CREATE VIEW " + SqlIdent(name) + " AS SELECT * FROM " + Reader(kind, full, false));
						exposed.push_back({kind, name, full});
					} else if (kind == "sqlite") {
						Exec(con, "ATTACH " + SqlLiteral(full) + " AS " + SqlIdent(name) + " (TYPE sqlite, READ_ONLY)");
						exposed.push_back({kind, name, full});
					} else if (kind == "duckdb") {
						Exec(con, "ATTACH " + SqlLiteral(full) + " AS " + SqlIdent(name) + " (READ_ONLY)");
						exposed.push_back({kind, name, full});
					}
				}
			} catch (std::exception &ex) {
				Log("  skipped " + entry + ": " + ex.what());
			}
		}
	}
	// Views written by hand win over discovered ones of the same name.
	for (auto &view : config.views) {
		Exec(con, "CREATE OR REPLACE VIEW " + SqlIdent(view.name) + " AS " + view.sql);
		exposed.push_back({"view", view.name, view.sql});
	}
	// Tables of attached databases also under their plain names, so a mapping can name them.
	if (config.expose_attached_tables) {
		auto names = Column(con, "SELECT table_name FROM duckdb_tables() WHERE database_name = 'memory' "
		                         "UNION SELECT view_name FROM duckdb_views() WHERE database_name = 'memory'");
		std::set<std::string> taken(names.begin(), names.end());
		auto attached = exposed;
		for (auto &db : attached) {
			if (db.kind != "sqlite" && db.kind != "duckdb") {
				continue;
			}
			for (auto &table : Column(con, "SELECT table_name FROM duckdb_tables() WHERE database_name = " +
			                                   SqlLiteral(db.name) + " ORDER BY table_name")) {
				if (taken.count(table)) {
					Log("  skipped " + db.name + "." + table + ": a view named " + table + " already exists");
					continue;
				}
				Exec(con, "CREATE VIEW " + SqlIdent(table) + " AS SELECT * FROM " + SqlIdent(db.name) + "." +
				              SqlIdent(table));
				taken.insert(table);
				exposed.push_back({"table", table, db.name + "." + table});
			}
		}
	}
	return exposed;
}

void ConfigureSparql(duckdb::Connection &con, const Config &config) {
	Exec(con, std::string("SET GLOBAL sparql_typed_results = ") + (config.typed_results ? "true" : "false"));
	if (!config.ontology.empty()) {
		Exec(con, "SET GLOBAL sparql_ontology = " + SqlLiteral(config.ontology));
	}
	if (!config.unique_keys.empty()) {
		Exec(con, "SET GLOBAL sparql_unique_keys = " + SqlLiteral(config.unique_keys));
	}
	if (!config.mapping.empty()) {
		// Macros, so SQL clients write sparql('...') instead of repeating the mapping path.
		auto mapping = SqlLiteral(config.mapping);
		Exec(con, "CREATE MACRO sparql(q) AS TABLE SELECT * FROM execute_sparql(q, " + mapping + ")");
		Exec(con, "CREATE MACRO sparql_sql(q) AS sparql_to_sql(q, " + mapping + ")");
	}
}

std::vector<std::string> DetectKeys(duckdb::Connection &con, const std::vector<Exposed> &exposed,
                                    const std::string &mapping_glob) {
	// Column names the mapping puts in templates: {col} (R2RML) and $(col) (YARRRML).
	std::set<std::string> candidates;
	std::regex placeholder(R"(\{([^{}\s]+)\}|\$\(([^()\s]+)\))");
	for (auto &file : GlobFiles(con, mapping_glob)) {
		std::ifstream in(file);
		std::stringstream text;
		text << in.rdbuf();
		auto s = text.str();
		for (std::sregex_iterator it(s.begin(), s.end(), placeholder), end; it != end; ++it) {
			candidates.insert((*it)[1].matched ? (*it)[1].str() : (*it)[2].str());
		}
	}
	std::vector<std::string> keys;
	for (auto &e : exposed) {
		if (e.kind == "sqlite" || e.kind == "duckdb") {
			continue;
		}
		auto table = SqlIdent(e.name);
		for (auto &col : Column(con, "SELECT column_name FROM (DESCRIBE " + table + ")")) {
			if (!candidates.count(col)) {
				continue;
			}
			auto unique = Column(con, "SELECT count(*) = count(DISTINCT " + SqlIdent(col) + ") AND count(*) = count(" +
			                              SqlIdent(col) + ") FROM " + table);
			if (!unique.empty() && unique[0] == "true") {
				keys.push_back(e.name + "(" + col + ")");
				break;
			}
		}
	}
	return keys;
}

namespace {

// The directory a glob's files live under: everything before the first wildcard.
std::string GlobRoot(const std::string &pattern) {
	auto wild = pattern.find_first_of("*?[{");
	auto fixed = pattern.substr(0, wild);
	fs::path p(fixed);
	if (wild == std::string::npos && !fs::is_directory(p)) {
		p = p.parent_path();
	} else if (!fixed.empty() && fixed.back() != '/') {
		p = p.parent_path();
	}
	return fs::absolute(p.empty() ? fs::path(".") : p).lexically_normal().string();
}

} // namespace

void LockDown(duckdb::Connection &con, const Config &config) {
	std::set<std::string> dirs;
	dirs.insert(fs::absolute(config.data_dir).lexically_normal().string());
	if (!config.config_dir.empty()) {
		dirs.insert(config.config_dir);
	}
	for (auto &glob : {config.mapping, config.ontology}) {
		if (!glob.empty()) {
			dirs.insert(GlobRoot(glob));
		}
	}
	std::string list;
	for (auto dir : dirs) {
		if (dir.empty() || dir.back() != '/') {
			dir += "/";
		}
		list += (list.empty() ? "" : ", ") + SqlLiteral(dir);
	}
	// allow_persistent_secrets is turned off in main, before the database opens.
	Exec(con, "SET allowed_directories = [" + list + "]");
	Exec(con, "SET enable_external_access = false");
	Exec(con, "SET lock_configuration = true");
}

} // namespace dgl
