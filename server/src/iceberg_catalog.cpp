#include "iceberg_catalog.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <stdexcept>

namespace dgl {

namespace fs = std::filesystem;

namespace {

struct CatalogError : std::runtime_error {
	CatalogError(int status, std::string type, const std::string &message)
	    : std::runtime_error(message), status(status), type(std::move(type)) {
	}
	int status;
	std::string type;
};

CatalogResponse Error(int status, const std::string &type, const std::string &message) {
	Json inner = Json::Object();
	inner.Set("message", message);
	inner.Set("type", type);
	inner.Set("code", status);
	Json body = Json::Object();
	body.Set("error", inner);
	return {status, body.Dump()};
}

CatalogResponse Ok(const Json &body, int status = 200) {
	return {status, body.Dump()};
}

int64_t NowMs() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

std::string Uuid() {
	static std::mt19937_64 rng(std::random_device {}());
	static std::mutex mutex;
	std::lock_guard<std::mutex> lock(mutex);
	auto a = rng(), b = rng();
	char buf[40];
	snprintf(buf, sizeof(buf), "%08x-%04x-4%03x-%04x-%012llx", static_cast<unsigned>(a >> 32),
	         static_cast<unsigned>((a >> 16) & 0xffff), static_cast<unsigned>(a & 0xfff),
	         static_cast<unsigned>(0x8000 | ((b >> 48) & 0x3fff)),
	         static_cast<unsigned long long>(b & 0xffffffffffffULL));
	return buf;
}

std::string ReadFile(const fs::path &path) {
	std::ifstream in(path, std::ios::binary);
	std::stringstream text;
	text << in.rdbuf();
	return text.str();
}

// Writes a file so readers never see it half-written.
void WriteAtomically(const fs::path &path, const std::string &content) {
	auto tmp = path;
	tmp += ".tmp-" + Uuid();
	{
		std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
		out << content;
		if (!out) {
			throw CatalogError(500, "ServiceFailureException", "cannot write " + tmp.string());
		}
	}
	fs::rename(tmp, path);
}

bool ValidName(const std::string &name) {
	if (name.empty() || name.size() > 255 || name[0] == '.') {
		return false;
	}
	for (char c : name) {
		if (c == '/' || c == '\\' || c == '\0' || c == '\x1f') {
			return false;
		}
	}
	return true;
}

// The table's current metadata file: version-hint.text names it (a number N for vN.metadata.json,
// or a file stem), else the newest *.metadata.json.
fs::path CurrentMetadata(const fs::path &table, int &version) {
	auto meta = table / "metadata";
	version = 0;
	if (!fs::is_directory(meta)) {
		return {};
	}
	auto hint = meta / "version-hint.text";
	if (fs::exists(hint)) {
		auto text = ReadFile(hint);
		text.erase(std::remove_if(text.begin(), text.end(), ::isspace), text.end());
		if (!text.empty() && std::all_of(text.begin(), text.end(), ::isdigit)) {
			version = std::stoi(text);
			auto path = meta / ("v" + text + ".metadata.json");
			if (fs::exists(path)) {
				return path;
			}
		}
		for (auto &candidate : {meta / text, meta / (text + ".metadata.json")}) {
			if (!text.empty() && fs::is_regular_file(candidate)) {
				return candidate;
			}
		}
	}
	std::vector<fs::path> files;
	for (auto &entry : fs::directory_iterator(meta)) {
		auto name = entry.path().filename().string();
		if (name.size() > 14 && name.compare(name.size() - 14, 14, ".metadata.json") == 0) {
			files.push_back(entry.path());
		}
	}
	if (files.empty()) {
		return {};
	}
	std::sort(files.begin(), files.end(),
	          [](const fs::path &a, const fs::path &b) { return fs::last_write_time(a) < fs::last_write_time(b); });
	return files.back();
}

int64_t IntOr(const Json *v, int64_t fallback) {
	return v && v->IsNumber() ? v->AsInt() : fallback;
}

// The highest field id in a schema (nested structs, lists and maps included).
int64_t MaxFieldId(const Json &type) {
	int64_t best = 0;
	if (type.IsObject()) {
		for (auto &m : type.Members()) {
			if ((m.first == "id" || m.first == "element-id" || m.first == "key-id" || m.first == "value-id") &&
			    m.second.IsNumber()) {
				best = std::max(best, m.second.AsInt());
			} else {
				best = std::max(best, MaxFieldId(m.second));
			}
		}
	} else if (type.IsArray()) {
		for (auto &item : type.Items()) {
			best = std::max(best, MaxFieldId(item));
		}
	}
	return best;
}

Json &Ensure(Json &object, const std::string &key, Json initial) {
	if (!object.Get(key)) {
		object.Set(key, std::move(initial));
	}
	return *object.GetMutable(key);
}

void ApplyUpdate(Json &md, const Json &u) {
	auto action_node = u.Get("action");
	if (!action_node || !action_node->IsString()) {
		throw CatalogError(400, "BadRequestException", "an update without an action");
	}
	auto action = action_node->AsString();
	if (action == "assign-uuid") {
		md.Set("table-uuid", *u.Get("uuid"));
	} else if (action == "upgrade-format-version") {
		md.Set("format-version", *u.Get("format-version"));
	} else if (action == "add-schema") {
		auto schema = *u.Get("schema");
		Ensure(md, "schemas", Json::Array()).Push(schema);
		auto last =
		    std::max({IntOr(md.Get("last-column-id"), 0), IntOr(u.Get("last-column-id"), 0), MaxFieldId(schema)});
		md.Set("last-column-id", last);
	} else if (action == "set-current-schema") {
		auto id = IntOr(u.Get("schema-id"), -1);
		if (id == -1) {
			auto &schemas = md.Get("schemas")->Items();
			id = IntOr(schemas.back().Get("schema-id"), 0);
		}
		md.Set("current-schema-id", id);
	} else if (action == "add-spec") {
		auto spec = *u.Get("spec");
		Ensure(md, "partition-specs", Json::Array()).Push(spec);
		int64_t last = IntOr(md.Get("last-partition-id"), 999);
		if (auto fields = spec.Get("fields")) {
			for (auto &f : fields->Items()) {
				last = std::max(last, IntOr(f.Get("field-id"), 0));
			}
		}
		md.Set("last-partition-id", last);
	} else if (action == "set-default-spec") {
		auto id = IntOr(u.Get("spec-id"), -1);
		if (id == -1) {
			id = IntOr(md.Get("partition-specs")->Items().back().Get("spec-id"), 0);
		}
		md.Set("default-spec-id", id);
	} else if (action == "add-sort-order") {
		Ensure(md, "sort-orders", Json::Array()).Push(*u.Get("sort-order"));
	} else if (action == "set-default-sort-order") {
		auto id = IntOr(u.Get("sort-order-id"), -1);
		if (id == -1) {
			id = IntOr(md.Get("sort-orders")->Items().back().Get("order-id"), 0);
		}
		md.Set("default-sort-order-id", id);
	} else if (action == "set-location") {
		md.Set("location", *u.Get("location"));
	} else if (action == "set-properties") {
		auto &props = Ensure(md, "properties", Json::Object());
		if (auto updates = u.Get("updates")) {
			for (auto &m : updates->Members()) {
				props.Set(m.first, m.second);
			}
		}
	} else if (action == "remove-properties") {
		auto &props = Ensure(md, "properties", Json::Object());
		if (auto removals = u.Get("removals")) {
			for (auto &key : removals->Items()) {
				props.Remove(key.AsString());
			}
		}
	} else if (action == "add-snapshot") {
		auto snapshot = *u.Get("snapshot");
		Ensure(md, "snapshots", Json::Array()).Push(snapshot);
		md.Set("last-sequence-number",
		       std::max(IntOr(md.Get("last-sequence-number"), 0), IntOr(snapshot.Get("sequence-number"), 0)));
		Json entry = Json::Object();
		entry.Set("snapshot-id", *snapshot.Get("snapshot-id"));
		entry.Set("timestamp-ms", *snapshot.Get("timestamp-ms"));
		Ensure(md, "snapshot-log", Json::Array()).Push(entry);
	} else if (action == "set-snapshot-ref") {
		auto name = u.Get("ref-name")->AsString();
		Json ref = Json::Object();
		for (auto &m : u.Members()) {
			if (m.first != "action" && m.first != "ref-name") {
				ref.Set(m.first, m.second);
			}
		}
		Ensure(md, "refs", Json::Object()).Set(name, ref);
		if (name == "main") {
			md.Set("current-snapshot-id", *u.Get("snapshot-id"));
		}
	} else if (action == "remove-snapshots" || action == "remove-snapshot-ref" || action == "remove-statistics") {
		// Expiring snapshots would leave their files to a cleanup this catalog does not do: keep them.
	} else {
		throw CatalogError(400, "BadRequestException", "update '" + action + "' is not supported by this catalog");
	}
}

void CheckRequirement(const Json &md, const Json &r) {
	auto type = r.Get("type") && r.Get("type")->IsString() ? r.Get("type")->AsString() : "";
	auto conflict = [&](const std::string &what) {
		throw CatalogError(409, "CommitFailedException", "requirement failed: " + what);
	};
	if (type == "assert-create") {
		conflict("the table already exists");
	} else if (type == "assert-table-uuid") {
		auto want = r.Get("uuid");
		auto have = md.Get("table-uuid");
		if (want && have && want->Dump() != have->Dump()) {
			conflict("table uuid changed");
		}
	} else if (type == "assert-ref-snapshot-id") {
		auto ref = r.Get("ref")->AsString();
		auto want = r.Get("snapshot-id");
		auto refs = md.Get("refs");
		auto have = refs ? refs->Get(ref) : nullptr;
		bool want_none = !want || want->IsNull();
		if (want_none != !have || (have && want && have->Get("snapshot-id")->AsInt() != want->AsInt())) {
			conflict("branch " + ref + " moved");
		}
	} else if (type == "assert-current-schema-id") {
		if (IntOr(md.Get("current-schema-id"), -1) != IntOr(r.Get("current-schema-id"), -2)) {
			conflict("the current schema changed");
		}
	} else if (type == "assert-last-assigned-field-id") {
		if (IntOr(md.Get("last-column-id"), -1) != IntOr(r.Get("last-assigned-field-id"), -2)) {
			conflict("columns were added");
		}
	}
	// assert-default-spec-id, assert-default-sort-order-id, assert-last-assigned-partition-id: this
	// catalog never changes those on its own, so a writer's view of them cannot go stale.
}

std::vector<std::string> SplitPath(const std::string &path) {
	std::vector<std::string> parts;
	size_t start = 0;
	while (start <= path.size()) {
		auto slash = path.find('/', start);
		auto part = path.substr(start, slash == std::string::npos ? std::string::npos : slash - start);
		if (!part.empty()) {
			parts.push_back(part);
		}
		if (slash == std::string::npos) {
			break;
		}
		start = slash + 1;
	}
	return parts;
}

} // namespace

IcebergCatalog::IcebergCatalog(std::string warehouse) : warehouse_(std::move(warehouse)) {
}

void IcebergCatalog::AddAlias(const std::string &ns, const std::string &relative_folder) {
	aliases_[ns] = relative_folder;
}

std::string IcebergCatalog::NamespaceFolder(const std::string &ns) const {
	auto it = aliases_.find(ns);
	return (fs::path(warehouse_) / (it != aliases_.end() ? it->second : ns)).lexically_normal().string();
}

std::string IcebergCatalog::TableFolder(const std::string &ns, const std::string &table) const {
	return (fs::path(NamespaceFolder(ns)) / table).string();
}

void IcebergCatalog::EnsureNamespace(const std::string &ns) {
	auto folder = NamespaceFolder(ns);
	fs::create_directories(folder);
	// Marks a folder as a namespace, so discovery reads its tables rather than its Parquet files.
	std::ofstream(fs::path(folder) / ".iceberg-namespace").flush();
}

std::vector<std::string> IcebergCatalog::Namespaces() const {
	std::vector<std::string> out;
	for (auto &alias : aliases_) {
		if (fs::is_directory(NamespaceFolder(alias.first))) {
			out.push_back(alias.first);
		}
	}
	std::error_code ec;
	for (auto &entry : fs::directory_iterator(warehouse_, ec)) {
		auto name = entry.path().filename().string();
		if (entry.is_directory() && fs::exists(entry.path() / ".iceberg-namespace") && !aliases_.count(name)) {
			out.push_back(name);
		}
	}
	std::sort(out.begin(), out.end());
	return out;
}

std::vector<std::string> IcebergCatalog::Tables(const std::string &ns) const {
	std::vector<std::string> out;
	std::error_code ec;
	for (auto &entry : fs::directory_iterator(NamespaceFolder(ns), ec)) {
		if (entry.is_directory() && fs::is_directory(entry.path() / "metadata")) {
			out.push_back(entry.path().filename().string());
		}
	}
	std::sort(out.begin(), out.end());
	return out;
}

CatalogResponse IcebergCatalog::CreateTable(const std::string &ns, const Json &request) {
	auto name_node = request.Get("name");
	auto schema_node = request.Get("schema");
	if (!name_node || !name_node->IsString() || !schema_node) {
		return Error(400, "BadRequestException", "create table needs a name and a schema");
	}
	auto name = name_node->AsString();
	if (!ValidName(name)) {
		return Error(400, "BadRequestException", "invalid table name: " + name);
	}
	auto folder = fs::path(TableFolder(ns, name));
	int version = 0;
	if (!CurrentMetadata(folder, version).empty()) {
		return Error(409, "AlreadyExistsException", "table " + ns + "." + name + " already exists");
	}
	auto schema = *schema_node;
	if (!schema.Get("schema-id")) {
		schema.Set("schema-id", 0);
	}
	Json md = Json::Object();
	md.Set("format-version", 2);
	md.Set("table-uuid", Uuid());
	md.Set("location", fs::absolute(folder).lexically_normal().string());
	md.Set("last-sequence-number", 0);
	md.Set("last-updated-ms", NowMs());
	md.Set("last-column-id", MaxFieldId(schema));
	md.Set("current-schema-id", *schema.Get("schema-id"));
	md.Set("schemas", Json::ArrayOf(std::vector<Json> {schema}));
	Json spec = request.Get("partition-spec") ? *request.Get("partition-spec") : Json::Object();
	if (!spec.Get("spec-id")) {
		spec.Set("spec-id", 0);
	}
	if (!spec.Get("fields")) {
		spec.Set("fields", Json::Array());
	}
	md.Set("default-spec-id", *spec.Get("spec-id"));
	md.Set("partition-specs", Json::ArrayOf(std::vector<Json> {spec}));
	int64_t last_partition = 999;
	for (auto &f : spec.Get("fields")->Items()) {
		last_partition = std::max(last_partition, IntOr(f.Get("field-id"), 0));
	}
	md.Set("last-partition-id", last_partition);
	Json order = request.Get("write-order") ? *request.Get("write-order") : Json::Object();
	if (!order.Get("order-id")) {
		order.Set("order-id", 0);
	}
	if (!order.Get("fields")) {
		order.Set("fields", Json::Array());
	}
	md.Set("default-sort-order-id", *order.Get("order-id"));
	md.Set("sort-orders", Json::ArrayOf(std::vector<Json> {order}));
	md.Set("properties", request.Get("properties") ? *request.Get("properties") : Json::Object());
	md.Set("current-snapshot-id", -1);
	md.Set("snapshots", Json::Array());
	md.Set("snapshot-log", Json::Array());
	md.Set("metadata-log", Json::Array());
	md.Set("refs", Json::Object());

	bool staged = request.Get("stage-create") && request.Get("stage-create")->GetType() == Json::Type::BOOL &&
	              request.Get("stage-create")->AsBool();
	if (staged) {
		// A staged create writes nothing: the table appears when its commit (assert-create) arrives.
		staged_[ns + "\x1f" + name] = md;
		Json body = Json::Object();
		body.Set("metadata", md);
		body.Set("config", Json::Object());
		return Ok(body);
	}
	fs::create_directories(folder / "metadata");
	fs::create_directories(folder / "data");
	auto path = folder / "metadata" / "v1.metadata.json";
	WriteAtomically(path, md.Dump());
	WriteAtomically(folder / "metadata" / "version-hint.text", "1");
	Json body = Json::Object();
	body.Set("metadata-location", fs::absolute(path).string());
	body.Set("metadata", md);
	body.Set("config", Json::Object());
	return Ok(body);
}

CatalogResponse IcebergCatalog::LoadTable(const std::string &ns, const std::string &table) {
	int version = 0;
	auto path = CurrentMetadata(TableFolder(ns, table), version);
	if (path.empty()) {
		return Error(404, "NoSuchTableException", "table " + ns + "." + table + " does not exist");
	}
	Json body = Json::Object();
	body.Set("metadata-location", fs::absolute(path).string());
	body.Set("metadata", Json::Parse(ReadFile(path)));
	body.Set("config", Json::Object());
	return Ok(body);
}

Json IcebergCatalog::Commit(const std::string &ns, const std::string &table, const Json &requirements,
                            const Json &updates, std::string &metadata_location) {
	auto folder = fs::path(TableFolder(ns, table));
	int version = 0;
	auto current = CurrentMetadata(folder, version);
	bool creating = false;
	for (auto &r : requirements.Items()) {
		creating =
		    creating || (r.Get("type") && r.Get("type")->IsString() && r.Get("type")->AsString() == "assert-create");
	}
	Json md;
	if (creating) {
		// Completing a staged create: the table must still not exist.
		if (!current.empty()) {
			throw CatalogError(409, "CommitFailedException", "table " + ns + "." + table + " already exists");
		}
		auto staged = staged_.find(ns + "\x1f" + table);
		if (staged != staged_.end()) {
			md = staged->second;
			staged_.erase(staged);
		} else {
			md = Json::Object();
			md.Set("format-version", 2);
			md.Set("table-uuid", Uuid());
			md.Set("location", fs::absolute(folder).lexically_normal().string());
			md.Set("last-sequence-number", 0);
			md.Set("last-column-id", 0);
			md.Set("schemas", Json::Array());
			md.Set("partition-specs", Json::Array());
			md.Set("sort-orders", Json::Array());
			md.Set("snapshots", Json::Array());
			md.Set("snapshot-log", Json::Array());
			md.Set("metadata-log", Json::Array());
			md.Set("refs", Json::Object());
			md.Set("properties", Json::Object());
			md.Set("current-snapshot-id", -1);
		}
		fs::create_directories(folder / "metadata");
		fs::create_directories(folder / "data");
	} else if (current.empty()) {
		throw CatalogError(404, "NoSuchTableException", "table " + ns + "." + table + " does not exist");
	} else {
		md = Json::Parse(ReadFile(current));
	}
	for (auto &r : requirements.Items()) {
		if (!creating) {
			CheckRequirement(md, r);
		}
	}
	for (auto &u : updates.Items()) {
		ApplyUpdate(md, u);
	}
	if (!current.empty()) {
		Json log_entry = Json::Object();
		log_entry.Set("metadata-file", fs::absolute(current).string());
		log_entry.Set("timestamp-ms", IntOr(md.Get("last-updated-ms"), NowMs()));
		Ensure(md, "metadata-log", Json::Array()).Push(log_entry);
	}
	md.Set("last-updated-ms", NowMs());
	// Next version: after the hint's number, or after every vN file already there.
	int next = version + 1;
	while (fs::exists(folder / "metadata" / ("v" + std::to_string(next) + ".metadata.json"))) {
		next++;
	}
	auto path = folder / "metadata" / ("v" + std::to_string(next) + ".metadata.json");
	WriteAtomically(path, md.Dump());
	WriteAtomically(folder / "metadata" / "version-hint.text", std::to_string(next));
	metadata_location = fs::absolute(path).string();
	return md;
}

CatalogResponse IcebergCatalog::CommitTable(const std::string &ns, const std::string &table, const Json &request) {
	std::string location;
	auto md = Commit(ns, table, request.Get("requirements") ? *request.Get("requirements") : Json::Array(),
	                 request.Get("updates") ? *request.Get("updates") : Json::Array(), location);
	Json body = Json::Object();
	body.Set("metadata-location", location);
	body.Set("metadata", md);
	return Ok(body);
}

CatalogResponse IcebergCatalog::Handle(const std::string &method, const std::string &path, const std::string &body) {
	try {
		std::lock_guard<std::mutex> lock(mutex_);
		auto parts = SplitPath(path);
		if (parts.empty() || parts[0] != "v1") {
			return Error(404, "NotFoundException", "not found: " + path);
		}
		parts.erase(parts.begin());
		Json request = body.empty() ? Json::Object() : Json::Parse(body);
		if (parts.size() == 1 && parts[0] == "config") {
			Json config = Json::Object();
			config.Set("defaults", Json::Object());
			config.Set("overrides", Json::Object());
			return Ok(config);
		}
		if (parts.size() == 2 && parts[0] == "transactions" && parts[1] == "commit" && method == "POST") {
			// All tables' requirements first, then the commits: each table commits atomically.
			auto changes = request.Get("table-changes");
			if (!changes) {
				return Error(400, "BadRequestException", "table-changes is missing");
			}
			for (auto &change : changes->Items()) {
				auto id = change.Get("identifier");
				auto ns = id->Get("namespace")->Items()[0].AsString();
				auto table = id->Get("name")->AsString();
				std::string location;
				Commit(ns, table, change.Get("requirements") ? *change.Get("requirements") : Json::Array(),
				       change.Get("updates") ? *change.Get("updates") : Json::Array(), location);
			}
			return {204, ""};
		}
		if (parts.empty() || parts[0] != "namespaces") {
			return Error(404, "NotFoundException", "not found: " + path);
		}
		if (parts.size() == 1) {
			if (method == "GET") {
				Json list = Json::Array();
				for (auto &ns : Namespaces()) {
					list.Push(Json::ArrayOf(std::vector<std::string> {ns}));
				}
				Json result = Json::Object();
				result.Set("namespaces", list);
				return Ok(result);
			}
			if (method == "POST") {
				auto ns_node = request.Get("namespace");
				if (!ns_node || !ns_node->IsArray() || ns_node->Size() != 1) {
					return Error(400, "BadRequestException", "this catalog has one-level namespaces");
				}
				auto ns = ns_node->Items()[0].AsString();
				if (!ValidName(ns)) {
					return Error(400, "BadRequestException", "invalid namespace: " + ns);
				}
				EnsureNamespace(ns);
				Json result = Json::Object();
				result.Set("namespace", Json::ArrayOf(std::vector<std::string> {ns}));
				result.Set("properties", Json::Object());
				return Ok(result);
			}
		}
		auto ns = parts.size() > 1 ? parts[1] : "";
		if (!ValidName(ns)) {
			return Error(400, "BadRequestException", "this catalog has one-level namespaces");
		}
		auto namespaces = Namespaces();
		auto ns_exists = std::find(namespaces.begin(), namespaces.end(), ns) != namespaces.end();
		if (parts.size() == 2) {
			if (!ns_exists) {
				return Error(404, "NoSuchNamespaceException", "namespace " + ns + " does not exist");
			}
			Json result = Json::Object();
			result.Set("namespace", Json::ArrayOf(std::vector<std::string> {ns}));
			result.Set("properties", Json::Object());
			return method == "HEAD" ? CatalogResponse {204, ""} : Ok(result);
		}
		if (parts[2] != "tables") {
			return Error(404, "NotFoundException", "not found: " + path);
		}
		if (!ns_exists) {
			return Error(404, "NoSuchNamespaceException", "namespace " + ns + " does not exist");
		}
		if (parts.size() == 3) {
			if (method == "GET") {
				Json ids = Json::Array();
				for (auto &t : Tables(ns)) {
					Json id = Json::Object();
					id.Set("namespace", Json::ArrayOf(std::vector<std::string> {ns}));
					id.Set("name", t);
					ids.Push(id);
				}
				Json result = Json::Object();
				result.Set("identifiers", ids);
				return Ok(result);
			}
			if (method == "POST") {
				return CreateTable(ns, request);
			}
		}
		if (parts.size() == 4) {
			auto table = parts[3];
			if (!ValidName(table)) {
				return Error(400, "BadRequestException", "invalid table name: " + table);
			}
			if (method == "GET" || method == "HEAD") {
				auto loaded = LoadTable(ns, table);
				if (method == "HEAD" && loaded.status == 200) {
					return {204, ""};
				}
				return loaded;
			}
			if (method == "POST") {
				return CommitTable(ns, table, request);
			}
			if (method == "DELETE") {
				return Error(501, "NotImplementedException", "dropping tables is not supported; delete the folder");
			}
		}
		return Error(405, "MethodNotAllowedException", method + " " + path + " is not supported");
	} catch (CatalogError &e) {
		return Error(e.status, e.type, e.what());
	} catch (std::exception &e) {
		return Error(500, "ServiceFailureException", e.what());
	}
}

} // namespace dgl
