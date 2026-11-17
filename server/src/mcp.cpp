#include "mcp.hpp"

#include "setup.hpp"

#include <algorithm>
#include <fstream>
#include <regex>
#include <set>
#include <sstream>

namespace dgl {

namespace {

const std::string RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const std::string RDFS = "http://www.w3.org/2000/01/rdf-schema#";
const std::string XSD = "http://www.w3.org/2001/XMLSchema#";
const std::string SKOS = "http://www.w3.org/2004/02/skos/core#";
const size_t MAX_LISTED_VALUES = 10;

// A tools/call for a tool that does not exist: a protocol error, not a tool error.
struct UnknownTool : std::runtime_error {
	using std::runtime_error::runtime_error;
};

const char *INSTRUCTIONS =
    "A knowledge graph over relational data and data-lake files, queried with SPARQL.\n"
    "Start with list_classes, then describe_class for the classes you need; it lists the properties\n"
    "their instances have. Use find_entity to turn a name into an IRI. Write SPARQL with full IRIs in\n"
    "angle brackets, or PREFIX declarations. A class or property that does not exist is an error, not\n"
    "an empty result: check the name with list_classes or describe_class. The prefixes rdf:, rdfs:,\n"
    "xsd: and owl: are predeclared. Results are capped; add LIMIT or narrow the query when a result\n"
    "says truncated. Prefer a template tool when one fits.";

const std::regex &IriPattern() {
	static const std::regex pattern(R"(^[A-Za-z][A-Za-z0-9+.\-]*:[^\s<>"{}|\\^`]*$)");
	return pattern;
}

// An IRI from an agent, with or without angle brackets; refuses anything that is not one.
std::string CheckIri(std::string text) {
	auto b = text.find_first_not_of(" \t\r\n");
	auto e = text.find_last_not_of(" \t\r\n");
	text = b == std::string::npos ? "" : text.substr(b, e - b + 1);
	if (text.size() >= 2 && text.front() == '<' && text.back() == '>') {
		text = text.substr(1, text.size() - 2);
	}
	if (!std::regex_match(text, IriPattern())) {
		throw std::runtime_error("not an absolute IRI: '" + text + "'");
	}
	return text;
}

std::string Trim(const std::string &text) {
	auto b = text.find_first_not_of(" \t\r\n");
	auto e = text.find_last_not_of(" \t\r\n");
	return b == std::string::npos ? "" : text.substr(b, e - b + 1);
}

std::string LowerAscii(std::string s) {
	std::transform(s.begin(), s.end(), s.begin(), ::tolower);
	return s;
}

// The query with PREFIX declarations for the standard prefixes it uses but does not declare.
std::string WithStandardPrefixes(const std::string &query) {
	static const std::vector<std::pair<std::string, std::string>> standard {
	    {"rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
	    {"rdfs", RDFS},
	    {"xsd", XSD},
	    {"owl", "http://www.w3.org/2002/07/owl#"}};
	std::string head;
	for (auto &entry : standard) {
		auto token = entry.first + ":";
		bool used = false;
		for (size_t pos = query.find(token); pos != std::string::npos; pos = query.find(token, pos + 1)) {
			char before = pos == 0 ? ' ' : query[pos - 1];
			if (!(std::isalnum(static_cast<unsigned char>(before)) || before == '_' || before == '-')) {
				used = true;
				break;
			}
		}
		std::regex declared("\\bPREFIX\\s+" + entry.first + "\\s*:", std::regex::icase);
		if (used && !std::regex_search(query, declared)) {
			head += "PREFIX " + entry.first + ": <" + entry.second + ">\n";
		}
	}
	return head + query;
}

// A binding as the agent sees it: <iri>, _:blank, a number, a boolean, or a string.
// xsd:decimal stays its exact text: as a JSON number it could pick up binary rounding.
Json Value(const std::optional<Term> &b) {
	if (!b) {
		return Json();
	}
	if (b->kind == Term::Kind::IRI) {
		return Json("<" + b->value + ">");
	}
	if (b->kind == Term::Kind::BNODE) {
		return Json("_:" + b->value);
	}
	static const std::set<std::string> integers {"integer", "int",  "long", "short", "byte", "nonNegativeInteger",
	                                             "positiveInteger", "unsignedInt", "unsignedLong"};
	auto &dt = b->datatype;
	if (dt.rfind(XSD, 0) == 0) {
		auto local = dt.substr(XSD.size());
		try {
			size_t used = 0;
			if (integers.count(local)) {
				auto v = std::stoll(b->value, &used);
				if (used == b->value.size()) {
					return Json(static_cast<int64_t>(v));
				}
			} else if (local == "double" || local == "float") {
				auto v = std::stod(b->value, &used);
				if (used == b->value.size()) {
					return Json(v);
				}
			} else if (local == "boolean") {
				return Json(b->value == "true" || b->value == "1");
			}
		} catch (std::exception &) {
		}
	}
	if (!b->lang.empty()) {
		return Json(b->value + "@" + b->lang);
	}
	return Json(b->value);
}

std::string LocalName(const std::string &iri) {
	auto pos = iri.find_last_of("#/");
	return pos == std::string::npos ? iri : iri.substr(pos + 1);
}

std::string Bracket(const std::string &iri) {
	return "<" + iri + ">";
}

std::string ReadFile(const std::string &path) {
	std::ifstream in(path, std::ios::binary);
	std::stringstream text;
	text << in.rdbuf();
	return text.str();
}

Json Schema(std::vector<std::tuple<std::string, std::string, std::string>> props, std::vector<std::string> required) {
	Json schema = Json::Object();
	schema.Set("type", "object");
	Json properties = Json::Object();
	for (auto &p : props) {
		Json prop = Json::Object();
		prop.Set("type", std::get<1>(p));
		if (!std::get<2>(p).empty()) {
			prop.Set("description", std::get<2>(p));
		}
		properties.Set(std::get<0>(p), prop);
	}
	schema.Set("properties", properties);
	schema.Set("required", Json::ArrayOf(required));
	return schema;
}

Json Tool(const std::string &name, const std::string &description, Json schema) {
	Json tool = Json::Object();
	tool.Set("name", name);
	tool.Set("description", description);
	tool.Set("inputSchema", std::move(schema));
	return tool;
}

std::string StringArg(const Json &args, const std::string &name, bool required = true) {
	auto v = args.Get(name);
	if (!v || v->IsNull()) {
		if (required) {
			throw std::runtime_error("missing argument: " + name);
		}
		return "";
	}
	if (!v->IsString()) {
		throw std::runtime_error(name + " must be a string");
	}
	return v->AsString();
}

int64_t IntArg(const Json &args, const std::string &name, int64_t fallback) {
	auto v = args.Get(name);
	if (!v || v->IsNull()) {
		return fallback;
	}
	if (v->IsString()) {
		return std::stoll(v->AsString());
	}
	return v->AsInt();
}

} // namespace

McpServer::McpServer(duckdb::DuckDB &db, const SparqlExecutor &executor, const Config &config,
                     std::vector<std::string> ontology_files)
    : db_(db), executor_(executor), config_(config), ontology_files_(std::move(ontology_files)) {
	static const std::set<std::string> types {"string", "iri", "integer", "decimal", "boolean"};
	static const std::regex name_pattern("[A-Za-z_][A-Za-z0-9_]*");
	for (auto &tmpl : config_.templates) {
		for (auto &p : tmpl.params) {
			if (!types.count(p.type)) {
				throw std::runtime_error("template " + tmpl.name + ": parameter " + p.name + " has unknown type " +
				                         p.type);
			}
			if (!std::regex_match(p.name, name_pattern)) {
				throw std::runtime_error("template " + tmpl.name + ": bad parameter name " + p.name);
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Plumbing

Json McpServer::Run(const std::string &query, int64_t limit) {
	limit = std::max<int64_t>(1, std::min<int64_t>(limit, config_.mcp_max_rows));
	SparqlResult r;
	try {
		r = executor_.Execute(query, static_cast<size_t>(limit));
	} catch (SparqlError &e) {
		throw ToolError(e.what());
	}
	Json out = Json::Object();
	if (r.kind == SparqlResult::Kind::ASK) {
		out.Set("boolean", r.boolean);
		return out;
	}
	if (r.kind == SparqlResult::Kind::CONSTRUCT) {
		Json triples = Json::Array();
		for (auto &t : r.triples) {
			Json triple = Json::Array();
			triple.Push(t.subject.kind == Term::Kind::BNODE ? "_:" + t.subject.value : t.subject.value);
			triple.Push(t.predicate);
			if (t.object.kind == Term::Kind::IRI) {
				triple.Push(t.object.value);
			} else if (t.object.kind == Term::Kind::BNODE) {
				triple.Push("_:" + t.object.value);
			} else {
				triple.Push(t.object.lang.empty() ? t.object.value : t.object.value + "@" + t.object.lang);
			}
			triples.Push(triple);
		}
		out.Set("triples", triples);
		out.Set("truncated", r.truncated);
		return out;
	}
	out.Set("columns", Json::ArrayOf(r.variables));
	Json rows = Json::Array();
	for (auto &row : r.rows) {
		Json values = Json::Array();
		for (auto &b : row) {
			values.Push(Value(b));
		}
		rows.Push(values);
	}
	out.Set("rows", rows);
	out.Set("truncated", r.truncated);
	return out;
}

const std::map<std::string, McpServer::OntologyEntry> &McpServer::Ontology() {
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (ontology_loaded_) {
			return ontology_;
		}
	}
	std::map<std::string, OntologyEntry> info;
	if (!ontology_files_.empty()) {
		std::string list;
		for (auto &f : ontology_files_) {
			list += (list.empty() ? "" : ", ") + SqlLiteral(f);
		}
		duckdb::Connection con(db_);
		auto result = con.Query("SELECT subject, predicate, object FROM read_rdf([" + list +
		                        "], prefix_expansion = true)");
		if (result->HasError()) {
			throw ToolError("reading the ontology: " + result->GetError());
		}
		while (auto chunk = result->Fetch()) {
			if (chunk->size() == 0) {
				break;
			}
			for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
				auto s = chunk->GetValue(0, r).ToString();
				auto p = chunk->GetValue(1, r).ToString();
				auto o = chunk->GetValue(2, r).ToString();
				auto &entry = info[s];
				if (p == RDFS + "label" || p == SKOS + "prefLabel") {
					entry.labels.push_back(o);
				} else if (p == RDFS + "comment" || p == SKOS + "definition") {
					entry.comments.push_back(o);
				} else if (p == RDFS + "subClassOf" && o.rfind("_:", 0) != 0) {
					entry.superclasses.push_back(o);
				}
			}
		}
	}
	std::lock_guard<std::mutex> lock(mutex_);
	if (!ontology_loaded_) {
		ontology_ = std::move(info);
		ontology_loaded_ = true;
	}
	return ontology_;
}

const std::vector<std::pair<std::string, int64_t>> &McpServer::ClassCounts() {
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (counts_loaded_) {
			return counts_;
		}
	}
	auto r = Run("SELECT ?c (COUNT(DISTINCT ?x) AS ?n) WHERE { ?x a ?c } GROUP BY ?c ORDER BY ?c",
	             config_.mcp_max_rows);
	std::vector<std::pair<std::string, int64_t>> counts;
	for (auto &row : r.Get("rows")->Items()) {
		auto c = row.Items()[0].AsString();
		auto n = row.Items()[1];
		counts.emplace_back(c.substr(1, c.size() - 2), n.IsNumber() ? n.AsInt() : std::stoll(n.AsString()));
	}
	std::lock_guard<std::mutex> lock(mutex_);
	if (!counts_loaded_) {
		counts_ = std::move(counts);
		counts_loaded_ = true;
	}
	return counts_;
}

int64_t McpServer::InstancesOf(const std::string &cls) {
	for (auto &entry : ClassCounts()) {
		if (entry.first == cls) {
			return entry.second;
		}
	}
	return 0;
}

std::string McpServer::ResolveClass(const std::string &raw) {
	auto name = Trim(raw);
	if (name.find(':') != std::string::npos && name[0] != '<' && name.find('/') == std::string::npos &&
	    name.find('#') == std::string::npos) {
		throw ToolError("use a full IRI or a bare class name, not a prefixed name");
	}
	if (name[0] == '<' || name.find("://") != std::string::npos || name.rfind("urn:", 0) == 0) {
		return CheckIri(name);
	}
	std::vector<std::string> matches;
	for (auto &entry : ClassCounts()) {
		if (LocalName(entry.first) == name) {
			matches.push_back(entry.first);
		}
	}
	if (matches.size() == 1) {
		return matches[0];
	}
	if (matches.empty()) {
		throw ToolError("no class named '" + name + "'; list_classes shows them all");
	}
	std::string all;
	for (auto &m : matches) {
		all += (all.empty() ? "" : ", ") + m;
	}
	throw ToolError("'" + name + "' is ambiguous: " + all);
}

std::map<std::string, Json> McpServer::ClassesOf(const std::vector<std::string> &entities) {
	std::set<std::string> iris;
	for (auto &e : entities) {
		if (e.size() > 2 && e.front() == '<' && e.back() == '>' &&
		    std::regex_match(e.substr(1, e.size() - 2), IriPattern())) {
			iris.insert(e);
		}
	}
	std::map<std::string, Json> out;
	if (iris.empty()) {
		return out;
	}
	std::string values;
	for (auto &i : iris) {
		values += i + " ";
	}
	auto r = Run("SELECT ?x ?c WHERE { VALUES ?x { " + values + "} ?x a ?c } ORDER BY ?x ?c", config_.mcp_max_rows);
	for (auto &row : r.Get("rows")->Items()) {
		auto x = row.Items()[0].AsString();
		if (!out.count(x)) {
			out[x] = Json::Array();
		}
		out[x].Push(row.Items()[1]);
	}
	return out;
}

// ---------------------------------------------------------------------------
// Tools

Json McpServer::ListClasses() {
	auto &onto = Ontology();
	Json classes = Json::Array();
	for (auto &entry : ClassCounts()) {
		Json c = Json::Object();
		c.Set("class", Bracket(entry.first));
		c.Set("instances", entry.second);
		auto it = onto.find(entry.first);
		if (it != onto.end()) {
			if (!it->second.labels.empty()) {
				c.Set("label", it->second.labels[0]);
			}
			if (!it->second.comments.empty()) {
				c.Set("comment", it->second.comments[0]);
			}
			if (!it->second.superclasses.empty()) {
				Json supers = Json::Array();
				for (auto &s : it->second.superclasses) {
					supers.Push(Bracket(s));
				}
				c.Set("superclasses", supers);
			}
		}
		classes.Push(c);
	}
	Json out = Json::Object();
	out.Set("classes", classes);
	return out;
}

Json McpServer::DescribeClass(const std::string &raw) {
	auto c = ResolveClass(raw);
	{
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = class_cache_.find(c);
		if (it != class_cache_.end()) {
			return it->second;
		}
	}
	auto &onto = Ontology();
	auto props = Run("SELECT ?p (COUNT(?o) AS ?n) WHERE { ?x a <" + c + "> ; ?p ?o } GROUP BY ?p ORDER BY ?p", 1000);
	Json properties = Json::Array();
	for (auto &row : props.Get("rows")->Items()) {
		auto p = row.Items()[0].AsString();
		if (p == Bracket(RDF_TYPE)) {
			continue; // the class itself and its superclasses: already above
		}
		// One example per property, from its own query: SAMPLE() would lose whether it is an IRI.
		auto ex = Run("SELECT ?o WHERE { ?x a <" + c + "> ; " + p + " ?o }", 1);
		Json entry = Json::Object();
		entry.Set("property", p);
		entry.Set("statements", row.Items()[1]);
		entry.Set("example", ex.Get("rows")->Size() ? ex.Get("rows")->Items()[0].Items()[0] : Json());
		auto pinfo = onto.find(p.substr(1, p.size() - 2));
		if (pinfo != onto.end()) {
			if (!pinfo->second.labels.empty()) {
				entry.Set("label", pinfo->second.labels[0]);
			}
			if (!pinfo->second.comments.empty()) {
				entry.Set("comment", pinfo->second.comments[0]);
			}
		}
		// A literal property with only a few different values (a status, a country code): list them.
		auto few = Run("SELECT DISTINCT ?o WHERE { ?x a <" + c + "> ; " + p + " ?o FILTER(isLiteral(?o)) } ORDER BY ?o",
		               MAX_LISTED_VALUES);
		if (few.Get("rows")->Size() && !few.Get("truncated")->AsBool()) {
			Json values = Json::Array();
			for (auto &v : few.Get("rows")->Items()) {
				values.Push(v.Items()[0]);
			}
			entry.Set("all_values", values);
		}
		properties.Push(entry);
	}
	Json out = Json::Object();
	out.Set("class", Bracket(c));
	auto info = onto.find(c);
	out.Set("label", info != onto.end() && !info->second.labels.empty() ? Json(info->second.labels[0]) : Json());
	out.Set("comment", info != onto.end() && !info->second.comments.empty() ? Json(info->second.comments[0]) : Json());
	Json supers = Json::Array();
	if (info != onto.end()) {
		for (auto &s : info->second.superclasses) {
			supers.Push(Bracket(s));
		}
	}
	out.Set("superclasses", supers);
	std::vector<std::string> subs;
	for (auto &entry : onto) {
		auto &sc = entry.second.superclasses;
		if (std::find(sc.begin(), sc.end(), c) != sc.end()) {
			subs.push_back(Bracket(entry.first));
		}
	}
	std::sort(subs.begin(), subs.end());
	out.Set("subclasses", Json::ArrayOf(subs));
	out.Set("instances", InstancesOf(c));
	out.Set("properties", properties);
	std::lock_guard<std::mutex> lock(mutex_);
	class_cache_[c] = out;
	return out;
}

Json McpServer::FindEntity(const std::string &raw, const std::string &cls, int64_t limit) {
	auto text = Trim(raw);
	if (text.empty()) {
		throw ToolError("give some text to search for");
	}
	auto wanted = LowerAscii(text);
	auto needle = "\"" + EscapeLiteral(wanted) + "\"";
	auto typed = cls.empty() ? "" : "?x a <" + ResolveClass(cls) + "> . ";
	auto r = Run("SELECT DISTINCT ?x ?p ?text WHERE { " + typed + "?x ?p ?text FILTER(isLiteral(?text) && "
	             "CONTAINS(LCASE(STR(?text)), " + needle + ")) } ORDER BY ?x",
	             limit);
	std::vector<Json> exact, other;
	std::vector<std::string> entities;
	for (auto &row : r.Get("rows")->Items()) {
		Json m = Json::Object();
		m.Set("entity", row.Items()[0]);
		m.Set("property", row.Items()[1]);
		m.Set("text", row.Items()[2]);
		auto t = row.Items()[2];
		bool is_exact = t.IsString() ? LowerAscii(t.AsString()) == wanted : LowerAscii(t.Dump()) == wanted;
		m.Set("exact", is_exact);
		if (row.Items()[0].IsString()) {
			entities.push_back(row.Items()[0].AsString());
		}
		(is_exact ? exact : other).push_back(m);
	}
	auto classes = ClassesOf(entities);
	Json matches = Json::Array();
	for (auto *group : {&exact, &other}) { // exact matches first; the rest keep their order
		for (auto &m : *group) {
			auto e = m.Get("entity");
			auto it = e && e->IsString() ? classes.find(e->AsString()) : classes.end();
			m.Set("classes", it != classes.end() ? it->second : Json::Array());
			matches.Push(m);
		}
	}
	Json out = Json::Object();
	out.Set("matches", matches);
	out.Set("truncated", *r.Get("truncated"));
	return out;
}

Json McpServer::DescribeEntity(const std::string &raw, int64_t limit) {
	std::string e;
	try {
		e = CheckIri(raw);
	} catch (std::runtime_error &ex) {
		throw ToolError(ex.what());
	}
	auto out_rows = Run("SELECT ?p ?o WHERE { <" + e + "> ?p ?o } ORDER BY ?p", limit);
	auto in_rows = Run("SELECT ?s ?p WHERE { ?s ?p <" + e + "> } ORDER BY ?p ?s", limit);
	Json properties = Json::Array();
	for (auto &row : out_rows.Get("rows")->Items()) {
		Json p = Json::Object();
		p.Set("property", row.Items()[0]);
		p.Set("value", row.Items()[1]);
		properties.Push(p);
	}
	Json referenced = Json::Array();
	for (auto &row : in_rows.Get("rows")->Items()) {
		Json p = Json::Object();
		p.Set("subject", row.Items()[0]);
		p.Set("property", row.Items()[1]);
		referenced.Push(p);
	}
	Json out = Json::Object();
	out.Set("entity", Bracket(e));
	out.Set("properties", properties);
	out.Set("referenced_by", referenced);
	out.Set("truncated", out_rows.Get("truncated")->AsBool() || in_rows.Get("truncated")->AsBool());
	return out;
}

Json McpServer::Sparql(const std::string &query, int64_t limit) {
	return Run(WithStandardPrefixes(query), limit);
}

Json McpServer::Template(const QueryTemplate &tmpl, const Json &args) {
	auto params = tmpl.params;
	// $company_id before $company.
	std::sort(params.begin(), params.end(),
	          [](const TemplateParam &a, const TemplateParam &b) { return a.name.size() > b.name.size(); });
	auto text = tmpl.query;
	for (auto &p : params) {
		auto v = args.Get(p.name);
		if (!v || v->IsNull()) {
			throw ToolError(p.name + ": missing");
		}
		std::string rendered;
		try {
			if (p.type == "string") {
				rendered = "\"" + EscapeLiteral(v->IsString() ? v->AsString() : v->Dump()) + "\"";
			} else if (p.type == "iri") {
				rendered = "<" + CheckIri(v->AsString()) + ">";
			} else if (p.type == "integer") {
				rendered = std::to_string(v->IsString() ? std::stoll(v->AsString()) : v->AsInt());
			} else if (p.type == "decimal") {
				auto s = v->IsString() ? Trim(v->AsString()) : v->Dump();
				if (!std::regex_match(s, std::regex(R"([+-]?(\d+(\.\d*)?|\.\d+))"))) {
					throw std::runtime_error("not a decimal: " + s);
				}
				rendered = s;
			} else {
				rendered = (v->GetType() == Json::Type::BOOL ? v->AsBool() : v->AsString() == "true") ? "true" : "false";
			}
		} catch (std::exception &ex) {
			throw ToolError(p.name + ": " + ex.what());
		}
		for (size_t pos = text.find("$" + p.name); pos != std::string::npos;
		     pos = text.find("$" + p.name, pos + rendered.size())) {
			text.replace(pos, p.name.size() + 1, rendered);
		}
	}
	return Sparql(text, tmpl.limit);
}

// ---------------------------------------------------------------------------
// Protocol

Json McpServer::ToolList() const {
	Json tools = Json::Array();
	tools.Push(Tool("list_classes",
	                "The classes in the data (asserted and inferred from the ontology), with instance counts, labels, "
	                "comments and superclasses. Start here.",
	                Schema({}, {})));
	tools.Push(Tool("describe_class",
	                "One class: its label, comment, super- and subclasses, instance count, and every property its "
	                "instances have, with its comment (units, codes), how many statements use it, an example, and all "
	                "its values when there are only a few (a status or a country code). class_iri is a full IRI or a "
	                "bare name such as Employee.",
	                Schema({{"class_iri", "string", ""}}, {"class_iri"})));
	tools.Push(Tool("find_entity",
	                "Entities with a text value containing `text` (case-insensitive), optionally only instances of "
	                "class_iri. Use it to turn a name into an IRI. Matches whose whole text equals `text` come first, "
	                "marked \"exact\": true; the rest only contain it (\"Customer 4242\" also finds \"Customer 42420\"). "
	                "Each match lists the entity's classes, inferred ones included, to tell apart a product and an "
	                "order line with the same name.",
	                Schema({{"text", "string", ""}, {"class_iri", "string", ""}, {"limit", "integer", ""}}, {"text"})));
	tools.Push(Tool("describe_entity",
	                "Everything stated about one entity: its properties and values, and which entities refer to it.",
	                Schema({{"entity_iri", "string", ""}, {"limit", "integer", ""}}, {"entity_iri"})));
	tools.Push(Tool("sparql",
	                "Run a read-only SPARQL 1.1 SELECT, ASK or CONSTRUCT query. IRIs come back as <...>, integers and "
	                "doubles as numbers, decimals as exact text in canonical form (\"13742.39\", \"15076.0\"). At most "
	                "`limit` rows (the server caps it); `truncated` says whether the query had more rows than that.",
	                Schema({{"query", "string", ""}, {"limit", "integer", ""}}, {"query"})));
	for (auto &tmpl : config_.templates) {
		std::vector<std::tuple<std::string, std::string, std::string>> props;
		std::vector<std::string> required;
		std::string description = tmpl.description;
		for (auto &p : tmpl.params) {
			auto type = p.type == "integer" ? "integer" : p.type == "boolean" ? "boolean" : "string";
			props.emplace_back(p.name, type, p.description);
			required.push_back(p.name);
			description += "\n" + p.name + " (" + p.type + "): " + p.description;
		}
		tools.Push(Tool(tmpl.name, description, Schema(props, required)));
	}
	Json out = Json::Object();
	out.Set("tools", tools);
	return out;
}

Json McpServer::CallTool(const std::string &name, const Json &args) {
	if (name == "list_classes") {
		return ListClasses();
	}
	if (name == "describe_class") {
		return DescribeClass(StringArg(args, "class_iri"));
	}
	if (name == "find_entity") {
		return FindEntity(StringArg(args, "text"), StringArg(args, "class_iri", false), IntArg(args, "limit", 20));
	}
	if (name == "describe_entity") {
		return DescribeEntity(StringArg(args, "entity_iri"), IntArg(args, "limit", 100));
	}
	if (name == "sparql") {
		return Sparql(StringArg(args, "query"), IntArg(args, "limit", 100));
	}
	for (auto &tmpl : config_.templates) {
		if (tmpl.name == name) {
			return Template(tmpl, args);
		}
	}
	throw UnknownTool("unknown tool: " + name);
}

std::optional<Json> McpServer::Handle(const Json &message) {
	auto id = message.Get("id");
	auto method_node = message.Get("method");
	if (!id) {
		return std::nullopt; // a notification, such as notifications/initialized
	}
	Json reply = Json::Object();
	reply.Set("jsonrpc", "2.0");
	reply.Set("id", *id);
	auto error = [&](int code, const std::string &text) {
		Json e = Json::Object();
		e.Set("code", code);
		e.Set("message", text);
		reply.Set("error", e);
		return reply;
	};
	if (!method_node || !method_node->IsString()) {
		return error(-32600, "invalid request");
	}
	auto method = method_node->AsString();
	auto params_node = message.Get("params");
	Json params = params_node && params_node->IsObject() ? *params_node : Json::Object();

	if (method == "initialize") {
		Json result = Json::Object();
		auto version = params.Get("protocolVersion");
		result.Set("protocolVersion", version && version->IsString() ? version->AsString() : "2025-06-18");
		Json caps = Json::Object();
		Json tools = Json::Object();
		tools.Set("listChanged", false);
		caps.Set("tools", tools);
		if (!ontology_files_.empty()) {
			Json resources = Json::Object();
			resources.Set("listChanged", false);
			caps.Set("resources", resources);
		}
		result.Set("capabilities", caps);
		Json info = Json::Object();
		info.Set("name", "duckgraphlake");
		info.Set("version", "0.1.0");
		result.Set("serverInfo", info);
		result.Set("instructions", INSTRUCTIONS);
		reply.Set("result", result);
		return reply;
	}
	if (method == "ping") {
		reply.Set("result", Json::Object());
		return reply;
	}
	if (method == "tools/list") {
		reply.Set("result", ToolList());
		return reply;
	}
	if (method == "tools/call") {
		auto name = params.Get("name");
		if (!name || !name->IsString()) {
			return error(-32602, "tools/call needs a name");
		}
		auto args_node = params.Get("arguments");
		Json args = args_node && args_node->IsObject() ? *args_node : Json::Object();
		Json result = Json::Object();
		Json content = Json::Array();
		Json text = Json::Object();
		text.Set("type", "text");
		try {
			auto value = CallTool(name->AsString(), args);
			text.Set("text", value.Dump());
			content.Push(text);
			result.Set("content", content);
			result.Set("structuredContent", value);
			result.Set("isError", false);
		} catch (UnknownTool &ex) {
			return error(-32602, ex.what());
		} catch (std::exception &ex) {
			// Shown to the agent, so it can correct the call.
			text.Set("text", ex.what());
			content.Push(text);
			result.Set("content", content);
			result.Set("isError", true);
		}
		reply.Set("result", result);
		return reply;
	}
	if (method == "resources/list") {
		Json list = Json::Array();
		if (!ontology_files_.empty()) {
			Json r = Json::Object();
			r.Set("uri", "ontology://turtle");
			r.Set("name", "ontology");
			r.Set("mimeType", "text/turtle");
			r.Set("description", "The domain ontology the server reasons with");
			list.Push(r);
		}
		Json result = Json::Object();
		result.Set("resources", list);
		reply.Set("result", result);
		return reply;
	}
	if (method == "resources/read") {
		auto uri = params.Get("uri");
		if (!uri || !uri->IsString() || uri->AsString() != "ontology://turtle" || ontology_files_.empty()) {
			return error(-32602, "unknown resource");
		}
		std::string text;
		for (auto &f : ontology_files_) {
			text += (text.empty() ? "" : "\n") + ReadFile(f);
		}
		Json c = Json::Object();
		c.Set("uri", "ontology://turtle");
		c.Set("mimeType", "text/turtle");
		c.Set("text", text);
		Json result = Json::Object();
		result.Set("contents", Json::ArrayOf(std::vector<Json> {c}));
		reply.Set("result", result);
		return reply;
	}
	if (method == "resources/templates/list") {
		Json result = Json::Object();
		result.Set("resourceTemplates", Json::Array());
		reply.Set("result", result);
		return reply;
	}
	if (method == "prompts/list") {
		Json result = Json::Object();
		result.Set("prompts", Json::Array());
		reply.Set("result", result);
		return reply;
	}
	return error(-32601, "method not found: " + method);
}

} // namespace dgl
