#include "odata.hpp"

#include "log.hpp"
#include "setup.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <functional>
#include <regex>
#include <set>
#include <stdexcept>

namespace dgl {

namespace {

const std::string XSD = "http://www.w3.org/2001/XMLSchema#";
const char *NAMESPACE = "DuckGraphLake";

struct ODataError : std::runtime_error {
	ODataError(int status, const std::string &message) : std::runtime_error(message), status(status) {
	}
	int status;
};

std::string EdmType(const std::string &datatype) {
	if (datatype.rfind(XSD, 0) != 0) {
		return "Edm.String";
	}
	auto local = datatype.substr(XSD.size());
	if (local == "int" || local == "short" || local == "byte" || local == "unsignedShort" || local == "unsignedByte") {
		return "Edm.Int32";
	}
	if (local == "integer" || local == "long" || local == "nonNegativeInteger" || local == "positiveInteger" ||
	    local == "nonPositiveInteger" || local == "negativeInteger" || local == "unsignedInt" ||
	    local == "unsignedLong") {
		return "Edm.Int64";
	}
	if (local == "decimal") {
		return "Edm.Decimal";
	}
	if (local == "double" || local == "float") {
		return "Edm.Double";
	}
	if (local == "boolean") {
		return "Edm.Boolean";
	}
	if (local == "date") {
		return "Edm.Date";
	}
	if (local == "dateTime" || local == "dateTimeStamp") {
		return "Edm.DateTimeOffset";
	}
	if (local == "time") {
		return "Edm.TimeOfDay";
	}
	return "Edm.String";
}

// A valid OData identifier from an IRI's local name.
std::string Identifier(const std::string &iri) {
	auto pos = iri.find_last_of("#/:");
	auto local = pos == std::string::npos ? iri : iri.substr(pos + 1);
	std::string out;
	for (char c : local) {
		out.push_back(std::isalnum(static_cast<unsigned char>(c)) || c == '_' ? c : '_');
	}
	if (out.empty() || std::isdigit(static_cast<unsigned char>(out[0]))) {
		out = "_" + out;
	}
	return out;
}

std::string Unique(const std::string &name, std::set<std::string> &taken) {
	auto candidate = name;
	for (int i = 2; taken.count(candidate); i++) {
		candidate = name + "_" + std::to_string(i);
	}
	taken.insert(candidate);
	return candidate;
}

std::string Xml(const std::string &text) {
	std::string out;
	for (char c : text) {
		switch (c) {
		case '&':
			out += "&amp;";
			break;
		case '<':
			out += "&lt;";
			break;
		case '>':
			out += "&gt;";
			break;
		case '"':
			out += "&quot;";
			break;
		default:
			out.push_back(c);
		}
	}
	return out;
}

bool SafeIri(const std::string &iri) {
	static const std::regex pattern(R"(^[A-Za-z][A-Za-z0-9+.\-]*:[^\s<>"{}|\\^`]*$)");
	return std::regex_match(iri, pattern);
}

std::string IriRef(const std::string &iri) {
	if (!SafeIri(iri)) {
		throw ODataError(400, "not an IRI: " + iri);
	}
	return "<" + iri + ">";
}

std::string StringLiteral(const std::string &text) {
	return "\"" + EscapeLiteral(text) + "\"";
}

// A term as the JSON value of a property of the given Edm type.
Json JsonValue(const std::optional<Term> &term, const std::string &edm) {
	if (!term) {
		return Json();
	}
	auto &v = term->value;
	if (edm == "Edm.Int32" || edm == "Edm.Int64") {
		try {
			size_t used = 0;
			auto n = std::stoll(v, &used);
			if (used == v.size()) {
				return Json(static_cast<int64_t>(n));
			}
		} catch (std::exception &) {
		}
		return Json(v);
	}
	if (edm == "Edm.Double" || edm == "Edm.Decimal") {
		try {
			size_t used = 0;
			auto d = std::stod(v, &used);
			if (used == v.size() && std::isfinite(d)) {
				return Json(d);
			}
		} catch (std::exception &) {
		}
		return Json(v); // INF, -INF, NaN: OData writes these as strings
	}
	if (edm == "Edm.Boolean") {
		return Json(v == "true" || v == "1");
	}
	if (edm == "Edm.DateTimeOffset") {
		auto t = v;
		auto tpos = t.find('T');
		if (tpos == std::string::npos) {
			auto space = t.find(' ');
			if (space != std::string::npos) {
				t[space] = 'T';
			}
		}
		bool zoned = t.size() > 10 && (t.back() == 'Z' || std::regex_search(t, std::regex(R"([+-]\d{2}:?\d{2}$)")));
		return Json(zoned ? t : t + "Z");
	}
	return Json(term->kind == Term::Kind::BNODE ? "_:" + v : v);
}

// ---------------------------------------------------------------------------
// $filter

struct Token {
	enum class Kind { IDENT, STRING, NUMBER, DATE, DATETIME, LPAREN, RPAREN, COMMA, SLASH, END };
	Kind kind;
	std::string text;
};

std::vector<Token> Tokenize(const std::string &s) {
	static const std::regex datetime(R"(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:\d{2})?)");
	static const std::regex date(R"(^\d{4}-\d{2}-\d{2})");
	static const std::regex number(R"(^-?\d+(\.\d+)?([eE][+-]?\d+)?[dDmMfFlL]?)");
	std::vector<Token> out;
	size_t i = 0;
	while (i < s.size()) {
		char c = s[i];
		if (std::isspace(static_cast<unsigned char>(c))) {
			i++;
			continue;
		}
		if (c == '(' || c == ')' || c == ',' || c == '/') {
			out.push_back({c == '('   ? Token::Kind::LPAREN
			               : c == ')' ? Token::Kind::RPAREN
			               : c == ',' ? Token::Kind::COMMA
			                          : Token::Kind::SLASH,
			               std::string(1, c)});
			i++;
			continue;
		}
		if (c == '\'') {
			std::string text;
			i++;
			for (;;) {
				if (i >= s.size()) {
					throw ODataError(400, "$filter: unterminated string");
				}
				if (s[i] == '\'') {
					if (i + 1 < s.size() && s[i + 1] == '\'') {
						text.push_back('\'');
						i += 2;
						continue;
					}
					i++;
					break;
				}
				text.push_back(s[i++]);
			}
			out.push_back({Token::Kind::STRING, text});
			continue;
		}
		auto rest = s.substr(i);
		std::smatch m;
		if (std::regex_search(rest, m, datetime)) {
			out.push_back({Token::Kind::DATETIME, m[0].str()});
			i += m[0].length();
			continue;
		}
		if (std::regex_search(rest, m, date)) {
			out.push_back({Token::Kind::DATE, m[0].str()});
			i += m[0].length();
			continue;
		}
		if (std::regex_search(rest, m, number)) {
			auto text = m[0].str();
			if (std::isalpha(static_cast<unsigned char>(text.back()))) {
				text.pop_back();
			}
			out.push_back({Token::Kind::NUMBER, text});
			i += m[0].length();
			continue;
		}
		if (std::isalpha(static_cast<unsigned char>(c)) || c == '_' || c == '$') {
			size_t j = i;
			while (j < s.size() && (std::isalnum(static_cast<unsigned char>(s[j])) || s[j] == '_' || s[j] == '$')) {
				j++;
			}
			out.push_back({Token::Kind::IDENT, s.substr(i, j - i)});
			i = j;
			continue;
		}
		throw ODataError(400, std::string("$filter: unexpected character '") + c + "'");
	}
	out.push_back({Token::Kind::END, ""});
	return out;
}

struct Operand {
	enum class Kind { ID, PROPERTY, STRING, OTHER, NUL };
	Kind kind = Kind::OTHER;
	std::string sparql;
	const ODataProperty *property = nullptr;
	std::string text; // a string literal's value
};

class FilterParser {
public:
	FilterParser(const ODataEntitySet &set, std::set<size_t> &used, const std::string &text,
	             const std::function<const ODataEntitySet *(const std::string &)> &find_set)
	    : set_(set), used_(used), tokens_(Tokenize(text)), find_set_(find_set) {
	}

	// Graph patterns the filter needs besides the entity's own properties (navigation paths).
	std::vector<std::string> patterns;

	std::string Parse() {
		auto e = Or();
		if (Peek().kind != Token::Kind::END) {
			throw ODataError(400, "$filter: unexpected '" + Peek().text + "'");
		}
		return e.sparql;
	}

private:
	const Token &Peek() const {
		return tokens_[pos_];
	}
	bool Keyword(const char *word) {
		if (Peek().kind == Token::Kind::IDENT && Peek().text == word) {
			pos_++;
			return true;
		}
		return false;
	}
	void Expect(Token::Kind kind, const char *what) {
		if (Peek().kind != kind) {
			throw ODataError(400, std::string("$filter: expected ") + what);
		}
		pos_++;
	}

	Operand Or() {
		auto left = And();
		while (Keyword("or")) {
			auto right = And();
			left = Bool("(" + left.sparql + " || " + right.sparql + ")");
		}
		return left;
	}
	Operand And() {
		auto left = Not();
		while (Keyword("and")) {
			auto right = Not();
			left = Bool("(" + left.sparql + " && " + right.sparql + ")");
		}
		return left;
	}
	Operand Not() {
		if (Keyword("not")) {
			return Bool("!(" + Not().sparql + ")");
		}
		return Comparison();
	}
	static Operand Bool(const std::string &sparql) {
		Operand o;
		o.sparql = sparql;
		return o;
	}

	// The operand as text: STR() of a term, a string literal as is.
	static std::string AsString(const Operand &o) {
		if (o.kind == Operand::Kind::ID || o.kind == Operand::Kind::PROPERTY) {
			return "STR(" + o.sparql + ")";
		}
		return o.sparql;
	}

	Operand Comparison() {
		auto left = Primary();
		static const std::map<std::string, std::string> ops {{"eq", "="},  {"ne", "!="}, {"gt", ">"},
		                                                     {"ge", ">="}, {"lt", "<"},  {"le", "<="}};
		if (Peek().kind != Token::Kind::IDENT || !ops.count(Peek().text)) {
			return left;
		}
		auto op_name = Peek().text;
		pos_++;
		auto op = ops.at(op_name);
		auto right = Primary();
		if (left.kind == Operand::Kind::NUL) {
			std::swap(left, right);
		}
		if (right.kind == Operand::Kind::NUL) {
			if ((left.kind != Operand::Kind::PROPERTY && left.kind != Operand::Kind::ID) || (op != "=" && op != "!=")) {
				throw ODataError(400, "$filter: only a property can be compared with null, with eq or ne");
			}
			return Bool(std::string(op == "=" ? "!" : "") + "BOUND(" + left.sparql + ")");
		}
		if (right.kind == Operand::Kind::ID || right.kind == Operand::Kind::PROPERTY) {
			std::swap(left, right);
			static const std::map<std::string, std::string> flipped {{"=", "="},   {"!=", "!="}, {">", "<"},
			                                                         {">=", "<="}, {"<", ">"},   {"<=", ">="}};
			op = flipped.at(op);
		}
		if (left.kind == Operand::Kind::ID && right.kind == Operand::Kind::STRING && (op == "=" || op == "!=")) {
			return Bool(left.sparql + " " + op + " " + IriRef(right.text)); // a key lookup, not a string compare
		}
		bool textual =
		    left.kind == Operand::Kind::ID || (left.kind == Operand::Kind::PROPERTY &&
		                                       (left.property->iri_valued || left.property->edm_type == "Edm.String"));
		if (textual && right.kind == Operand::Kind::STRING) {
			return Bool(AsString(left) + " " + op + " " + right.sparql);
		}
		return Bool(left.sparql + " " + op + " " + right.sparql);
	}

	Operand Function(const std::string &name) {
		std::vector<Operand> args;
		Expect(Token::Kind::LPAREN, "(");
		if (Peek().kind != Token::Kind::RPAREN) {
			args.push_back(Or());
			while (Peek().kind == Token::Kind::COMMA) {
				pos_++;
				args.push_back(Or());
			}
		}
		Expect(Token::Kind::RPAREN, ")");
		auto need = [&](size_t n) {
			if (args.size() != n) {
				throw ODataError(400, "$filter: " + name + " takes " + std::to_string(n) + " argument(s)");
			}
		};
		Operand o;
		if (name == "contains" || name == "startswith" || name == "endswith") {
			need(2);
			static const std::map<std::string, std::string> fn {
			    {"contains", "CONTAINS"}, {"startswith", "STRSTARTS"}, {"endswith", "STRENDS"}};
			o.sparql = fn.at(name) + "(" + AsString(args[0]) + ", " + AsString(args[1]) + ")";
		} else if (name == "tolower" || name == "toupper") {
			need(1);
			o.sparql = std::string(name == "tolower" ? "LCASE" : "UCASE") + "(" + AsString(args[0]) + ")";
		} else if (name == "length") {
			need(1);
			o.sparql = "STRLEN(" + AsString(args[0]) + ")";
		} else if (name == "trim") {
			need(1);
			o.sparql = "REPLACE(" + AsString(args[0]) + ", \"^\\\\s+|\\\\s+$\", \"\")";
		} else if (name == "concat") {
			need(2);
			o.sparql = "CONCAT(" + AsString(args[0]) + ", " + AsString(args[1]) + ")";
		} else if (name == "year" || name == "month" || name == "day") {
			need(1);
			std::string fn = name;
			std::transform(fn.begin(), fn.end(), fn.begin(), ::toupper);
			o.sparql = fn + "(" + args[0].sparql + ")";
		} else {
			throw ODataError(501, "$filter: function " + name + " is not supported");
		}
		return o;
	}

	Operand Primary() {
		auto token = Peek();
		Operand o;
		switch (token.kind) {
		case Token::Kind::LPAREN: {
			pos_++;
			auto inner = Or();
			Expect(Token::Kind::RPAREN, ")");
			inner.sparql = "(" + inner.sparql + ")";
			return inner;
		}
		case Token::Kind::STRING:
			pos_++;
			o.kind = Operand::Kind::STRING;
			o.text = token.text;
			o.sparql = StringLiteral(token.text);
			return o;
		case Token::Kind::NUMBER:
			pos_++;
			o.sparql = token.text;
			return o;
		case Token::Kind::DATE:
			pos_++;
			o.sparql = "\"" + token.text + "\"^^<" + XSD + "date>";
			return o;
		case Token::Kind::DATETIME: {
			pos_++;
			auto t = token.text;
			if (!t.empty() && t.back() == 'Z') {
				t.pop_back(); // the data's timestamps carry no zone; compare them as given
			}
			o.sparql = "\"" + t + "\"^^<" + XSD + "dateTime>";
			return o;
		}
		case Token::Kind::IDENT: {
			pos_++;
			if (token.text == "null") {
				o.kind = Operand::Kind::NUL;
				return o;
			}
			if (token.text == "true" || token.text == "false") {
				o.sparql = token.text;
				return o;
			}
			if (Peek().kind == Token::Kind::LPAREN) {
				return Function(token.text);
			}
			if (token.text == "ID") {
				o.kind = Operand::Kind::ID;
				o.sparql = "?id";
				return o;
			}
			for (size_t k = 0; k < set_.navigations.size(); k++) {
				auto &nav = set_.navigations[k];
				if (nav.name != token.text) {
					continue;
				}
				// nav (eq null, eq '<iri>'), nav/ID, or nav/property: one level of navigation.
				auto var = "?f" + std::to_string(k);
				AddPattern("OPTIONAL { ?id <" + nav.predicate + "> " + var + " }");
				o.kind = Operand::Kind::ID;
				o.sparql = var;
				if (Peek().kind != Token::Kind::SLASH) {
					return o;
				}
				pos_++;
				if (Peek().kind != Token::Kind::IDENT) {
					throw ODataError(400, "$filter: expected a property after " + nav.name + "/");
				}
				auto member = tokens_[pos_++].text;
				if (member == "ID") {
					return o;
				}
				auto target = find_set_(nav.target);
				for (size_t j = 0; target && j < target->properties.size(); j++) {
					auto &p = target->properties[j];
					if (p.name == member) {
						auto pvar = var + "_" + std::to_string(j);
						AddPattern("OPTIONAL { ?id <" + nav.predicate + "> " + var + " . " + var + " <" + p.predicate +
						           "> " + pvar + " }");
						o.kind = Operand::Kind::PROPERTY;
						o.property = &p;
						o.sparql = pvar;
						return o;
					}
				}
				throw ODataError(400, "$filter: " + nav.target + " has no property " + member);
			}
			for (size_t i = 0; i < set_.properties.size(); i++) {
				if (set_.properties[i].name == token.text) {
					used_.insert(i);
					o.kind = Operand::Kind::PROPERTY;
					o.property = &set_.properties[i];
					o.sparql = "?p" + std::to_string(i);
					return o;
				}
			}
			throw ODataError(400, "$filter: " + set_.name + " has no property " + token.text);
		}
		default:
			throw ODataError(400, "$filter: unexpected '" + token.text + "'");
		}
	}

	void AddPattern(const std::string &pattern) {
		if (std::find(patterns.begin(), patterns.end(), pattern) == patterns.end()) {
			patterns.push_back(pattern);
		}
	}

	const ODataEntitySet &set_;
	std::set<size_t> &used_;
	std::vector<Token> tokens_;
	std::function<const ODataEntitySet *(const std::string &)> find_set_;
	size_t pos_ = 0;
};

std::vector<std::string> SplitList(const std::string &text) {
	std::vector<std::string> out;
	std::string current;
	int depth = 0;
	for (char c : text) {
		if (c == '(') {
			depth++;
		} else if (c == ')') {
			depth--;
		}
		if (c == ',' && depth == 0) {
			out.push_back(current);
			current.clear();
		} else {
			current.push_back(c);
		}
	}
	out.push_back(current);
	for (auto &s : out) {
		auto b = s.find_first_not_of(' ');
		auto e = s.find_last_not_of(' ');
		s = b == std::string::npos ? "" : s.substr(b, e - b + 1);
	}
	out.erase(std::remove(out.begin(), out.end(), ""), out.end());
	return out;
}

std::string PercentEncode(const std::string &text) {
	static const char *hex = "0123456789ABCDEF";
	std::string out;
	for (unsigned char c : text) {
		if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' || c == '$') {
			out.push_back(static_cast<char>(c));
		} else {
			out.push_back('%');
			out.push_back(hex[c >> 4]);
			out.push_back(hex[c & 15]);
		}
	}
	return out;
}

std::string Param(const ODataRequest &request, const std::string &name) {
	auto it = request.query.find(name);
	return it == request.query.end() ? "" : it->second;
}

int64_t IntParam(const ODataRequest &request, const std::string &name, int64_t fallback) {
	auto text = Param(request, name);
	if (text.empty()) {
		return fallback;
	}
	try {
		size_t used = 0;
		auto v = std::stoll(text, &used);
		if (used != text.size() || v < 0) {
			throw std::invalid_argument("");
		}
		return v;
	} catch (std::exception &) {
		throw ODataError(400, name + " must be a non-negative integer");
	}
}

ODataResponse JsonResponse(int status, const Json &body) {
	ODataResponse r;
	r.status = status;
	r.body = body.Dump();
	return r;
}

ODataResponse ErrorResponse(int status, const std::string &message) {
	Json error = Json::Object();
	Json inner = Json::Object();
	inner.Set("code", std::to_string(status));
	inner.Set("message", message);
	error.Set("error", inner);
	ODataResponse r = JsonResponse(status, error);
	r.content_type = "application/json;charset=utf-8";
	return r;
}

} // namespace

// ---------------------------------------------------------------------------
// The model

ODataService::ODataService(duckdb::DuckDB &db, const SparqlExecutor &executor, int64_t max_page)
    : db_(db), executor_(executor), max_page_(max_page) {
}

void ODataService::Load(const std::string &mapping, const std::vector<std::string> &ontology_files) {
	duckdb::Connection con(db_);
	// rdfs:subClassOf, transitively: superclasses[c] holds every class c is a subclass of.
	std::map<std::string, std::set<std::string>> superclasses;
	if (!ontology_files.empty()) {
		std::string list;
		for (auto &f : ontology_files) {
			list += (list.empty() ? "" : ", ") + SqlLiteral(f);
		}
		auto sub =
		    con.Query("SELECT subject, object FROM read_rdf([" + list +
		              "], prefix_expansion = true) WHERE predicate = "
		              "'http://www.w3.org/2000/01/rdf-schema#subClassOf' AND object NOT LIKE '\\_:%' ESCAPE '\\'");
		if (!sub->HasError()) {
			while (auto chunk = sub->Fetch()) {
				if (chunk->size() == 0) {
					break;
				}
				for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
					superclasses[chunk->GetValue(0, r).ToString()].insert(chunk->GetValue(1, r).ToString());
				}
			}
			for (bool changed = true; changed;) {
				changed = false;
				for (auto &entry : superclasses) {
					std::set<std::string> add;
					for (auto &parent : entry.second) {
						auto it = superclasses.find(parent);
						if (it != superclasses.end()) {
							add.insert(it->second.begin(), it->second.end());
						}
					}
					for (auto &a : add) {
						changed = entry.second.insert(a).second || changed;
					}
				}
			}
		}
	}
	auto result = con.Query("SELECT class, predicate, object_kind, datatype, target_class FROM rdf_mapping_schema(" +
	                        SqlLiteral(mapping) + ") WHERE class IS NOT NULL ORDER BY class, predicate");
	if (result->HasError()) {
		throw std::runtime_error("reading the mapping for OData: " + result->GetError());
	}
	struct PredicateInfo {
		std::set<std::string> kinds, datatypes, targets;
	};
	std::map<std::string, std::map<std::string, PredicateInfo>> classes;
	while (auto chunk = result->Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
			auto cls = chunk->GetValue(0, r).ToString();
			auto &preds = classes[cls];
			auto predicate = chunk->GetValue(1, r);
			if (predicate.IsNull()) {
				continue;
			}
			auto &info = preds[predicate.ToString()];
			auto kind = chunk->GetValue(2, r);
			auto datatype = chunk->GetValue(3, r);
			auto target = chunk->GetValue(4, r);
			if (!kind.IsNull()) {
				info.kinds.insert(kind.ToString());
			}
			if (!datatype.IsNull()) {
				info.datatypes.insert(datatype.ToString());
			}
			if (!target.IsNull()) {
				info.targets.insert(target.ToString());
			}
		}
	}

	// A subclass inherits its superclasses' properties: every Bike is a Product, so it has a name.
	auto own = classes;
	for (auto &entry : classes) {
		auto it = superclasses.find(entry.first);
		if (it == superclasses.end()) {
			continue;
		}
		for (auto &super : it->second) {
			auto from = own.find(super);
			if (from == own.end()) {
				continue;
			}
			for (auto &pred : from->second) {
				auto &info = entry.second[pred.first];
				info.kinds.insert(pred.second.kinds.begin(), pred.second.kinds.end());
				info.datatypes.insert(pred.second.datatypes.begin(), pred.second.datatypes.end());
				info.targets.insert(pred.second.targets.begin(), pred.second.targets.end());
			}
		}
	}

	std::vector<ODataEntitySet> loaded;
	std::set<std::string> set_names;
	std::map<std::string, std::string> set_of_class;
	for (auto &entry : classes) {
		ODataEntitySet set;
		set.class_iri = entry.first;
		set.name = Unique(Identifier(entry.first), set_names);
		set_of_class[entry.first] = set.name;
		loaded.push_back(set);
	}
	// An OData navigation property has one target type. A reference that can point to several
	// classes (several triples maps share the object's IRI template) navigates to the class that
	// covers them all, if one does (a common superclass: ofProduct -> Product); otherwise to the
	// most specific candidate (not a superclass of another), then the one with the most properties,
	// then the first by name.
	auto richness = [&](const std::string &cls) {
		return classes.count(cls) ? classes.at(cls).size() : 0;
	};
	auto is_sub = [&](const std::string &sub, const std::string &super) {
		auto it = superclasses.find(sub);
		return it != superclasses.end() && it->second.count(super) > 0;
	};
	auto better = [&](const std::string &a, const std::string &b) {
		return b.empty() || richness(a) > richness(b) || (richness(a) == richness(b) && a < b);
	};
	auto choose_target = [&](const std::set<std::string> &all) {
		std::set<std::string> candidates;
		for (auto &t : all) {
			if (set_of_class.count(t)) {
				candidates.insert(t);
			}
		}
		std::string chosen;
		for (auto &c : candidates) {
			bool covers = true;
			for (auto &other : candidates) {
				covers = covers && (other == c || is_sub(other, c));
			}
			if (covers && better(c, chosen)) {
				chosen = c;
			}
		}
		if (!chosen.empty()) {
			return chosen;
		}
		for (auto &c : candidates) {
			bool super_of_another = false;
			for (auto &other : candidates) {
				super_of_another = super_of_another || (other != c && is_sub(other, c));
			}
			if (!super_of_another && better(c, chosen)) {
				chosen = c;
			}
		}
		return chosen;
	};
	for (auto &set : loaded) {
		std::set<std::string> names {"ID"};
		for (auto &entry : classes[set.class_iri]) {
			auto &info = entry.second;
			bool literal = info.kinds.count("literal") > 0;
			bool iri = info.kinds.count("iri") > 0 || info.kinds.count("bnode") > 0;
			auto target = choose_target(info.targets);
			if (iri && !literal && !target.empty()) {
				ODataNavigation nav;
				nav.name = Unique(Identifier(entry.first), names);
				nav.predicate = entry.first;
				nav.target = set_of_class[target];
				set.navigations.push_back(nav);
				continue;
			}
			ODataProperty prop;
			prop.name = Unique(Identifier(entry.first), names);
			prop.predicate = entry.first;
			prop.iri_valued = iri && !literal;
			prop.edm_type =
			    literal && !iri && info.datatypes.size() == 1 ? EdmType(*info.datatypes.begin()) : "Edm.String";
			set.properties.push_back(prop);
		}
	}
	std::unique_lock<std::shared_mutex> lock(mutex_);
	sets_ = std::move(loaded);
}

void ODataService::Clear() {
	std::unique_lock<std::shared_mutex> lock(mutex_);
	sets_.clear();
}

const ODataEntitySet *ODataService::FindSet(const std::string &name) const {
	for (auto &set : sets_) {
		if (set.name == name) {
			return &set;
		}
	}
	return nullptr;
}

std::string ODataService::Metadata() const {
	std::string ns = NAMESPACE;
	std::string out = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
	                  "<edmx:Edmx Version=\"4.0\" xmlns:edmx=\"http://docs.oasis-open.org/odata/ns/edmx\">\n"
	                  "  <edmx:Reference Uri=\"https://oasis-tcs.github.io/odata-vocabularies/vocabularies/"
	                  "Org.OData.Core.V1.xml\">\n"
	                  "    <edmx:Include Namespace=\"Org.OData.Core.V1\" Alias=\"Core\"/>\n"
	                  "  </edmx:Reference>\n"
	                  "  <edmx:Reference Uri=\"https://oasis-tcs.github.io/odata-vocabularies/vocabularies/"
	                  "Org.OData.Capabilities.V1.xml\">\n"
	                  "    <edmx:Include Namespace=\"Org.OData.Capabilities.V1\" Alias=\"Capabilities\"/>\n"
	                  "  </edmx:Reference>\n"
	                  "  <edmx:DataServices>\n"
	                  "    <Schema Namespace=\"" +
	                  ns + "\" xmlns=\"http://docs.oasis-open.org/odata/ns/edm\">\n";
	for (auto &set : sets_) {
		out += "      <EntityType Name=\"" + set.name + "\">\n";
		out += "        <Key><PropertyRef Name=\"ID\"/></Key>\n";
		out += "        <Annotation Term=\"Core.Description\" String=\"" + Xml(set.class_iri) + "\"/>\n";
		out += "        <Property Name=\"ID\" Type=\"Edm.String\" Nullable=\"false\"/>\n";
		for (auto &p : set.properties) {
			out += "        <Property Name=\"" + p.name + "\" Type=\"" + p.edm_type + "\">\n";
			out += "          <Annotation Term=\"Core.Description\" String=\"" + Xml(p.predicate) + "\"/>\n";
			out += "        </Property>\n";
		}
		for (auto &n : set.navigations) {
			out += "        <NavigationProperty Name=\"" + n.name + "\" Type=\"" + ns + "." + n.target + "\">\n";
			out += "          <Annotation Term=\"Core.Description\" String=\"" + Xml(n.predicate) + "\"/>\n";
			out += "        </NavigationProperty>\n";
		}
		out += "      </EntityType>\n";
	}
	out += "      <EntityContainer Name=\"Container\">\n";
	for (auto &set : sets_) {
		out += "        <EntitySet Name=\"" + set.name + "\" EntityType=\"" + ns + "." + set.name + "\">\n";
		// Read-only, and what queries it answers (Mendix reads these for external entities).
		for (auto &restriction :
		     std::vector<std::pair<std::string, std::string>> {{"InsertRestrictions", "Insertable"},
		                                                       {"UpdateRestrictions", "Updatable"},
		                                                       {"DeleteRestrictions", "Deletable"}}) {
			out += "          <Annotation Term=\"Capabilities." + restriction.first +
			       "\"><Record><PropertyValue Property=\"" + restriction.second +
			       "\" Bool=\"false\"/></Record></Annotation>\n";
		}
		out += "          <Annotation Term=\"Capabilities.CountRestrictions\"><Record><PropertyValue "
		       "Property=\"Countable\" Bool=\"true\"/></Record></Annotation>\n";
		out += "          <Annotation Term=\"Capabilities.TopSupported\" Bool=\"true\"/>\n";
		out += "          <Annotation Term=\"Capabilities.SkipSupported\" Bool=\"true\"/>\n";
		out += "          <Annotation Term=\"Capabilities.FilterRestrictions\"><Record><PropertyValue "
		       "Property=\"Filterable\" Bool=\"true\"/></Record></Annotation>\n";
		out += "          <Annotation Term=\"Capabilities.SortRestrictions\"><Record><PropertyValue "
		       "Property=\"Sortable\" Bool=\"true\"/></Record></Annotation>\n";
		for (auto &n : set.navigations) {
			out += "          <NavigationPropertyBinding Path=\"" + n.name + "\" Target=\"" + n.target + "\"/>\n";
		}
		out += "        </EntitySet>\n";
	}
	out += "      </EntityContainer>\n    </Schema>\n  </edmx:DataServices>\n</edmx:Edmx>\n";
	return out;
}

// ---------------------------------------------------------------------------
// Requests

std::map<std::string, Json> ODataService::FetchByIds(const ODataEntitySet &set, const std::vector<std::string> &ids,
                                                     const std::vector<std::string> &select) const {
	std::map<std::string, Json> out;
	std::string values;
	for (auto &id : ids) {
		if (SafeIri(id)) {
			values += "<" + id + "> ";
		}
	}
	if (values.empty()) {
		return out;
	}
	std::vector<size_t> props;
	for (size_t i = 0; i < set.properties.size(); i++) {
		if (select.empty() || std::find(select.begin(), select.end(), set.properties[i].name) != select.end()) {
			props.push_back(i);
		}
	}
	std::string vars = "?id";
	std::string where = "VALUES ?id { " + values + "} ?id a <" + set.class_iri + "> .";
	for (auto i : props) {
		vars += " ?p" + std::to_string(i);
		where += " OPTIONAL { ?id <" + set.properties[i].predicate + "> ?p" + std::to_string(i) + " }";
	}
	auto result = executor_.Execute("SELECT " + vars + " WHERE { " + where + " }", 0);
	for (auto &row : result.rows) {
		if (!row[0] || out.count(row[0]->value)) {
			continue;
		}
		Json entity = Json::Object();
		entity.Set("ID", row[0]->value);
		for (size_t c = 0; c < props.size(); c++) {
			auto &p = set.properties[props[c]];
			entity.Set(p.name, JsonValue(row[c + 1], p.edm_type));
		}
		out[row[0]->value] = entity;
	}
	return out;
}

ODataResponse ODataService::Collection(const ODataEntitySet &set, const ODataRequest &request, const std::string &key,
                                       bool count_only) const {
	std::set<size_t> used;
	std::string filter;
	std::vector<std::string> filter_patterns;
	auto filter_text = Param(request, "$filter");
	if (!filter_text.empty()) {
		FilterParser parser(set, used, filter_text, [this](const std::string &name) { return FindSet(name); });
		filter = parser.Parse();
		filter_patterns = parser.patterns;
	}
	// $select: properties (and navigation properties, which are only returned with $expand).
	std::vector<size_t> selected;
	auto select_text = Param(request, "$select");
	if (select_text.empty() || select_text == "*") {
		for (size_t i = 0; i < set.properties.size(); i++) {
			selected.push_back(i);
		}
	} else {
		for (auto &name : SplitList(select_text)) {
			if (name == "ID") {
				continue;
			}
			bool found = false;
			for (size_t i = 0; i < set.properties.size(); i++) {
				if (set.properties[i].name == name) {
					selected.push_back(i);
					found = true;
				}
			}
			for (auto &n : set.navigations) {
				found = found || n.name == name;
			}
			if (!found) {
				throw ODataError(400, "$select: " + set.name + " has no property " + name);
			}
		}
	}
	// $orderby
	std::vector<std::string> order;
	for (auto &item : SplitList(Param(request, "$orderby"))) {
		auto space = item.find(' ');
		auto name = item.substr(0, space);
		auto dir = space == std::string::npos ? "asc" : item.substr(space + 1);
		std::string var;
		if (name == "ID") {
			var = "?id";
		}
		for (size_t i = 0; i < set.properties.size() && var.empty(); i++) {
			if (set.properties[i].name == name) {
				var = "?p" + std::to_string(i);
				used.insert(i);
			}
		}
		if (var.empty()) {
			throw ODataError(400, "$orderby: " + set.name + " has no property " + name);
		}
		order.push_back(dir == "desc" ? "DESC(" + var + ")" : var);
	}
	// $expand: navigation properties to inline, each with an optional ($select=...).
	std::vector<const ODataNavigation *> expand;
	std::vector<std::vector<std::string>> expand_select;
	for (auto &item : SplitList(Param(request, "$expand"))) {
		auto paren = item.find('(');
		auto name = item.substr(0, paren);
		std::vector<std::string> sub_select;
		if (paren != std::string::npos && item.back() == ')') {
			auto options = item.substr(paren + 1, item.size() - paren - 2);
			auto pos = options.find("$select=");
			if (pos != std::string::npos) {
				auto list = options.substr(pos + 8);
				list = list.substr(0, list.find(';'));
				sub_select = SplitList(list);
			}
		}
		const ODataNavigation *found = nullptr;
		for (auto &n : set.navigations) {
			if (n.name == name) {
				found = &n;
			}
		}
		if (!found) {
			throw ODataError(400, "$expand: " + set.name + " has no navigation property " + name);
		}
		expand.push_back(found);
		expand_select.push_back(sub_select);
	}

	std::set<size_t> needed(used.begin(), used.end());
	needed.insert(selected.begin(), selected.end());
	std::string where;
	if (!key.empty()) {
		where += "VALUES ?id { " + IriRef(key) + " } ";
	}
	where += "?id a <" + set.class_iri + "> .";
	for (auto i : needed) {
		where += " OPTIONAL { ?id <" + set.properties[i].predicate + "> ?p" + std::to_string(i) + " }";
	}
	for (size_t e = 0; e < expand.size(); e++) {
		where += " OPTIONAL { ?id <" + expand[e]->predicate + "> ?n" + std::to_string(e) + " }";
	}
	for (auto &pattern : filter_patterns) {
		where += " " + pattern;
	}
	if (!filter.empty()) {
		where += " FILTER(" + filter + ")";
	}

	auto count_query = "SELECT (COUNT(DISTINCT ?id) AS ?n) WHERE { " + where + " }";
	auto run_count = [&]() -> int64_t {
		auto r = executor_.Execute(count_query, 0);
		return r.rows.empty() || !r.rows[0][0] ? 0 : std::stoll(r.rows[0][0]->value);
	};
	if (count_only) {
		ODataResponse r;
		r.content_type = "text/plain;charset=utf-8";
		r.body = std::to_string(run_count());
		return r;
	}

	auto top = IntParam(request, "$top", -1);
	auto skip = IntParam(request, "$skip", 0);
	int64_t page = top < 0 ? max_page_ : std::min(top, max_page_);
	std::string vars = "?id";
	for (auto i : needed) {
		vars += " ?p" + std::to_string(i);
	}
	for (size_t e = 0; e < expand.size(); e++) {
		vars += " ?n" + std::to_string(e);
	}
	std::string order_by = " ORDER BY";
	for (auto &o : order) {
		order_by += " " + o;
	}
	order_by += " ?id";
	auto query = "SELECT " + vars + " WHERE { " + where + " }" + order_by + " LIMIT " + std::to_string(page + 1) +
	             (skip ? " OFFSET " + std::to_string(skip) : "");
	auto result = executor_.Execute(query, 0);

	// Rows to entities: the first row per ID (a property with several values shows one).
	std::vector<std::string> var_names(result.variables.begin(), result.variables.end());
	auto column = [&](const std::string &name) {
		return static_cast<size_t>(std::find(var_names.begin(), var_names.end(), name) - var_names.begin());
	};
	std::vector<Json> entities;
	std::vector<std::vector<std::string>> targets(expand.size());
	std::vector<std::vector<std::string>> entity_targets;
	std::set<std::string> seen;
	bool more = false;
	for (auto &row : result.rows) {
		auto &id = row[column("id")];
		if (!id || seen.count(id->value)) {
			continue;
		}
		if (static_cast<int64_t>(entities.size()) == page) {
			more = true;
			break;
		}
		seen.insert(id->value);
		Json entity = Json::Object();
		entity.Set("ID", id->kind == Term::Kind::BNODE ? "_:" + id->value : id->value);
		for (auto i : selected) {
			auto &p = set.properties[i];
			entity.Set(p.name, JsonValue(row[column("p" + std::to_string(i))], p.edm_type));
		}
		std::vector<std::string> mine;
		for (size_t e = 0; e < expand.size(); e++) {
			auto &t = row[column("n" + std::to_string(e))];
			mine.push_back(t ? t->value : "");
			if (t) {
				targets[e].push_back(t->value);
			}
		}
		entity_targets.push_back(mine);
		entities.push_back(entity);
	}
	for (size_t e = 0; e < expand.size(); e++) {
		auto target_set = FindSet(expand[e]->target);
		auto fetched = FetchByIds(*target_set, targets[e], expand_select[e]);
		for (size_t k = 0; k < entities.size(); k++) {
			auto &t = entity_targets[k][e];
			auto it = t.empty() ? fetched.end() : fetched.find(t);
			// The link annotation tells clients that read the payload without the model (Olingo) that
			// the object beside it is an expanded navigation property, not a complex value.
			auto id = entities[k].Get("ID");
			if (id && id->IsString()) {
				std::string quoted;
				for (char c : id->AsString()) {
					quoted += c == '\'' ? std::string("''") : std::string(1, c);
				}
				entities[k].Set(expand[e]->name + "@odata.navigationLink",
				                set.name + "('" + PercentEncode(quoted) + "')/" + expand[e]->name);
			}
			entities[k].Set(expand[e]->name, it == fetched.end() ? Json() : it->second);
		}
	}

	auto context = request.base_url + "/$metadata#" + set.name;
	if (!key.empty()) {
		if (entities.empty()) {
			return ErrorResponse(404, set.name + " has no entity " + key);
		}
		Json entity = Json::Object();
		entity.Set("@odata.context", context + "/$entity");
		for (auto &m : entities[0].Members()) {
			entity.Set(m.first, m.second);
		}
		return JsonResponse(200, entity);
	}
	Json body = Json::Object();
	body.Set("@odata.context", context);
	if (Param(request, "$count") == "true") {
		body.Set("@odata.count", run_count());
	}
	Json value = Json::Array();
	for (auto &e : entities) {
		value.Push(e);
	}
	body.Set("value", value);
	if (more && top < 0) {
		// Server-driven paging: the same request from the next page on.
		std::string link = request.base_url + "/" + set.name + "?";
		for (auto &q : request.query) {
			if (q.first != "$skip") {
				link += PercentEncode(q.first) + "=" + PercentEncode(q.second) + "&";
			}
		}
		body.Set("@odata.nextLink", link + "$skip=" + std::to_string(skip + page));
	}
	return JsonResponse(200, body);
}

ODataResponse ODataService::Navigate(const ODataEntitySet &set, const std::string &key, const std::string &nav_name,
                                     const ODataRequest &request) const {
	const ODataNavigation *nav = nullptr;
	for (auto &n : set.navigations) {
		if (n.name == nav_name) {
			nav = &n;
		}
	}
	if (!nav) {
		return ErrorResponse(404, set.name + " has no navigation property " + nav_name);
	}
	auto result = executor_.Execute(
	    "SELECT ?t WHERE { " + IriRef(key) + " a <" + set.class_iri + "> ; <" + nav->predicate + "> ?t } LIMIT 1", 0);
	if (result.rows.empty() || !result.rows[0][0]) {
		ODataResponse r;
		r.status = 204; // no related entity
		return r;
	}
	auto target = FindSet(nav->target);
	auto fetched = FetchByIds(*target, {result.rows[0][0]->value}, {});
	if (fetched.empty()) {
		ODataResponse r;
		r.status = 204;
		return r;
	}
	Json entity = Json::Object();
	entity.Set("@odata.context", request.base_url + "/$metadata#" + target->name + "/$entity");
	for (auto &m : fetched.begin()->second.Members()) {
		entity.Set(m.first, m.second);
	}
	return JsonResponse(200, entity);
}

ODataResponse ODataService::Handle(const ODataRequest &request) const {
	std::shared_lock<std::shared_mutex> lock(mutex_);
	try {
		auto path = request.path;
		while (!path.empty() && path.back() == '/') {
			path.pop_back();
		}
		if (path.empty()) {
			Json body = Json::Object();
			body.Set("@odata.context", request.base_url + "/$metadata");
			Json value = Json::Array();
			for (auto &set : sets_) {
				Json entry = Json::Object();
				entry.Set("name", set.name);
				entry.Set("kind", "EntitySet");
				entry.Set("url", set.name);
				value.Push(entry);
			}
			body.Set("value", value);
			return JsonResponse(200, body);
		}
		if (path == "/$metadata") {
			ODataResponse r;
			r.content_type = "application/xml;charset=utf-8";
			r.body = Metadata();
			return r;
		}
		// /Set, /Set/$count, /Set('key'), /Set(ID='key'), /Set('key')/nav
		size_t pos = 1;
		while (pos < path.size() && path[pos] != '(' && path[pos] != '/') {
			pos++;
		}
		auto set = FindSet(path.substr(1, pos - 1));
		if (!set) {
			return ErrorResponse(404, "no entity set " + path.substr(1, pos - 1));
		}
		std::string key;
		if (pos < path.size() && path[pos] == '(') {
			auto close = pos + 1;
			bool quoted = false;
			for (; close < path.size(); close++) {
				if (path[close] == '\'') {
					quoted = !quoted;
				} else if (path[close] == ')' && !quoted) {
					break;
				}
			}
			if (close >= path.size()) {
				return ErrorResponse(400, "unterminated key");
			}
			auto inner = path.substr(pos + 1, close - pos - 1);
			if (inner.rfind("ID=", 0) == 0) {
				inner = inner.substr(3);
			}
			if (inner.size() < 2 || inner.front() != '\'' || inner.back() != '\'') {
				return ErrorResponse(400, "a key is a quoted IRI: " + set->name + "('http://...')");
			}
			for (size_t i = 1; i + 1 < inner.size(); i++) {
				key.push_back(inner[i]);
				if (inner[i] == '\'' && i + 2 < inner.size() && inner[i + 1] == '\'') {
					i++;
				}
			}
			pos = close + 1;
			// Some HTTP clients (Apache HttpClient, under Olingo) collapse "//" in a path, so the key
			// http://x/1 arrives as http:/x/1. No http(s) IRI has that form: put the slash back.
			static const std::regex collapsed(R"(^(https?):/([^/]))");
			key = std::regex_replace(key, collapsed, "$1://$2");
		}
		auto rest = path.substr(pos);
		if (rest == "/$count" && key.empty()) {
			return Collection(*set, request, "", true);
		}
		if (rest.empty()) {
			return Collection(*set, request, key, false);
		}
		if (!key.empty() && rest.size() > 1 && rest[0] == '/') {
			return Navigate(*set, key, rest.substr(1), request);
		}
		return ErrorResponse(404, "not found: " + request.path);
	} catch (ODataError &e) {
		return ErrorResponse(e.status, e.what());
	} catch (SparqlError &e) {
		return ErrorResponse(e.status, e.what());
	} catch (std::exception &e) {
		return ErrorResponse(500, e.what());
	}
}

} // namespace dgl
