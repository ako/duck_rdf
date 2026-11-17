#include "sparql.hpp"

#include "duckdb/main/query_result_stream.hpp"

#include <algorithm>
#include <cctype>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <thread>

namespace dgl {

const char *XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";

std::string SqlLiteral(const std::string &text) {
	std::string out = "'";
	for (char c : text) {
		if (c == '\'') {
			out += "''";
		} else {
			out.push_back(c);
		}
	}
	out += "'";
	return out;
}

std::string SqlIdent(const std::string &name) {
	std::string out = "\"";
	for (char c : name) {
		if (c == '"') {
			out += "\"\"";
		} else {
			out.push_back(c);
		}
	}
	out += "\"";
	return out;
}

namespace {

// Interrupts a connection's running query after a timeout, unless finished first.
class Watchdog {
public:
	Watchdog(duckdb::Connection &con, double seconds) {
		if (seconds <= 0) {
			return;
		}
		thread_ = std::thread([this, &con, seconds]() {
			std::unique_lock<std::mutex> lock(mutex_);
			auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(int64_t(seconds * 1000));
			if (!cv_.wait_until(lock, deadline, [this]() { return done_; })) {
				fired = true;
				con.Interrupt();
			}
		});
	}
	~Watchdog() {
		{
			std::lock_guard<std::mutex> lock(mutex_);
			done_ = true;
		}
		cv_.notify_all();
		if (thread_.joinable()) {
			thread_.join();
		}
	}
	std::atomic<bool> fired {false};

private:
	std::mutex mutex_;
	std::condition_variable cv_;
	bool done_ = false;
	std::thread thread_;
};

// An error as a SPARQL client should see it: no internal SQL position marker.
std::string CleanMessage(const std::string &message) {
	auto text = message;
	auto pos = text.find("\n\nLINE ");
	if (pos != std::string::npos) {
		text = text.substr(0, pos);
	}
	while (!text.empty() && (text.back() == '\n' || text.back() == ' ')) {
		text.pop_back();
	}
	return text;
}

[[noreturn]] void ThrowFor(const duckdb::ErrorData &error, bool interrupted, double timeout) {
	using duckdb::ExceptionType;
	if (interrupted || error.Type() == ExceptionType::INTERRUPT) {
		char buf[64];
		snprintf(buf, sizeof(buf), "query cancelled after %g s", timeout);
		throw SparqlError(503, buf);
	}
	auto message = CleanMessage(error.RawMessage());
	switch (error.Type()) {
	case ExceptionType::INVALID_INPUT:
	case ExceptionType::PARSER:
	case ExceptionType::BINDER:
		throw SparqlError(400, message);
	default:
		throw SparqlError(500, message);
	}
}

Term TagTerm(const std::string &value, const std::string &tag) {
	Term t;
	t.value = value;
	if (tag == "I") {
		t.kind = Term::Kind::IRI;
	} else if (tag == "B") {
		t.kind = Term::Kind::BNODE;
		if (t.value.rfind("_:", 0) == 0) {
			t.value = t.value.substr(2);
		}
	} else if (!tag.empty() && tag[0] == '@') {
		t.lang = tag.substr(1);
	} else if (!tag.empty() && tag[0] == 'D' && tag.substr(1) != XSD_STRING) {
		t.datatype = tag.substr(1);
	}
	// "L", xsd:string or no tag (a computed value of unknown kind): a plain literal.
	return t;
}

// A CONSTRUCT node in read_rdf's columns: no datatype means an IRI or a blank node.
Term NodeTerm(const std::string &value) {
	Term t;
	if (value.rfind("_:", 0) == 0) {
		t.kind = Term::Kind::BNODE;
		t.value = value.substr(2);
	} else {
		t.kind = Term::Kind::IRI;
		t.value = value;
	}
	return t;
}

} // namespace

SparqlResult SparqlExecutor::Execute(const std::string &query, size_t max_rows) const {
	if (mapping_.empty()) {
		throw SparqlError(503, "no mapping configured (--mapping, or [rdf] mapping in the config file)");
	}
	duckdb::Connection con(db_);
	Watchdog watchdog(con, timeout_);
	auto sql = "SELECT * FROM execute_sparql(" + SqlLiteral(query) + ", " + SqlLiteral(mapping_) +
	           ", typed := false, term_tags := true)";
	auto result = con.Submit(sql);
	if (result->HasError()) {
		ThrowFor(result->GetErrorObject(), watchdog.fired, timeout_);
	}
	duckdb::QueryResultStream<> stream(std::move(result));
	std::vector<std::string> columns;
	for (auto &name : stream.GetNames()) {
		columns.push_back(name.GetIdentifierName());
	}

	SparqlResult out;
	const std::vector<std::string> construct_columns {"subject", "predicate", "object", "object_datatype",
	                                                  "object_lang"};
	if (columns.size() == 1 && columns[0] == "ask") {
		out.kind = SparqlResult::Kind::ASK;
	} else if (columns == construct_columns) {
		out.kind = SparqlResult::Kind::CONSTRUCT;
	} else {
		out.kind = SparqlResult::Kind::SELECT;
		for (size_t i = 0; i + 1 < columns.size(); i += 2) {
			out.variables.push_back(columns[i].substr(2)); // v_<var>, d_<var> pairs
		}
	}

	size_t count = 0;
	try {
		while (auto chunk = stream.Fetch()) {
			for (duckdb::idx_t r = 0; r < chunk->size(); r++) {
				if (max_rows && count == max_rows) {
					out.truncated = true;
					break;
				}
				count++;
				if (out.kind == SparqlResult::Kind::ASK) {
					auto v = chunk->GetValue(0, r);
					out.boolean = !v.IsNull() && v.GetValue<bool>();
				} else if (out.kind == SparqlResult::Kind::CONSTRUCT) {
					SparqlResult::Triple triple;
					triple.subject = NodeTerm(chunk->GetValue(0, r).ToString());
					triple.predicate = chunk->GetValue(1, r).ToString();
					auto object = chunk->GetValue(2, r).ToString();
					auto datatype = chunk->GetValue(3, r);
					auto lang = chunk->GetValue(4, r);
					if (datatype.IsNull()) {
						triple.object = NodeTerm(object);
					} else {
						triple.object.value = object;
						if (!lang.IsNull() && !lang.ToString().empty()) {
							triple.object.lang = lang.ToString();
						} else if (datatype.ToString() != XSD_STRING) {
							triple.object.datatype = datatype.ToString();
						}
					}
					out.triples.push_back(std::move(triple));
				} else {
					std::vector<std::optional<Term>> row;
					for (duckdb::idx_t c = 0; c + 1 < chunk->ColumnCount(); c += 2) {
						auto v = chunk->GetValue(c, r);
						if (v.IsNull()) {
							row.emplace_back();
							continue;
						}
						auto tag = chunk->GetValue(c + 1, r);
						row.emplace_back(TagTerm(v.ToString(), tag.IsNull() ? "" : tag.ToString()));
					}
					out.rows.push_back(std::move(row));
				}
			}
			if (out.truncated) {
				break;
			}
		}
	} catch (std::exception &ex) {
		ThrowFor(duckdb::ErrorData(ex), watchdog.fired, timeout_);
	}
	if (stream.HasError()) {
		ThrowFor(stream.GetErrorObject(), watchdog.fired, timeout_);
	}
	return out;
}

// ---------------------------------------------------------------------------
// Escapes

std::string EscapeLiteral(const std::string &text) {
	std::string out;
	for (char c : text) {
		switch (c) {
		case '\\':
			out += "\\\\";
			break;
		case '"':
			out += "\\\"";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			out.push_back(c);
		}
	}
	return out;
}

namespace {

std::string EscapeIri(const std::string &iri) {
	std::string out;
	for (unsigned char c : iri) {
		if (c <= 0x20 || std::string("<>\"{}|^`\\").find(static_cast<char>(c)) != std::string::npos) {
			char buf[8];
			snprintf(buf, sizeof(buf), "\\u%04X", c);
			out += buf;
		} else {
			out.push_back(static_cast<char>(c));
		}
	}
	return out;
}

std::string XmlEscape(const std::string &text) {
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

std::string Attr(const std::string &text) {
	return "\"" + XmlEscape(text) + "\"";
}

std::string CsvField(const std::string &text) {
	if (text.find_first_of(",\"\r\n") == std::string::npos) {
		return text;
	}
	std::string out = "\"";
	for (char c : text) {
		if (c == '"') {
			out += "\"\"";
		} else {
			out.push_back(c);
		}
	}
	return out + "\"";
}

void JsonString(const std::string &s, std::string &out) {
	out.push_back('"');
	for (unsigned char c : s) {
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (c < 0x20) {
				char buf[8];
				snprintf(buf, sizeof(buf), "\\u%04x", c);
				out += buf;
			} else {
				out.push_back(static_cast<char>(c));
			}
		}
	}
	out.push_back('"');
}

} // namespace

std::string NtTerm(const Term &term) {
	switch (term.kind) {
	case Term::Kind::IRI:
		return "<" + EscapeIri(term.value) + ">";
	case Term::Kind::BNODE:
		return "_:" + term.value;
	default:
		break;
	}
	auto lit = "\"" + EscapeLiteral(term.value) + "\"";
	if (!term.lang.empty()) {
		return lit + "@" + term.lang;
	}
	if (!term.datatype.empty()) {
		return lit + "^^<" + EscapeIri(term.datatype) + ">";
	}
	return lit;
}

// ---------------------------------------------------------------------------
// Writers

std::string WriteSelect(const std::string &format, const SparqlResult &result) {
	std::string out;
	if (format == "json") {
		out += "{\"head\":{\"vars\":[";
		for (size_t i = 0; i < result.variables.size(); i++) {
			if (i) {
				out += ",";
			}
			JsonString(result.variables[i], out);
		}
		out += "]},\"results\":{\"bindings\":[";
		for (size_t r = 0; r < result.rows.size(); r++) {
			if (r) {
				out += ",";
			}
			out += "{";
			bool first = true;
			for (size_t i = 0; i < result.variables.size(); i++) {
				auto &b = result.rows[r][i];
				if (!b) {
					continue;
				}
				if (!first) {
					out += ",";
				}
				first = false;
				JsonString(result.variables[i], out);
				out += ":{\"type\":";
				out += b->kind == Term::Kind::IRI     ? "\"uri\""
				       : b->kind == Term::Kind::BNODE ? "\"bnode\""
				                                      : "\"literal\"";
				out += ",\"value\":";
				JsonString(b->value, out);
				if (!b->lang.empty()) {
					out += ",\"xml:lang\":";
					JsonString(b->lang, out);
				} else if (!b->datatype.empty()) {
					out += ",\"datatype\":";
					JsonString(b->datatype, out);
				}
				out += "}";
			}
			out += "}";
		}
		out += "]}}";
		return out;
	}
	if (format == "xml") {
		out += "<?xml version=\"1.0\"?>\n<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n<head>\n";
		for (auto &v : result.variables) {
			out += "  <variable name=" + Attr(v) + "/>\n";
		}
		out += "</head>\n<results>\n";
		for (auto &row : result.rows) {
			out += "  <result>\n";
			for (size_t i = 0; i < result.variables.size(); i++) {
				auto &b = row[i];
				if (!b) {
					continue;
				}
				std::string inner;
				if (b->kind == Term::Kind::IRI) {
					inner = "<uri>" + XmlEscape(b->value) + "</uri>";
				} else if (b->kind == Term::Kind::BNODE) {
					inner = "<bnode>" + XmlEscape(b->value) + "</bnode>";
				} else {
					std::string attrs;
					if (!b->lang.empty()) {
						attrs = " xml:lang=" + Attr(b->lang);
					} else if (!b->datatype.empty()) {
						attrs = " datatype=" + Attr(b->datatype);
					}
					inner = "<literal" + attrs + ">" + XmlEscape(b->value) + "</literal>";
				}
				out += "    <binding name=" + Attr(result.variables[i]) + ">" + inner + "</binding>\n";
			}
			out += "  </result>\n";
		}
		out += "</results>\n</sparql>\n";
		return out;
	}
	if (format == "csv") {
		for (size_t i = 0; i < result.variables.size(); i++) {
			out += (i ? "," : "") + CsvField(result.variables[i]);
		}
		out += "\r\n";
		for (auto &row : result.rows) {
			for (size_t i = 0; i < row.size(); i++) {
				if (i) {
					out += ",";
				}
				if (row[i]) {
					out += CsvField(row[i]->kind == Term::Kind::BNODE ? "_:" + row[i]->value : row[i]->value);
				}
			}
			out += "\r\n";
		}
		return out;
	}
	// tsv
	for (size_t i = 0; i < result.variables.size(); i++) {
		out += (i ? "\t?" : "?") + result.variables[i];
	}
	out += "\n";
	for (auto &row : result.rows) {
		for (size_t i = 0; i < row.size(); i++) {
			if (i) {
				out += "\t";
			}
			if (row[i]) {
				out += NtTerm(*row[i]);
			}
		}
		out += "\n";
	}
	return out;
}

std::string WriteAsk(const std::string &format, bool value) {
	if (format == "json") {
		return std::string("{\"head\":{},\"boolean\":") + (value ? "true" : "false") + "}";
	}
	if (format == "xml") {
		return std::string("<?xml version=\"1.0\"?>\n<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n"
		                   "<head/>\n<boolean>") +
		       (value ? "true" : "false") + "</boolean>\n</sparql>\n";
	}
	return std::string(value ? "true" : "false") + "\n";
}

std::string WriteNTriples(const SparqlResult &result) {
	std::string out;
	for (auto &t : result.triples) {
		Term p;
		p.kind = Term::Kind::IRI;
		p.value = t.predicate;
		out += NtTerm(t.subject) + " " + NtTerm(p) + " " + NtTerm(t.object) + " .\n";
	}
	return out;
}

namespace {

bool NameStartChar(char c) {
	return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
}
bool NameChar(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '.' || c == '-';
}

} // namespace

std::string WriteRdfXml(const SparqlResult &result) {
	std::vector<std::pair<std::string, std::string>> namespaces; // namespace, prefix
	std::string body;
	for (auto &t : result.triples) {
		auto &p = t.predicate;
		// The longest NCName suffix of the predicate is its local name.
		size_t start = p.size();
		while (start > 0 && NameChar(p[start - 1])) {
			start--;
		}
		while (start < p.size() && !NameStartChar(p[start])) {
			start++;
		}
		if (start == 0 || start >= p.size()) {
			throw SparqlError(406, "predicate <" + p + "> has no RDF/XML form; ask for text/turtle instead");
		}
		auto ns = p.substr(0, start);
		auto local = p.substr(start);
		std::string prefix;
		for (auto &entry : namespaces) {
			if (entry.first == ns) {
				prefix = entry.second;
			}
		}
		if (prefix.empty()) {
			prefix = "ns" + std::to_string(namespaces.size());
			namespaces.emplace_back(ns, prefix);
		}
		auto about = t.subject.kind == Term::Kind::BNODE ? "rdf:nodeID=" + Attr(t.subject.value)
		                                                 : "rdf:about=" + Attr(t.subject.value);
		std::string prop;
		auto qname = prefix + ":" + local;
		if (t.object.kind == Term::Kind::IRI) {
			prop = "<" + qname + " rdf:resource=" + Attr(t.object.value) + "/>";
		} else if (t.object.kind == Term::Kind::BNODE) {
			prop = "<" + qname + " rdf:nodeID=" + Attr(t.object.value) + "/>";
		} else {
			std::string attrs;
			if (!t.object.lang.empty()) {
				attrs = " xml:lang=" + Attr(t.object.lang);
			} else if (!t.object.datatype.empty()) {
				attrs = " rdf:datatype=" + Attr(t.object.datatype);
			}
			prop = "<" + qname + attrs + ">" + XmlEscape(t.object.value) + "</" + qname + ">";
		}
		body += "  <rdf:Description " + about + ">" + prop + "</rdf:Description>\n";
	}
	std::string decls;
	for (auto &entry : namespaces) {
		decls += "\n    xmlns:" + entry.second + "=" + Attr(entry.first);
	}
	return "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<rdf:RDF "
	       "xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"" +
	       decls + ">\n" + body + "</rdf:RDF>\n";
}

// ---------------------------------------------------------------------------
// Content negotiation

std::string Negotiate(const std::string &accept, const std::vector<std::string> &offered, const std::string &fallback) {
	auto trim = [](std::string s) {
		auto b = s.find_first_not_of(" \t");
		auto e = s.find_last_not_of(" \t");
		return b == std::string::npos ? std::string() : s.substr(b, e - b + 1);
	};
	if (trim(accept).empty()) {
		return fallback;
	}
	std::string best;
	double best_q = 0;
	size_t pos = 0;
	while (pos <= accept.size()) {
		auto comma = accept.find(',', pos);
		auto part = accept.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
		pos = comma == std::string::npos ? accept.size() + 1 : comma + 1;
		std::vector<std::string> fields;
		size_t fpos = 0;
		while (fpos <= part.size()) {
			auto semi = part.find(';', fpos);
			fields.push_back(trim(part.substr(fpos, semi == std::string::npos ? std::string::npos : semi - fpos)));
			fpos = semi == std::string::npos ? part.size() + 1 : semi + 1;
		}
		auto media = fields[0];
		std::transform(media.begin(), media.end(), media.begin(), ::tolower);
		double q = 1;
		for (size_t i = 1; i < fields.size(); i++) {
			if (fields[i].rfind("q=", 0) == 0) {
				q = std::atof(fields[i].c_str() + 2);
			}
		}
		if (q <= 0 || media.empty()) {
			continue;
		}
		std::string candidate;
		if (media == "*/*") {
			candidate = fallback;
		} else if (media == "application/*" || media == "text/*") {
			auto major = media.substr(0, media.size() - 1);
			for (auto &o : offered) {
				if (o.rfind(major, 0) == 0) {
					candidate = o;
					break;
				}
			}
		} else if (std::find(offered.begin(), offered.end(), media) != offered.end()) {
			candidate = media;
		}
		// Earlier entries win ties, as clients list their preference first.
		if (!candidate.empty() && q > best_q) {
			best = candidate;
			best_q = q;
		}
	}
	return best;
}

} // namespace dgl
