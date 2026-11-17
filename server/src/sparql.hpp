// Running SPARQL over the server's database, and the SPARQL 1.1 result formats.
//
// Queries go through execute_sparql(..., term_tags := true), so each value comes back
// with its kind (IRI, blank node, literal) and its datatype or language: what the
// standard result formats need.
#pragma once

#include "duckdb.hpp"

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace dgl {

extern const char *XSD_STRING;

struct Term {
	enum class Kind { IRI, BNODE, LITERAL };
	Kind kind = Kind::LITERAL;
	std::string value;    // a blank node's label without "_:"
	std::string datatype; // empty: xsd:string (or no datatype)
	std::string lang;
};

struct SparqlResult {
	enum class Kind { SELECT, ASK, CONSTRUCT };
	struct Triple {
		Term subject;
		std::string predicate;
		Term object;
	};
	Kind kind = Kind::SELECT;
	std::vector<std::string> variables;
	std::vector<std::vector<std::optional<Term>>> rows; // SELECT; nullopt is unbound
	bool boolean = false;                                // ASK
	std::vector<Triple> triples;                         // CONSTRUCT
	bool truncated = false;
};

class SparqlError : public std::runtime_error {
public:
	SparqlError(int status, const std::string &message) : std::runtime_error(message), status(status) {
	}
	int status;
};

class SparqlExecutor {
public:
	SparqlExecutor(duckdb::DuckDB &db, std::string mapping, double timeout_seconds)
	    : db_(db), mapping_(std::move(mapping)), timeout_(timeout_seconds) {
	}

	// Fetches at most max_rows rows (0: no limit) and says whether there were more.
	SparqlResult Execute(const std::string &query, size_t max_rows) const;

	const std::string &Mapping() const {
		return mapping_;
	}

private:
	duckdb::DuckDB &db_;
	std::string mapping_;
	double timeout_;
};

// SQL helpers.
std::string SqlLiteral(const std::string &text);
std::string SqlIdent(const std::string &name);

// Text escapes shared by the writers and by MCP's query building.
std::string EscapeLiteral(const std::string &text); // the inside of a "..." SPARQL/N-Triples string
std::string NtTerm(const Term &term);

// Result writers. Each returns the body for one media type.
std::string WriteSelect(const std::string &format, const SparqlResult &result); // json, xml, csv, tsv
std::string WriteAsk(const std::string &format, bool value);
std::string WriteNTriples(const SparqlResult &result); // also valid Turtle
std::string WriteRdfXml(const SparqlResult &result);   // throws SparqlError(406) for predicates without a QName

// Content negotiation: the best offered media type for an Accept header, or "" for none.
std::string Negotiate(const std::string &accept, const std::vector<std::string> &offered, const std::string &fallback);

} // namespace dgl
