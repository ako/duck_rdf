#pragma once

#include <cstdint>
#include <string>

namespace duckdb {

class ClientContext;
class ExtensionLoader;

void RegisterSparqlToSql(ExtensionLoader &loader);

// Loads mapping_path, parses sparql_text, and translates it into a standalone
// SQL query for the "duckdb" dialect. Shared by the sparql_to_sql scalar
// function, execute_sparql's bind_replace, and the SPARQL parser extension so
// all three raise identical errors. context may be null (as when called from
// the parser extension, which has no ClientContext) - the mapping-existence
// pre-check and the best-effort type catalog are simply skipped in that case.
//! `ontology_override`, when given, replaces the sparql_ontology setting for
//! this call: a path or glob of RDF files whose class and property hierarchies
//! the query is answered under (an empty string means no ontology).
//! `term_tags` adds a d_<var> column beside each SELECT output column with the
//! term's type tag ("I", "B", "D<datatype>", "@<lang>", "L"; NULL when unbound),
//! which a SPARQL results serialiser needs to tell IRIs from literals.
//! Whether SELECT output columns are returned as native SQL values (typed
//! results) or as the VARCHAR lexical form of each RDF term.
enum class SparqlResultTypes : uint8_t {
	FROM_SETTING, //!< the sparql_typed_results setting (VARCHAR when there is no ClientContext)
	VARCHAR,
	NATIVE
};

std::string TranslateSparqlToSql(ClientContext *context, const std::string &sparql_text,
                                 const std::string &mapping_path,
                                 SparqlResultTypes result_types = SparqlResultTypes::FROM_SETTING,
                                 const std::string *ontology_override = nullptr, bool term_tags = false);

} // namespace duckdb
