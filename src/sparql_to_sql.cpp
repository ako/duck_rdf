#include "include/sparql_to_sql.hpp"
#include "include/r2rml_copy.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/type_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include <r2rml/R2RMLMapping.h>
#include <r2rml/SQLResultSet.h>
#include <r2rml/SQLRow.h>
#include <r2rml/SQLValue.h>
#include <sparql-parser/ParseError.h>
#include <sparql-parser/Parser.h>
#include <sparql-parser/ast/Query.h>
#include <sparql2sql/DuckDbDialect.h>
#include <sparql2sql/Ontology.h>
#include <sparql2sql/Translator.h>
#include <sparql2sql/TypeCatalog.h>
#include <sparql2sql/VocabularyCheck.h>
#include <sql2rdf/TypeCatalogLoader.h>
#include <exception>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace duckdb {

static std::string DescribeParseError(const sparql::ParseError &e) {
	return "SPARQL parse error: " + e.message() + " (line " + std::to_string(e.line()) + ", column " +
	       std::to_string(e.column()) + ", near '" + e.nearText() + "')";
}

// Best-effort column-type/constraint catalog for translateQuery(). It backs:
// (a) the native-join-key optimization (an equi-join between two base columns
// of comparable declared type is emitted uncast instead of the
// always-correct-but-slower VARCHAR-cast form), (b) R2RML Section 10.2's
// natural-datatype inference on bare rr:column literals, (c) dropping the "IS
// NOT NULL" guard on a column the DDL declares NOT NULL, and (d) dropping a
// candidate arm's DISTINCT once a declared PRIMARY KEY/UNIQUE constraint
// proves its projected columns already determine the row. R2RML/YARRRML
// mappings carry no SQL type or constraint info of their own, so
// sql2rdf::loadTypeCatalog (sql2rdf v2.1.9+) reads it from three sources:
// every base table's columns + nullability via one information_schema.columns
// sweep, each base table's PRIMARY KEY/UNIQUE constraints via
// information_schema.table_constraints/key_column_usage, and each
// rr:sqlQuery logical table's result columns via DESCRIBE (mapping may be
// nullptr to skip the last one; a view gets no constraint facts, since no
// backend reports constraints for an arbitrary query's result). Any failure
// here must never break translation - fall back to nullptr (today's
// behavior).
static std::unique_ptr<sparql2sql::TypeCatalog> BuildTypeCatalog(ClientContext &context,
                                                                 const r2rml::R2RMLMapping *mapping) {
	auto catalog = make_uniq<sparql2sql::TypeCatalog>();
	try {
		ClientContextSQLConnection conn(context, /*ignore_case=*/false);
		sql2rdf::loadTypeCatalog(conn, mapping, *catalog);
	} catch (...) {
		return nullptr;
	}
	return std::move(catalog);
}

static constexpr const char *UNKNOWN_TERMS_SETTING = "sparql_unknown_terms";

// Whether a class or property the mapping can never produce is an error
// ('error', the default) or just a pattern that matches nothing ('ignore').
// The raw-SPARQL ParserExtension path has no ClientContext and always checks.
static bool ShouldCheckVocabulary(ClientContext *context) {
	if (!context) {
		return true;
	}
	Value setting;
	if (!context->TryGetCurrentSetting(UNKNOWN_TERMS_SETTING, setting) || setting.IsNull()) {
		return true;
	}
	return StringUtil::Lower(setting.ToString()) != "ignore";
}

static constexpr const char *TYPED_RESULTS_SETTING = "sparql_typed_results";

static bool UseTypedResults(ClientContext *context, SparqlResultTypes result_types) {
	if (result_types != SparqlResultTypes::FROM_SETTING) {
		return result_types == SparqlResultTypes::NATIVE;
	}
	Value setting;
	if (!context || !context->TryGetCurrentSetting(TYPED_RESULTS_SETTING, setting) || setting.IsNull()) {
		return false;
	}
	return BooleanValue::Get(setting.DefaultCastAs(LogicalType::BOOLEAN));
}

static constexpr const char *ONTOLOGY_SETTING = "sparql_ontology";

static std::string OntologyPath(ClientContext *context, const std::string *ontology_override) {
	if (ontology_override) {
		return *ontology_override;
	}
	Value setting;
	if (!context || !context->TryGetCurrentSetting(ONTOLOGY_SETTING, setting) || setting.IsNull()) {
		return std::string();
	}
	return setting.ToString();
}

// Read the class and property hierarchy axioms of an ontology file (any RDF
// format read_rdf accepts, a glob, compressed or not) through the calling
// connection, and expand the mapping with them.
static void ApplyOntology(ClientContext &context, const std::string &path, r2rml::R2RMLMapping &mapping) {
	const std::string sql =
	    "SELECT subject AS s, predicate AS p, object AS o FROM read_rdf(" + KeywordHelper::WriteQuoted(path, '\'') +
	    ", prefix_expansion = true) WHERE predicate IN ("
	    "'http://www.w3.org/2000/01/rdf-schema#subClassOf', 'http://www.w3.org/2000/01/rdf-schema#subPropertyOf', "
	    "'http://www.w3.org/2002/07/owl#equivalentClass', 'http://www.w3.org/2002/07/owl#equivalentProperty') "
	    "AND object_datatype IS NULL AND object_lang IS NULL AND contains(object, ':')";
	sparql2sql::Ontology ontology;
	try {
		ClientContextSQLConnection conn(context, /*ignore_case=*/false);
		auto rows = conn.execute(sql);
		while (rows->next()) {
			const auto &row = rows->getCurrentRow();
			ontology.addTriple(row.getValue("s")->asString(), row.getValue("p")->asString(),
			                   row.getValue("o")->asString());
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("SPARQL ontology error: could not read '%s': %s", path, e.what());
	}
	sparql2sql::saturateMapping(mapping, ontology);
}

static void ValidateUnknownTermsSetting(ClientContext &context, SetScope scope, Value &parameter) {
	auto value = StringUtil::Lower(parameter.ToString());
	if (value != "error" && value != "ignore") {
		throw InvalidInputException("%s must be 'error' or 'ignore', got '%s'", UNKNOWN_TERMS_SETTING,
		                            parameter.ToString());
	}
	parameter = Value(value);
}

std::string TranslateSparqlToSql(ClientContext *context, const std::string &sparql_text,
                                 const std::string &mapping_path, SparqlResultTypes result_types,
                                 const std::string *ontology_override) {
	if (ResolveMappingFiles(mapping_path).empty()) {
		throw IOException("Mapping file not found: " + mapping_path);
	}

	r2rml::R2RMLMapping mapping;
	try {
		auto paths = ResolveMappingFiles(mapping_path);
		mapping = ParseR2RMLOrYarrrmlMapping(paths, mapping_path, true);
	} catch (const std::runtime_error &e) {
		throw InvalidInputException("R2RML/YARRRML mapping parse error: %s", e.what());
	}

	if (!mapping.isValid()) {
		throw InvalidInputException(
		    "Mapping '%s' is not a valid full R2RML mapping. sparql_to_sql() translates a SPARQL query into "
		    "a standalone SQL query, so every TriplesMap in the mapping must declare an rr:logicalTable (or "
		    "YARRRML 'sources') naming the table/view to query. An inside-out-only mapping (one that "
		    "can_call_inside_out() accepts but is_valid_r2rml() rejects) is not sufficient here.",
		    mapping_path.c_str());
	}

	const std::string ontology_path = OntologyPath(context, ontology_override);
	if (!ontology_path.empty()) {
		if (!context) {
			throw InvalidInputException("SPARQL ontology error: an ontology needs a database connection to read it");
		}
		ApplyOntology(*context, ontology_path, mapping);
	}

	std::unique_ptr<sparql::ast::Query> query;
	try {
		sparql::Parser parser;
		query = parser.parseString(sparql_text);
	} catch (const sparql::ParseError &e) {
		throw InvalidInputException(DescribeParseError(e));
	} catch (const std::exception &e) {
		throw InvalidInputException("SPARQL parse error: %s", e.what());
	}

	if (ShouldCheckVocabulary(context)) {
		try {
			sparql2sql::checkVocabulary(*query, mapping);
		} catch (const std::exception &e) {
			throw InvalidInputException("SPARQL vocabulary error: %s. To get an empty result instead, run SET %s = "
			                            "'ignore'.",
			                            e.what(), UNKNOWN_TERMS_SETTING);
		}
	}

	std::string sql;
	try {
		sparql2sql::DuckDbDialect dialect;
		auto catalog = context ? BuildTypeCatalog(*context, &mapping) : nullptr;
		sparql2sql::TranslateOptions options;
		options.typedResults = UseTypedResults(context, result_types);
		sql = sparql2sql::translateQuery(*query, mapping, dialect, catalog.get(), options);
	} catch (const std::exception &e) {
		throw InvalidInputException("SPARQL-to-SQL translation error: %s", e.what());
	}

	return sql;
}

inline void SparqlToSql(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &sparql_vector = args.data[0];
	auto &mapping_vector = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    sparql_vector, mapping_vector, result, args.size(), [&](string_t sparql_text, string_t mapping_path_str) {
		    std::string sql =
		        TranslateSparqlToSql(&state.GetContext(), sparql_text.GetString(), mapping_path_str.GetString());
		    return StringVector::AddString(result, sql);
	    });
}

void RegisterSparqlToSql(ExtensionLoader &loader) {
	ScalarFunction sparql_to_sql_sf("sparql_to_sql", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                SparqlToSql);
	sparql_to_sql_sf.SetFallible();
	CreateScalarFunctionInfo info(sparql_to_sql_sf);
	FunctionDescription desc;
	desc.description =
	    "Translate a SPARQL SELECT, ASK or CONSTRUCT query into an equivalent SQL query, using an R2RML or YARRRML "
	    "mapping "
	    "file(s) "
	    "in reverse. The mapping must be a full R2RML mapping (every TriplesMap has an rr:logicalTable or YARRRML "
	    "'sources' entry) - inside-out-only mappings are not accepted. Throws a detailed error naming the mapping "
	    "file(s), the SPARQL syntax problem, or the unsupported SPARQL construct on failure. Currently only the "
	    "'duckdb' SQL dialect is supported.";
	desc.examples.push_back(
	    "SELECT sparql_to_sql('SELECT ?e ?name WHERE { ?e <http://example.com/ns#name> ?name }', 'mapping.ttl')");
	info.descriptions.push_back(desc);
	loader.RegisterFunction(std::move(info));

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption(
	    UNKNOWN_TERMS_SETTING,
	    "What sparql_to_sql/execute_sparql do when a SPARQL query names a class or property that no map in the "
	    "mapping produces: 'error' (default) raises an error naming the term, with a suggestion when it looks like "
	    "a typo; 'ignore' translates it to a pattern that matches nothing.",
	    LogicalType::VARCHAR, Value("error"), ValidateUnknownTermsSetting);
	config.AddExtensionOption(
	    TYPED_RESULTS_SETTING,
	    "When true, sparql_to_sql/execute_sparql return each SELECT column whose datatype the mapping settles as a "
	    "native value (BIGINT, DECIMAL, DOUBLE, BOOLEAN, DATE, TIMESTAMP, or the source column's own type) instead of "
	    "VARCHAR. IRIs, strings and per-row datatypes stay VARCHAR. execute_sparql's typed parameter overrides it.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption(
	    ONTOLOGY_SETTING,
	    "Path or glob of RDF files (any format read_rdf accepts) whose rdfs:subClassOf, rdfs:subPropertyOf, "
	    "owl:equivalentClass and owl:equivalentProperty axioms sparql_to_sql/execute_sparql answer queries under: a "
	    "query for a class or property also matches its subclasses and subproperties. Empty (the default) means no "
	    "ontology. execute_sparql's ontology parameter overrides it.",
	    LogicalType::VARCHAR, Value(""));
}

} // namespace duckdb
