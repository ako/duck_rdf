#include "include/sparql_to_sql.hpp"
#include "include/r2rml_copy.hpp"
#include <map>
#include <set>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
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
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/object_cache.hpp"
#include <r2rml/R2RMLMapping.h>
#include <r2rml/SQLResultSet.h>
#include <r2rml/SQLRow.h>
#include <r2rml/SQLValue.h>
#include <sparql-parser/ParseError.h>
#include <sparql-parser/Parser.h>
#include <sparql-parser/ast/Query.h>
#include <sparql2sql/DuckDbDialect.h>
#include <sparql2sql/LogicalTableSource.h>
#include <sparql2sql/TermMapSql.h>
#include <r2rml/ConstantTermMap.h>
#include <r2rml/LogicalTable.h>
#include <r2rml/PredicateObjectMap.h>
#include <r2rml/ReferencingObjectMap.h>
#include <r2rml/SubjectMap.h>
#include <r2rml/TriplesMap.h>
#include <r2rml/TemplateTermMap.h>
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
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
static constexpr const char *UNIQUE_KEYS_SETTING = "sparql_unique_keys";

// Parse the sparql_unique_keys setting: `table(col, ...); table(col); ...`,
// table and column names as the mapping writes them. Throws on bad syntax.
static std::vector<std::pair<std::string, std::set<std::string>>> ParseUniqueKeys(const std::string &text) {
	std::vector<std::pair<std::string, std::set<std::string>>> keys;
	for (auto &entry : StringUtil::Split(text, ';')) {
		StringUtil::Trim(entry);
		if (entry.empty()) {
			continue;
		}
		auto open = entry.find('(');
		if (open == std::string::npos || entry.back() != ')') {
			throw InvalidInputException("%s: expected table(column, ...), got '%s'", UNIQUE_KEYS_SETTING, entry);
		}
		std::string table = entry.substr(0, open);
		StringUtil::Trim(table);
		std::set<std::string> columns;
		for (auto &col : StringUtil::Split(entry.substr(open + 1, entry.size() - open - 2), ',')) {
			StringUtil::Trim(col);
			if (!col.empty()) {
				columns.insert(col);
			}
		}
		if (table.empty() || columns.empty()) {
			throw InvalidInputException("%s: expected table(column, ...), got '%s'", UNIQUE_KEYS_SETTING, entry);
		}
		keys.emplace_back(table, columns);
	}
	return keys;
}

static void ValidateUniqueKeysSetting(ClientContext &context, SetScope scope, Value &parameter) {
	ParseUniqueKeys(parameter.ToString());
}

static std::unique_ptr<sparql2sql::TypeCatalog> BuildTypeCatalog(ClientContext &context,
                                                                 const r2rml::R2RMLMapping *mapping) {
	auto catalog = make_uniq<sparql2sql::TypeCatalog>();
	try {
		ClientContextSQLConnection conn(context, /*ignore_case=*/false);
		sql2rdf::loadTypeCatalog(conn, mapping, *catalog);
	} catch (...) {
		return nullptr;
	}
	// Keys the database cannot declare (a view over Parquet or Iceberg files),
	// asserted by whoever runs the query. They let the translator drop the
	// per-table DISTINCT the RDF set semantics otherwise require.
	Value keys;
	if (context.TryGetCurrentSetting(UNIQUE_KEYS_SETTING, keys) && !keys.IsNull()) {
		for (auto &key : ParseUniqueKeys(keys.ToString())) {
			catalog->uniqueKeys[key.first].push_back(key.second);
		}
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

// Read the ontology axioms sparql2sql uses (class and property hierarchies,
// domains, ranges, inverse, symmetric and transitive properties) from RDF
// files (any format read_rdf accepts, a glob, compressed or not) through the
// calling connection.
static sparql2sql::Ontology ReadOntology(ClientContext &context, const std::string &path) {
	const std::string sql =
	    "SELECT subject AS s, predicate AS p, object AS o FROM read_rdf(" + KeywordHelper::WriteQuoted(path, '\'') +
	    ", prefix_expansion = true) WHERE (predicate IN ("
	    "'http://www.w3.org/2000/01/rdf-schema#subClassOf', 'http://www.w3.org/2000/01/rdf-schema#subPropertyOf', "
	    "'http://www.w3.org/2002/07/owl#equivalentClass', 'http://www.w3.org/2002/07/owl#equivalentProperty', "
	    "'http://www.w3.org/2000/01/rdf-schema#domain', 'http://www.w3.org/2000/01/rdf-schema#range', "
	    "'http://www.w3.org/2002/07/owl#inverseOf') OR (predicate = "
	    "'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' AND object IN ("
	    "'http://www.w3.org/2002/07/owl#TransitiveProperty', 'http://www.w3.org/2002/07/owl#SymmetricProperty'))) "
	    "AND object_datatype IS NULL AND object_lang IS NULL AND contains(object, ':')";
	if (FileSystem::GetFileSystem(context).Glob(path).empty()) {
		throw InvalidInputException("SPARQL ontology error: no file matches '%s'", path);
	}
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
		// The nested query reports DuckDB's error as JSON; keep only its message.
		throw InvalidInputException("SPARQL ontology error: could not read '%s': %s", path, ErrorData(e).RawMessage());
	}
	return ontology;
}

// Everything about a mapping that does not depend on the query: the parsed
// mapping, saturated with the ontology, and the type catalog. Building it is
// most of the cost of a translation (reading the catalog alone is a few
// milliseconds), so it is cached; see GetPreparedMapping.
struct PreparedMapping {
	r2rml::R2RMLMapping mapping;
	sparql2sql::Ontology ontology;
	std::unique_ptr<sparql2sql::TypeCatalog> catalog;
};

static std::shared_ptr<const PreparedMapping> PrepareMapping(ClientContext *context, const std::string &mapping_path,
                                                             const std::string &ontology_path) {
	auto paths = ResolveMappingFiles(mapping_path);
	if (paths.empty()) {
		throw IOException("Mapping file not found: " + mapping_path);
	}
	auto prepared = std::make_shared<PreparedMapping>();
	try {
		prepared->mapping = ParseR2RMLOrYarrrmlMapping(paths, mapping_path, true);
	} catch (const std::runtime_error &e) {
		throw InvalidInputException("R2RML/YARRRML mapping parse error: %s", e.what());
	}

	if (!prepared->mapping.isValid()) {
		throw InvalidInputException(
		    "Mapping '%s' is not a valid full R2RML mapping. sparql_to_sql() translates a SPARQL query into "
		    "a standalone SQL query, so every TriplesMap in the mapping must declare an rr:logicalTable (or "
		    "YARRRML 'sources') naming the table/view to query. An inside-out-only mapping (one that "
		    "can_call_inside_out() accepts but is_valid_r2rml() rejects) is not sufficient here.",
		    mapping_path.c_str());
	}

	if (!ontology_path.empty()) {
		if (!context) {
			throw InvalidInputException("SPARQL ontology error: an ontology needs a database connection to read it");
		}
		prepared->ontology = ReadOntology(*context, ontology_path);
		sparql2sql::saturateMapping(prepared->mapping, prepared->ontology);
	}
	if (context) {
		prepared->catalog = BuildTypeCatalog(*context, &prepared->mapping);
	}
	return prepared;
}

static constexpr const char *CACHE_SETTING = "sparql_mapping_cache";

static bool UseMappingCache(ClientContext &context) {
	Value setting;
	if (!context.TryGetCurrentSetting(CACHE_SETTING, setting) || setting.IsNull()) {
		return true;
	}
	return BooleanValue::Get(setting.DefaultCastAs(LogicalType::BOOLEAN));
}

// Appends path, size and modification time of `path` to `token`; false if the
// file cannot be opened.
static bool AppendFileStamp(FileSystem &fs, const std::string &path, std::string &token) {
	try {
		auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		token += path + '\x1f' + std::to_string(fs.GetFileSize(*handle)) + '\x1f' +
		         std::to_string(fs.GetLastModifiedTime(*handle).value) + '\x1e';
		return true;
	} catch (...) {
		return false;
	}
}

// What a cached PreparedMapping depends on besides its key: the mapping and
// ontology files (path, size, modification time) and every attached
// database's catalog version, which DuckDB bumps on each committed schema
// change. Empty when it cannot be cached: a file that cannot be read, or
// catalog changes not yet committed in this transaction (other connections
// do not see them).
static std::string ValidityToken(ClientContext &context, const std::string &mapping_path,
                                 const std::string &ontology_path) {
	std::string token;
	auto &local_fs = FileSystem::GetFileSystem(context);
	for (auto &path : ResolveMappingFiles(mapping_path)) {
		if (!AppendFileStamp(local_fs, path, token)) {
			return std::string();
		}
	}
	token += '\x1d';
	if (!ontology_path.empty()) {
		auto &fs = FileSystem::GetFileSystem(context);
		for (auto &file : fs.Glob(ontology_path)) {
			if (!AppendFileStamp(fs, file.path, token)) {
				return std::string();
			}
		}
	}
	token += '\x1d';
	for (auto &db : DatabaseManager::Get(context).GetDatabases(context)) {
		auto version = db->GetCatalog().GetCatalogVersion(context);
		if (version.IsValid() && version.GetIndex() >= TRANSACTION_ID_START) {
			return std::string();
		}
		token += db->GetName() + '=' + (version.IsValid() ? std::to_string(version.GetIndex()) : "?") + ';';
	}
	return token;
}

class PreparedMappingCacheEntry : public ObjectCacheEntry {
public:
	PreparedMappingCacheEntry(std::string token_p, std::shared_ptr<const PreparedMapping> prepared_p)
	    : token(std::move(token_p)), prepared(std::move(prepared_p)) {
	}
	static string ObjectType() {
		return "rdf_prepared_mapping";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(idx_t(256) * 1024); // a rough size, so DuckDB may evict it
	}

	const std::string token;
	const std::shared_ptr<const PreparedMapping> prepared;
};

// The prepared mapping for this mapping, ontology and set of declared keys:
// from DuckDB's object cache when nothing it depends on has changed, freshly
// built (and cached) otherwise.
static std::shared_ptr<const PreparedMapping>
GetPreparedMapping(ClientContext *context, const std::string &mapping_path, const std::string &ontology_path) {
	if (!context || !UseMappingCache(*context)) {
		return PrepareMapping(context, mapping_path, ontology_path);
	}
	Value keys;
	std::string key_setting;
	if (context->TryGetCurrentSetting(UNIQUE_KEYS_SETTING, keys) && !keys.IsNull()) {
		key_setting = keys.ToString();
	}
	const std::string cache_key =
	    std::string("rdf_prepared_mapping\x1f") + mapping_path + '\x1f' + ontology_path + '\x1f' + key_setting;
	const std::string token = ValidityToken(*context, mapping_path, ontology_path);
	auto &cache = ObjectCache::GetObjectCache(*context);
	if (!token.empty()) {
		auto entry = cache.Get<PreparedMappingCacheEntry>(cache_key);
		if (entry && entry->token == token) {
			return entry->prepared;
		}
	}
	auto prepared = PrepareMapping(context, mapping_path, ontology_path);
	if (!token.empty()) {
		cache.Put(cache_key, make_shared_ptr<PreparedMappingCacheEntry>(token, prepared));
	}
	return prepared;
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
                                 const std::string *ontology_override, bool term_tags) {
	auto prepared = GetPreparedMapping(context, mapping_path, OntologyPath(context, ontology_override));

	std::unique_ptr<sparql::ast::Query> query;
	try {
		sparql::Parser parser;
		query = parser.parseString(sparql_text);
	} catch (const sparql::ParseError &e) {
		throw InvalidInputException(DescribeParseError(e));
	} catch (const std::exception &e) {
		throw InvalidInputException("SPARQL parse error: %s", e.what());
	}
	sparql2sql::applyTransitiveProperties(*query, prepared->ontology);

	if (ShouldCheckVocabulary(context)) {
		try {
			sparql2sql::checkVocabulary(*query, prepared->mapping);
		} catch (const std::exception &e) {
			throw InvalidInputException("SPARQL vocabulary error: %s. To get an empty result instead, run SET %s = "
			                            "'ignore'.",
			                            e.what(), UNKNOWN_TERMS_SETTING);
		}
	}

	std::string sql;
	try {
		sparql2sql::DuckDbDialect dialect;
		sparql2sql::TranslateOptions options;
		options.typedResults = UseTypedResults(context, result_types);
		options.termTags = term_tags;
		sql = sparql2sql::translateQuery(*query, prepared->mapping, dialect, prepared->catalog.get(), options);
	} catch (const std::exception &e) {
		throw InvalidInputException("SPARQL-to-SQL translation error: %s", e.what());
	}

	return sql;
}

// ============================================================
// rdf_mapping_schema(mapping): what the mapping can produce, per class
// ============================================================
//
// One row per (class, predicate, object shape) a triples map declares: the classes its subject
// map asserts (after saturation with the ontology, so inferred classes are included), each
// constant predicate of its predicate-object maps, and what the object is - an IRI, a blank
// node or a literal with its datatype and language as the translator itself infers them (an
// rr:column literal without rr:datatype gets its datatype from the column's SQL type). For a
// referencing object map, target_class is the parent triples map's class. Read from the parsed
// mapping alone: nothing in the data is scanned. Used by servers that expose the graph as typed
// entities (OData).

// A template with its column names left out: "http://x/{id}" and "http://x/{company_id}" build
// the same IRIs from the same key values.
static std::string TemplateShape(const std::string &templ) {
	std::string out;
	bool in_column = false;
	for (size_t i = 0; i < templ.size(); i++) {
		char c = templ[i];
		if (c == '\\' && i + 1 < templ.size()) {
			out.push_back(c);
			out.push_back(templ[++i]);
		} else if (c == '{') {
			in_column = true;
			out += "{}";
		} else if (c == '}') {
			in_column = false;
		} else if (!in_column) {
			out.push_back(c);
		}
	}
	return out;
}

struct MappingSchemaRow {
	std::string cls, predicate, kind, datatype, lang, target_class, source;
	bool has_cls = false, has_datatype = false, has_lang = false, has_target = false;
};

struct MappingSchemaBindData : public TableFunctionData {
	std::vector<MappingSchemaRow> rows;
};

struct MappingSchemaState : public GlobalTableFunctionState {
	idx_t position = 0;
};

static const char *TermKindName(rdf::TermKind kind) {
	switch (kind) {
	case rdf::TermKind::Iri:
		return "iri";
	case rdf::TermKind::BlankNode:
		return "bnode";
	case rdf::TermKind::Literal:
		return "literal";
	default:
		return "unknown";
	}
}

static unique_ptr<FunctionData> MappingSchemaBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("rdf_mapping_schema: the mapping path cannot be NULL");
	}
	auto mapping_path = input.inputs[0].GetValue<string>();
	std::string ontology_override;
	bool has_override = false;
	auto it = input.named_parameters.find("ontology");
	if (it != input.named_parameters.end() && !it->second.IsNull()) {
		ontology_override = it->second.GetValue<string>();
		has_override = true;
	}
	auto prepared =
	    GetPreparedMapping(&context, mapping_path, OntologyPath(&context, has_override ? &ontology_override : nullptr));
	auto result = make_uniq<MappingSchemaBindData>();
	sparql2sql::DuckDbDialect dialect;
	// The classes of the subjects each IRI template produces: an object built from the same
	// template refers to them (how mappings usually link without an rr:parentTriplesMap).
	std::map<std::string, std::set<std::string>> template_classes;
	for (auto &tm : prepared->mapping.triplesMaps) {
		if (!tm || !tm->subjectMap) {
			continue;
		}
		auto value = tm->subjectMap->valueTermMap();
		auto templ = dynamic_cast<const r2rml::TemplateTermMap *>(value);
		if (templ) {
			for (auto &c : tm->subjectMap->classIRIs) {
				template_classes[TemplateShape(templ->templateString)].insert(c);
			}
		}
	}
	for (auto &tm : prepared->mapping.triplesMaps) {
		if (!tm || !tm->subjectMap) {
			continue;
		}
		std::string identity = tm->logicalTable ? sparql2sql::logicalTableIdentity(*tm->logicalTable) : std::string();
		std::vector<std::string> classes = tm->subjectMap->classIRIs;
		std::vector<std::pair<bool, std::string>> subjects;
		if (classes.empty()) {
			subjects.emplace_back(false, std::string());
		}
		for (auto &c : classes) {
			subjects.emplace_back(true, c);
		}
		for (auto &pom : tm->predicateObjectMaps) {
			for (auto &pm : pom->predicateMaps) {
				auto constant = dynamic_cast<const r2rml::ConstantTermMap *>(pm.get());
				if (!constant || !constant->constantValue.isIri()) {
					continue; // a computed predicate has no single name
				}
				auto &predicate = constant->constantValue.lexical();
				for (auto &om : pom->objectMaps) {
					std::vector<MappingSchemaRow> shapes;
					auto ref = dynamic_cast<const r2rml::ReferencingObjectMap *>(om.get());
					if (ref) {
						MappingSchemaRow row;
						row.kind = "iri";
						auto parent = ref->parentTriplesMap;
						if (parent && parent->subjectMap && !parent->subjectMap->classIRIs.empty()) {
							for (auto &pc : parent->subjectMap->classIRIs) {
								row.target_class = pc;
								row.has_target = true;
								shapes.push_back(row);
							}
						} else {
							shapes.push_back(row);
						}
					} else {
						auto info =
						    sparql2sql::termMapToSqlExpr(*om, "t", dialect, prepared->catalog.get(), identity).term;
						MappingSchemaRow row;
						row.kind = TermKindName(info.kind);
						if (!info.datatypeIri.empty()) {
							row.datatype = info.datatypeIri;
							row.has_datatype = true;
						}
						if (!info.lang.empty()) {
							row.lang = info.lang;
							row.has_lang = true;
						}
						auto templ = dynamic_cast<const r2rml::TemplateTermMap *>(om.get());
						auto targets = templ && info.kind == rdf::TermKind::Iri
						                   ? template_classes.find(TemplateShape(templ->templateString))
						                   : template_classes.end();
						if (targets != template_classes.end()) {
							for (auto &target : targets->second) {
								row.target_class = target;
								row.has_target = true;
								shapes.push_back(row);
							}
						} else {
							shapes.push_back(row);
						}
					}
					for (auto &subject : subjects) {
						for (auto shape : shapes) {
							shape.cls = subject.second;
							shape.has_cls = subject.first;
							shape.predicate = predicate;
							shape.source = tm->id;
							result->rows.push_back(shape);
						}
					}
				}
			}
		}
		// A class with no predicate-object maps still appears, with a NULL predicate.
		if (tm->predicateObjectMaps.empty()) {
			for (auto &subject : subjects) {
				if (subject.first) {
					MappingSchemaRow row;
					row.cls = subject.second;
					row.has_cls = true;
					row.source = tm->id;
					result->rows.push_back(row);
				}
			}
		}
	}
	names = {"class", "predicate", "object_kind", "datatype", "language", "target_class", "triples_map"};
	return_types = vector<LogicalType>(names.size(), LogicalType::VARCHAR);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> MappingSchemaInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<MappingSchemaState>();
}

static void MappingSchemaFunc(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<MappingSchemaBindData>();
	auto &state = input.global_state->Cast<MappingSchemaState>();
	idx_t count = 0;
	auto opt = [](bool has, const std::string &v) {
		return has ? Value(v) : Value();
	};
	while (state.position < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = data.rows[state.position++];
		output.data[0].SetValue(count, opt(row.has_cls, row.cls));
		output.data[1].SetValue(count, opt(!row.predicate.empty(), row.predicate));
		output.data[2].SetValue(count, opt(!row.kind.empty(), row.kind));
		output.data[3].SetValue(count, opt(row.has_datatype, row.datatype));
		output.data[4].SetValue(count, opt(row.has_lang, row.lang));
		output.data[5].SetValue(count, opt(row.has_target, row.target_class));
		output.data[6].SetValue(count, opt(!row.source.empty(), row.source));
		count++;
	}
	output.SetChildCardinality(count);
}

static void RegisterMappingSchema(ExtensionLoader &loader) {
	TableFunction tf("rdf_mapping_schema", {LogicalType::VARCHAR}, MappingSchemaFunc, MappingSchemaBind,
	                 MappingSchemaInit);
	tf.GetSignature().AddKeywordOnly("ontology", LogicalType::VARCHAR, Value());
	CreateTableFunctionInfo info(tf);
	FunctionDescription desc;
	desc.description =
	    "What an R2RML or YARRRML mapping can produce, per class: one row per class, predicate and object shape "
	    "(object_kind iri, bnode, literal or unknown; datatype and language for literals; target_class for a "
	    "reference to another triples map). Uses the sparql_ontology setting (or the ontology parameter), so "
	    "classes the ontology implies are included. Reads the mapping only, not the data.";
	desc.examples.push_back("SELECT * FROM rdf_mapping_schema('mapping.ttl')");
	info.descriptions.push_back(desc);
	loader.RegisterFunction(std::move(info));
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
	RegisterMappingSchema(loader);

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
	    "Path or glob of RDF files (any format read_rdf accepts) whose axioms sparql_to_sql/execute_sparql answer "
	    "queries under: rdfs:subClassOf, rdfs:subPropertyOf, owl:equivalentClass, owl:equivalentProperty, "
	    "rdfs:domain, rdfs:range, owl:inverseOf, owl:SymmetricProperty and owl:TransitiveProperty. A query for a "
	    "class or property then also matches what the ontology implies. Empty (the default) means no ontology. "
	    "execute_sparql's ontology parameter overrides it.",
	    LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption(
	    UNIQUE_KEYS_SETTING,
	    "Keys for tables and views that cannot declare them (views over Parquet or Iceberg files), as "
	    "'table(column, ...); table(column)' with the names the mapping uses. Each listed column set must be unique "
	    "and non-null in the data: sparql_to_sql/execute_sparql then drop the DISTINCT that RDF set semantics "
	    "otherwise require for that table. A wrong key returns duplicate rows. Empty (the default) means none.",
	    LogicalType::VARCHAR, Value(""), ValidateUniqueKeysSetting);
	config.AddExtensionOption(
	    CACHE_SETTING,
	    "When true (the default), sparql_to_sql/execute_sparql keep each parsed mapping, with its ontology and the "
	    "column types and keys read from the catalog, in DuckDB's object cache, and reuse it until a mapping or "
	    "ontology file changes or a schema change is committed. Set it to false to rebuild them for every query, "
	    "e.g. when an attached database that has no catalog version (SQLite, Postgres) changes its schema.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true));
}

} // namespace duckdb
