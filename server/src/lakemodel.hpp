// What the server makes of the data in it, kept up to date while it runs:
//
//  - A schema per Mendix app, with readable views over its topic tables: <app>.<entity> (the
//    current state of each CDC entity), <app>.<entity>_history (every change), <app>.<entity>__<assoc>
//    (one row per association link) and <app>.<event> (each business event type, its payload
//    fields as columns).
//  - The effective mapping: a folder of this run (in the temp directory) that holds links to the
//    configured mapping files and to the mappings agents set over MCP (kept in
//    <data>/.duckgraphlake/mappings/), and a mapping generated for each app's CDC entities. SPARQL,
//    OData and MCP read it as one glob; the SPARQL translator's cache notices when its files change.
//    The data folder is only written to when an agent defines something.
//  - Views an agent defined over MCP, kept in <data>/.duckgraphlake/views/ and created again at
//    start-up.
//
// Refresh() recomputes all of it from the database; it is cheap when nothing changed.
#pragma once

#include "config.hpp"
#include "json.hpp"

#include "duckdb.hpp"

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace dgl {

// Where a topic table came from, read from its name and columns.
struct TopicTable {
	std::string table;                    // the raw view, e.g. local__cdc.TestApp.Order.1.local
	std::string topic;                    // cdc.TestApp.Order.1.local
	std::string kind;                     // cdc, events, raw
	std::string app;                      // TestApp (cdc: from the topic; events: from ce_source)
	std::string entity;                   // Order (cdc)
	std::string revision;                 // 1 (cdc: the major revision in the topic)
	std::string current;                  // <table>_current (cdc)
	std::string view;                     // the readable view: testapp."order" (cdc)
	std::string history;                  // testapp.order_history (cdc)
	std::vector<std::string> event_views; // testapp.orderplaceevent, ... (events)
};

struct MappingFile {
	std::string name;   // the file name in the mapping folder
	std::string origin; // configured, agent, generated
	std::string path;   // the file (for configured ones, what the link points to)
};

class LakeModel {
public:
	LakeModel(duckdb::DuckDB &db, const Config &config);
	~LakeModel(); // removes this run's mapping folder

	// This run's mapping folder (SQL clients must be allowed to read it).
	const std::string &MappingDir() const {
		return mapping_dir_;
	}

	// The glob SPARQL, OData and MCP read the mapping through.
	std::string MappingGlob() const;
	bool HasMapping() const;
	std::vector<MappingFile> MappingFiles() const;
	std::string MappingOf(const std::string &name) const; // a mapping file's text

	// Rebuilds the app schemas and the generated mappings, and links the configured mapping files.
	// Returns true when the effective mapping changed.
	bool Refresh();

	// The topic tables found by the last Refresh.
	std::vector<TopicTable> Topics() const;
	// What is known about a table or view, by any of its names.
	std::optional<TopicTable> Find(const std::string &name) const;

	// Agent changes, kept in the data folder. Throw std::runtime_error with what was wrong.
	void DefineView(const std::string &name, const std::string &select, const std::string &description);
	void DropView(const std::string &name);
	std::vector<std::pair<std::string, std::string>> AgentViews() const; // name, SELECT
	void SetMapping(const std::string &name, const std::string &text, const std::string &format);
	void RemoveMapping(const std::string &name);
	// Creates the agent's views again (at start-up); returns the problems, one per view.
	std::vector<std::string> RestoreViews();

	// A digest of the configured mapping files and init script (paths, sizes, times), for the watcher.
	std::string WatchStamp() const;

private:
	void LinkConfiguredMappings();
	void BuildAppViews(duckdb::Connection &con, std::vector<TopicTable> &topics);
	std::string GenerateMapping(duckdb::Connection &con, const std::string &app, const std::vector<TopicTable> &topics);
	bool WriteIfChanged(const std::string &path, const std::string &text);

	duckdb::DuckDB &db_;
	const Config &config_;
	std::string state_dir_, mapping_dir_, agent_mapping_dir_, views_dir_;
	mutable std::mutex mutex_;
	std::vector<TopicTable> topics_;
	std::map<std::string, std::string> view_sql_; // what each generated view was created with
	std::string mapping_stamp_;                   // the mapping files as of the last Refresh
};

// A Kafka topic as the Kafka endpoint sees it.
struct TopicState {
	std::string topic, table;
	int partitions = 0;
	int64_t buffered = 0; // received, not yet committed
};

// The topics with what arrived (records, last offset and time), what the server made of them
// (kind, app, entity, readable views) and records that were not change events on a CDC topic.
Json TopicReport(duckdb::DuckDB &db, const LakeModel &model, const std::vector<TopicState> &topics);

// A name as an identifier an agent can type unquoted where possible: lowercase, [a-z0-9_].
std::string PlainName(const std::string &name);

} // namespace dgl
