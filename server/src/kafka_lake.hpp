// Kafka topics written to Iceberg tables shaped like WarpStream Tableflow's.
//
// Each topic is written to one Iceberg table in the "tableflow" namespace
// (<data>/warpstream/_tableflow/<table>/). The table is named <cluster>__<topic> unless the
// Tableflow configuration names it. Its columns are the fields of the table's inline JSON Schema,
// parsed from each record's JSON value, plus a `warpstream` struct with the record's partition,
// offset, key, raw value and timestamp. A topic without a configured table gets only the
// `warpstream` struct.
//
// Produced records go to a buffer in a local DuckDB file (<data>/.duckgraphlake/kafka.duckdb) and
// are acknowledged once they are there. A background thread commits each table's buffered
// records to Iceberg as one snapshot, every flush interval, through the server's own Iceberg
// REST catalog (a second, internal DuckDB attached to /iceberg), then removes them from the
// buffer. A restart carries on from the buffer and the tables' highest offsets.
#pragma once

#include "iceberg_catalog.hpp"
#include "json.hpp"

#include "duckdb.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace dgl {

// One table of a WarpStream Tableflow configuration (the subset this server uses).
struct TableflowTable {
	std::string cluster;
	std::string topic;
	std::string name;
	std::vector<std::pair<std::string, std::string>> columns; // name, DuckDB type
	std::string json_structure;                               // for json_transform
	bool skip_raw_values = false;
};

struct TableflowConfig {
	std::vector<TableflowTable> tables;
	std::vector<std::string> notes; // settings read but not applied locally
};

// Reads a Tableflow YAML file: tables[] with source_cluster_name, source_topic, name,
// source_format: json, schema_mode: inline, input_schema (JSON Schema), field_type_remapping,
// skip_raw_record_values. Throws std::runtime_error.
TableflowConfig LoadTableflowConfig(const std::string &path);

struct KafkaRecord {
	int64_t timestamp_ms = 0;
	std::optional<std::string> key;
	std::optional<std::string> value;
	std::vector<std::pair<std::string, std::optional<std::string>>> headers;
};

struct KafkaLakeOptions {
	std::string data_dir;
	std::string catalog_url; // http://127.0.0.1:<port>/iceberg
	std::string token;
	std::string cluster = "local";
	int default_partitions = 1;
	int flush_ms = 1000;
	bool unwrap_cdc = true; // change events (Debezium envelope with schema) get a column per field
	TableflowConfig tableflow;
};

class KafkaLake {
public:
	// Called after a table's first commit, with its name and folder: the main database then
	// creates a view over it.
	using TableCallback = std::function<void(const std::string &table, const std::string &folder)>;

	// A table's columns (name, DuckDB type), as the catalog has them.
	using Columns = std::vector<std::pair<std::string, std::string>>;

	KafkaLake(KafkaLakeOptions options, IcebergCatalog &catalog, TableCallback on_table);
	~KafkaLake();

	// Called after a commit that changed a table's shape (a column added) or brought a new
	// (ce_source, ce_type) pair of event headers. Set before Start.
	void SetChangeCallback(TableCallback on_change) {
		on_change_ = std::move(on_change);
	}

	void Start();
	void Stop();

	// Records received but not yet committed, per topic.
	std::map<std::string, int64_t> Buffered();

	int Partitions(const std::string &topic);                   // 0: no such topic
	void CreateTopic(const std::string &topic, int partitions); // idempotent
	std::vector<std::pair<std::string, int>> Topics();
	std::string TableFor(const std::string &topic) const;

	// Buffers the records; returns the first one's offset. Throws std::runtime_error.
	int64_t Append(const std::string &topic, int32_t partition, const std::vector<KafkaRecord> &records);

	static bool ValidTopicName(const std::string &name);

private:
	const TableflowTable *Configured(const std::string &topic) const;
	// Opens the buffer file and the namespace folder: on the first topic, so a server nobody
	// produces to leaves the data folder untouched. Under mutex_.
	void EnsureOpen();
	int64_t NextOffset(const std::string &topic, int32_t partition); // under mutex_
	void FlushLoop();
	void Flush();
	bool FlushTopic(duckdb::Connection &con, const std::string &topic, int64_t up_to_seq);
	Columns TableColumns(duckdb::Connection &con, const std::string &qualified);

	KafkaLakeOptions options_;
	IcebergCatalog &catalog_;
	TableCallback on_table_;
	TableCallback on_change_;
	std::set<std::string> event_kinds_;    // topic + ce_source + ce_type seen (under mutex_)
	std::set<std::string> changed_topics_; // a new event kind since the last commit (under mutex_)
	std::unique_ptr<duckdb::DuckDB> db_;
	std::unique_ptr<duckdb::Connection> append_con_;

	std::mutex mutex_;
	std::map<std::string, int> topics_;
	std::map<std::pair<std::string, int32_t>, int64_t> next_offset_;
	int64_t next_seq_ = 1;

	std::thread flusher_;
	std::mutex flush_mutex_;
	std::condition_variable flush_cv_;
	bool stopping_ = false;
	std::atomic<bool> open_ {false};
	bool attached_ = false;
	std::map<std::string, bool> table_ready_; // table -> exists in the catalog
	std::map<std::string, bool> table_announced_;
	std::map<std::string, Columns> table_columns_; // by table, read from the catalog
};

} // namespace dgl
