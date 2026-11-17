// Change events (Debezium envelope, Kafka Connect JSON with the schema embedded) unwrapped into
// table rows. This is what Mendix's Change Data Capture publishes: one topic per entity, each
// value {"schema": {...}, "payload": {"before", "after", "op", "ts_ms", "source", ...}}.
#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace dgl {

struct ChangeEvent {
	// The entity's fields, in schema order, with their DuckDB types.
	std::vector<std::pair<std::string, std::string>> columns;
	// One JSON object: each field's value (from "after", or "before" for a delete), with decimals,
	// timestamps and dates already in their text forms, plus a "cdc" object (see CDC_TYPE).
	std::string row;
};

// The type of the "cdc" column of a change-event table.
extern const char *CDC_TYPE;

// The record value as a change event, or nothing when it is not one (not JSON, no embedded
// schema, no "op").
std::optional<ChangeEvent> UnwrapChangeEvent(const std::string &value);

// Exposed for tests: a Kafka Connect Decimal (base64 big-endian two's complement) as text.
std::string ConnectDecimal(const std::string &base64, int scale);

} // namespace dgl
