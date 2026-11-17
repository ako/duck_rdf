#include "guide.hpp"

#include "guide_topics.hpp"

#include <stdexcept>

namespace dgl {

std::map<std::string, std::string> GuideValues(const Config &config, const std::string &token) {
	std::map<std::string, std::string> v;
	auto host = config.host == "0.0.0.0" || config.host == "::" ? std::string("localhost") : config.host;
	auto kafka_host = config.kafka_advertised_host.empty() ? host : config.kafka_advertised_host;
	v["host"] = host;
	v["http"] = "http://" + host + ":" + std::to_string(config.http_port);
	v["sql_port"] = std::to_string(config.sql_port);
	v["pg_port"] = std::to_string(config.pg_port);
	v["pg_url"] = config.pg_port
	                  ? "jdbc:postgresql://" + host + ":" + std::to_string(config.pg_port) + "/lake?sslmode=disable"
	                  : std::string("(off: pg_port = 0)");
	v["kafka"] = config.kafka_port ? kafka_host + ":" + std::to_string(config.kafka_port) : std::string("(off)");
	v["cluster"] = config.kafka_cluster;
	v["data"] = config.data_dir;
	if (token.empty() || token == "duckgraphlake") {
		v["token"] = config.Loopback() ? "'duckgraphlake' (the default on a loopback address)"
		                               : "the one given with --token or DUCKGRAPHLAKE_TOKEN";
	} else {
		v["token"] = "the one given with --token or DUCKGRAPHLAKE_TOKEN";
	}
	return v;
}

namespace {

std::string Fill(std::string text, const std::map<std::string, std::string> &values) {
	std::string out;
	size_t pos = 0;
	while (true) {
		auto open = text.find("{{", pos);
		if (open == std::string::npos) {
			out += text.substr(pos);
			break;
		}
		auto close = text.find("}}", open + 2);
		if (close == std::string::npos) {
			out += text.substr(pos);
			break;
		}
		auto key = text.substr(open + 2, close - open - 2);
		auto it = values.find(key);
		out += text.substr(pos, open - pos);
		out += it != values.end() ? it->second : text.substr(open, close + 2 - open); // unknown: as written
		pos = close + 2;
	}
	return out;
}

} // namespace

std::vector<GuideTopic> GuideTopics(const std::map<std::string, std::string> &values) {
	std::vector<GuideTopic> topics;
	for (auto &source : GuideSources()) {
		GuideTopic t;
		t.name = source.first;
		t.text = Fill(source.second, values);
		auto first = t.text.substr(0, t.text.find('\n'));
		auto colon = first.find(": ");
		t.summary = colon == std::string::npos ? first : first.substr(colon + 2);
		topics.push_back(t);
	}
	return topics;
}

GuideTopic Guide(const std::string &name, const std::map<std::string, std::string> &values) {
	for (auto &t : GuideTopics(values)) {
		if (t.name == name) {
			return t;
		}
	}
	throw std::runtime_error("no help topic '" + name + "'.\n\n" + GuideIndex(values));
}

std::string GuideIndex(const std::map<std::string, std::string> &values) {
	std::string out =
	    "Help topics (duckgraphlake help <topic>; also GET /api/guide/<topic> and the MCP guide tool):\n\n";
	for (auto &t : GuideTopics(values)) {
		auto name = t.name;
		name.resize(12, ' ');
		out += "  " + name + t.summary + "\n";
	}
	return out;
}

} // namespace dgl
