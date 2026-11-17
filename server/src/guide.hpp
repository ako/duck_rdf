// The help topics (server/guide/*.md, compiled in): `duckgraphlake help [topic]`, GET /api/guide and
// the MCP guide tool and guide:// resources. {{placeholders}} are filled in with this server's
// addresses, so what an agent reads is what it can connect to.
#pragma once

#include "config.hpp"

#include <map>
#include <string>
#include <vector>

namespace dgl {

struct GuideTopic {
	std::string name;    // quickstart
	std::string summary; // the text after "# quickstart: "
	std::string text;    // Markdown, placeholders filled in
};

// The values for the placeholders, from the configuration (and the token, when one was given).
std::map<std::string, std::string> GuideValues(const Config &config, const std::string &token);

std::vector<GuideTopic> GuideTopics(const std::map<std::string, std::string> &values);
// One topic; throws std::runtime_error listing the topics when there is no such one.
GuideTopic Guide(const std::string &name, const std::map<std::string, std::string> &values);
// The list of topics as text, for `help` and the MCP guide tool without a topic.
std::string GuideIndex(const std::map<std::string, std::string> &values);

} // namespace dgl
