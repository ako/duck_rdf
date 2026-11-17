#include "json.hpp"

#include "yyjson.hpp"

#include <cmath>
#include <cstdio>
#include <cstdlib>

namespace dgl {

bool Json::AsBool() const {
	if (type_ != Type::BOOL) {
		throw std::runtime_error("expected a boolean");
	}
	return bool_;
}

int64_t Json::AsInt() const {
	if (type_ == Type::INT) {
		return int_;
	}
	if (type_ == Type::DOUBLE && std::floor(double_) == double_) {
		return static_cast<int64_t>(double_);
	}
	throw std::runtime_error("expected an integer");
}

double Json::AsDouble() const {
	if (type_ == Type::DOUBLE) {
		return double_;
	}
	if (type_ == Type::INT) {
		return static_cast<double>(int_);
	}
	throw std::runtime_error("expected a number");
}

const std::string &Json::AsString() const {
	if (type_ != Type::STRING) {
		throw std::runtime_error("expected a string");
	}
	return string_;
}

Json &Json::Set(const std::string &key, Json value) {
	if (type_ == Type::NUL) {
		type_ = Type::OBJECT;
	}
	for (auto &member : members_) {
		if (member.first == key) {
			member.second = std::move(value);
			return member.second;
		}
	}
	members_.emplace_back(key, std::move(value));
	return members_.back().second;
}

const Json *Json::Get(const std::string &key) const {
	for (auto &member : members_) {
		if (member.first == key) {
			return &member.second;
		}
	}
	return nullptr;
}

Json &Json::operator[](const std::string &key) {
	if (type_ == Type::NUL) {
		type_ = Type::OBJECT;
	}
	for (auto &member : members_) {
		if (member.first == key) {
			return member.second;
		}
	}
	members_.emplace_back(key, Json());
	return members_.back().second;
}

void JsonQuote(const std::string &s, std::string &out) {
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
		case '\b':
			out += "\\b";
			break;
		case '\f':
			out += "\\f";
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

void Json::DumpTo(std::string &out) const {
	switch (type_) {
	case Type::NUL:
		out += "null";
		break;
	case Type::BOOL:
		out += bool_ ? "true" : "false";
		break;
	case Type::INT:
		out += std::to_string(int_);
		break;
	case Type::DOUBLE: {
		if (!std::isfinite(double_)) {
			out += "null";
			break;
		}
		char buf[32];
		snprintf(buf, sizeof(buf), "%.17g", double_);
		// The shortest form that reads back the same, as most JSON writers print it.
		for (int precision = 1; precision < 17; precision++) {
			char shorter[32];
			snprintf(shorter, sizeof(shorter), "%.*g", precision, double_);
			if (std::strtod(shorter, nullptr) == double_) {
				snprintf(buf, sizeof(buf), "%s", shorter);
				break;
			}
		}
		out += buf;
		break;
	}
	case Type::STRING:
		JsonQuote(string_, out);
		break;
	case Type::ARRAY: {
		out.push_back('[');
		bool first = true;
		for (auto &item : items_) {
			if (!first) {
				out.push_back(',');
			}
			first = false;
			item.DumpTo(out);
		}
		out.push_back(']');
		break;
	}
	case Type::OBJECT: {
		out.push_back('{');
		bool first = true;
		for (auto &member : members_) {
			if (!first) {
				out.push_back(',');
			}
			first = false;
			JsonQuote(member.first, out);
			out.push_back(':');
			member.second.DumpTo(out);
		}
		out.push_back('}');
		break;
	}
	}
}

std::string Json::Dump() const {
	std::string out;
	DumpTo(out);
	return out;
}

namespace {

using namespace duckdb_yyjson; // NOLINT: the yyjson macros call its functions unqualified

Json Convert(yyjson_val *val) {
	switch (yyjson_get_type(val)) {
	case YYJSON_TYPE_NULL:
		return Json();
	case YYJSON_TYPE_BOOL:
		return Json(yyjson_get_bool(val));
	case YYJSON_TYPE_NUM:
		if (yyjson_is_real(val)) {
			return Json(yyjson_get_real(val));
		}
		if (yyjson_is_uint(val)) {
			return Json(static_cast<int64_t>(yyjson_get_uint(val)));
		}
		return Json(static_cast<int64_t>(yyjson_get_sint(val)));
	case YYJSON_TYPE_STR:
		return Json(std::string(yyjson_get_str(val), yyjson_get_len(val)));
	case YYJSON_TYPE_ARR: {
		Json arr = Json::Array();
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(val, idx, max, item) {
			arr.Push(Convert(item));
		}
		return arr;
	}
	case YYJSON_TYPE_OBJ: {
		Json obj = Json::Object();
		size_t idx, max;
		yyjson_val *key, *item;
		yyjson_obj_foreach(val, idx, max, key, item) {
			obj.Set(std::string(yyjson_get_str(key), yyjson_get_len(key)), Convert(item));
		}
		return obj;
	}
	default:
		return Json();
	}
}

} // namespace

Json Json::Parse(const std::string &text) {
	yyjson_read_err err;
	yyjson_doc *doc = yyjson_read_opts(const_cast<char *>(text.data()), text.size(), 0, nullptr, &err);
	if (!doc) {
		throw std::runtime_error(std::string("invalid JSON: ") + err.msg);
	}
	Json result = Convert(yyjson_doc_get_root(doc));
	yyjson_doc_free(doc);
	return result;
}

} // namespace dgl
