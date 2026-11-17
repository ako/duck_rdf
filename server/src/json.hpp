// A small JSON value: built for responses, parsed (through DuckDB's bundled yyjson)
// for MCP requests. Objects keep their insertion order, which keeps output stable.
#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace dgl {

class Json {
public:
	enum class Type { NUL, BOOL, INT, DOUBLE, STRING, ARRAY, OBJECT };

	Json() = default;
	Json(std::nullptr_t) {
	}
	Json(bool b) : type_(Type::BOOL), bool_(b) {
	}
	template <class T,
	          typename std::enable_if<std::is_integral<T>::value && !std::is_same<T, bool>::value, int>::type = 0>
	Json(T i) : type_(Type::INT), int_(static_cast<int64_t>(i)) {
	}
	Json(double d) : type_(Type::DOUBLE), double_(d) {
	}
	Json(const char *s) : type_(Type::STRING), string_(s) {
	}
	Json(std::string s) : type_(Type::STRING), string_(std::move(s)) {
	}

	static Json Array() {
		Json j;
		j.type_ = Type::ARRAY;
		return j;
	}
	static Json Object() {
		Json j;
		j.type_ = Type::OBJECT;
		return j;
	}
	template <class T>
	static Json ArrayOf(const std::vector<T> &items) {
		Json j = Array();
		for (auto &item : items) {
			j.Push(Json(item));
		}
		return j;
	}

	Type GetType() const {
		return type_;
	}
	bool IsNull() const {
		return type_ == Type::NUL;
	}
	bool IsString() const {
		return type_ == Type::STRING;
	}
	bool IsObject() const {
		return type_ == Type::OBJECT;
	}
	bool IsArray() const {
		return type_ == Type::ARRAY;
	}
	bool IsNumber() const {
		return type_ == Type::INT || type_ == Type::DOUBLE;
	}

	bool AsBool() const;
	int64_t AsInt() const;
	double AsDouble() const;
	const std::string &AsString() const;

	// Objects: Set replaces an existing key; Get returns nullptr when absent.
	Json &Set(const std::string &key, Json value);
	const Json *Get(const std::string &key) const;
	Json *GetMutable(const std::string &key);
	void Remove(const std::string &key);
	Json &operator[](const std::string &key);
	const std::vector<std::pair<std::string, Json>> &Members() const {
		return members_;
	}

	// Arrays.
	void Push(Json value) {
		if (type_ == Type::NUL) {
			type_ = Type::ARRAY;
		}
		items_.push_back(std::move(value));
	}
	std::vector<Json> &MutableItems() {
		return items_;
	}
	const std::vector<Json> &Items() const {
		return items_;
	}
	size_t Size() const {
		return type_ == Type::ARRAY ? items_.size() : members_.size();
	}

	std::string Dump() const;
	void DumpTo(std::string &out) const;
	static Json Parse(const std::string &text); // throws std::runtime_error

private:
	Type type_ = Type::NUL;
	bool bool_ = false;
	int64_t int_ = 0;
	double double_ = 0;
	std::string string_;
	std::vector<Json> items_;
	std::vector<std::pair<std::string, Json>> members_;
};

// A JSON string literal, quotes included.
void JsonQuote(const std::string &s, std::string &out);

} // namespace dgl
