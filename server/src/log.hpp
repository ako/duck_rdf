#pragma once

#include <iostream>
#include <mutex>
#include <string>

namespace dgl {

inline void Log(const std::string &message) {
	static std::mutex mutex;
	std::lock_guard<std::mutex> lock(mutex);
	std::cerr << "[duckgraphlake] " << message << std::endl;
}

} // namespace dgl
