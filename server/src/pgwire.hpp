// A PostgreSQL wire-protocol (v3) endpoint over the server's DuckDB database, so that
// PostgreSQL clients (psql, the PostgreSQL JDBC driver, and so Mendix's external database
// connector) can query it. The SQL is DuckDB's; the protocol is PostgreSQL's.
//
// Supported: the startup handshake (password = the server token, TLS refused so clients fall
// back to plain), simple queries (several statements per message), the extended protocol
// (Parse, Bind, Describe, Execute with a row limit, Close, Sync, Flush), text and binary
// formats for the common scalar types, CancelRequest, and the session commands clients send
// on connect (SET, SHOW, DISCARD), which are answered without touching DuckDB's settings.
#pragma once

#include "duckdb.hpp"

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace dgl {

class PgSession;

class PgServer {
public:
	PgServer(duckdb::DuckDB &db, std::string token);
	~PgServer();

	// Binds and starts accepting on a background thread; throws std::runtime_error.
	void Start(const std::string &host, int port);
	void Stop();

	// For CancelRequest: interrupt the session with this key, if it exists.
	void Cancel(int32_t pid, int32_t secret);
	// A new session's process id for BackendKeyData.
	int32_t Register(PgSession *session, int32_t secret);
	void Unregister(int32_t pid);

	duckdb::DuckDB &Database() {
		return db_;
	}
	const std::string &Token() const {
		return token_;
	}

private:
	void AcceptLoop();

	duckdb::DuckDB &db_;
	std::string token_;
	int listen_fd_ = -1;
	std::atomic<bool> stopping_ {false};
	std::thread accept_thread_;
	std::mutex sessions_mutex_;
	std::map<int32_t, std::pair<PgSession *, int32_t>> sessions_; // pid -> session, secret
	std::atomic<int32_t> next_pid_ {1000};
};

} // namespace dgl
