// A Kafka-compatible endpoint for producers: a single broker that writes every topic to an
// Iceberg table (see kafka_lake.hpp), standing in locally for a WarpStream agent and Tableflow.
//
// Requests: ApiVersions (0-3), Metadata (4-8, auto-creating topics), InitProducerId (0-1, for
// idempotent producers), CreateTopics (2-4) and Produce (3-8, record batch format v2, with no
// compression, gzip or zstd). There is no consuming: Fetch is listed in ApiVersions (librdkafka
// needs it there to produce the v2 record format) but closes the connection; consumer groups and
// transactions are not offered.
#pragma once

#include "kafka_lake.hpp"

#include <atomic>
#include <string>
#include <thread>

namespace dgl {

class KafkaServer {
public:
	KafkaServer(KafkaLake &lake, std::string advertised_host, int advertised_port);
	~KafkaServer();

	void Start(const std::string &host, int port); // throws std::runtime_error
	void Stop();

private:
	void AcceptLoop();
	void Serve(int fd);

	KafkaLake &lake_;
	std::string advertised_host_;
	int advertised_port_;
	int listen_fd_ = -1;
	std::atomic<bool> stopping_ {false};
	std::thread accept_thread_;
	std::atomic<int64_t> next_producer_id_ {1000};
};

} // namespace dgl
