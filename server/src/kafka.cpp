#include "kafka.hpp"

#include "log.hpp"

#include "miniz.hpp"
#include "zstd.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <stdexcept>
#include <vector>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0 // macOS: SO_NOSIGPIPE is set on each socket instead
#endif

namespace dgl {

namespace {

// Kafka API keys and error codes used here.
enum ApiKey : int16_t {
	PRODUCE = 0,
	FETCH = 1,
	METADATA = 3,
	API_VERSIONS = 18,
	CREATE_TOPICS = 19,
	INIT_PRODUCER_ID = 22
};
enum ErrorCode : int16_t {
	NONE = 0,
	CORRUPT_MESSAGE = 2,
	UNKNOWN_TOPIC_OR_PARTITION = 3,
	INVALID_TOPIC = 17,
	UNSUPPORTED_VERSION = 35,
	TOPIC_ALREADY_EXISTS = 36,
	KAFKA_STORAGE_ERROR = 56,
	UNSUPPORTED_COMPRESSION_TYPE = 76
};

struct Supported {
	int16_t key, min, max;
};
// Fetch is advertised but not served: librdkafka enables the v2 record format (and zstd) only for
// brokers that list Fetch 4+ (10+ for zstd), even in a producer that never fetches. A Fetch request
// closes the connection, as consuming is not offered.
const Supported SUPPORTED[] = {{PRODUCE, 3, 8},      {FETCH, 4, 11},        {METADATA, 4, 8},
                               {API_VERSIONS, 0, 3}, {CREATE_TOPICS, 2, 4}, {INIT_PRODUCER_ID, 0, 1}};

struct KafkaError : std::runtime_error {
	using std::runtime_error::runtime_error;
};

int64_t NowMs() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

bool ReadExact(int fd, void *buffer, size_t n) {
	auto p = static_cast<char *>(buffer);
	while (n > 0) {
		auto got = ::recv(fd, p, n, 0);
		if (got <= 0) {
			return false;
		}
		p += got;
		n -= static_cast<size_t>(got);
	}
	return true;
}

bool WriteAll(int fd, const std::string &data) {
	const char *p = data.data();
	size_t n = data.size();
	while (n > 0) {
		auto sent = ::send(fd, p, n, MSG_NOSIGNAL);
		if (sent <= 0) {
			return false;
		}
		p += sent;
		n -= static_cast<size_t>(sent);
	}
	return true;
}

// Big-endian Kafka primitives, and the varints of the record format and flexible versions.
class In {
public:
	In(const char *data, size_t size) : data_(data), size_(size) {
	}
	explicit In(const std::string &s) : In(s.data(), s.size()) {
	}
	explicit In(std::string &&) = delete; // would point into a temporary
	bool AtEnd() const {
		return pos_ >= size_;
	}
	size_t Remaining() const {
		return size_ - pos_;
	}
	uint64_t Big(int bytes) {
		Need(bytes);
		uint64_t v = 0;
		for (int i = 0; i < bytes; i++) {
			v = (v << 8) | static_cast<uint8_t>(data_[pos_++]);
		}
		return v;
	}
	int8_t I8() {
		return static_cast<int8_t>(Big(1));
	}
	int16_t I16() {
		return static_cast<int16_t>(Big(2));
	}
	int32_t I32() {
		return static_cast<int32_t>(Big(4));
	}
	int64_t I64() {
		return static_cast<int64_t>(Big(8));
	}
	bool Bool() {
		return I8() != 0;
	}
	std::string Raw(size_t n) {
		Need(n);
		std::string s(data_ + pos_, n);
		pos_ += n;
		return s;
	}
	std::optional<std::string> NullableString() {
		auto len = I16();
		if (len < 0) {
			return std::nullopt;
		}
		return Raw(static_cast<size_t>(len));
	}
	std::string String() {
		auto s = NullableString();
		return s ? *s : "";
	}
	std::optional<std::string> NullableBytes() {
		auto len = I32();
		if (len < 0) {
			return std::nullopt;
		}
		return Raw(static_cast<size_t>(len));
	}
	uint64_t UVarint() {
		uint64_t v = 0;
		for (int shift = 0; shift < 64; shift += 7) {
			Need(1);
			auto b = static_cast<uint8_t>(data_[pos_++]);
			v |= static_cast<uint64_t>(b & 0x7f) << shift;
			if (!(b & 0x80)) {
				return v;
			}
		}
		throw KafkaError("varint too long");
	}
	int64_t Varint() {
		auto v = UVarint();
		return static_cast<int64_t>((v >> 1) ^ (~(v & 1) + 1));
	}
	std::string CompactString() {
		auto len = UVarint();
		return len == 0 ? "" : Raw(len - 1);
	}
	void SkipTags() {
		auto n = UVarint();
		for (uint64_t i = 0; i < n; i++) {
			UVarint();
			Raw(UVarint());
		}
	}

private:
	void Need(size_t n) const {
		if (pos_ + n > size_) {
			throw KafkaError("request too short");
		}
	}
	const char *data_;
	size_t size_;
	size_t pos_ = 0;
};

class Out {
public:
	void Big(uint64_t v, int bytes) {
		for (int i = bytes - 1; i >= 0; i--) {
			buf_.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
		}
	}
	void I8(int8_t v) {
		Big(static_cast<uint8_t>(v), 1);
	}
	void I16(int16_t v) {
		Big(static_cast<uint16_t>(v), 2);
	}
	void I32(int32_t v) {
		Big(static_cast<uint32_t>(v), 4);
	}
	void I64(int64_t v) {
		Big(static_cast<uint64_t>(v), 8);
	}
	void Bool(bool v) {
		I8(v ? 1 : 0);
	}
	void String(const std::string &s) {
		I16(static_cast<int16_t>(s.size()));
		buf_ += s;
	}
	void NullableString(const std::optional<std::string> &s) {
		if (!s) {
			I16(-1);
		} else {
			String(*s);
		}
	}
	void UVarint(uint64_t v) {
		while (v >= 0x80) {
			buf_.push_back(static_cast<char>((v & 0x7f) | 0x80));
			v >>= 7;
		}
		buf_.push_back(static_cast<char>(v));
	}
	std::string &Data() {
		return buf_;
	}

private:
	std::string buf_;
};

// ---------------------------------------------------------------------------
// Record batches (format v2)

std::string Gunzip(const std::string &in) {
	using namespace duckdb_miniz; // NOLINT
	if (in.size() < 18 || static_cast<uint8_t>(in[0]) != 0x1f || static_cast<uint8_t>(in[1]) != 0x8b) {
		throw KafkaError("not gzip data");
	}
	auto flags = static_cast<uint8_t>(in[3]);
	size_t pos = 10;
	if (flags & 4) { // FEXTRA
		pos += 2 + (static_cast<uint8_t>(in[pos]) | (static_cast<uint8_t>(in[pos + 1]) << 8));
	}
	if (flags & 8) { // FNAME
		pos = in.find('\0', pos) + 1;
	}
	if (flags & 16) { // FCOMMENT
		pos = in.find('\0', pos) + 1;
	}
	if (flags & 2) { // FHCRC
		pos += 2;
	}
	mz_stream stream {};
	if (mz_inflateInit2(&stream, -MZ_DEFAULT_WINDOW_BITS) != MZ_OK) {
		throw KafkaError("gzip: cannot start inflating");
	}
	std::string out;
	char chunk[65536];
	stream.next_in = reinterpret_cast<const unsigned char *>(in.data() + pos);
	stream.avail_in = static_cast<unsigned int>(in.size() - pos);
	int status;
	do {
		stream.next_out = reinterpret_cast<unsigned char *>(chunk);
		stream.avail_out = sizeof(chunk);
		status = mz_inflate(&stream, MZ_NO_FLUSH);
		if (status != MZ_OK && status != MZ_STREAM_END) {
			mz_inflateEnd(&stream);
			throw KafkaError("gzip: corrupt data");
		}
		out.append(chunk, sizeof(chunk) - stream.avail_out);
	} while (status != MZ_STREAM_END && (stream.avail_in > 0 || stream.avail_out == 0));
	mz_inflateEnd(&stream);
	return out;
}

std::string Unzstd(const std::string &in) {
	using namespace duckdb_zstd; // NOLINT
	auto ctx = ZSTD_createDCtx();
	ZSTD_inBuffer input {in.data(), in.size(), 0};
	std::string out;
	char chunk[65536];
	while (input.pos < input.size) {
		ZSTD_outBuffer output {chunk, sizeof(chunk), 0};
		auto rc = ZSTD_decompressStream(ctx, &output, &input);
		if (ZSTD_isError(rc)) {
			ZSTD_freeDCtx(ctx);
			throw KafkaError(std::string("zstd: ") + ZSTD_getErrorName(rc));
		}
		out.append(chunk, output.pos);
		if (rc == 0 && input.pos == input.size) {
			break;
		}
	}
	ZSTD_freeDCtx(ctx);
	return out;
}

struct BatchError {
	int16_t code;
	std::string message;
};

// The records of every batch in a partition's record set.
std::vector<KafkaRecord> ParseRecords(const std::string &data) {
	std::vector<KafkaRecord> out;
	In in(data);
	while (in.Remaining() >= 61) {
		in.I64(); // base offset: the broker assigns offsets
		auto length = in.I32();
		if (length < 49 || static_cast<size_t>(length) > in.Remaining()) {
			throw BatchError {CORRUPT_MESSAGE, "truncated record batch"};
		}
		auto batch_bytes = in.Raw(static_cast<size_t>(length));
		In batch(batch_bytes);
		batch.I32(); // partition leader epoch
		auto magic = batch.I8();
		if (magic != 2) {
			throw BatchError {CORRUPT_MESSAGE, "only record batch format v2 (Kafka 0.11+) is supported"};
		}
		batch.I32(); // crc
		auto attributes = batch.I16();
		batch.I32(); // last offset delta
		auto base_timestamp = batch.I64();
		batch.I64(); // max timestamp
		batch.I64(); // producer id
		batch.I16(); // producer epoch
		batch.I32(); // base sequence
		auto count = batch.I32();
		if (attributes & 0x20) {
			continue; // a control batch (transaction markers): no data
		}
		bool log_append_time = attributes & 0x08;
		auto body = batch.Raw(batch.Remaining());
		switch (attributes & 0x07) {
		case 0:
			break;
		case 1:
			body = Gunzip(body);
			break;
		case 4:
			body = Unzstd(body);
			break;
		default:
			throw BatchError {UNSUPPORTED_COMPRESSION_TYPE,
			                  "use compression.type none, gzip or zstd: snappy and lz4 are not supported"};
		}
		In records(body);
		auto now = NowMs();
		for (int32_t i = 0; i < count; i++) {
			auto len = records.Varint();
			auto record_bytes = records.Raw(static_cast<size_t>(len));
			In record(record_bytes);
			record.I8(); // attributes
			auto ts_delta = record.Varint();
			record.Varint(); // offset delta
			KafkaRecord r;
			r.timestamp_ms = log_append_time ? now : base_timestamp + ts_delta;
			auto key_len = record.Varint();
			if (key_len >= 0) {
				r.key = record.Raw(static_cast<size_t>(key_len));
			}
			auto value_len = record.Varint();
			if (value_len >= 0) {
				r.value = record.Raw(static_cast<size_t>(value_len));
			}
			// Headers (e.g. CloudEvents ce_*) are read past: Tableflow's tables do not keep them.
			auto headers = record.Varint();
			for (int64_t h = 0; h < headers; h++) {
				record.Raw(static_cast<size_t>(record.Varint()));
				auto hv = record.Varint();
				if (hv > 0) {
					record.Raw(static_cast<size_t>(hv));
				}
			}
			out.push_back(std::move(r));
		}
	}
	return out;
}

} // namespace

KafkaServer::KafkaServer(KafkaLake &lake, std::string advertised_host, int advertised_port)
    : lake_(lake), advertised_host_(std::move(advertised_host)), advertised_port_(advertised_port) {
}

KafkaServer::~KafkaServer() {
	Stop();
}

void KafkaServer::Start(const std::string &host, int port) {
	addrinfo hints {};
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;
	addrinfo *found = nullptr;
	if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &found) != 0 || !found) {
		throw std::runtime_error("cannot resolve " + host);
	}
	listen_fd_ = ::socket(found->ai_family, found->ai_socktype, found->ai_protocol);
	int yes = 1;
	setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
	bool ok =
	    listen_fd_ >= 0 && ::bind(listen_fd_, found->ai_addr, found->ai_addrlen) == 0 && ::listen(listen_fd_, 64) == 0;
	freeaddrinfo(found);
	if (!ok) {
		throw std::runtime_error("cannot listen on " + host + ":" + std::to_string(port) + ": " + strerror(errno));
	}
	accept_thread_ = std::thread([this]() { AcceptLoop(); });
}

void KafkaServer::Stop() {
	if (stopping_.exchange(true)) {
		return;
	}
	if (listen_fd_ >= 0) {
		::shutdown(listen_fd_, SHUT_RDWR);
		::close(listen_fd_);
	}
	if (accept_thread_.joinable()) {
		accept_thread_.join();
	}
}

void KafkaServer::AcceptLoop() {
	while (!stopping_) {
		int fd = ::accept(listen_fd_, nullptr, nullptr);
		if (fd < 0) {
			if (stopping_) {
				return;
			}
			continue;
		}
		int yes = 1;
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
#ifdef SO_NOSIGPIPE
		setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes));
#endif
		std::thread([this, fd]() {
			Serve(fd);
			::close(fd);
		}).detach();
	}
}

void KafkaServer::Serve(int fd) {
	try {
		for (;;) {
			uint32_t size_be;
			if (!ReadExact(fd, &size_be, 4)) {
				return;
			}
			auto size = ntohl(size_be);
			if (size < 8 || size > (100u << 20)) {
				return;
			}
			std::string request(size, '\0');
			if (!ReadExact(fd, &request[0], size)) {
				return;
			}
			In in(request);
			auto api = in.I16();
			auto version = in.I16();
			auto correlation = in.I32();
			in.NullableString(); // client id
			bool flexible = api == API_VERSIONS && version >= 3;
			if (flexible) {
				in.SkipTags();
			}

			const Supported *supported = nullptr;
			for (auto &s : SUPPORTED) {
				if (s.key == api) {
					supported = &s;
				}
			}
			Out body;
			bool flexible_response = false;
			bool respond = true;

			if (api == API_VERSIONS) {
				// Clients send their newest version first; an unknown one gets a v0 answer to retry with.
				bool known = version >= 0 && version <= 3;
				int16_t v = known ? version : 0;
				body.I16(known ? NONE : UNSUPPORTED_VERSION);
				if (v >= 3) {
					body.UVarint(sizeof(SUPPORTED) / sizeof(SUPPORTED[0]) + 1);
					for (auto &s : SUPPORTED) {
						body.I16(s.key);
						body.I16(s.min);
						body.I16(s.max);
						body.UVarint(0);
					}
					body.I32(0);     // throttle time
					body.UVarint(0); // tagged fields
				} else {
					body.I32(sizeof(SUPPORTED) / sizeof(SUPPORTED[0]));
					for (auto &s : SUPPORTED) {
						body.I16(s.key);
						body.I16(s.min);
						body.I16(s.max);
					}
					if (v >= 1) {
						body.I32(0);
					}
				}
			} else if (api == FETCH) {
				Log("kafka: a client tried to consume; this endpoint only accepts producers. Closing the connection");
				return;
			} else if (!supported || version < supported->min || version > supported->max) {
				Log("kafka: unsupported request api " + std::to_string(api) + " v" + std::to_string(version) +
				    "; closing the connection");
				return;
			} else if (api == METADATA) {
				auto count = in.I32();
				std::vector<std::string> requested;
				bool all = count < 0;
				for (int32_t i = 0; i < count; i++) {
					requested.push_back(in.String());
				}
				bool auto_create = in.Bool();
				std::vector<std::pair<std::string, int>> topics;
				std::vector<std::pair<std::string, int16_t>> errors;
				if (all) {
					topics = lake_.Topics();
				} else {
					for (auto &name : requested) {
						if (!KafkaLake::ValidTopicName(name)) {
							errors.emplace_back(name, INVALID_TOPIC);
							continue;
						}
						auto partitions = lake_.Partitions(name);
						if (!partitions && auto_create) {
							lake_.CreateTopic(name, 0);
							partitions = lake_.Partitions(name);
						}
						if (partitions) {
							topics.emplace_back(name, partitions);
						} else {
							errors.emplace_back(name, UNKNOWN_TOPIC_OR_PARTITION);
						}
					}
				}
				body.I32(0); // throttle
				body.I32(1); // brokers
				body.I32(0);
				body.String(advertised_host_);
				body.I32(advertised_port_);
				body.NullableString(std::nullopt); // rack
				body.NullableString(std::string("duckgraphlake"));
				body.I32(0); // controller
				body.I32(static_cast<int32_t>(topics.size() + errors.size()));
				for (auto &t : topics) {
					body.I16(NONE);
					body.String(t.first);
					body.Bool(false);
					body.I32(t.second);
					for (int32_t p = 0; p < t.second; p++) {
						body.I16(NONE);
						body.I32(p);
						body.I32(0); // leader
						if (version >= 7) {
							body.I32(0); // leader epoch
						}
						body.I32(1);
						body.I32(0); // replicas
						body.I32(1);
						body.I32(0); // in sync
						if (version >= 5) {
							body.I32(0); // offline
						}
					}
					if (version >= 8) {
						body.I32(INT32_MIN);
					}
				}
				for (auto &e : errors) {
					body.I16(e.second);
					body.String(e.first);
					body.Bool(false);
					body.I32(0);
					if (version >= 8) {
						body.I32(INT32_MIN);
					}
				}
				if (version >= 8) {
					body.I32(INT32_MIN);
				}
			} else if (api == INIT_PRODUCER_ID) {
				in.NullableString(); // transactional id: transactions are not offered, so none arrive
				in.I32();
				body.I32(0);
				body.I16(NONE);
				body.I64(next_producer_id_++);
				body.I16(0);
			} else if (api == CREATE_TOPICS) {
				auto count = in.I32();
				std::vector<std::pair<std::string, int32_t>> requested;
				for (int32_t i = 0; i < count; i++) {
					auto name = in.String();
					auto partitions = in.I32();
					in.I16(); // replication factor
					auto assignments = in.I32();
					for (int32_t a = 0; a < assignments; a++) {
						in.I32();
						auto brokers = in.I32();
						for (int32_t b = 0; b < brokers; b++) {
							in.I32();
						}
					}
					auto configs = in.I32();
					for (int32_t c = 0; c < configs; c++) {
						in.String();
						in.NullableString();
					}
					requested.emplace_back(name, partitions);
				}
				in.I32(); // timeout
				bool validate_only = in.Bool();
				body.I32(0);
				body.I32(static_cast<int32_t>(requested.size()));
				for (auto &t : requested) {
					body.String(t.first);
					if (!KafkaLake::ValidTopicName(t.first)) {
						body.I16(INVALID_TOPIC);
						body.NullableString(std::string("invalid topic name"));
					} else if (lake_.Partitions(t.first)) {
						body.I16(TOPIC_ALREADY_EXISTS);
						body.NullableString(std::string("topic " + t.first + " already exists"));
					} else {
						if (!validate_only) {
							lake_.CreateTopic(t.first, t.second);
						}
						body.I16(NONE);
						body.NullableString(std::nullopt);
					}
				}
			} else if (api == PRODUCE) {
				in.NullableString(); // transactional id
				auto acks = in.I16();
				in.I32(); // timeout
				auto topic_count = in.I32();
				body.I32(topic_count);
				for (int32_t t = 0; t < topic_count; t++) {
					auto topic = in.String();
					auto partition_count = in.I32();
					body.String(topic);
					body.I32(partition_count);
					for (int32_t p = 0; p < partition_count; p++) {
						auto partition = in.I32();
						auto records = in.NullableBytes();
						int16_t code = NONE;
						std::string message;
						int64_t base_offset = -1;
						try {
							if (!KafkaLake::ValidTopicName(topic)) {
								throw BatchError {INVALID_TOPIC, "invalid topic name"};
							}
							if (!lake_.Partitions(topic)) {
								lake_.CreateTopic(topic, 0); // as brokers with auto.create.topics.enable do
							}
							if (partition < 0 || partition >= lake_.Partitions(topic)) {
								throw BatchError {UNKNOWN_TOPIC_OR_PARTITION, "no such partition"};
							}
							auto parsed = records ? ParseRecords(*records) : std::vector<KafkaRecord>();
							base_offset = parsed.empty() ? -1 : lake_.Append(topic, partition, parsed);
						} catch (BatchError &e) {
							code = e.code;
							message = e.message;
						} catch (KafkaError &e) {
							code = CORRUPT_MESSAGE;
							message = e.what();
						} catch (std::exception &e) {
							code = KAFKA_STORAGE_ERROR;
							message = e.what();
							Log("kafka: produce to " + topic + " failed: " + message);
						}
						body.I32(partition);
						body.I16(code);
						body.I64(base_offset);
						body.I64(-1); // log append time
						if (version >= 5) {
							body.I64(0); // log start offset
						}
						if (version >= 8) {
							body.I32(0); // record errors
							body.NullableString(message.empty() ? std::nullopt : std::optional<std::string>(message));
						}
					}
				}
				body.I32(0); // throttle
				respond = acks != 0;
			}

			if (!respond) {
				continue;
			}
			Out frame;
			frame.I32(0); // size, set below
			frame.I32(correlation);
			if (flexible_response) {
				frame.UVarint(0);
			}
			frame.Data() += body.Data();
			auto total = static_cast<uint32_t>(frame.Data().size() - 4);
			uint32_t total_be = htonl(total);
			std::memcpy(&frame.Data()[0], &total_be, 4);
			if (!WriteAll(fd, frame.Data())) {
				return;
			}
		}
	} catch (std::exception &ex) {
		Log(std::string("kafka: closing a connection: ") + ex.what());
	}
}

} // namespace dgl
