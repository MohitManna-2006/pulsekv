// PulseKV v2 data-plane node -- gRPC shim.
//
// This process is the network face of one data-plane node. It implements
// NodeService (proto/node.proto) using gRPC's C++ API and forwards every
// request across the `extern "C"` boundary in node/engine/include/pulsekv_engine.h
// into the pure-C storage engine. See node/README.md for why the boundary sits
// here rather than making the engine speak gRPC itself.
//
// THE RULE THIS FILE IS HELD TO: no storage logic. Every handler below unpacks
// a protobuf, validates it against limits, calls exactly one engine function,
// packs the result, and returns a status. Hashing, locking, eviction, tier
// placement, and spill I/O all live in C, testable with no network stack in
// the picture. If a handler here ever grows a branch that depends on what is
// *in* the store, that branch belongs in node/engine/.
//
// The only header this file includes from node/engine/ is pulsekv_engine.h,
// and that is enforced by CMake rather than by discipline: the engine target
// exports include/ as PUBLIC and src/ as PRIVATE, so hashtable.h and tiering.h
// are not on this file's include path at all.

#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>

#if PULSEKV_HAVE_GRPC_REFLECTION
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#endif

#include "node.grpc.pb.h"
#include "pulsekv_engine.h"

namespace nodev1 = pulsekv::node::v1;

namespace {

// The unary/chunked dividing line, taken from the contract itself rather than
// duplicated as a local constant.
constexpr uint64_t kUnaryValueLimit =
    static_cast<uint64_t>(nodev1::UNARY_VALUE_LIMIT_BYTES);

// gRPC's own message ceiling, set above the unary limit on purpose. The
// headroom is what lets a slightly-oversized unary Put reach our handler and
// get a specific "use PutChunked" reply, instead of being killed by the
// transport with a generic RESOURCE_EXHAUSTED. Far past this, the transport
// limit does apply -- at which point refusing to buffer it is the right answer
// anyway.
constexpr int kMaxMessageBytes = 8 * 1024 * 1024;

// GetChunked frame size. Comfortably inside kMaxMessageBytes with room for the
// message overhead, and large enough that a multi-megabyte value is a handful
// of frames rather than hundreds.
constexpr size_t kChunkBytes = 1024 * 1024;

// Keys are identifiers, not payloads. Bounding them keeps a hostile client from
// turning the in-RAM index into the attack surface, and the engine's key length
// is a uint32 that this must not be allowed to overflow.
constexpr size_t kMaxKeyBytes = 64 * 1024;

// Self-pipe so the signal handler does nothing but an async-signal-safe
// write(2). grpc::Server::Shutdown() is emphatically not safe to call from a
// handler, so the handler only wakes main up and main does the real work.
int g_shutdown_pipe[2] = {-1, -1};

void HandleSignal(int sig) {
  const unsigned char byte = static_cast<unsigned char>(sig);
  ssize_t n;
  do {
    n = write(g_shutdown_pipe[1], &byte, 1);
  } while (n < 0 && errno == EINTR);
  (void)n;
}

grpc::Status Invalid(const std::string& why) {
  return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, why);
}

// One place that decides how an engine result reaches the wire. PK_ENGINE_OK
// and PK_ENGINE_NOT_FOUND are handled by the callers, because a miss is a
// successful response with found = false rather than an error.
grpc::Status EngineFailure(pk_engine_result_t rc, const char* op) {
  const std::string detail =
      std::string(op) + ": " + pk_engine_strerror(rc);
  switch (rc) {
    case PK_ENGINE_TOO_LARGE:
      return grpc::Status(grpc::StatusCode::OUT_OF_RANGE, detail);
    case PK_ENGINE_INVALID:
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, detail);
    case PK_ENGINE_NOMEM:
      return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, detail);
    case PK_ENGINE_IO_ERROR:
      // The NVMe tier failed to hand back a value it is holding. That is this
      // node's problem, not the caller's request being wrong.
      return grpc::Status(grpc::StatusCode::INTERNAL, detail);
    case PK_ENGINE_OK:
    case PK_ENGINE_NOT_FOUND:
      break;
  }
  return grpc::Status(grpc::StatusCode::UNKNOWN, detail);
}

grpc::Status ValidateKey(const std::string& key) {
  if (key.empty())
    return Invalid("key must not be empty");
  if (key.size() > kMaxKeyBytes)
    return Invalid("key of " + std::to_string(key.size()) +
                   " bytes exceeds the " + std::to_string(kMaxKeyBytes) +
                   "-byte limit");
  return grpc::Status::OK;
}

const uint8_t* Bytes(const std::string& s) {
  return reinterpret_cast<const uint8_t*>(s.data());
}

class NodeServiceImpl final : public nodev1::NodeService::Service {
 public:
  NodeServiceImpl(std::string node_id, pk_engine_t* engine)
      : node_id_(std::move(node_id)),
        engine_(engine),
        started_(std::chrono::steady_clock::now()) {}

  grpc::Status HealthCheck(grpc::ServerContext* /*context*/,
                           const nodev1::HealthCheckRequest* /*request*/,
                           nodev1::HealthCheckResponse* response) override {
    response->set_ok(true);
    response->set_node_id(node_id_);
    response->set_uptime_seconds(UptimeSeconds());
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext* /*context*/,
                   const nodev1::GetRequest* request,
                   nodev1::GetResponse* response) override {
    const std::string& key = request->key();
    grpc::Status bad = ValidateKey(key);
    if (!bad.ok())
      return bad;

    uint8_t* value = nullptr;
    uint64_t len = 0;
    pk_engine_result_t rc = pk_engine_get(
        engine_, Bytes(key), static_cast<uint32_t>(key.size()), &value, &len);

    if (rc == PK_ENGINE_NOT_FOUND) {
      // A miss is a normal, successful response. Reporting it as a gRPC error
      // would make every cache miss look like a failure in the caller's metrics.
      response->set_found(false);
      return grpc::Status::OK;
    }
    if (rc != PK_ENGINE_OK)
      return EngineFailure(rc, "Get");

    if (len > kUnaryValueLimit) {
      pk_engine_free_value(value);
      return grpc::Status(
          grpc::StatusCode::FAILED_PRECONDITION,
          "stored value is " + std::to_string(len) + " bytes, above the " +
              std::to_string(kUnaryValueLimit) +
              "-byte unary limit; use NodeService.GetChunked");
    }

    response->set_found(true);
    if (len > 0)
      response->set_value(value, static_cast<size_t>(len));
    pk_engine_free_value(value);
    return grpc::Status::OK;
  }

  grpc::Status Put(grpc::ServerContext* /*context*/,
                   const nodev1::PutRequest* request,
                   nodev1::PutResponse* response) override {
    const std::string& key = request->key();
    grpc::Status bad = ValidateKey(key);
    if (!bad.ok())
      return bad;

    const std::string& value = request->value();
    // Loud and specific beats quietly working until it doesn't: an oversized
    // unary write is refused with the name of the RPC that can carry it.
    if (value.size() > kUnaryValueLimit) {
      return Invalid("value of " + std::to_string(value.size()) +
                     " bytes exceeds the " + std::to_string(kUnaryValueLimit) +
                     "-byte unary limit; use NodeService.PutChunked");
    }

    pk_engine_result_t rc =
        pk_engine_put(engine_, Bytes(key), static_cast<uint32_t>(key.size()),
                      Bytes(value), value.size());
    if (rc != PK_ENGINE_OK)
      return EngineFailure(rc, "Put");

    response->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status PutChunked(grpc::ServerContext* /*context*/,
                          grpc::ServerReader<nodev1::PutChunk>* reader,
                          nodev1::PutResponse* response) override {
    nodev1::PutChunk chunk;
    std::string key;
    std::string buffer;
    uint64_t total_length = 0;
    uint32_t total_chunks = 0;
    uint32_t next_index = 0;

    while (reader->Read(&chunk)) {
      // gRPC already guarantees stream ordering, so a gap or a repeat means a
      // buggy client rather than a network reordering. Rejecting is both
      // cheaper and more honest than reassembling around it.
      if (chunk.chunk_index() != next_index) {
        return Invalid("expected chunk_index " + std::to_string(next_index) +
                       ", got " + std::to_string(chunk.chunk_index()) +
                       "; chunks must arrive in order starting at 0");
      }

      if (next_index == 0) {
        key = chunk.key();
        grpc::Status bad = ValidateKey(key);
        if (!bad.ok())
          return bad;

        total_chunks = chunk.total_chunks();
        if (total_chunks == 0)
          return Invalid("first chunk must declare total_chunks > 0");

        total_length = chunk.total_length();
        // Checked BEFORE a single byte is buffered or reserved, so a corrupt or
        // hostile total_length cannot turn into an allocation.
        const uint64_t max_value = pk_engine_max_value_bytes(engine_);
        if (total_length > max_value) {
          return grpc::Status(
              grpc::StatusCode::OUT_OF_RANGE,
              "declared total_length " + std::to_string(total_length) +
                  " exceeds this node's max-value-bytes of " +
                  std::to_string(max_value));
        }
        buffer.reserve(static_cast<size_t>(total_length));
      } else {
        // The key may be repeated identically or omitted; anything else means
        // two different writes got spliced into one stream.
        if (!chunk.key().empty() && chunk.key() != key)
          return Invalid("chunk " + std::to_string(next_index) +
                         " carries a different key than chunk 0");
        if (chunk.total_chunks() != 0 && chunk.total_chunks() != total_chunks)
          return Invalid("total_chunks changed mid-stream");
        if (chunk.total_length() != 0 && chunk.total_length() != total_length)
          return Invalid("total_length changed mid-stream");
      }

      // Cut a lying stream off at the point it starts lying, rather than after
      // buffering everything it wanted to send.
      if (buffer.size() + chunk.data().size() > total_length) {
        return Invalid("chunk data exceeds the declared total_length of " +
                       std::to_string(total_length));
      }
      buffer.append(chunk.data());

      next_index++;
      if (next_index > total_chunks)
        return Invalid("received more chunks than the declared total_chunks of " +
                       std::to_string(total_chunks));
    }

    if (next_index == 0)
      return Invalid("PutChunked stream contained no chunks");
    if (next_index != total_chunks) {
      return Invalid("stream ended after " + std::to_string(next_index) +
                     " chunks but declared " + std::to_string(total_chunks));
    }
    if (buffer.size() != total_length) {
      return Invalid("stream carried " + std::to_string(buffer.size()) +
                     " bytes but declared total_length " +
                     std::to_string(total_length));
    }

    pk_engine_result_t rc =
        pk_engine_put(engine_, Bytes(key), static_cast<uint32_t>(key.size()),
                      Bytes(buffer), buffer.size());
    if (rc != PK_ENGINE_OK)
      return EngineFailure(rc, "PutChunked");

    response->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status GetChunked(
      grpc::ServerContext* context, const nodev1::GetRequest* request,
      grpc::ServerWriter<nodev1::GetChunk>* writer) override {
    const std::string& key = request->key();
    grpc::Status bad = ValidateKey(key);
    if (!bad.ok())
      return bad;

    uint8_t* value = nullptr;
    uint64_t len = 0;
    pk_engine_result_t rc = pk_engine_get(
        engine_, Bytes(key), static_cast<uint32_t>(key.size()), &value, &len);

    if (rc == PK_ENGINE_NOT_FOUND) {
      // An empty stream, mirroring Get's found = false. Documented in
      // proto/node.proto so a caller does not have to guess.
      return grpc::Status::OK;
    }
    if (rc != PK_ENGINE_OK)
      return EngineFailure(rc, "GetChunked");

    // A zero-length value is still a hit, so it gets one empty chunk rather
    // than the empty stream that means "miss".
    const uint32_t total_chunks =
        len == 0 ? 1u
                 : static_cast<uint32_t>((len + kChunkBytes - 1) / kChunkBytes);

    uint64_t offset = 0;
    for (uint32_t i = 0; i < total_chunks; i++) {
      if (context->IsCancelled()) {
        pk_engine_free_value(value);
        return grpc::Status(grpc::StatusCode::CANCELLED, "client went away");
      }

      const size_t n =
          static_cast<size_t>(std::min<uint64_t>(kChunkBytes, len - offset));

      nodev1::GetChunk out;
      out.set_chunk_index(i);
      out.set_total_chunks(total_chunks);
      out.set_total_length(len);
      if (n > 0)
        out.set_data(value + offset, n);

      if (!writer->Write(out)) {
        pk_engine_free_value(value);
        return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                            "stream closed before the value was fully sent");
      }
      offset += n;
    }

    pk_engine_free_value(value);
    return grpc::Status::OK;
  }

  grpc::Status PrefixMatch(
      grpc::ServerContext* context, const nodev1::PrefixMatchRequest* request,
      grpc::ServerWriter<nodev1::PrefixMatchResponse>* writer) override {
    const std::string& prefix = request->prefix();
    if (prefix.size() > kMaxKeyBytes)
      return Invalid("prefix is longer than the maximum key length");

    // Two passes on purpose. The engine hands back matching KEYS under the
    // shard locks, then releases them; values are fetched one at a time
    // afterwards. That means no shard lock is ever held while a multi-megabyte
    // value is copied or written to a possibly-slow client.
    //
    // Cost is O(total keys): the table has no ordered iteration, so this is a
    // full scan of all 256 shards. Honest about it rather than pretending
    // otherwise -- see docs/pulsekv-v2-phase1-summary.md.
    pk_engine_keyset_t keys;
    pk_engine_result_t rc = pk_engine_scan_prefix(
        engine_, prefix.empty() ? nullptr : Bytes(prefix),
        static_cast<uint32_t>(prefix.size()), &keys);
    if (rc != PK_ENGINE_OK)
      return EngineFailure(rc, "PrefixMatch");

    grpc::Status status = grpc::Status::OK;
    for (size_t i = 0; i < keys.count; i++) {
      if (context->IsCancelled()) {
        status = grpc::Status(grpc::StatusCode::CANCELLED, "client went away");
        break;
      }

      uint8_t* value = nullptr;
      uint64_t len = 0;
      // peek, not get: walking the whole keyspace must not promote every
      // spilled value it touches and evict the actual working set.
      pk_engine_result_t vrc = pk_engine_peek(engine_, keys.keys[i].key,
                                              keys.keys[i].key_len, &value, &len);
      if (vrc == PK_ENGINE_NOT_FOUND) {
        // Evicted between the scan and the fetch. Expected; the scan is not a
        // snapshot and does not claim to be.
        continue;
      }
      if (vrc != PK_ENGINE_OK) {
        pk_engine_free_value(value);
        continue;  // a single unreadable spill file must not fail the scan
      }

      nodev1::PrefixMatchResponse out;
      out.set_key(keys.keys[i].key, keys.keys[i].key_len);
      if (len > kUnaryValueLimit) {
        // Too big to inline. Say so explicitly rather than sending an empty
        // value that looks identical to a genuinely empty one.
        out.set_value_omitted(true);
      } else if (len > 0) {
        out.set_value(value, static_cast<size_t>(len));
      }
      pk_engine_free_value(value);

      if (!writer->Write(out)) {
        status = grpc::Status(grpc::StatusCode::UNAVAILABLE,
                              "stream closed during the scan");
        break;
      }
    }

    pk_engine_free_keyset(&keys);
    return status;
  }

  grpc::Status Capacity(grpc::ServerContext* /*context*/,
                        const nodev1::CapacityRequest* /*request*/,
                        nodev1::CapacityResponse* response) override {
    pk_engine_capacity_t cap;
    pk_engine_capacity(engine_, &cap);
    response->set_resident_keys(cap.resident_keys);
    response->set_bytes_in_ram_tier(cap.bytes_in_ram_tier);
    response->set_bytes_in_nvme_tier(cap.bytes_in_nvme_tier);
    return grpc::Status::OK;
  }

 private:
  int64_t UptimeSeconds() const {
    const auto elapsed = std::chrono::steady_clock::now() - started_;
    return std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
  }

  const std::string node_id_;
  pk_engine_t* const engine_;
  const std::chrono::steady_clock::time_point started_;
};

struct Options {
  std::string node_id;
  std::string host = "127.0.0.1";
  int port = 0;
  std::string data_dir;
  uint64_t ram_budget_bytes = PK_ENGINE_DEFAULT_RAM_BUDGET_BYTES;
  uint64_t max_value_bytes = PK_ENGINE_DEFAULT_MAX_VALUE_BYTES;
};

void PrintUsage(const char* argv0) {
  std::fprintf(
      stderr,
      "usage: %s --node-id ID --port PORT [options]\n"
      "\n"
      "  --node-id ID              identity this node reports from HealthCheck\n"
      "  --port PORT               TCP port to serve NodeService on\n"
      "  --host HOST               bind address (default 127.0.0.1)\n"
      "  --data-dir PATH           NVMe spill directory for this node.\n"
      "                            The engine creates and EXCLUSIVELY OWNS\n"
      "                            PATH/spill/, and purges it at startup and\n"
      "                            shutdown -- spill files are unreachable once\n"
      "                            the in-RAM index naming them is gone. Nodes\n"
      "                            must not share a directory. Omitting this\n"
      "                            disables the NVMe tier: eviction then drops\n"
      "                            entries instead of spilling them.\n"
      "  --ram-budget-bytes N      RAM tier budget, in bytes (default %llu).\n"
      "                            Divided evenly across 256 shards.\n"
      "  --max-value-bytes N       hard ceiling per value, in bytes (default %llu)\n",
      argv0, (unsigned long long)PK_ENGINE_DEFAULT_RAM_BUDGET_BYTES,
      (unsigned long long)PK_ENGINE_DEFAULT_MAX_VALUE_BYTES);
}

bool ParseU64(const char* text, uint64_t* out, const char* flag) {
  char* end = nullptr;
  errno = 0;
  const unsigned long long parsed = std::strtoull(text, &end, 10);
  if (end == text || *end != '\0' || errno == ERANGE || parsed == 0) {
    std::fprintf(stderr, "error: %s expects a positive integer, got %s\n", flag,
                 text);
    return false;
  }
  *out = static_cast<uint64_t>(parsed);
  return true;
}

// Hand-rolled rather than pulling in gflags/absl::flags: a handful of options
// does not justify a dependency in a process whose entire job is to be thin.
bool ParseOptions(int argc, char** argv, Options* out) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    std::string name = arg;
    std::string value;
    bool inlined = false;
    const size_t eq = arg.find('=');
    if (arg.rfind("--", 0) == 0 && eq != std::string::npos) {
      name = arg.substr(0, eq);
      value = arg.substr(eq + 1);
      inlined = true;
    }

    auto next_value = [&](const char* flag) -> bool {
      if (inlined) return true;
      if (i + 1 >= argc) {
        std::fprintf(stderr, "error: %s requires a value\n", flag);
        return false;
      }
      value = argv[++i];
      return true;
    };

    if (name == "--node-id") {
      if (!next_value("--node-id")) return false;
      out->node_id = value;
    } else if (name == "--host") {
      if (!next_value("--host")) return false;
      out->host = value;
    } else if (name == "--data-dir") {
      if (!next_value("--data-dir")) return false;
      out->data_dir = value;
    } else if (name == "--ram-budget-bytes") {
      if (!next_value("--ram-budget-bytes")) return false;
      if (!ParseU64(value.c_str(), &out->ram_budget_bytes, "--ram-budget-bytes"))
        return false;
    } else if (name == "--max-value-bytes") {
      if (!next_value("--max-value-bytes")) return false;
      if (!ParseU64(value.c_str(), &out->max_value_bytes, "--max-value-bytes"))
        return false;
    } else if (name == "--port") {
      if (!next_value("--port")) return false;
      char* end = nullptr;
      const long parsed = std::strtol(value.c_str(), &end, 10);
      if (end == value.c_str() || *end != '\0' || parsed <= 0 || parsed > 65535) {
        std::fprintf(stderr, "error: --port %s is not in 1..65535\n",
                     value.c_str());
        return false;
      }
      out->port = static_cast<int>(parsed);
    } else if (name == "--help" || name == "-h") {
      PrintUsage(argv[0]);
      std::exit(0);
    } else {
      std::fprintf(stderr, "error: unknown argument %s\n", arg.c_str());
      return false;
    }
  }

  if (out->node_id.empty()) {
    std::fprintf(stderr, "error: --node-id is required\n");
    return false;
  }
  if (out->port == 0) {
    std::fprintf(stderr, "error: --port is required\n");
    return false;
  }
  if (out->max_value_bytes > static_cast<uint64_t>(UINT32_MAX)) {
    // Not a hard engine limit, but a value this size in one contiguous
    // allocation is not something Phase 1 should pretend to support: the
    // chunked path buffers it whole. Phase 6 is where streaming straight into
    // the engine belongs.
    std::fprintf(stderr,
                 "error: --max-value-bytes above %llu is not supported in "
                 "Phase 1; chunked writes are buffered whole\n",
                 (unsigned long long)UINT32_MAX);
    return false;
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!ParseOptions(argc, argv, &options)) {
    PrintUsage(argv[0]);
    return 2;
  }

  // Unbuffered-ish logging: these processes are launched into log files by
  // deploy/run-local-cluster.sh, and a half-written startup line in a file
  // nobody flushed is a bad first debugging experience.
  std::setvbuf(stdout, nullptr, _IOLBF, 0);

  if (pipe(g_shutdown_pipe) != 0) {
    std::fprintf(stderr, "[%s] fatal: pipe: %s\n", options.node_id.c_str(),
                 std::strerror(errno));
    return 1;
  }

  struct sigaction sa;
  std::memset(&sa, 0, sizeof(sa));
  sa.sa_handler = HandleSignal;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_RESTART;
  sigaction(SIGINT, &sa, nullptr);
  sigaction(SIGTERM, &sa, nullptr);

  pk_engine_config_t engine_cfg;
  std::memset(&engine_cfg, 0, sizeof(engine_cfg));
  engine_cfg.data_dir =
      options.data_dir.empty() ? nullptr : options.data_dir.c_str();
  engine_cfg.ram_budget_bytes = options.ram_budget_bytes;
  engine_cfg.max_value_bytes = options.max_value_bytes;

  pk_engine_t* engine = pk_engine_create(&engine_cfg);
  if (engine == nullptr) {
    // A data-dir that was asked for but is unusable fails here rather than
    // degrading the node to a fraction of its configured capacity while
    // reporting itself healthy.
    std::fprintf(stderr, "[%s] fatal: could not create the storage engine",
                 options.node_id.c_str());
    if (!options.data_dir.empty())
      std::fprintf(stderr, " (is --data-dir %s writable?)",
                   options.data_dir.c_str());
    std::fprintf(stderr, "\n");
    return 1;
  }

  const std::string address = options.host + ":" + std::to_string(options.port);
  NodeServiceImpl service(options.node_id, engine);

#if PULSEKV_HAVE_GRPC_REFLECTION
  // Lets `grpcurl` talk to a running node without being handed the .proto
  // files. Purely a development affordance.
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
#endif

  grpc::ServerBuilder builder;

  // gRPC C++ enables SO_REUSEPORT on listening sockets by default. On a
  // single-machine dev cluster that is actively harmful: two nodes handed the
  // same port would BOTH bind successfully, and the kernel would split
  // connections between them. Debugging a cluster where half the requests go
  // to a node that thinks it is someone else is not a good afternoon. Turning
  // it off makes a port collision a loud, immediate failure instead.
  builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);

  builder.SetMaxReceiveMessageSize(kMaxMessageBytes);
  builder.SetMaxSendMessageSize(kMaxMessageBytes);

  int bound_port = 0;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials(),
                           &bound_port);
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (!server || bound_port == 0) {
    // BuildAndStart reports a bind failure by leaving bound_port at 0 rather
    // than by throwing, so this check is the only thing standing between a
    // port collision and a node that looks started but answers nothing.
    std::fprintf(stderr, "[%s] fatal: could not bind %s (port in use?)\n",
                 options.node_id.c_str(), address.c_str());
    pk_engine_destroy(engine);
    return 1;
  }

  std::printf("[%s] NodeService listening on %s (pid %d)\n",
              options.node_id.c_str(), address.c_str(),
              static_cast<int>(getpid()));
  std::printf("[%s] engine: ram-budget=%llu bytes (%llu per shard), "
              "max-value=%llu bytes, unary limit=%llu bytes\n",
              options.node_id.c_str(),
              (unsigned long long)pk_engine_ram_budget_bytes(engine),
              (unsigned long long)(pk_engine_ram_budget_bytes(engine) / 256),
              (unsigned long long)pk_engine_max_value_bytes(engine),
              (unsigned long long)kUnaryValueLimit);
  if (options.data_dir.empty()) {
    std::printf("[%s] engine: NO --data-dir, NVMe tier disabled -- eviction "
                "drops entries instead of spilling them\n",
                options.node_id.c_str());
  } else {
    std::printf("[%s] engine: nvme tier at %s/spill (purged at start and stop)\n",
                options.node_id.c_str(), options.data_dir.c_str());
  }

  std::thread serving([&server] { server->Wait(); });

  // Block until a signal arrives on the self-pipe.
  unsigned char sig = 0;
  ssize_t n;
  do {
    n = read(g_shutdown_pipe[0], &sig, 1);
  } while (n < 0 && errno == EINTR);

  std::printf("[%s] received signal %d, shutting down\n",
              options.node_id.c_str(), static_cast<int>(sig));
  server->Shutdown();
  serving.join();

  // After Shutdown() returns and the serving thread has joined, no handler can
  // still be inside the engine -- which is exactly the precondition
  // pk_engine_destroy requires.
  pk_engine_capacity_t cap;
  pk_engine_capacity(engine, &cap);
  std::printf("[%s] final: %llu keys, %llu bytes RAM, %llu bytes NVMe, "
              "%llu spills, %llu promotions, %llu spill errors\n",
              options.node_id.c_str(), (unsigned long long)cap.resident_keys,
              (unsigned long long)cap.bytes_in_ram_tier,
              (unsigned long long)cap.bytes_in_nvme_tier,
              (unsigned long long)cap.spills,
              (unsigned long long)cap.promotions,
              (unsigned long long)cap.spill_errors);

  pk_engine_destroy(engine);
  std::printf("[%s] stopped\n", options.node_id.c_str());
  return 0;
}
