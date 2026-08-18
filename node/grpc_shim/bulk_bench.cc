// PulseKV v2 bulk-transfer benchmark -- Phase 6, step 6.3.
//
// THIS IS A BENCHMARK HARNESS, NOT AN ADAPTER. Phase 7 builds the real SGLang
// HiCache backend. What this is doing that resembles one is the "synthetic
// adapter" the phase needs: a separate, co-located process that pulls
// multi-megabyte values out of a node over the shared-memory path, which is
// exactly the shape a real adapter has and exactly the case the design doc's
// HiCache reference pattern describes.
//
// The discipline is v1's, unchanged: every single read is verified byte-for-byte
// against what was written, an unverified read fails the whole run, and a
// transport that turns out not to help gets reported as not helping.
//
// The two cases the phase asks about map onto the two socket families:
//
//   node-to-node     TCP. A different process, possibly a different host. This
//                    is the path Phase 4's replication forwarding and catch-up
//                    scan take for large values.
//   node-to-adapter  Unix socket, same host, with a memfd handed over. No
//                    payload bytes cross the socket at all.

#include <errno.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <thread>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "bulk.h"
#include "metadata.grpc.pb.h"
#include "node.grpc.pb.h"

namespace nodev1 = pulsekv::node::v1;
namespace metadatav1 = pulsekv::metadata::v1;
namespace bulk = pulsekv::bulk;

namespace {

constexpr int kMaxMessageBytes = 8 * 1024 * 1024;
constexpr size_t kChunkBytes = 1024 * 1024;

using Clock = std::chrono::steady_clock;

// Same xorshift64* the engine tests and pulsekv-smoke use, so a value that
// round-trips byte-for-byte here means the same thing it means there.
std::vector<uint8_t> DeterministicValue(size_t length, uint64_t seed) {
  std::vector<uint8_t> out(length);
  uint64_t state = seed * 2862933555777941757ULL + 3037000493ULL;
  if (state == 0) state = 1;
  size_t offset = 0;
  while (offset < length) {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    uint64_t word = state * 0x2545F4914F6CDD1DULL;
    size_t n = std::min(sizeof(word), length - offset);
    memcpy(out.data() + offset, &word, n);
    offset += n;
  }
  return out;
}

struct Sample {
  std::vector<double> millis;
  uint64_t bytes = 0;
  uint64_t verified = 0;
  uint64_t failures = 0;
  bool used_shared_memory = false;

  void Add(double ms, uint64_t transferred) {
    millis.push_back(ms);
    bytes += transferred;
  }

  double Percentile(double fraction) const {
    if (millis.empty()) return 0.0;
    std::vector<double> sorted = millis;
    std::sort(sorted.begin(), sorted.end());
    size_t index = static_cast<size_t>(fraction * (sorted.size() - 1) + 0.5);
    return sorted[std::min(index, sorted.size() - 1)];
  }

  double TotalSeconds() const {
    double total = 0.0;
    for (double ms : millis) total += ms;
    return total / 1000.0;
  }

  double ThroughputMiBs() const {
    double seconds = TotalSeconds();
    if (seconds <= 0.0) return 0.0;
    return (static_cast<double>(bytes) / (1024.0 * 1024.0)) / seconds;
  }

  // AggregateMiBs divides by WALL CLOCK rather than by summed per-request time.
  // With concurrent readers those are very different numbers, and the aggregate
  // is the one that says what the server can actually deliver.
  double AggregateMiBs(double wall_seconds) const {
    if (wall_seconds <= 0.0) return 0.0;
    return (static_cast<double>(bytes) / (1024.0 * 1024.0)) / wall_seconds;
  }

  void Merge(const Sample& other) {
    millis.insert(millis.end(), other.millis.begin(), other.millis.end());
    bytes += other.bytes;
    verified += other.verified;
    failures += other.failures;
    used_shared_memory = used_shared_memory || other.used_shared_memory;
  }

  double wall_seconds = 0.0;
};

void Report(const char* label, const Sample& sample, const Sample* baseline) {
  if (sample.millis.empty()) {
    std::printf("  %-28s SKIPPED\n", label);
    return;
  }
  const double aggregate = sample.AggregateMiBs(sample.wall_seconds);
  std::printf("  %-28s p50 %8.2f ms  p95 %8.2f ms  p99 %8.2f ms  %9.1f MiB/s agg  "
              "verified %llu/%llu",
              label, sample.Percentile(0.50), sample.Percentile(0.95),
              sample.Percentile(0.99), aggregate,
              (unsigned long long)sample.verified,
              (unsigned long long)(sample.verified + sample.failures));
  if (baseline != nullptr && !baseline->millis.empty()) {
    const double base_aggregate = baseline->AggregateMiBs(baseline->wall_seconds);
    if (base_aggregate > 0.0) std::printf("  %5.2fx vs baseline", aggregate / base_aggregate);
  }
  if (sample.used_shared_memory) std::printf("  [shared memory]");
  std::printf("\n");
}

// ---------------------------------------------------------------------------
// gRPC baseline (Phase 1's chunked path)
// ---------------------------------------------------------------------------

std::shared_ptr<grpc::Channel> DialNode(const std::string& address) {
  grpc::ChannelArguments args;
  args.SetMaxSendMessageSize(kMaxMessageBytes);
  args.SetMaxReceiveMessageSize(kMaxMessageBytes);
  return grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
}

bool PutChunked(nodev1::NodeService::Stub& stub, const std::string& key,
                const std::vector<uint8_t>& value, std::string* error) {
  grpc::ClientContext context;
  nodev1::PutResponse response;
  auto writer = stub.PutChunked(&context, &response);

  const size_t total_chunks = value.empty() ? 1 : (value.size() + kChunkBytes - 1) / kChunkBytes;
  for (size_t i = 0; i < total_chunks; i++) {
    const size_t lo = i * kChunkBytes;
    const size_t hi = std::min(lo + kChunkBytes, value.size());
    nodev1::PutChunk chunk;
    chunk.set_chunk_index(static_cast<uint32_t>(i));
    chunk.set_total_chunks(static_cast<uint32_t>(total_chunks));
    chunk.set_total_length(value.size());
    if (hi > lo) chunk.set_data(value.data() + lo, hi - lo);
    if (i == 0) chunk.set_key(key);
    if (!writer->Write(chunk)) break;
  }
  writer->WritesDone();
  grpc::Status status = writer->Finish();
  if (!status.ok()) {
    *error = status.error_message();
    return false;
  }
  return true;
}

bool GetChunked(nodev1::NodeService::Stub& stub, const std::string& key,
                std::vector<uint8_t>* out, std::string* error) {
  grpc::ClientContext context;
  nodev1::GetRequest request;
  request.set_key(key);
  auto reader = stub.GetChunked(&context, request);

  out->clear();
  nodev1::GetChunk chunk;
  uint32_t expected = 0;
  while (reader->Read(&chunk)) {
    if (chunk.chunk_index() != expected) {
      *error = "chunk arrived out of order";
      return false;
    }
    expected++;
    out->insert(out->end(), chunk.data().begin(), chunk.data().end());
  }
  grpc::Status status = reader->Finish();
  if (!status.ok()) {
    *error = status.error_message();
    return false;
  }
  return expected > 0;
}

// ---------------------------------------------------------------------------
// Measurement passes
// ---------------------------------------------------------------------------

struct Settings {
  std::string node_address = "127.0.0.1:7100";
  std::string node_id;
  std::string socket_dir = "/tmp/pulsekv-bulk";
  int bulk_port_offset = 1000;
  size_t value_bytes = 8 * 1024 * 1024;
  int iterations = 20;
  int warmup = 3;
  // Concurrent readers. Single-stream loopback measures raw bandwidth, which is
  // enormous and hides the thing that actually limits a storage server: CPU
  // spent per byte. With several readers the transports separate on cost rather
  // than on link speed, which is the workload a node serving adapters has.
  int concurrency = 1;
  bool run_grpc = true;
  bool run_tcp = true;
  bool run_unix_inline = true;
  bool run_unix_memfd = true;
};

bool VerifyAgainst(const uint8_t* got, size_t got_len, const std::vector<uint8_t>& want) {
  return got_len == want.size() && memcmp(got, want.data(), want.size()) == 0;
}

// MeasureGrpc is the Phase 1 baseline every other number is compared against.
Sample MeasureGrpc(nodev1::NodeService::Stub& stub, const std::string& key,
                   const std::vector<uint8_t>& want, const Settings& settings) {
  Sample sample;
  std::vector<uint8_t> got;
  std::string error;
  for (int i = 0; i < settings.warmup + settings.iterations; i++) {
    const auto started = Clock::now();
    bool ok = GetChunked(stub, key, &got, &error);
    const double ms = std::chrono::duration<double, std::milli>(Clock::now() - started).count();
    if (i < settings.warmup) continue;
    if (!ok || !VerifyAgainst(got.data(), got.size(), want)) {
      sample.failures++;
      continue;
    }
    sample.verified++;
    sample.Add(ms, got.size());
  }
  return sample;
}

// MeasureBulk covers all three bulk variants; which one it is depends entirely
// on the endpoint it was handed and whether memfd was requested.
Sample MeasureBulk(const bulk::Endpoint& endpoint, bool accept_memfd, const std::string& key,
                   const std::vector<uint8_t>& want, const Settings& settings,
                   std::string* error) {
  Sample sample;
  bulk::ClientOptions options;
  options.accept_memfd = accept_memfd;
  options.expect_node_id = settings.node_id;

  auto client = bulk::Client::Connect(endpoint, options, error);
  if (!client) return sample;

  for (int i = 0; i < settings.warmup + settings.iterations; i++) {
    bulk::Blob blob;
    bool found = false;
    std::string local_error;
    const auto started = Clock::now();
    bool ok = client->Get(key, &blob, &found, &local_error);
    // The mapping is what a real consumer reads through, so verification runs
    // against it directly -- copying it out first would measure a copy this
    // path exists to avoid.
    bool verified = ok && found && VerifyAgainst(blob.data(), blob.size(), want);
    const double ms = std::chrono::duration<double, std::milli>(Clock::now() - started).count();

    if (i == 0) sample.used_shared_memory = blob.mapped();
    if (i < settings.warmup) continue;
    if (!verified) {
      sample.failures++;
      if (error->empty() && !local_error.empty()) *error = local_error;
      continue;
    }
    sample.verified++;
    sample.Add(ms, blob.size());
  }
  return sample;
}

// RunConcurrent fans a per-thread measurement out across settings.concurrency
// workers and reports one merged Sample timed on the wall clock.
//
// Every worker uses its OWN key. Sharing one key would let the engine's RAM
// tier serve every reader from the same hot entry, which measures the cache
// rather than the transport.
template <typename PerThread>
Sample RunConcurrent(const Settings& settings, PerThread body) {
  std::vector<Sample> per_thread(static_cast<size_t>(settings.concurrency));
  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(settings.concurrency));

  const auto started = Clock::now();
  for (int i = 0; i < settings.concurrency; i++) {
    workers.emplace_back([&, i] { per_thread[static_cast<size_t>(i)] = body(i); });
  }
  for (auto& worker : workers) worker.join();
  const double wall =
      std::chrono::duration<double>(Clock::now() - started).count();

  Sample merged;
  for (const auto& sample : per_thread) merged.Merge(sample);
  merged.wall_seconds = wall;
  return merged;
}

// ---------------------------------------------------------------------------
// Replication verifier (Phase 6 exit criterion 3)
// ---------------------------------------------------------------------------
//
// Phase 4's forwarding and catch-up now move large values over the bulk
// transport. This proves they still land byte-for-byte, by reading the physical
// nodes directly rather than through any routing layer.
//
// Ownership comes from the control plane's published shard_to_owners rather
// than being recomputed here. Reimplementing rendezvous hashing in C++ to check
// rendezvous hashing would be checking a copy against a copy.

struct Ownership {
  uint32_t shard_count = 0;
  std::string primary;
  std::vector<std::string> replicas;
  std::vector<std::string> holder_addresses;  // primary first, then replicas
  std::vector<std::string> holder_ids;
};

// Splits a comma-separated endpoint list. Phase 5 made the control plane a Raft
// group, so every tool takes the whole list and falls back across it.
std::vector<std::string> SplitEndpoints(const std::string& text) {
  std::vector<std::string> out;
  size_t start = 0;
  while (start <= text.size()) {
    size_t comma = text.find(',', start);
    size_t end = comma == std::string::npos ? text.size() : comma;
    std::string piece = text.substr(start, end - start);
    size_t lo = piece.find_first_not_of(" \t");
    size_t hi = piece.find_last_not_of(" \t");
    if (lo != std::string::npos) out.push_back(piece.substr(lo, hi - lo + 1));
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  return out;
}

bool FetchOwnership(const std::string& control_plane, const std::string& key,
                    Ownership* out, std::string* error) {
  metadatav1::GetNodeListResponse nodes;
  metadatav1::GetShardMapResponse shards;

  // Any replica answers: a follower serves its own applied Raft log, which is a
  // prefix of the leader's committed one. Both RPCs go to the SAME replica so
  // they cannot describe two different memberships.
  bool answered = false;
  for (const std::string& address : SplitEndpoints(control_plane)) {
    auto channel = DialNode(address);
    auto metadata = metadatav1::ClusterMetadataService::NewStub(channel);
    {
      grpc::ClientContext context;
      metadatav1::GetNodeListRequest request;
      grpc::Status status = metadata->GetNodeList(&context, request, &nodes);
      if (!status.ok()) { *error = "GetNodeList on " + address + ": " + status.error_message(); continue; }
    }
    {
      grpc::ClientContext context;
      metadatav1::GetShardMapRequest request;
      grpc::Status status = metadata->GetShardMap(&context, request, &shards);
      if (!status.ok()) { *error = "GetShardMap on " + address + ": " + status.error_message(); continue; }
    }
    answered = true;
    break;
  }
  if (!answered) {
    if (error->empty()) *error = "no control-plane replica answered";
    return false;
  }
  if (shards.shard_count() == 0) { *error = "control plane published no shards"; return false; }

  std::map<std::string, std::string> address_of;
  for (const auto& node : nodes.nodes()) address_of[node.node_id()] = node.address();

  out->shard_count = shards.shard_count();
  const uint32_t shard = bulk::ShardForKey(key, out->shard_count);
  auto owners = shards.shard_to_owners().find(shard);
  if (owners == shards.shard_to_owners().end()) {
    *error = "control plane published no owners for shard " + std::to_string(shard);
    return false;
  }
  out->primary = owners->second.primary();
  out->holder_ids.push_back(out->primary);
  for (const auto& replica : owners->second.replicas()) {
    out->replicas.push_back(replica);
    out->holder_ids.push_back(replica);
  }
  for (const std::string& id : out->holder_ids) {
    auto found = address_of.find(id);
    if (found == address_of.end()) { *error = "no address for holder " + id; return false; }
    out->holder_addresses.push_back(found->second);
  }
  return true;
}

// RunVerifyReplication writes (optionally) one oversized value and then waits,
// bounded, for every holder the control plane names to serve it byte-for-byte.
int RunVerifyReplication(const std::string& control_plane, const std::string& key,
                         size_t value_bytes, bool write_first, int timeout_seconds) {
  Ownership ownership;
  std::string error;
  if (!FetchOwnership(control_plane, key, &ownership, &error)) {
    std::fprintf(stderr, "fatal: %s\n", error.c_str());
    return 1;
  }
  if (ownership.replicas.empty()) {
    std::fprintf(stderr,
                 "fatal: shard for %s has no replica; this check needs replication_factor >= 1\n",
                 key.c_str());
    return 1;
  }

  const std::vector<uint8_t> want = DeterministicValue(value_bytes, 0x9E37ULL);
  std::printf("key           %s -> shard %u\n", key.c_str(),
              bulk::ShardForKey(key, ownership.shard_count));
  std::printf("holders       %s", ownership.holder_ids[0].c_str());
  for (size_t i = 1; i < ownership.holder_ids.size(); i++)
    std::printf(", %s", ownership.holder_ids[i].c_str());
  std::printf("\n");
  std::printf("payload       %.2f MiB (above the 4 MiB unary limit, so it takes the "
              "chunked/bulk path)\n", static_cast<double>(value_bytes) / (1024.0 * 1024.0));

  if (write_first) {
    // Written straight at the primary so the replication being checked is the
    // node's own forwarding, not the SDK's routing.
    auto channel = DialNode(ownership.holder_addresses[0]);
    auto stub = nodev1::NodeService::NewStub(channel);
    if (!PutChunked(*stub, key, want, &error)) {
      std::fprintf(stderr, "fatal: seeding the primary failed: %s\n", error.c_str());
      return 1;
    }
    std::printf("wrote         %s on the primary %s\n",
                key.c_str(), ownership.holder_ids[0].c_str());
  }

  // Replication is asynchronous by default, and catch-up is bounded by a poll
  // interval, so this waits rather than sampling once.
  const auto deadline = Clock::now() + std::chrono::seconds(timeout_seconds);
  std::vector<bool> verified(ownership.holder_addresses.size(), false);
  for (;;) {
    size_t done = 0;
    for (size_t i = 0; i < ownership.holder_addresses.size(); i++) {
      if (verified[i]) { done++; continue; }
      auto channel = DialNode(ownership.holder_addresses[i]);
      auto stub = nodev1::NodeService::NewStub(channel);
      std::vector<uint8_t> got;
      std::string local_error;
      if (GetChunked(*stub, key, &got, &local_error) &&
          got.size() == want.size() && memcmp(got.data(), want.data(), want.size()) == 0) {
        verified[i] = true;
        done++;
      }
    }
    if (done == ownership.holder_addresses.size()) break;
    if (Clock::now() >= deadline) {
      for (size_t i = 0; i < ownership.holder_addresses.size(); i++) {
        if (!verified[i]) {
          std::fprintf(stderr, "FAILED: holder %s (%s) does not serve %s byte-for-byte\n",
                       ownership.holder_ids[i].c_str(),
                       ownership.holder_addresses[i].c_str(), key.c_str());
        }
      }
      return 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  std::printf("verified      all %zu holder(s) serve the value byte-for-byte\n",
              ownership.holder_addresses.size());
  return 0;
}

void PrintUsage(const char* argv0) {
  std::fprintf(stderr,
      "usage: %s [options]\n"
      "\n"
      "Measures multi-megabyte transfer out of a RUNNING pulsekv-node across every\n"
      "transport Phase 6 provides, verifying every byte of every read.\n"
      "\n"
      "  --node ADDRESS         NodeService address (default 127.0.0.1:7100)\n"
      "  --node-id ID           expected node ID; the bulk handshake verifies it\n"
      "  --socket-dir PATH      bulk unix socket directory (default /tmp/pulsekv-bulk)\n"
      "  --bulk-port-offset N   bulk TCP port = service port + N (default 1000)\n"
      "  --value-bytes N        payload size per transfer (default 8388608)\n"
      "  --iterations N         measured transfers per transport (default 20)\n"
      "  --warmup N             unmeasured transfers first (default 3)\n"
      "  --concurrency N        parallel readers, each with its own connection\n"
      "                         and its own key (default 1). Single-stream\n"
      "                         loopback measures link speed; concurrency\n"
      "                         measures CPU per byte, which is what limits a\n"
      "                         real storage server.\n"
      "  --only LIST            comma-separated subset of: grpc,tcp,unix,memfd\n"
      "\n"
      "replication verifier (Phase 6 exit criterion 3):\n"
      "  --mode verify-replication   write one oversized value at its primary and\n"
      "                              wait for every holder the control plane names\n"
      "                              to serve it byte-for-byte\n"
      "  --mode verify-only          same, without writing: proves catch-up refilled\n"
      "                              a restarted node\n"
      "  --control-plane LIST        metadata address(es) for the verifier\n"
      "  --key NAME                  key to use (default bulk-replication-check)\n"
      "  --verify-timeout N          seconds to wait for convergence (default 30)\n",
      argv0);
}

bool ParseSize(const char* text, size_t* out) {
  char* end = nullptr;
  errno = 0;
  unsigned long long parsed = strtoull(text, &end, 10);
  if (end == text || *end != '\0' || errno == ERANGE || parsed == 0) return false;
  *out = static_cast<size_t>(parsed);
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Settings settings;
  std::setvbuf(stdout, nullptr, _IOLBF, 0);

  std::string mode = "bench";
  std::string control_plane = "127.0.0.1:7000";
  std::string verify_key = "bulk-replication-check";
  int verify_timeout = 30;

  for (int i = 1; i < argc; i++) {
    std::string flag = argv[i];
    auto next = [&](const char* name) -> const char* {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "error: %s requires a value\n", name);
        std::exit(2);
      }
      return argv[++i];
    };
    if (flag == "--node") settings.node_address = next("--node");
    else if (flag == "--node-id") settings.node_id = next("--node-id");
    else if (flag == "--socket-dir") settings.socket_dir = next("--socket-dir");
    else if (flag == "--bulk-port-offset") settings.bulk_port_offset = atoi(next("--bulk-port-offset"));
    else if (flag == "--value-bytes") {
      if (!ParseSize(next("--value-bytes"), &settings.value_bytes)) {
        std::fprintf(stderr, "error: --value-bytes must be a positive integer\n");
        return 2;
      }
    } else if (flag == "--iterations") settings.iterations = atoi(next("--iterations"));
    else if (flag == "--warmup") settings.warmup = atoi(next("--warmup"));
    else if (flag == "--concurrency") settings.concurrency = atoi(next("--concurrency"));
    else if (flag == "--mode") mode = next("--mode");
    else if (flag == "--control-plane") control_plane = next("--control-plane");
    else if (flag == "--key") verify_key = next("--key");
    else if (flag == "--verify-timeout") verify_timeout = atoi(next("--verify-timeout"));
    else if (flag == "--only") {
      std::string list = next("--only");
      settings.run_grpc = list.find("grpc") != std::string::npos;
      settings.run_tcp = list.find("tcp") != std::string::npos;
      settings.run_unix_inline = list.find("unix") != std::string::npos;
      settings.run_unix_memfd = list.find("memfd") != std::string::npos;
    } else if (flag == "-h" || flag == "--help") {
      PrintUsage(argv[0]);
      return 0;
    } else {
      std::fprintf(stderr, "error: unknown argument %s\n", flag.c_str());
      PrintUsage(argv[0]);
      return 2;
    }
  }
  if (settings.iterations <= 0 || settings.warmup < 0 || settings.concurrency <= 0) {
    std::fprintf(stderr,
                 "error: --iterations and --concurrency must be positive, "
                 "--warmup non-negative\n");
    return 2;
  }

  if (mode == "verify-replication" || mode == "verify-only") {
    return RunVerifyReplication(control_plane, verify_key, settings.value_bytes,
                                mode == "verify-replication", verify_timeout);
  }
  if (mode != "bench") {
    std::fprintf(stderr,
                 "error: unknown --mode %s (want bench, verify-replication, or verify-only)\n",
                 mode.c_str());
    return 2;
  }

  auto channel = DialNode(settings.node_address);
  auto stub = nodev1::NodeService::NewStub(channel);

  // Identity first: everything after this assumes we are talking to the node we
  // think we are, and the bulk handshake checks the same thing independently.
  {
    grpc::ClientContext context;
    nodev1::HealthCheckRequest request;
    nodev1::HealthCheckResponse response;
    grpc::Status status = stub->HealthCheck(&context, request, &response);
    if (!status.ok()) {
      std::fprintf(stderr, "fatal: %s did not answer HealthCheck: %s\n",
                   settings.node_address.c_str(), status.error_message().c_str());
      return 1;
    }
    if (settings.node_id.empty()) settings.node_id = response.node_id();
    else if (settings.node_id != response.node_id()) {
      std::fprintf(stderr, "fatal: %s reports node %s, expected %s\n",
                   settings.node_address.c_str(), response.node_id().c_str(),
                   settings.node_id.c_str());
      return 1;
    }
  }

  // One key and one distinct payload per reader.
  std::vector<std::string> keys;
  std::vector<std::vector<uint8_t>> wants;
  for (int i = 0; i < settings.concurrency; i++) {
    keys.push_back("bulk-bench:" + std::to_string(settings.value_bytes) + ":" +
                   std::to_string(i));
    wants.push_back(DeterministicValue(settings.value_bytes,
                                       0x6ULL + static_cast<uint64_t>(i)));
  }

  // Seeded through the gRPC chunked path so the values are in place regardless
  // of whether any bulk transport works at all.
  for (int i = 0; i < settings.concurrency; i++) {
    std::string error;
    if (!PutChunked(*stub, keys[static_cast<size_t>(i)], wants[static_cast<size_t>(i)],
                    &error)) {
      std::fprintf(stderr, "fatal: could not seed the benchmark value: %s\n", error.c_str());
      return 1;
    }
  }

  const bulk::Endpoint endpoint = bulk::EndpointForPeer(
      settings.node_address, settings.bulk_port_offset, settings.socket_dir);

  std::printf("=== PulseKV Phase 6 bulk transfer benchmark ===\n");
  std::printf("node          %s (%s)\n", settings.node_address.c_str(), settings.node_id.c_str());
  std::printf("payload       %.2f MiB x %d iterations (%d warmup) x %d reader(s)\n",
              static_cast<double>(settings.value_bytes) / (1024.0 * 1024.0),
              settings.iterations, settings.warmup, settings.concurrency);
  std::printf("bulk tcp      %s\n", endpoint.tcp.empty() ? "(none)" : endpoint.tcp.c_str());
  std::printf("bulk unix     %s%s\n",
              endpoint.unix_path.empty() ? "(none)" : endpoint.unix_path.c_str(),
              access(endpoint.unix_path.c_str(), F_OK) == 0 ? "" : "  [absent: not same-host]");
  std::printf("memfd         %s\n", bulk::MemfdSupported() ? "supported" : "UNSUPPORTED");
  std::printf("\n");

  std::mutex error_mu;

  Sample grpc_sample;
  if (settings.run_grpc) {
    grpc_sample = RunConcurrent(settings, [&](int index) {
      // A private channel per reader: sharing one would multiplex every
      // transfer onto a single HTTP/2 connection and measure that instead.
      auto local_channel = DialNode(settings.node_address);
      auto local_stub = nodev1::NodeService::NewStub(local_channel);
      return MeasureGrpc(*local_stub, keys[static_cast<size_t>(index)],
                         wants[static_cast<size_t>(index)], settings);
    });
  }

  auto run_bulk = [&](const bulk::Endpoint& endpoint_template, bool accept_memfd,
                      std::string* error_out) {
    return RunConcurrent(settings, [&](int index) {
      std::string local_error;
      Sample sample = MeasureBulk(endpoint_template, accept_memfd,
                                  keys[static_cast<size_t>(index)],
                                  wants[static_cast<size_t>(index)], settings, &local_error);
      if (!local_error.empty()) {
        std::lock_guard<std::mutex> lock(error_mu);
        if (error_out->empty()) *error_out = local_error;
      }
      return sample;
    });
  };

  Sample tcp_sample;
  std::string tcp_error;
  if (settings.run_tcp && !endpoint.tcp.empty()) {
    bulk::Endpoint tcp_only;
    tcp_only.tcp = endpoint.tcp;  // no unix path: force the TCP path
    tcp_sample = run_bulk(tcp_only, /*accept_memfd=*/false, &tcp_error);
  }

  Sample unix_sample;
  std::string unix_error;
  if (settings.run_unix_inline && !endpoint.unix_path.empty()) {
    bulk::Endpoint unix_only;
    unix_only.unix_path = endpoint.unix_path;
    unix_sample = run_bulk(unix_only, /*accept_memfd=*/false, &unix_error);
  }

  Sample memfd_sample;
  std::string memfd_error;
  if (settings.run_unix_memfd && !endpoint.unix_path.empty()) {
    bulk::Endpoint unix_only;
    unix_only.unix_path = endpoint.unix_path;
    memfd_sample = run_bulk(unix_only, /*accept_memfd=*/true, &memfd_error);
  }

  std::printf("node-to-node (cross-process, TCP -- the replication path)\n");
  Report("gRPC chunked (Phase 1)", grpc_sample, nullptr);
  Report("bulk TCP", tcp_sample, &grpc_sample);
  if (!tcp_error.empty()) std::printf("      tcp note: %s\n", tcp_error.c_str());

  std::printf("\nnode-to-adapter (same host, unix socket)\n");
  Report("bulk unix, inline bytes", unix_sample, &grpc_sample);
  if (!unix_error.empty()) std::printf("      unix note: %s\n", unix_error.c_str());
  Report("bulk unix, memfd handoff", memfd_sample, &grpc_sample);
  if (!memfd_error.empty()) std::printf("      memfd note: %s\n", memfd_error.c_str());

  // Any unverified read fails the run outright. A benchmark that reports a
  // number it did not check is worse than no benchmark.
  const uint64_t failures = grpc_sample.failures + tcp_sample.failures +
                            unix_sample.failures + memfd_sample.failures;
  const uint64_t verified = grpc_sample.verified + tcp_sample.verified +
                            unix_sample.verified + memfd_sample.verified;
  std::printf("\n%llu transfer(s) verified byte-for-byte, %llu failed\n",
              (unsigned long long)verified, (unsigned long long)failures);
  if (failures > 0) {
    std::fprintf(stderr, "FAILED: %llu transfer(s) did not verify\n",
                 (unsigned long long)failures);
    return 1;
  }
  if (verified == 0) {
    std::fprintf(stderr, "FAILED: no transport produced a verified transfer\n");
    return 1;
  }
  return 0;
}
