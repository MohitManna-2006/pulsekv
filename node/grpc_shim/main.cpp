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
//
// PHASE 6 ADDED A SECOND, NON-gRPC DATA PATH. node/grpc_shim/bulk.{h,cc} is a
// raw framed socket protocol with a shared-memory handoff, used for values too
// large to want protobuf in the middle of. It is strictly an optimisation: every
// use of it here falls back to the gRPC chunked path on any failure, and a node
// started without it behaves exactly as it did in Phase 5.
//
// PHASE 4 MADE THIS PROCESS A gRPC CLIENT AS WELL AS A SERVER. Replication is a
// network-layer concern and lives entirely on this side of the engine boundary:
// pk_engine_put has no idea whether it is storing a client's write or a copy
// forwarded by a peer, and node/engine/ has no concept of a primary, a replica,
// or a peer at all. What is new here is a background poller that reads shard
// ownership from ClusterMetadataService, a cache of NodeService stubs pointed at
// this node's replica peers, and the forwarding those two enable. The rule above
// still holds -- the new code branches on WHERE a key belongs, never on what is
// stored under it.

#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>

#if PULSEKV_HAVE_GRPC_REFLECTION
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#endif

#include "bulk.h"
#include "metadata.grpc.pb.h"
#include "node.grpc.pb.h"
#include "pulsekv_engine.h"

namespace nodev1 = pulsekv::node::v1;
namespace metadatav1 = pulsekv::metadata::v1;
namespace bulk = pulsekv::bulk;

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

// ===========================================================================
// Phase 4: replication
// ===========================================================================

// The client-side channel ceiling, mirroring the server's. A primary forwarding
// a 6 MiB value to a replica needs the same headroom the client had to send it.
constexpr int kPeerMaxMessageBytes = kMaxMessageBytes;

// PutChunked frame size used when forwarding a value too large for unary Put.
// Matched to the Go SDK's transport.ChunkSize so a replicated write is framed
// the same way the original was.
constexpr size_t kReplicaChunkBytes = 1024 * 1024;

// How many times the poller re-reads the metadata pair looking for two
// responses that describe the same topology. Same bound, and the same reason,
// as maxCoherenceAttempts in control/internal/topology.
constexpr int kMaxCoherenceAttempts = 8;

// Background replication is best-effort, so its queue is bounded in both
// directions: a burst that outruns the replica links is dropped and counted
// rather than growing until the node dies of it. Losing a replica copy costs a
// recompute; losing the node costs everything on it.
constexpr size_t kAsyncWorkerCount = 4;
constexpr size_t kAsyncQueueDepth = 1024;
constexpr size_t kAsyncQueueBytes = 64ULL * 1024 * 1024;

// Strong-ack fan-out uses one thread per (write, replica). That is the opt-in
// slow path, but it still needs a ceiling so a flood of strong-ack writes to an
// unreachable replica cannot exhaust the process's thread budget. Over the cap,
// the fan-out reports fewer acks than requested, which surfaces as a loud
// DEADLINE_EXCEEDED rather than a silent success.
constexpr size_t kMaxAckThreads = 256;

// Deadline for one background (fire-and-forget) replica write. Generous, since
// nothing is waiting on it, but finite so a black-holed peer cannot pin a
// worker thread forever.
constexpr int64_t kAsyncForwardTimeoutMs = 5000;

// Deadline for one catch-up scan of a peer. A full PrefixMatch is O(total keys)
// on the peer, so this is much larger than a normal RPC budget.
constexpr int64_t kCatchUpTimeoutMs = 30000;

// Failed replication is expected during churn, and one log line per dropped
// write would bury everything else. Report at most one line per peer per
// interval, carrying the count that was suppressed.
constexpr int64_t kFailureLogIntervalMs = 5000;

// The poll interval used while ownership is visibly unsettled, rather than the
// configured steady-state one.
//
// This exists because the steady-state interval is also the width of the window
// in which this node's idea of who holds its shards is stale. Async replication
// tolerates that -- a write in the window is simply not replicated. A
// require_replica_acks write does not: it is refused with INVALID_ARGUMENT,
// because refusing is better than hanging. Making the window narrow where it is
// most likely to be hit costs a handful of extra metadata reads.
//
// Two situations count as unsettled:
//   * the view just changed, so membership is moving and the next move is
//     probably close behind;
//   * this node holds no shard at all, which at 256 shards means it has just
//     started and is not yet in the ring, or has just been removed from it.
//     Neither is a resting state, and both end with a change worth seeing.
constexpr int64_t kSettlingPollIntervalMs = 200;

using Clock = std::chrono::steady_clock;

std::chrono::system_clock::time_point DeadlineFromNow(int64_t millis) {
  return std::chrono::system_clock::now() + std::chrono::milliseconds(millis);
}

// ShardForKey now lives in bulk.h so the node and the benchmark share exactly
// one implementation of the hash that must match the Go router. Brought into
// this namespace unqualified because every call site below predates the move.
using bulk::ShardForKey;

// One published, immutable ownership snapshot, already projected down to the
// three questions this node actually asks of it. Everything else in the cluster
// topology is deliberately discarded here: a node needs to know who holds
// copies of ITS shards, not who owns the other 200.
struct TopologyView {
  // The publisher's content-derived identity. Used only as a change key --
  // equal fingerprints mean equal content, which is the whole point of it being
  // a SHA-256 over the topology rather than a counter.
  std::string fingerprint;
  uint64_t generation = 0;
  uint32_t shard_count = 0;
  uint32_t replication_factor = 0;
  size_t live_nodes = 0;

  // Shards this node is the PRIMARY for -> the addresses it must forward
  // writes to, in promotion order.
  std::unordered_map<uint32_t, std::vector<std::string>> replica_targets;

  // Shards this node holds a copy of, as primary or replica -> the addresses of
  // the OTHER nodes holding that shard. This is what catch-up reads from.
  std::unordered_map<uint32_t, std::vector<std::string>> peer_sources;

  // Every live NodeService address, used to retire stubs for departed peers.
  std::unordered_set<std::string> live_addresses;

  // Address -> node ID, so a bulk connection can verify it reached the peer the
  // topology names rather than trusting a convention-derived socket path.
  std::unordered_map<std::string, std::string> node_id_of_address;

  bool Serves(uint32_t shard) const { return peer_sources.count(shard) != 0; }
};

// What one client write needs to know about its own replication, resolved once
// under the topology lock so a handler never reads a half-swapped view.
struct ReplicaPlan {
  bool enabled = false;       // --metadata-addr was supplied
  bool have_topology = false; // a coherent snapshot has been published
  bool is_primary = false;    // this node primaries the key's shard
  uint32_t shard = 0;
  std::vector<std::string> targets;
};

// Lazily-created, address-keyed NodeService stubs, reused exactly the way the
// Go SDK reuses its node connections. Channels are expensive to build and cheap
// to keep, and a primary talks to the same two or three peers indefinitely.
class PeerClients {
 public:
  std::shared_ptr<nodev1::NodeService::Stub> Get(const std::string& address) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = stubs_.find(address);
      if (it != stubs_.end())
        return it->second;
    }

    grpc::ChannelArguments args;
    args.SetMaxSendMessageSize(kPeerMaxMessageBytes);
    args.SetMaxReceiveMessageSize(kPeerMaxMessageBytes);
    auto channel = grpc::CreateCustomChannel(
        address, grpc::InsecureChannelCredentials(), args);
    auto stub = std::shared_ptr<nodev1::NodeService::Stub>(
        nodev1::NodeService::NewStub(channel).release());

    std::lock_guard<std::mutex> lock(mu_);
    // Another thread may have won the race; one channel per address is the
    // point, so keep whichever landed first.
    auto existing = stubs_.find(address);
    if (existing != stubs_.end())
      return existing->second;
    channels_[address] = channel;
    stubs_[address] = stub;
    return stub;
  }

  // Drop stubs for addresses that are no longer live. An in-flight forward
  // still holds its own shared_ptr, so this only releases the cache's reference.
  void Retain(const std::unordered_set<std::string>& keep) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto it = stubs_.begin(); it != stubs_.end();) {
      if (keep.count(it->first) != 0) {
        ++it;
        continue;
      }
      channels_.erase(it->first);
      it = stubs_.erase(it);
    }
  }

  void Clear() {
    std::lock_guard<std::mutex> lock(mu_);
    stubs_.clear();
    channels_.clear();
  }

 private:
  std::mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channels_;
  std::unordered_map<std::string, std::shared_ptr<nodev1::NodeService::Stub>> stubs_;
};

// Lazily-created, address-keyed bulk transport connections.
//
// Deliberately a separate cache from PeerClients rather than a field on it: a
// bulk connection is optional and can legitimately fail to exist for a peer
// whose gRPC stub is perfectly healthy (different host, no memfd, listener
// disabled). Keeping them apart means a missing bulk connection can never look
// like a missing peer.
class BulkClients {
 public:
  BulkClients(int port_offset, std::string socket_dir, int64_t timeout_ms)
      : port_offset_(port_offset), socket_dir_(std::move(socket_dir)), timeout_ms_(timeout_ms) {}

  // Returns nullptr whenever the bulk path is unavailable for this peer. Every
  // caller treats that as "use gRPC", which is why it is not an error.
  bulk::Client* Get(const std::string& service_address, const std::string& expect_node_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = clients_.find(service_address);
    if (it != clients_.end())
      return it->second.get();
    // A peer that failed to connect is remembered as unavailable so a
    // cross-host peer is not re-dialed on every single forwarded write.
    if (unavailable_.count(service_address) != 0)
      return nullptr;

    bulk::Endpoint endpoint =
        bulk::EndpointForPeer(service_address, port_offset_, socket_dir_);
    bulk::ClientOptions options;
    options.io_timeout_ms = timeout_ms_;
    options.accept_memfd = true;
    options.expect_node_id = expect_node_id;

    std::string error;
    auto client = bulk::Client::Connect(endpoint, options, &error);
    if (!client) {
      unavailable_.insert(service_address);
      return nullptr;
    }
    bulk::Client* raw = client.get();
    clients_[service_address] = std::move(client);
    return raw;
  }

  // Drops a connection that failed mid-request. The next attempt redials once;
  // a peer that keeps failing lands in unavailable_ and stops being retried.
  void Drop(const std::string& service_address) {
    std::lock_guard<std::mutex> lock(mu_);
    clients_.erase(service_address);
  }

  void Retain(const std::unordered_set<std::string>& keep) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto it = clients_.begin(); it != clients_.end();) {
      it = keep.count(it->first) != 0 ? std::next(it) : clients_.erase(it);
    }
    for (auto it = unavailable_.begin(); it != unavailable_.end();) {
      it = keep.count(*it) != 0 ? std::next(it) : unavailable_.erase(it);
    }
  }

  void Clear() {
    std::lock_guard<std::mutex> lock(mu_);
    clients_.clear();
    unavailable_.clear();
  }

 private:
  std::mutex mu_;
  std::unordered_map<std::string, std::unique_ptr<bulk::Client>> clients_;
  std::unordered_set<std::string> unavailable_;
  const int port_offset_;
  const std::string socket_dir_;
  const int64_t timeout_ms_;
};

// A bounded work queue for background replica writes.
//
// Bounded in tasks AND in bytes, because the two limits catch different
// failures: thousands of tiny writes to a stalled peer, and a handful of
// multi-megabyte ones. Submit() refuses rather than blocks -- a client write
// must never wait on the background path, which is the entire reason that path
// exists.
class AsyncQueue {
 public:
  AsyncQueue(size_t workers, size_t max_tasks, size_t max_bytes)
      : max_tasks_(max_tasks), max_bytes_(max_bytes) {
    for (size_t i = 0; i < workers; i++)
      workers_.emplace_back([this] { Run(); });
  }

  ~AsyncQueue() { Stop(); }

  bool Submit(std::function<void()> task, size_t bytes) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (stopping_ || tasks_.size() >= max_tasks_ ||
          (queued_bytes_ + bytes > max_bytes_ && !tasks_.empty())) {
        dropped_.fetch_add(1, std::memory_order_relaxed);
        return false;
      }
      queued_bytes_ += bytes;
      tasks_.push_back({std::move(task), bytes});
    }
    cv_.notify_one();
    return true;
  }

  void Stop() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (stopping_)
        return;
      stopping_ = true;
    }
    cv_.notify_all();
    for (auto& worker : workers_) {
      if (worker.joinable())
        worker.join();
    }
    workers_.clear();
  }

  uint64_t dropped() const { return dropped_.load(std::memory_order_relaxed); }

 private:
  struct Entry {
    std::function<void()> task;
    size_t bytes;
  };

  void Run() {
    for (;;) {
      Entry entry;
      {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
        // Drain on stop rather than discarding: these writes were already
        // acknowledged to a client, and finishing them costs one bounded RPC.
        if (tasks_.empty())
          return;
        entry = std::move(tasks_.front());
        tasks_.pop_front();
        queued_bytes_ -= entry.bytes;
      }
      entry.task();
    }
  }

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Entry> tasks_;
  std::vector<std::thread> workers_;
  size_t queued_bytes_ = 0;
  const size_t max_tasks_;
  const size_t max_bytes_;
  bool stopping_ = false;
  std::atomic<uint64_t> dropped_{0};
};

// Shared state for one strong-ack fan-out. Held by shared_ptr so the waiting
// handler can return the instant it has enough acks while the remaining
// forwards finish on their own.
struct AckFanout {
  std::mutex mu;
  std::condition_variable cv;
  uint32_t launched = 0;
  uint32_t completed = 0;
  uint32_t acked = 0;
  std::string first_error;
};

struct ReplicationOptions {
  std::string node_id;
  std::string self_address;

  // Every control-plane replica this node may read ownership from.
  //
  // Phase 5 made the metadata plane a Raft group, so the replica this node
  // happens to prefer can be down or mid-election while the group is perfectly
  // healthy. Reading from a follower is safe rather than sloppy: a replica
  // answers from its own applied log, which is always a prefix of the leader's
  // committed one, so it can be slightly behind but never contradictory.
  std::vector<std::string> metadata_addresses;

  int64_t poll_interval_ms = 2000;
  int64_t ack_timeout_ms = 2000;
  bool catch_up = true;

  // Phase 6. When enabled, a forwarded value too large for the unary path is
  // pushed over the bulk transport instead of PutChunked, and a catch-up scan
  // pulls large values the same way. Both fall back to gRPC on ANY failure --
  // that is what keeps this an optimisation rather than a new way to lose data.
  bool bulk_enabled = false;
  int bulk_port_offset = 1000;
  std::string bulk_socket_dir;

  const std::string& PrimaryMetadataAddress() const {
    static const std::string kNone;
    return metadata_addresses.empty() ? kNone : metadata_addresses.front();
  }
};

// Splits a comma-separated endpoint list, dropping blanks and duplicates. A
// duplicate would only make the fallback retry the same replica twice.
std::vector<std::string> SplitEndpoints(const std::string& text) {
  std::vector<std::string> out;
  size_t start = 0;
  while (start <= text.size()) {
    const size_t comma = text.find(',', start);
    const size_t end = comma == std::string::npos ? text.size() : comma;
    std::string piece = text.substr(start, end - start);
    // Trim surrounding whitespace so a hand-written flag with spaces works.
    size_t lo = piece.find_first_not_of(" \t");
    size_t hi = piece.find_last_not_of(" \t");
    if (lo != std::string::npos) {
      piece = piece.substr(lo, hi - lo + 1);
      if (std::find(out.begin(), out.end(), piece) == out.end())
        out.push_back(piece);
    }
    if (comma == std::string::npos)
      break;
    start = comma + 1;
  }
  return out;
}

// Owns everything replication needs: the published topology, the peer stubs,
// the background queue, the poller, and the catch-up worker.
//
// Created as a shared_ptr and captured as one by every detached forward, so a
// straggling replica write can never outlive the object it is calling through.
class ReplicationManager : public std::enable_shared_from_this<ReplicationManager> {
 public:
  static std::shared_ptr<ReplicationManager> Create(ReplicationOptions options,
                                                    pk_engine_t* engine) {
    return std::shared_ptr<ReplicationManager>(
        new ReplicationManager(std::move(options), engine));
  }

  ~ReplicationManager() { Stop(); }

  void Start() {
    poller_ = std::thread([this] { PollLoop(); });
    if (options_.catch_up)
      catch_up_worker_ = std::thread([this] { CatchUpLoop(); });
  }

  void Stop() {
    {
      std::lock_guard<std::mutex> lock(state_mu_);
      if (stopping_)
        return;
      stopping_ = true;
    }
    state_cv_.notify_all();
    if (poller_.joinable())
      poller_.join();
    if (catch_up_worker_.joinable())
      catch_up_worker_.join();
    queue_.Stop();

    // Detached strong-ack forwards do not touch the engine, but they do call
    // through this object, so drain them before main destroys it. Each carries
    // its own deadline, which is what makes this wait finite.
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.ack_timeout_ms + 1000);
    while (ack_threads_.load(std::memory_order_relaxed) > 0 && Clock::now() < deadline)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    peers_.Clear();
    if (bulk_)
      bulk_->Clear();
  }

  // PlanFor resolves one key against the current topology in a single read, so
  // a handler's validation and its forwarding cannot straddle a view swap.
  ReplicaPlan PlanFor(const std::string& key) const {
    ReplicaPlan plan;
    plan.enabled = true;
    auto view = View();
    if (!view || view->shard_count == 0)
      return plan;

    plan.have_topology = true;
    plan.shard = ShardForKey(key, view->shard_count);
    auto it = view->replica_targets.find(plan.shard);
    if (it == view->replica_targets.end())
      return plan;  // this node holds the shard as a replica, or not at all
    plan.is_primary = true;
    plan.targets = it->second;
    return plan;
  }

  // Fire-and-forget. Returns immediately; the client is not waiting on this.
  void ForwardAsync(const ReplicaPlan& plan, const std::string& key,
                    const std::string& value) {
    if (plan.targets.empty())
      return;
    auto self = shared_from_this();
    auto payload = std::make_shared<std::pair<std::string, std::string>>(key, value);
    for (const std::string& address : plan.targets) {
      const bool queued = queue_.Submit(
          [self, address, payload] {
            self->SendOne(address, payload->first, payload->second,
                          kAsyncForwardTimeoutMs);
          },
          value.size());
      if (!queued)
        LogFailure(address, "background replication queue is full");
    }
  }

  // Blocks until `required` replicas have stored the write, or the ack timeout
  // expires, or every forward has finished. Returns how many acked.
  uint32_t ForwardAndWait(const ReplicaPlan& plan, const std::string& key,
                          const std::string& value, uint32_t required,
                          std::string* detail) {
    auto fanout = std::make_shared<AckFanout>();
    fanout->launched = static_cast<uint32_t>(plan.targets.size());

    auto self = shared_from_this();
    auto payload = std::make_shared<std::pair<std::string, std::string>>(key, value);
    const int64_t timeout_ms = options_.ack_timeout_ms;

    for (const std::string& address : plan.targets) {
      if (ack_threads_.fetch_add(1, std::memory_order_relaxed) >= kMaxAckThreads) {
        ack_threads_.fetch_sub(1, std::memory_order_relaxed);
        Complete(fanout, false, "strong-ack fan-out is at its thread limit");
        continue;
      }
      try {
        std::thread([self, fanout, address, payload, timeout_ms] {
          const std::string error =
              self->SendOne(address, payload->first, payload->second, timeout_ms);
          self->Complete(fanout, error.empty(), error);
          self->ack_threads_.fetch_sub(1, std::memory_order_relaxed);
        }).detach();
      } catch (const std::system_error& e) {
        ack_threads_.fetch_sub(1, std::memory_order_relaxed);
        Complete(fanout, false, std::string("could not start a forward thread: ") + e.what());
      }
    }

    std::unique_lock<std::mutex> lock(fanout->mu);
    fanout->cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&] {
      return fanout->acked >= required || fanout->completed >= fanout->launched;
    });
    if (detail != nullptr)
      *detail = fanout->first_error;
    return fanout->acked;
  }

  uint64_t dropped_writes() const { return queue_.dropped(); }

  struct BulkStats {
    uint64_t writes;
    uint64_t reads;
    uint64_t shared_memory_reads;
    uint64_t fallbacks;
    bool enabled;
  };

  BulkStats bulk_stats() const {
    return BulkStats{bulk_writes_.load(std::memory_order_relaxed),
                     bulk_reads_.load(std::memory_order_relaxed),
                     bulk_shared_memory_reads_.load(std::memory_order_relaxed),
                     bulk_fallbacks_.load(std::memory_order_relaxed),
                     bulk_ != nullptr};
  }

  std::shared_ptr<const TopologyView> View() const {
    std::lock_guard<std::mutex> lock(view_mu_);
    return view_;
  }

 private:
  ReplicationManager(ReplicationOptions options, pk_engine_t* engine)
      : options_(std::move(options)),
        engine_(engine),
        queue_(kAsyncWorkerCount, kAsyncQueueDepth, kAsyncQueueBytes) {
    if (options_.bulk_enabled) {
      bulk_ = std::make_unique<BulkClients>(options_.bulk_port_offset,
                                            options_.bulk_socket_dir,
                                            options_.ack_timeout_ms + kAsyncForwardTimeoutMs);
    }
    grpc::ChannelArguments args;
    args.SetMaxSendMessageSize(kPeerMaxMessageBytes);
    args.SetMaxReceiveMessageSize(kPeerMaxMessageBytes);
    for (const std::string& address : options_.metadata_addresses) {
      metadata_.push_back(MetadataEndpoint{
          address,
          metadatav1::ClusterMetadataService::NewStub(grpc::CreateCustomChannel(
              address, grpc::InsecureChannelCredentials(), args))});
    }
  }

  void Complete(const std::shared_ptr<AckFanout>& fanout, bool ok,
                const std::string& error) {
    {
      std::lock_guard<std::mutex> lock(fanout->mu);
      fanout->completed++;
      if (ok)
        fanout->acked++;
      else if (fanout->first_error.empty())
        fanout->first_error = error;
    }
    fanout->cv.notify_all();
  }

  // Forwards one write to one peer. Returns an empty string on success, or a
  // human-readable reason. `from_replication` is what stops the peer from
  // forwarding it onward -- the single rule preventing a fan-out loop.
  std::string SendOne(const std::string& address, const std::string& key,
                      const std::string& value, int64_t timeout_ms) {
    auto stub = peers_.Get(address);
    if (!stub)
      return "could not create a client for " + address;

    grpc::Status status;
    if (value.size() <= kUnaryValueLimit) {
      nodev1::PutRequest request;
      request.set_key(key);
      request.set_value(value);
      request.set_from_replication(true);
      nodev1::PutResponse response;
      grpc::ClientContext context;
      context.set_deadline(DeadlineFromNow(timeout_ms));
      status = stub->Put(&context, request, &response);
    } else {
      // Phase 6: a value above the unary limit is exactly the case the bulk
      // transport exists for. Try it; on ANY failure fall through to the
      // chunked gRPC path, which is still the correctness baseline.
      //
      // A bulk PUT stores locally and never forwards -- the same rule
      // from_replication encodes on the gRPC path -- so a replicated write
      // cannot fan out a second time no matter which transport carried it.
      if (SendOneBulk(address, key, value))
        return std::string();
      status = SendChunked(*stub, key, value, timeout_ms);
    }

    if (status.ok())
      return std::string();
    const std::string reason = "peer " + address + " rejected a replicated write: " +
                               status.error_message();
    LogFailure(address, reason);
    return reason;
  }

  // SendOneBulk returns true only on a complete, acknowledged bulk write.
  // Every other outcome is silent-and-false, because the caller's fallback is
  // the real error path.
  bool SendOneBulk(const std::string& address, const std::string& key,
                   const std::string& value) {
    if (bulk_ == nullptr)
      return false;
    bulk::Client* client = bulk_->Get(address, PeerNodeIdFor(address));
    if (client == nullptr)
      return false;

    std::string error;
    if (client->Put(key, reinterpret_cast<const uint8_t*>(value.data()), value.size(), &error)) {
      bulk_writes_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    // A failed request leaves the connection's framing in an unknown state, so
    // it is discarded rather than reused.
    bulk_->Drop(address);
    bulk_fallbacks_.fetch_add(1, std::memory_order_relaxed);
    LogFailure(address, "bulk write to " + address + " fell back to gRPC: " + error);
    return false;
  }

  // The node ID the current topology says owns this address, so the bulk
  // handshake can reject a convention-derived endpoint that turns out to
  // belong to somebody else.
  std::string PeerNodeIdFor(const std::string& address) const {
    auto view = View();
    if (!view)
      return std::string();
    auto it = view->node_id_of_address.find(address);
    return it == view->node_id_of_address.end() ? std::string() : it->second;
  }

  grpc::Status SendChunked(nodev1::NodeService::Stub& stub, const std::string& key,
                           const std::string& value, int64_t timeout_ms) {
    grpc::ClientContext context;
    context.set_deadline(DeadlineFromNow(timeout_ms));
    nodev1::PutResponse response;
    auto writer = stub.PutChunked(&context, &response);

    const size_t total_chunks =
        value.empty() ? 1 : (value.size() + kReplicaChunkBytes - 1) / kReplicaChunkBytes;
    for (size_t i = 0; i < total_chunks; i++) {
      const size_t lo = i * kReplicaChunkBytes;
      const size_t hi = std::min(lo + kReplicaChunkBytes, value.size());
      nodev1::PutChunk chunk;
      chunk.set_chunk_index(static_cast<uint32_t>(i));
      chunk.set_total_chunks(static_cast<uint32_t>(total_chunks));
      chunk.set_total_length(value.size());
      if (hi > lo)
        chunk.set_data(value.data() + lo, hi - lo);
      if (i == 0) {
        chunk.set_key(key);
        chunk.set_from_replication(true);
      }
      if (!writer->Write(chunk))
        break;  // the real status arrives from Finish
    }
    writer->WritesDone();
    return writer->Finish();
  }

  void LogFailure(const std::string& address, const std::string& reason) {
    std::lock_guard<std::mutex> lock(log_mu_);
    FailureLog& entry = failure_log_[address];
    entry.suppressed++;
    const auto now = Clock::now();
    if (entry.last_logged != Clock::time_point() &&
        now - entry.last_logged < std::chrono::milliseconds(kFailureLogIntervalMs)) {
      return;
    }
    entry.last_logged = now;
    std::printf("[%s] replication: %s (%llu occurrence(s) since the last report)\n",
                options_.node_id.c_str(), reason.c_str(),
                (unsigned long long)entry.suppressed);
    entry.suppressed = 0;
  }

  bool WaitOrStop(int64_t millis) {
    std::unique_lock<std::mutex> lock(state_mu_);
    state_cv_.wait_for(lock, std::chrono::milliseconds(millis), [this] { return stopping_; });
    return stopping_;
  }

  bool stopping() const {
    std::lock_guard<std::mutex> lock(state_mu_);
    return stopping_;
  }

  // -------------------------------------------------------------------------
  // Topology polling
  // -------------------------------------------------------------------------

  void PollLoop() {
    for (;;) {
      auto view = FetchView();
      const bool changed = view && Publish(view);

      // A failed fetch deliberately does NOT shorten the interval: it already
      // cost a full RPC deadline, and hammering an unreachable control plane
      // helps nobody.
      int64_t wait_ms = options_.poll_interval_ms;
      if (changed || (view && view->peer_sources.empty()))
        wait_ms = std::min(wait_ms, kSettlingPollIntervalMs);
      if (WaitOrStop(wait_ms))
        return;
    }
  }

  // FetchView reads a coherent topology, falling back across control-plane
  // replicas until one answers.
  //
  // Phase 5 added the fallback; the coherence rule inside FetchViewFrom is
  // unchanged and does not know a Raft group exists. The important structural
  // point is that ONE attempt is confined to ONE replica: the two RPCs must
  // observe the same publisher, and splitting them across a leader and a
  // slightly-behind follower would produce a fingerprint mismatch that looks
  // exactly like membership churn.
  std::shared_ptr<const TopologyView> FetchView() {
    const size_t count = metadata_.size();
    if (count == 0)
      return nullptr;

    const size_t start = preferred_metadata_.load(std::memory_order_relaxed) % count;
    for (size_t offset = 0; offset < count; offset++) {
      if (stopping())
        return nullptr;
      const size_t index = (start + offset) % count;
      auto view = FetchViewFrom(index);
      if (view) {
        // Stick with whatever answered, so a healthy node is not rotating
        // across replicas for no reason.
        if (index != start)
          preferred_metadata_.store(index, std::memory_order_relaxed);
        return view;
      }
    }
    return nullptr;
  }

  // The same coherence rule internal/topology.Fetch applies, kept deliberately
  // minimal: GetNodeList and GetShardMap are separate RPCs, so they can observe
  // different memberships. Retry until the two carry the same content-derived
  // fingerprint, then trust the content. Recomputing the SHA-256 here would
  // mean maintaining a second implementation of the canonical serialisation in
  // C++, and a bug in it would reject every valid topology.
  std::shared_ptr<const TopologyView> FetchViewFrom(size_t index) {
    const std::string& address = metadata_[index].address;
    auto& stub = metadata_[index].stub;

    for (int attempt = 0; attempt < kMaxCoherenceAttempts; attempt++) {
      if (stopping())
        return nullptr;

      metadatav1::GetNodeListResponse nodes;
      {
        grpc::ClientContext context;
        context.set_deadline(DeadlineFromNow(options_.poll_interval_ms));
        metadatav1::GetNodeListRequest request;
        grpc::Status status = stub->GetNodeList(&context, request, &nodes);
        if (!status.ok()) {
          LogFailure(address, "GetNodeList on " + address + " failed: " + status.error_message());
          return nullptr;
        }
      }

      metadatav1::GetShardMapResponse shards;
      {
        grpc::ClientContext context;
        context.set_deadline(DeadlineFromNow(options_.poll_interval_ms));
        metadatav1::GetShardMapRequest request;
        grpc::Status status = stub->GetShardMap(&context, request, &shards);
        if (!status.ok()) {
          LogFailure(address, "GetShardMap on " + address + " failed: " + status.error_message());
          return nullptr;
        }
      }

      if (nodes.topology_fingerprint().empty() || shards.topology_fingerprint().empty()) {
        LogFailure(address,
                   "metadata on " + address + " published no topology fingerprint; "
                   "replication needs a Phase 3 or later control plane");
        return nullptr;
      }
      if (nodes.topology_fingerprint() != shards.topology_fingerprint())
        continue;  // membership moved between the two calls
      return BuildView(address, nodes, shards);
    }
    LogFailure(address,
               "metadata topology on " + address + " did not converge across " +
                   std::to_string(kMaxCoherenceAttempts) + " attempts");
    return nullptr;
  }

  std::shared_ptr<const TopologyView> BuildView(
      const std::string& source,
      const metadatav1::GetNodeListResponse& nodes,
      const metadatav1::GetShardMapResponse& shards) {
    if (shards.shard_count() == 0) {
      LogFailure(source, "metadata on " + source + " published a zero shard count");
      return nullptr;
    }

    std::unordered_map<std::string, std::string> address_of;
    for (const auto& node : nodes.nodes()) {
      if (node.node_id().empty() || node.address().empty()) {
        LogFailure(source, "metadata on " + source + " published a node with no ID or address");
        return nullptr;
      }
      address_of[node.node_id()] = node.address();
    }

    auto view = std::make_shared<TopologyView>();
    view->fingerprint = shards.topology_fingerprint();
    view->generation = shards.topology_generation();
    view->shard_count = shards.shard_count();
    view->replication_factor = shards.replication_factor();
    view->live_nodes = address_of.size();
    for (const auto& entry : address_of) {
      view->live_addresses.insert(entry.second);
      view->node_id_of_address[entry.second] = entry.first;
    }

    for (const auto& entry : shards.shard_to_owners()) {
      const uint32_t shard = entry.first;
      const metadatav1::ShardOwners& owners = entry.second;

      const bool primary_is_me = owners.primary() == options_.node_id;
      bool replica_is_me = false;
      for (const std::string& replica : owners.replicas())
        replica_is_me = replica_is_me || replica == options_.node_id;
      if (!primary_is_me && !replica_is_me)
        continue;  // this node holds no copy of this shard

      std::vector<std::string> others;
      if (!primary_is_me) {
        auto it = address_of.find(owners.primary());
        if (it != address_of.end())
          others.push_back(it->second);
      }
      std::vector<std::string> targets;
      for (const std::string& replica : owners.replicas()) {
        if (replica == options_.node_id)
          continue;
        auto it = address_of.find(replica);
        if (it == address_of.end())
          continue;  // named in the owner map but not live; skip rather than fail
        // Never forward to ourselves. The owner map should make this
        // impossible, but a node that replicated to its own address would
        // deadlock a handler thread against its own server.
        if (it->second == options_.self_address)
          continue;
        if (primary_is_me)
          targets.push_back(it->second);
        others.push_back(it->second);
      }

      if (primary_is_me)
        view->replica_targets[shard] = std::move(targets);
      view->peer_sources[shard] = std::move(others);
    }
    return view;
  }

  // Returns true when this view differed from the one already installed.
  bool Publish(const std::shared_ptr<const TopologyView>& view) {
    std::shared_ptr<const TopologyView> previous;
    {
      std::lock_guard<std::mutex> lock(view_mu_);
      previous = view_;
      if (previous && previous->fingerprint == view->fingerprint)
        return false;  // same content; nothing gained, nothing lost
      view_ = view;
    }
    peers_.Retain(view->live_addresses);
    if (bulk_)
      bulk_->Retain(view->live_addresses);

    size_t primary_shards = view->replica_targets.size();
    size_t replicated = 0;
    for (const auto& entry : view->replica_targets)
      replicated += entry.second.size();
    std::printf("[%s] replication: topology generation %llu, %zu live node(s), "
                "replication factor %u; this node primaries %zu shard(s) with "
                "%zu replica target(s) and holds %zu shard(s) in total\n",
                options_.node_id.c_str(), (unsigned long long)view->generation,
                view->live_nodes, view->replication_factor, primary_shards,
                replicated, view->peer_sources.size());

    if (!options_.catch_up)
      return true;
    std::vector<uint32_t> gained;
    for (const auto& entry : view->peer_sources) {
      if (previous && previous->Serves(entry.first))
        continue;  // already held a copy; nothing to backfill
      gained.push_back(entry.first);
    }
    if (gained.empty())
      return true;
    {
      std::lock_guard<std::mutex> lock(state_mu_);
      // One pending job, always the newest. A backfill against a topology two
      // generations stale would copy keys this node no longer owns.
      pending_catch_up_ = CatchUpJob{view, std::move(gained)};
    }
    state_cv_.notify_all();
    return true;
  }

  // -------------------------------------------------------------------------
  // Catch-up on newly-owned shards
  // -------------------------------------------------------------------------

  struct CatchUpJob {
    std::shared_ptr<const TopologyView> view;
    std::vector<uint32_t> shards;
  };

  void CatchUpLoop() {
    for (;;) {
      CatchUpJob job;
      {
        std::unique_lock<std::mutex> lock(state_mu_);
        state_cv_.wait(lock, [this] { return stopping_ || pending_catch_up_.has_value(); });
        if (stopping_)
          return;
        job = std::move(*pending_catch_up_);
        pending_catch_up_.reset();
      }
      RunCatchUp(job);
    }
  }

  // Best-effort backfill of shards this node just started holding -- including,
  // on a fresh start, every shard it owns, which is exactly the "restarted node
  // has an empty spill tier" gap Phase 3 left open.
  //
  // Correctness-first and deliberately unoptimised: it asks a peer for its whole
  // keyspace and keeps the keys that hash into the shards just gained. That is
  // O(total keys on the peer), the same cost pk_engine_scan_prefix already
  // documents for PrefixMatch itself. What it does NOT do is one scan per
  // shard: newly-gained shards are grouped by the peer that will serve them, so
  // gaining 64 shards from one node is one scan, not 64.
  void RunCatchUp(const CatchUpJob& job) {
    std::unordered_map<std::string, std::unordered_set<uint32_t>> by_source;
    size_t unsourced = 0;
    for (uint32_t shard : job.shards) {
      auto it = job.view->peer_sources.find(shard);
      if (it == job.view->peer_sources.end() || it->second.empty()) {
        // Nobody else holds this shard. With replication off that is the normal
        // case; the shard repopulates from future writes, exactly as it did
        // before Phase 4.
        unsourced++;
        continue;
      }
      by_source[it->second.front()].insert(shard);
    }
    if (by_source.empty()) {
      if (unsourced > 0) {
        std::printf("[%s] catch-up: %zu newly-held shard(s) have no other live holder; "
                    "they will repopulate from future writes\n",
                    options_.node_id.c_str(), unsourced);
      }
      return;
    }

    const auto started = Clock::now();
    uint64_t copied = 0;
    uint64_t skipped = 0;
    for (const auto& entry : by_source) {
      if (stopping())
        return;
      copied += CatchUpFrom(job.view, entry.first, entry.second, &skipped);
    }
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - started).count();
    std::printf("[%s] catch-up: copied %llu key(s) for %zu newly-held shard(s) from "
                "%zu peer(s) in %lld ms (%llu skipped, %zu shard(s) had no source)\n",
                options_.node_id.c_str(), (unsigned long long)copied, job.shards.size(),
                by_source.size(), (long long)elapsed, (unsigned long long)skipped, unsourced);
  }

  uint64_t CatchUpFrom(const std::shared_ptr<const TopologyView>& view,
                       const std::string& address,
                       const std::unordered_set<uint32_t>& wanted, uint64_t* skipped) {
    auto stub = peers_.Get(address);
    if (!stub)
      return 0;

    grpc::ClientContext context;
    context.set_deadline(DeadlineFromNow(kCatchUpTimeoutMs));
    nodev1::PrefixMatchRequest request;  // empty prefix: the whole keyspace
    auto reader = stub->PrefixMatch(&context, request);

    uint64_t copied = 0;
    nodev1::PrefixMatchResponse match;
    while (reader->Read(&match)) {
      if (stopping()) {
        context.TryCancel();
        break;
      }
      if (match.key().empty())
        continue;
      const uint32_t shard = ShardForKey(match.key(), view->shard_count);
      if (wanted.count(shard) == 0)
        continue;

      std::string value;
      if (match.value_omitted()) {
        // Above the unary limit, so PrefixMatch did not inline it. Without this
        // second fetch every multi-megabyte value would silently fail to
        // backfill, which is precisely the size of value this cache exists for.
        //
        // Phase 6: try the bulk transport first. This is the single biggest
        // consumer of large transfers in the whole system -- a node that just
        // gained 29 shards pulls every oversized value in them -- and it falls
        // back to the chunked path per key, so a peer without a bulk listener
        // simply backfills the way it always did.
        if (!FetchBulk(address, match.key(), &value) &&
            !FetchChunked(*stub, match.key(), &value)) {
          (*skipped)++;
          continue;
        }
      } else {
        value = match.value();
      }

      pk_engine_result_t rc = pk_engine_put(
          engine_, Bytes(match.key()), static_cast<uint32_t>(match.key().size()),
          value.empty() ? nullptr : Bytes(value), value.size());
      if (rc != PK_ENGINE_OK) {
        (*skipped)++;
        continue;
      }
      copied++;
    }

    grpc::Status status = reader->Finish();
    if (!status.ok())
      LogFailure(address, "catch-up scan of " + address + " failed: " + status.error_message());
    return copied;
  }

  // FetchBulk pulls one oversized value over the bulk transport. Same contract
  // as SendOneBulk: true only on a complete, verified-length transfer, false
  // for every other outcome so the caller falls back.
  bool FetchBulk(const std::string& address, const std::string& key, std::string* out) {
    if (bulk_ == nullptr)
      return false;
    bulk::Client* client = bulk_->Get(address, PeerNodeIdFor(address));
    if (client == nullptr)
      return false;

    bulk::Blob blob;
    bool found = false;
    std::string error;
    if (!client->Get(key, &blob, &found, &error)) {
      bulk_->Drop(address);
      bulk_fallbacks_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    if (!found)
      return false;  // vanished between scan and fetch; the scan is not a snapshot

    // Copied out of the blob because the value is about to go into the engine,
    // which owns its own storage. On the shared-memory path this is the only
    // copy the receiver makes.
    out->assign(reinterpret_cast<const char*>(blob.data()), blob.size());
    bulk_reads_.fetch_add(1, std::memory_order_relaxed);
    if (blob.mapped())
      bulk_shared_memory_reads_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  bool FetchChunked(nodev1::NodeService::Stub& stub, const std::string& key,
                    std::string* out) {
    grpc::ClientContext context;
    context.set_deadline(DeadlineFromNow(kCatchUpTimeoutMs));
    nodev1::GetRequest request;
    request.set_key(key);
    auto reader = stub.GetChunked(&context, request);

    out->clear();
    nodev1::GetChunk chunk;
    uint32_t expected = 0;
    uint64_t total_length = 0;
    bool any = false;
    while (reader->Read(&chunk)) {
      if (chunk.chunk_index() != expected)
        return false;
      if (!any)
        total_length = chunk.total_length();
      any = true;
      expected++;
      out->append(chunk.data());
    }
    if (!reader->Finish().ok())
      return false;
    // An empty stream is a miss: the key disappeared between the scan and the
    // fetch, which the engine's own scan contract calls normal.
    return any && out->size() == total_length;
  }

  const ReplicationOptions options_;
  pk_engine_t* const engine_;

  mutable std::mutex view_mu_;
  std::shared_ptr<const TopologyView> view_;

  PeerClients peers_;
  std::unique_ptr<BulkClients> bulk_;
  std::atomic<uint64_t> bulk_writes_{0};
  std::atomic<uint64_t> bulk_reads_{0};
  std::atomic<uint64_t> bulk_shared_memory_reads_{0};
  std::atomic<uint64_t> bulk_fallbacks_{0};
  AsyncQueue queue_;
  // One entry per control-plane replica, built once at construction and never
  // mutated, so the poller can read it without a lock.
  struct MetadataEndpoint {
    std::string address;
    std::unique_ptr<metadatav1::ClusterMetadataService::Stub> stub;
  };
  std::vector<MetadataEndpoint> metadata_;
  std::atomic<size_t> preferred_metadata_{0};

  mutable std::mutex state_mu_;
  std::condition_variable state_cv_;
  bool stopping_ = false;
  std::optional<CatchUpJob> pending_catch_up_;

  std::thread poller_;
  std::thread catch_up_worker_;
  std::atomic<size_t> ack_threads_{0};

  struct FailureLog {
    Clock::time_point last_logged;
    uint64_t suppressed = 0;
  };
  std::mutex log_mu_;
  std::unordered_map<std::string, FailureLog> failure_log_;
};

class NodeServiceImpl final : public nodev1::NodeService::Service {
 public:
  NodeServiceImpl(std::string node_id, pk_engine_t* engine,
                  std::shared_ptr<ReplicationManager> replication)
      : node_id_(std::move(node_id)),
        engine_(engine),
        replication_(std::move(replication)),
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

    // A forwarded copy: store it and stop. This one branch is the entire
    // loop prevention -- a replica that forwarded onward would fan a single
    // client write out across the cluster.
    if (request->from_replication()) {
      pk_engine_result_t rc =
          pk_engine_put(engine_, Bytes(key), static_cast<uint32_t>(key.size()),
                        Bytes(value), value.size());
      if (rc != PK_ENGINE_OK)
        return EngineFailure(rc, "Put(replicated)");
      response->set_ok(true);
      return grpc::Status::OK;
    }

    const uint32_t required = request->require_replica_acks();
    ReplicaPlan plan;
    if (replication_)
      plan = replication_->PlanFor(key);

    // Validated BEFORE the local write, so a rejected request leaves nothing
    // stored. A caller who asked for more acks than exist has misjudged the
    // cluster, and an INVALID_ARGUMENT that also wrote the value would be a
    // confusing thing to reason about afterwards.
    if (required > 0) {
      grpc::Status refusal = RefuseUnachievableAcks(plan, required);
      if (!refusal.ok())
        return refusal;
    }

    pk_engine_result_t rc =
        pk_engine_put(engine_, Bytes(key), static_cast<uint32_t>(key.size()),
                      Bytes(value), value.size());
    if (rc != PK_ENGINE_OK)
      return EngineFailure(rc, "Put");
    response->set_ok(true);

    if (required == 0) {
      // The default path: answer now, replicate after. replicas_acked stays 0
      // because nothing was waited for, and claiming otherwise would be a lie
      // the caller could not check.
      if (replication_)
        replication_->ForwardAsync(plan, key, value);
      return grpc::Status::OK;
    }

    std::string detail;
    const uint32_t acked = replication_->ForwardAndWait(plan, key, value, required, &detail);
    response->set_replicas_acked(acked);
    if (acked >= required)
      return grpc::Status::OK;

    // gRPC discards the response message on a non-OK status, so the ack count
    // has to travel in the message text or the caller never sees it.
    std::string why = "replicated to " + std::to_string(acked) + " of the " +
                      std::to_string(required) +
                      " requested replica(s) within the ack timeout; the local "
                      "write is committed and is NOT rolled back";
    if (!detail.empty())
      why += " (" + detail + ")";
    return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, why);
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
    bool from_replication = false;

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

        // Chunk 0 only, exactly as documented in proto/node.proto. Reading it
        // from later chunks would let a stream change its mind halfway.
        from_replication = chunk.from_replication();

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
      return EngineFailure(rc, from_replication ? "PutChunked(replicated)" : "PutChunked");

    response->set_ok(true);

    // Same split as Put, minus the strong-ack half: PutChunk carries no
    // require_replica_acks, so a chunked write always replicates in the
    // background. See the field comment in proto/node.proto for why.
    if (!from_replication && replication_)
      replication_->ForwardAsync(replication_->PlanFor(key), key, buffer);
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

    // Phase 9 observability. Every value below was already being computed --
    // pk_engine_capacity has reported the tier counters since Phase 1, and the
    // bulk transport has counted its own traffic since Phase 6 -- and until now
    // reached nothing but one log line at shutdown. Forwarding them costs a
    // struct copy on a diagnostic RPC nothing calls in a request path.
    response->set_keys_in_ram_tier(cap.keys_in_ram_tier);
    response->set_keys_in_nvme_tier(cap.keys_in_nvme_tier);
    response->set_spills(cap.spills);
    response->set_promotions(cap.promotions);
    response->set_spill_errors(cap.spill_errors);
    response->set_evict_drops(cap.evict_drops);

    // Null when this node was started without --metadata-addr: it then does no
    // replication and owns no bulk transport, so reporting zeros is the honest
    // answer rather than an omission.
    if (replication_) {
      const auto bulk = replication_->bulk_stats();
      response->set_bulk_writes(bulk.writes);
      response->set_bulk_reads(bulk.reads);
      response->set_bulk_shared_memory_reads(bulk.shared_memory_reads);
      response->set_bulk_fallbacks(bulk.fallbacks);
    }
    return grpc::Status::OK;
  }

 private:
  // Every way a strong-ack request can be impossible rather than merely slow,
  // reported as INVALID_ARGUMENT with the reason. The alternative -- accepting
  // it and letting the fan-out time out -- would make a permanently
  // unsatisfiable request cost a full ack timeout on every retry, and would
  // report a configuration mistake as a transient failure.
  grpc::Status RefuseUnachievableAcks(const ReplicaPlan& plan, uint32_t required) const {
    if (!plan.enabled) {
      return Invalid("require_replica_acks=" + std::to_string(required) +
                     " but this node was started without --metadata-addr, so it "
                     "does not replicate at all");
    }
    if (!plan.have_topology) {
      return Invalid("require_replica_acks=" + std::to_string(required) +
                     " but this node has not yet read a coherent cluster topology");
    }
    if (!plan.is_primary) {
      return Invalid("require_replica_acks=" + std::to_string(required) +
                     " but this node is not the primary for shard " +
                     std::to_string(plan.shard) + "; write through the primary");
    }
    if (required > plan.targets.size()) {
      return Invalid("require_replica_acks=" + std::to_string(required) +
                     " exceeds the " + std::to_string(plan.targets.size()) +
                     " live replica(s) for shard " + std::to_string(plan.shard));
    }
    return grpc::Status::OK;
  }

  int64_t UptimeSeconds() const {
    const auto elapsed = std::chrono::steady_clock::now() - started_;
    return std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
  }

  const std::string node_id_;
  pk_engine_t* const engine_;
  // Null when --metadata-addr was not supplied: the node then behaves exactly
  // as it did in Phases 1-3, which keeps replication strictly opt-in.
  const std::shared_ptr<ReplicationManager> replication_;
  const std::chrono::steady_clock::time_point started_;
};

struct Options {
  std::string node_id;
  std::string host = "127.0.0.1";
  int port = 0;
  std::string data_dir;
  uint64_t ram_budget_bytes = PK_ENGINE_DEFAULT_RAM_BUDGET_BYTES;
  uint64_t max_value_bytes = PK_ENGINE_DEFAULT_MAX_VALUE_BYTES;

  // Phase 4. An empty metadata address disables replication entirely and the
  // node behaves exactly as it did in Phases 1-3.
  std::string metadata_addr;
  uint64_t topology_poll_interval_ms = 2000;
  uint64_t replica_ack_timeout_ms = 2000;
  bool catch_up = true;

  // Phase 6 bulk transport. On by default because it is strictly an
  // optimisation with a total fallback; --no-bulk-transport turns it off, which
  // is what the benchmark uses to measure the two paths against each other.
  bool bulk_transport = true;
  int bulk_port_offset = 1000;
  std::string bulk_socket_dir = "/tmp/pulsekv-bulk";
  std::string bulk_send_mode = "write";
  bool bulk_memfd = true;
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
      "  --max-value-bytes N       hard ceiling per value, in bytes (default %llu)\n"
      "\n"
      "replication (Phase 4; all optional):\n"
      "  --metadata-addr LIST      comma-separated ClusterMetadataService\n"
      "                            addresses to read shard ownership from, e.g.\n"
      "                            host:7000,host:7001,host:7002. Any replica may\n"
      "                            answer; the node prefers whichever did last and\n"
      "                            falls back across the rest, so one replica being\n"
      "                            down or mid-election is not visible here.\n"
      "                            WITHOUT THIS FLAG THE NODE DOES NOT REPLICATE\n"
      "                            AT ALL and behaves exactly as it did before\n"
      "                            Phase 4. With it, this node forwards writes for\n"
      "                            the shards it primaries to that shard's\n"
      "                            replicas, and backfills shards it newly starts\n"
      "                            holding.\n"
      "  --topology-poll-interval-ms N\n"
      "                            how often to re-read ownership (default %llu)\n"
      "  --replica-ack-timeout-ms N\n"
      "                            how long a require_replica_acks write waits\n"
      "                            for its acks (default %llu)\n"
      "  --no-catch-up             do not backfill newly-held shards from a peer.\n"
      "                            Replication still runs; only the initial copy\n"
      "                            is skipped.\n"
      "\n"
      "bulk transport (Phase 6; large values only, always optional):\n"
      "  --no-bulk-transport       do not listen for, or use, bulk transfers.\n"
      "                            Every large value then moves over the Phase 1\n"
      "                            chunked gRPC path, exactly as before.\n"
      "  --bulk-port-offset N      bulk TCP port = --port + N (default 1000)\n"
      "  --bulk-socket-dir PATH    directory for the same-host unix socket\n"
      "                            (default /tmp/pulsekv-bulk). A peer on another\n"
      "                            host simply will not find this path, which is\n"
      "                            how same-host is detected.\n"
      "  --bulk-send-mode MODE     how an inline value reaches the socket:\n"
      "                            write (default) or sendfile. Exposed because\n"
      "                            Phase 6 measured them rather than assuming.\n"
      "                            A vmsplice/splice mode was implemented, then\n"
      "                            removed for corrupting data under concurrent\n"
      "                            readers -- see node/grpc_shim/bulk.cc.\n"
      "  --no-bulk-memfd           never hand a memfd to a same-host peer; send\n"
      "                            the bytes inline instead. For measuring what\n"
      "                            the shared-memory path actually buys.\n",
      argv0, (unsigned long long)PK_ENGINE_DEFAULT_RAM_BUDGET_BYTES,
      (unsigned long long)PK_ENGINE_DEFAULT_MAX_VALUE_BYTES,
      (unsigned long long)2000, (unsigned long long)2000);
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
    } else if (name == "--metadata-addr") {
      if (!next_value("--metadata-addr")) return false;
      out->metadata_addr = value;
    } else if (name == "--topology-poll-interval-ms") {
      if (!next_value("--topology-poll-interval-ms")) return false;
      if (!ParseU64(value.c_str(), &out->topology_poll_interval_ms,
                    "--topology-poll-interval-ms"))
        return false;
    } else if (name == "--replica-ack-timeout-ms") {
      if (!next_value("--replica-ack-timeout-ms")) return false;
      if (!ParseU64(value.c_str(), &out->replica_ack_timeout_ms,
                    "--replica-ack-timeout-ms"))
        return false;
    } else if (name == "--no-catch-up") {
      out->catch_up = false;
    } else if (name == "--no-bulk-transport") {
      out->bulk_transport = false;
    } else if (name == "--no-bulk-memfd") {
      out->bulk_memfd = false;
    } else if (name == "--bulk-port-offset") {
      if (!next_value("--bulk-port-offset")) return false;
      char* end = nullptr;
      const long parsed = std::strtol(value.c_str(), &end, 10);
      if (end == value.c_str() || *end != '\0' || parsed == 0 || parsed < -65535 ||
          parsed > 65535) {
        std::fprintf(stderr, "error: --bulk-port-offset %s is not a usable offset\n",
                     value.c_str());
        return false;
      }
      out->bulk_port_offset = static_cast<int>(parsed);
    } else if (name == "--bulk-socket-dir") {
      if (!next_value("--bulk-socket-dir")) return false;
      out->bulk_socket_dir = value;
    } else if (name == "--bulk-send-mode") {
      if (!next_value("--bulk-send-mode")) return false;
      bulk::SendMode parsed;
      if (!bulk::ParseSendMode(value, &parsed)) {
        std::fprintf(stderr, "error: --bulk-send-mode %s is not write or sendfile\n",
                     value.c_str());
        return false;
      }
      out->bulk_send_mode = value;
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
  if (out->metadata_addr.empty() && !out->catch_up) {
    std::fprintf(stderr,
                 "error: --no-catch-up is meaningless without --metadata-addr; "
                 "replication is already off\n");
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

  // Built before the server starts so a write arriving on the very first
  // connection already sees a manager, even if its first topology poll has not
  // landed yet. A plan with have_topology = false is a correct answer; a null
  // pointer race would not be.
  std::shared_ptr<ReplicationManager> replication;
  if (!options.metadata_addr.empty()) {
    ReplicationOptions replication_options;
    replication_options.node_id = options.node_id;
    replication_options.self_address = address;
    replication_options.metadata_addresses = SplitEndpoints(options.metadata_addr);
    if (replication_options.metadata_addresses.empty()) {
      std::fprintf(stderr, "[%s] fatal: --metadata-addr listed no usable address\n",
                   options.node_id.c_str());
      pk_engine_destroy(engine);
      return 1;
    }
    replication_options.poll_interval_ms =
        static_cast<int64_t>(options.topology_poll_interval_ms);
    replication_options.ack_timeout_ms =
        static_cast<int64_t>(options.replica_ack_timeout_ms);
    replication_options.catch_up = options.catch_up;
    replication_options.bulk_enabled = options.bulk_transport;
    replication_options.bulk_port_offset = options.bulk_port_offset;
    replication_options.bulk_socket_dir = options.bulk_socket_dir;
    replication = ReplicationManager::Create(std::move(replication_options), engine);
  }

  // The bulk listener is opened before anything advertises this node as ready.
  // A peer that can reach us over gRPC but not over bulk would simply fall
  // back, so this is not a correctness requirement -- but it does avoid a
  // pointless burst of fallbacks in the first seconds after a restart.
  std::unique_ptr<bulk::Server> bulk_server;
  if (options.bulk_transport) {
    bulk::ServerOptions bulk_options;
    bulk_options.node_id = options.node_id;
    bulk_options.host = options.host;
    bulk_options.tcp_port = bulk::BulkPort(options.port, options.bulk_port_offset);
    bulk_options.unix_path =
        bulk::UnixSocketPath(options.bulk_socket_dir, options.host, options.port);
    bulk_options.max_value_bytes = pk_engine_max_value_bytes(engine);
    bulk_options.allow_memfd = options.bulk_memfd;
    bulk::ParseSendMode(options.bulk_send_mode, &bulk_options.send_mode);

    if (!options.bulk_socket_dir.empty()) {
      // Best effort: a directory we cannot create simply means no unix socket,
      // and the TCP listener still works.
      ::mkdir(options.bulk_socket_dir.c_str(), 0700);
    }

    std::string bulk_error;
    bulk_server = bulk::Server::Start(std::move(bulk_options), engine, &bulk_error);
    if (!bulk_server) {
      // Not fatal. A node without a bulk listener is a node that moves large
      // values over gRPC, which is exactly what every phase before this one did.
      std::fprintf(stderr,
                   "[%s] warning: bulk transport disabled (%s); large transfers "
                   "will use the chunked gRPC path\n",
                   options.node_id.c_str(), bulk_error.c_str());
    }
  }

  NodeServiceImpl service(options.node_id, engine, replication);

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
  if (bulk_server) {
    std::printf("[%s] bulk transport: tcp %s:%d, unix %s, send-mode %s, memfd %s\n",
                options.node_id.c_str(), options.host.c_str(), bulk_server->tcp_port(),
                bulk_server->unix_path().empty() ? "(none)"
                                                 : bulk_server->unix_path().c_str(),
                options.bulk_send_mode.c_str(),
                (options.bulk_memfd && bulk::MemfdSupported()) ? "on" : "off");
  } else {
    std::printf("[%s] bulk transport: DISABLED; large values use the chunked "
                "gRPC path\n",
                options.node_id.c_str());
  }
  if (replication) {
    std::printf("[%s] replication: reading ownership from [%s] every %llu ms, "
                "ack timeout %llu ms, catch-up %s\n",
                options.node_id.c_str(), options.metadata_addr.c_str(),
                (unsigned long long)options.topology_poll_interval_ms,
                (unsigned long long)options.replica_ack_timeout_ms,
                options.catch_up ? "on" : "off");
  } else {
    std::printf("[%s] replication: DISABLED (no --metadata-addr); writes are "
                "stored locally only\n",
                options.node_id.c_str());
  }

  // Started after the server is listening. The first thing catch-up does is
  // scan a peer, and a peer that scans back before this node can answer would
  // see an unnecessary failure.
  if (replication)
    replication->Start();

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

  // Stopped after the gRPC server and before the engine, for the same reason
  // replication is: its handlers call pk_engine_get/put, so every one of them
  // must be finished while the engine is still alive.
  if (bulk_server) {
    const auto& bulk_stats = bulk_server->stats();
    std::printf("[%s] bulk transport: %llu get(s), %llu put(s), %llu memfd handoff(s), "
                "%llu inline send(s), %llu error(s), %llu refused connection(s)\n",
                options.node_id.c_str(),
                (unsigned long long)bulk_stats.gets.load(),
                (unsigned long long)bulk_stats.puts.load(),
                (unsigned long long)bulk_stats.memfd_handoffs.load(),
                (unsigned long long)bulk_stats.inline_sends.load(),
                (unsigned long long)bulk_stats.errors.load(),
                (unsigned long long)bulk_stats.rejected_connections.load());
    bulk_server->Stop();
    bulk_server.reset();
  }

  // Stopped after the server, and before the engine: catch-up is the one piece
  // of replication that calls into the engine, so it must be joined while the
  // engine is still alive. Stop() drains the background queue and waits, with a
  // bound, for detached strong-ack forwards.
  if (replication) {
    replication->Stop();
    const auto bulk_client_stats = replication->bulk_stats();
    if (bulk_client_stats.enabled) {
      std::printf("[%s] bulk client: %llu write(s), %llu read(s) (%llu via shared "
                  "memory), %llu fallback(s) to gRPC\n",
                  options.node_id.c_str(),
                  (unsigned long long)bulk_client_stats.writes,
                  (unsigned long long)bulk_client_stats.reads,
                  (unsigned long long)bulk_client_stats.shared_memory_reads,
                  (unsigned long long)bulk_client_stats.fallbacks);
    }
    const uint64_t dropped = replication->dropped_writes();
    if (dropped > 0) {
      std::printf("[%s] replication: %llu background write(s) were dropped at the "
                  "queue bound over this process's lifetime\n",
                  options.node_id.c_str(), (unsigned long long)dropped);
    }
    replication.reset();
  }

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
