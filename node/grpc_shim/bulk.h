// PulseKV v2 bulk transport -- the large-blob data path.
//
// WHY THIS EXISTS, AND WHY IT IS NOT gRPC.
//
// Phase 1 proved chunked gRPC framing is *correct* for multi-megabyte KV-cache
// blocks. It is not fast. Every chunk is copied out of the engine, into a
// protobuf string, into gRPC's own slice buffers, and only then into the
// kernel; the receiver pays the mirror image. For a 64 MiB tensor block that is
// several hundred megabytes of memmove nobody asked for.
//
// This file is the transport the design doc's section 4.5 describes: chunked
// streaming that avoids full userspace copies where it can, following the
// SGLang HiCache pattern of keeping the tensor payload out of the control-plane
// RPC entirely and passing a shared-memory region instead.
//
// WHAT IT IS NOT. It does not replace the gRPC chunked path. That path stays
// the correctness baseline (Phase 1's tests) and the fallback for every case
// this one cannot serve -- a peer on another host, a kernel without memfd, a
// refused connection, a protocol error. Every entry point here returns false
// rather than throwing or hanging, and every caller is expected to fall back.
// A fast path that can fail the request is not a fast path, it is a bug.
//
// THE ENGINE BOUNDARY IS NOT CROSSED. Same rule as Phase 4's replication: this
// is a network-layer concern living above node/engine/include/pulsekv_engine.h.
// The engine hands out a private heap buffer and knows nothing about sockets,
// shared memory, or peers. That boundary is also the ceiling on how zero-copy
// this can be -- see docs/pulsekv-v2-phase6-summary.md, which measures exactly
// what it costs.

#ifndef PULSEKV_BULK_H
#define PULSEKV_BULK_H

#include <stdint.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "pulsekv_engine.h"

namespace pulsekv {
namespace bulk {

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------
//
// Fixed 32-byte headers, big-endian, explicit lengths on everything. Same
// discipline as v1's framing: a reader never guesses how much is coming, and
// every length is bounds-checked against a configured maximum BEFORE a byte is
// allocated for it.

constexpr uint32_t kMagic = 0x504B4231;  // "PKB1"
constexpr size_t kHeaderBytes = 32;

enum class Op : uint8_t {
  kPing = 1,  // returns the server's node ID; the identity half of the handshake
  kGet = 2,
  kPut = 3,
};

enum class Status : uint8_t {
  kOk = 0,
  kNotFound = 1,
  kError = 2,
  // The value is NOT inline. One memfd arrives out-of-band via SCM_RIGHTS and
  // the receiver maps it. Only ever sent over a Unix socket.
  kOkMemfd = 3,
};

// Request flags.
constexpr uint8_t kFlagAcceptMemfd = 1u << 0;

// How the server pushes an inline value onto the socket.
//
// Three strategies, kept switchable because the whole point of Phase 6 is to
// measure them against each other rather than assume. See §5 of the phase
// summary for what the measurements actually said.
enum class SendMode : uint8_t {
  // writev(header, value). One copy_from_user. The honest baseline.
  kWrite = 0,
  // NOTE: a vmsplice/splice mode used to sit here. It was removed after it was
  // measured corrupting data under concurrent readers -- the pipe holds
  // references to pages we then free. bulk.cc documents the failure and the
  // proof in full; do not re-add it without MSG_ZEROCOPY completions.
  //
  // Stage the value in a memfd, then sendfile() it. Zero copies onto the
  // socket, one copy to populate the memfd. Included because it is what a
  // value already living in a file -- the NVMe spill tier -- would cost, and
  // measuring it is how this phase quantifies the engine change it cannot make.
  kSendfile = 2,
};

const char* SendModeName(SendMode mode);
bool ParseSendMode(const std::string& text, SendMode* out);

// ShardForKey maps a key to a cluster shard.
//
// Must agree, bit for bit, with router.ShardForKey in
// control/internal/router/router.go: FNV-1a over the raw key bytes, 64-bit,
// modulo the shard count. It lives here rather than in main.cpp so the node and
// the benchmark share one implementation -- a second copy of this hash is how a
// node ends up replicating to the peers of some other shard.
uint32_t ShardForKey(const uint8_t* key, size_t key_len, uint32_t shard_count);
uint32_t ShardForKey(const std::string& key, uint32_t shard_count);

// ---------------------------------------------------------------------------
// Endpoint naming
// ---------------------------------------------------------------------------
//
// Derived by convention from a peer's NodeService address rather than
// advertised through the metadata plane. That is a deliberate Phase 6 choice:
// step 6.1 asks for no new service discovery, and the control plane is supposed
// to stay out of the bulk path entirely. The handshake verifies the server's
// node ID, so a convention collision is caught rather than silently misrouted.
//
// Phase 7 should promote this to an advertised capability on NodeInfo; see the
// summary's handoff.
struct Endpoint {
  std::string tcp;        // host:port, empty when unknown
  std::string unix_path;  // filesystem path, empty when unknown
};

// UnixSocketPath is keyed by the peer's SERVICE address, not by node ID, so a
// client looking for a remote peer's socket looks for a path that simply does
// not exist locally -- which is exactly the fallback signal wanted.
std::string UnixSocketPath(const std::string& socket_dir, const std::string& host, int port);

// BulkPort is the peer's bulk TCP port, derived from its service port.
int BulkPort(int service_port, int offset);

Endpoint EndpointForPeer(const std::string& service_address, int port_offset,
                         const std::string& socket_dir);

// ---------------------------------------------------------------------------
// Blob -- a received value
// ---------------------------------------------------------------------------
//
// Either an owned heap buffer (inline transfer) or a read-only mapping of a
// memfd the peer handed over (shared-memory transfer). The caller reads through
// data() either way and does not care which; mapped() exists so a benchmark can
// report honestly which path it actually got.
class Blob {
 public:
  Blob() = default;
  ~Blob();
  Blob(Blob&& other) noexcept;
  Blob& operator=(Blob&& other) noexcept;
  Blob(const Blob&) = delete;
  Blob& operator=(const Blob&) = delete;

  const uint8_t* data() const { return data_; }
  size_t size() const { return size_; }
  bool mapped() const { return mapping_ != nullptr; }
  void Reset();

  // Adopts an owned heap buffer.
  static Blob FromOwned(std::vector<uint8_t> bytes);
  // Adopts a mapping. Takes ownership of the mapping and closes fd itself.
  static Blob FromMapping(void* mapping, size_t length);

 private:
  std::vector<uint8_t> owned_;
  void* mapping_ = nullptr;
  const uint8_t* data_ = nullptr;
  size_t size_ = 0;
};

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

struct ServerOptions {
  std::string node_id;
  std::string host = "127.0.0.1";

  // 0 disables the TCP listener; an empty path disables the Unix listener.
  // A node with neither is simply a node without a bulk transport, which is a
  // supported configuration -- everything falls back to gRPC.
  int tcp_port = 0;
  std::string unix_path;

  uint64_t max_value_bytes = 0;  // 0 takes the engine's own ceiling
  size_t max_connections = 64;
  SendMode send_mode = SendMode::kWrite;
  bool allow_memfd = true;

  int64_t io_timeout_ms = 30000;
};

struct ServerStats {
  std::atomic<uint64_t> gets{0};
  std::atomic<uint64_t> puts{0};
  std::atomic<uint64_t> memfd_handoffs{0};
  std::atomic<uint64_t> inline_sends{0};
  std::atomic<uint64_t> errors{0};
  std::atomic<uint64_t> rejected_connections{0};
};

class Server {
 public:
  // Returns nullptr when no listener could be opened. That is not fatal to the
  // node: the caller logs it and keeps serving gRPC.
  static std::unique_ptr<Server> Start(ServerOptions options, pk_engine_t* engine,
                                       std::string* error);
  ~Server();

  void Stop();

  const ServerStats& stats() const { return stats_; }
  int tcp_port() const { return bound_tcp_port_; }
  const std::string& unix_path() const { return options_.unix_path; }

 private:
  class Impl;
  explicit Server(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
  ServerOptions options_;
  ServerStats stats_;
  int bound_tcp_port_ = 0;

  friend class Impl;
};

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

struct ClientOptions {
  int64_t io_timeout_ms = 30000;
  bool accept_memfd = true;
  // When set, the client refuses any endpoint whose PING reports a different
  // node ID. This is what makes a convention-derived endpoint safe.
  std::string expect_node_id;
};

// Client owns one connection. Callers cache instances per endpoint, the same
// way Phase 4's PeerClients caches gRPC stubs.
//
// EVERY method returns false on any failure, never throws, and never blocks
// past the configured timeout. False means "use the gRPC path", always.
class Client {
 public:
  ~Client();
  Client(Client&&) noexcept;
  Client& operator=(Client&&) noexcept;
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  // Connects and performs the PING handshake, verifying node identity when
  // ClientOptions::expect_node_id is set.
  static std::unique_ptr<Client> Connect(const Endpoint& endpoint, ClientOptions options,
                                         std::string* error);

  bool Get(const std::string& key, Blob* out, bool* found, std::string* error);
  bool Put(const std::string& key, const uint8_t* value, size_t length, std::string* error);

  bool over_unix_socket() const { return over_unix_socket_; }
  const std::string& peer_node_id() const { return peer_node_id_; }
  const std::string& address() const { return address_; }

 private:
  Client() = default;
  int fd_ = -1;
  bool over_unix_socket_ = false;
  bool accept_memfd_ = false;
  std::string peer_node_id_;
  std::string address_;
};

// MemfdSupported reports whether this kernel/libc can create sealable memfds.
// Checked once at startup so the shared-memory path can be disabled cleanly
// rather than failing per request.
bool MemfdSupported();

}  // namespace bulk
}  // namespace pulsekv

#endif  // PULSEKV_BULK_H
