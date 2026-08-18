#include "bulk.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <mutex>
#include <thread>

#if defined(__linux__)
#include <linux/memfd.h>
#include <sys/syscall.h>
#endif

namespace pulsekv {
namespace bulk {
namespace {

// ---------------------------------------------------------------------------
// Byte order and header packing
// ---------------------------------------------------------------------------
//
// Big-endian on the wire for the same reason the topology fingerprint uses it:
// unambiguous across machines, and free.

void PutU16(uint8_t* p, uint16_t v) {
  p[0] = static_cast<uint8_t>(v >> 8);
  p[1] = static_cast<uint8_t>(v);
}

void PutU32(uint8_t* p, uint32_t v) {
  p[0] = static_cast<uint8_t>(v >> 24);
  p[1] = static_cast<uint8_t>(v >> 16);
  p[2] = static_cast<uint8_t>(v >> 8);
  p[3] = static_cast<uint8_t>(v);
}

void PutU64(uint8_t* p, uint64_t v) {
  for (int i = 0; i < 8; i++) p[i] = static_cast<uint8_t>(v >> (56 - 8 * i));
}

uint32_t GetU32(const uint8_t* p) {
  return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
         (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}

uint64_t GetU64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; i++) v = (v << 8) | p[i];
  return v;
}

// Header layout, both directions:
//   [0..3]   magic
//   [4]      opcode / status
//   [5]      flags
//   [6..7]   reserved
//   [8..11]  key_len (request) / detail_len (response)
//   [12..19] value_len
//   [20..31] reserved
struct Header {
  uint8_t code = 0;
  uint8_t flags = 0;
  uint32_t aux_len = 0;
  uint64_t value_len = 0;
};

void EncodeHeader(const Header& header, uint8_t out[kHeaderBytes]) {
  memset(out, 0, kHeaderBytes);
  PutU32(out, kMagic);
  out[4] = header.code;
  out[5] = header.flags;
  PutU16(out + 6, 0);
  PutU32(out + 8, header.aux_len);
  PutU64(out + 12, header.value_len);
}

bool DecodeHeader(const uint8_t in[kHeaderBytes], Header* out) {
  if (GetU32(in) != kMagic) return false;
  out->code = in[4];
  out->flags = in[5];
  out->aux_len = GetU32(in + 8);
  out->value_len = GetU64(in + 12);
  return true;
}

// ---------------------------------------------------------------------------
// Bounded, restartable socket I/O
// ---------------------------------------------------------------------------
//
// Every loop here handles short reads/writes and EINTR. A partial write treated
// as a whole one is the classic way a binary protocol silently corrupts a
// multi-megabyte payload, so none of these return until the full length has
// moved or the connection has failed.

bool ReadFully(int fd, void* buffer, size_t length) {
  uint8_t* out = static_cast<uint8_t*>(buffer);
  size_t done = 0;
  while (done < length) {
    ssize_t n = recv(fd, out + done, length - done, 0);
    if (n > 0) {
      done += static_cast<size_t>(n);
      continue;
    }
    if (n == 0) return false;  // peer closed mid-message
    if (errno == EINTR) continue;
    return false;
  }
  return true;
}

bool WriteFully(int fd, const void* buffer, size_t length) {
  const uint8_t* in = static_cast<const uint8_t*>(buffer);
  size_t done = 0;
  while (done < length) {
    ssize_t n = send(fd, in + done, length - done, MSG_NOSIGNAL);
    if (n > 0) {
      done += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}

bool WriteVectored(int fd, const void* a, size_t a_len, const void* b, size_t b_len) {
  // One syscall for header+payload when the payload is small enough to be worth
  // coalescing; falls into the plain loop for the remainder.
  struct iovec iov[2];
  iov[0].iov_base = const_cast<void*>(a);
  iov[0].iov_len = a_len;
  iov[1].iov_base = const_cast<void*>(b);
  iov[1].iov_len = b_len;

  size_t total = a_len + b_len;
  size_t done = 0;
  int index = 0;
  while (done < total) {
    ssize_t n = writev(fd, iov + index, 2 - index);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    done += static_cast<size_t>(n);
    size_t consumed = static_cast<size_t>(n);
    while (index < 2 && consumed >= iov[index].iov_len) {
      consumed -= iov[index].iov_len;
      index++;
    }
    if (index < 2 && consumed > 0) {
      iov[index].iov_base = static_cast<uint8_t*>(iov[index].iov_base) + consumed;
      iov[index].iov_len -= consumed;
    }
  }
  return true;
}

void SetTimeouts(int fd, int64_t millis) {
  struct timeval tv;
  tv.tv_sec = static_cast<time_t>(millis / 1000);
  tv.tv_usec = static_cast<suseconds_t>((millis % 1000) * 1000);
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

void SetTcpTuning(int fd) {
  int one = 1;
  // Bulk transfers are big and latency-sensitive at the tail; Nagle only adds
  // delay when every write is already large.
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

// ---------------------------------------------------------------------------
// memfd
// ---------------------------------------------------------------------------

int MemfdCreate(const char* name, unsigned int flags) {
#if defined(__linux__) && defined(SYS_memfd_create)
  return static_cast<int>(syscall(SYS_memfd_create, name, flags));
#else
  (void)name;
  (void)flags;
  errno = ENOSYS;
  return -1;
#endif
}

#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif
#ifndef MFD_ALLOW_SEALING
#define MFD_ALLOW_SEALING 0x0002U
#endif
#ifndef F_ADD_SEALS
#define F_ADD_SEALS 1033
#endif
#ifndef F_SEAL_SHRINK
#define F_SEAL_SHRINK 0x0002
#endif
#ifndef F_SEAL_GROW
#define F_SEAL_GROW 0x0004
#endif
#ifndef F_SEAL_WRITE
#define F_SEAL_WRITE 0x0008
#endif

// StageInMemfd copies a value into a sealed, read-only memfd.
//
// Sealing matters: the receiver maps this region and reads it directly, so it
// needs a guarantee nobody can shrink or rewrite the pages underneath it. A
// shared mapping without seals would be a data race with the sender's next
// request.
//
// The copy here is the one this phase cannot remove. pk_engine_get hands back a
// private heap buffer; getting those bytes into a shareable region means
// copying them. Removing it requires the engine to be able to write into a
// caller-provided mapping, which is the boundary change Phase 6 was scoped away
// from -- and §5 of the summary measures exactly what it costs.
int StageInMemfd(const uint8_t* value, size_t length) {
  int fd = MemfdCreate("pulsekv-blob", MFD_CLOEXEC | MFD_ALLOW_SEALING);
  if (fd < 0) return -1;

  if (length > 0) {
    if (ftruncate(fd, static_cast<off_t>(length)) != 0) {
      close(fd);
      return -1;
    }
    void* mapping = mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
      close(fd);
      return -1;
    }
    memcpy(mapping, value, length);
    munmap(mapping, length);
  }

  // Seal after the content is final. F_SEAL_SEAL is deliberately omitted so a
  // future sender could still add seals; the three that matter for the reader
  // are all set.
  if (fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE) != 0) {
    // A kernel without sealing can still transfer, it just cannot promise
    // immutability. Refuse rather than hand over a region we cannot vouch for.
    close(fd);
    return -1;
  }
  return fd;
}

bool SendFdWithHeader(int socket_fd, const uint8_t header[kHeaderBytes], int payload_fd) {
  struct msghdr message;
  memset(&message, 0, sizeof(message));
  struct iovec iov;
  iov.iov_base = const_cast<uint8_t*>(header);
  iov.iov_len = kHeaderBytes;
  message.msg_iov = &iov;
  message.msg_iovlen = 1;

  union {
    char buffer[CMSG_SPACE(sizeof(int))];
    struct cmsghdr align;
  } control;
  memset(&control, 0, sizeof(control));
  message.msg_control = control.buffer;
  message.msg_controllen = sizeof(control.buffer);

  struct cmsghdr* cmsg = CMSG_FIRSTHDR(&message);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int));
  memcpy(CMSG_DATA(cmsg), &payload_fd, sizeof(int));

  for (;;) {
    ssize_t n = sendmsg(socket_fd, &message, MSG_NOSIGNAL);
    if (n == static_cast<ssize_t>(kHeaderBytes)) return true;
    if (n < 0 && errno == EINTR) continue;
    // A short send of a 32-byte header with ancillary data is not something
    // that happens on a healthy socket; treating it as failure is correct.
    return false;
  }
}

bool ReceiveFdWithHeader(int socket_fd, uint8_t header[kHeaderBytes], int* payload_fd) {
  struct msghdr message;
  memset(&message, 0, sizeof(message));
  struct iovec iov;
  iov.iov_base = header;
  iov.iov_len = kHeaderBytes;
  message.msg_iov = &iov;
  message.msg_iovlen = 1;

  union {
    char buffer[CMSG_SPACE(sizeof(int))];
    struct cmsghdr align;
  } control;
  memset(&control, 0, sizeof(control));
  message.msg_control = control.buffer;
  message.msg_controllen = sizeof(control.buffer);

  ssize_t n;
  do {
    n = recvmsg(socket_fd, &message, 0);
  } while (n < 0 && errno == EINTR);
  if (n != static_cast<ssize_t>(kHeaderBytes)) return false;

  *payload_fd = -1;
  for (struct cmsghdr* cmsg = CMSG_FIRSTHDR(&message); cmsg != nullptr;
       cmsg = CMSG_NXTHDR(&message, cmsg)) {
    if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS &&
        cmsg->cmsg_len == CMSG_LEN(sizeof(int))) {
      memcpy(payload_fd, CMSG_DATA(cmsg), sizeof(int));
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Inline send strategies
// ---------------------------------------------------------------------------

// WHY THERE IS NO splice() SEND PATH HERE.
//
// There was one. It was removed after it corrupted data, and the reason is
// worth keeping so nobody adds it back.
//
// The design doc names `sendfile`/`splice` as the first zero-copy step, so the
// obvious implementation is: vmsplice() the engine's value buffer into a pipe,
// then splice() the pipe into the socket. It benchmarked well and passed every
// single-threaded test.
//
// Under concurrent readers it produced wrong bytes. vmsplice() does not copy --
// it maps the caller's pages into the pipe BY REFERENCE, and splice() then
// hands those same references to the socket's send queue. The kernel is still
// referencing those pages after splice() returns, until TCP actually transmits
// them. We free the engine buffer immediately afterwards, the allocator hands
// those pages to another request thread, that thread writes its own value into
// them, and the kernel transmits whatever happens to be there now.
//
// Proven, not guessed: with the value copied into a deliberately leaked buffer
// whose pages could never be reused, the case that failed 50 of 80 transfers
// passed 80 of 80. See docs/pulsekv-v2-phase6-summary.md section 6.
//
// Doing it safely needs one of:
//   * MSG_ZEROCOPY with SO_EE_ORIGIN_ZEROCOPY completion notifications, so the
//     buffer is held until the kernel says it is done -- io_uring-class
//     machinery, which the design doc explicitly defers until the simple path
//     is measured. It now has been.
//   * a source the kernel already owns, which is what SendViaSendfile below
//     does with a memfd, and what a spilled value on the NVMe tier would be if
//     the engine exposed its file descriptor.
//
// SPLICE_F_GIFT is not an escape hatch either: it makes the "never touch this
// memory again" contract explicit rather than implicit, and freeing to the
// allocator violates it just as hard.

// SendViaSendfile stages the value in a memfd and sendfile()s it.
//
// Zero copies onto the socket, one copy to populate the memfd. Included because
// it is what a value that ALREADY lives in a file would cost minus that staging
// copy -- which is precisely the NVMe spill tier this phase cannot reach. The
// benchmark measures it so the summary can put a number on the engine change
// rather than assert one.
bool SendViaSendfile(int socket_fd, const uint8_t* value, size_t length) {
  int fd = StageInMemfd(value, length);
  if (fd < 0) return false;

  off_t offset = 0;
  bool ok = true;
  while (static_cast<size_t>(offset) < length) {
    ssize_t moved = sendfile(socket_fd, fd, &offset, length - static_cast<size_t>(offset));
    if (moved > 0) continue;
    if (moved < 0 && errno == EINTR) continue;
    ok = false;
    break;
  }
  close(fd);
  return ok;
}

bool SendInlineValue(int socket_fd, const uint8_t header[kHeaderBytes], const uint8_t* value,
                     size_t length, SendMode mode) {
  if (length == 0) return WriteFully(socket_fd, header, kHeaderBytes);

  switch (mode) {
    case SendMode::kSendfile:
      if (!WriteFully(socket_fd, header, kHeaderBytes)) return false;
      return SendViaSendfile(socket_fd, value, length);
    case SendMode::kWrite:
    default:
      return WriteVectored(socket_fd, header, kHeaderBytes, value, length);
  }
}

int ConnectTcp(const std::string& address, int64_t timeout_ms, std::string* error) {
  size_t colon = address.rfind(':');
  if (colon == std::string::npos) {
    if (error) *error = "bulk TCP address has no port: " + address;
    return -1;
  }
  std::string host = address.substr(0, colon);
  int port = atoi(address.c_str() + colon + 1);
  if (port <= 0 || port > 65535) {
    if (error) *error = "bulk TCP port out of range in " + address;
    return -1;
  }

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    if (error) *error = std::string("socket: ") + strerror(errno);
    return -1;
  }
  struct sockaddr_in target;
  memset(&target, 0, sizeof(target));
  target.sin_family = AF_INET;
  target.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &target.sin_addr) != 1) {
    if (error) *error = "bulk TCP host is not an IPv4 address: " + host;
    close(fd);
    return -1;
  }
  SetTimeouts(fd, timeout_ms);
  if (connect(fd, reinterpret_cast<struct sockaddr*>(&target), sizeof(target)) != 0) {
    if (error) *error = std::string("connect ") + address + ": " + strerror(errno);
    close(fd);
    return -1;
  }
  SetTcpTuning(fd);
  return fd;
}

int ConnectUnix(const std::string& path, int64_t timeout_ms, std::string* error) {
  if (path.size() + 1 > sizeof(((struct sockaddr_un*)nullptr)->sun_path)) {
    if (error) *error = "bulk unix socket path is too long: " + path;
    return -1;
  }
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    if (error) *error = std::string("socket: ") + strerror(errno);
    return -1;
  }
  struct sockaddr_un target;
  memset(&target, 0, sizeof(target));
  target.sun_family = AF_UNIX;
  memcpy(target.sun_path, path.c_str(), path.size());
  SetTimeouts(fd, timeout_ms);
  if (connect(fd, reinterpret_cast<struct sockaddr*>(&target), sizeof(target)) != 0) {
    if (error) *error = std::string("connect ") + path + ": " + strerror(errno);
    close(fd);
    return -1;
  }
  return fd;
}

}  // namespace

uint32_t ShardForKey(const uint8_t* key, size_t key_len, uint32_t shard_count) {
  if (shard_count == 0) return 0;
  uint64_t hash = 14695981039346656037ULL;  // FNV-1a 64-bit offset basis
  for (size_t i = 0; i < key_len; i++) {
    hash ^= static_cast<uint64_t>(key[i]);
    hash *= 1099511628211ULL;  // FNV-1a 64-bit prime
  }
  return static_cast<uint32_t>(hash % static_cast<uint64_t>(shard_count));
}

uint32_t ShardForKey(const std::string& key, uint32_t shard_count) {
  return ShardForKey(reinterpret_cast<const uint8_t*>(key.data()), key.size(), shard_count);
}

const char* SendModeName(SendMode mode) {
  switch (mode) {
    case SendMode::kSendfile: return "sendfile";
    case SendMode::kWrite: default: return "write";
  }
}

bool ParseSendMode(const std::string& text, SendMode* out) {
  if (text == "write") { *out = SendMode::kWrite; return true; }
  if (text == "sendfile") { *out = SendMode::kSendfile; return true; }
  return false;
}

bool MemfdSupported() {
  static const bool supported = [] {
    int fd = MemfdCreate("pulsekv-probe", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0) return false;
    close(fd);
    return true;
  }();
  return supported;
}

std::string UnixSocketPath(const std::string& socket_dir, const std::string& host, int port) {
  if (socket_dir.empty()) return std::string();
  std::string safe_host = host;
  for (char& c : safe_host) {
    if (c == '/' || c == ':') c = '-';
  }
  return socket_dir + "/pulsekv-bulk-" + safe_host + "-" + std::to_string(port) + ".sock";
}

int BulkPort(int service_port, int offset) {
  long port = static_cast<long>(service_port) + offset;
  if (port <= 0 || port > 65535) return 0;
  return static_cast<int>(port);
}

Endpoint EndpointForPeer(const std::string& service_address, int port_offset,
                         const std::string& socket_dir) {
  Endpoint endpoint;
  size_t colon = service_address.rfind(':');
  if (colon == std::string::npos) return endpoint;
  std::string host = service_address.substr(0, colon);
  int service_port = atoi(service_address.c_str() + colon + 1);
  if (service_port <= 0) return endpoint;

  int bulk_port = BulkPort(service_port, port_offset);
  if (bulk_port > 0) endpoint.tcp = host + ":" + std::to_string(bulk_port);
  endpoint.unix_path = UnixSocketPath(socket_dir, host, service_port);
  return endpoint;
}

// ---------------------------------------------------------------------------
// Blob
// ---------------------------------------------------------------------------

Blob::~Blob() { Reset(); }

Blob::Blob(Blob&& other) noexcept { *this = std::move(other); }

Blob& Blob::operator=(Blob&& other) noexcept {
  if (this == &other) return *this;
  Reset();
  owned_ = std::move(other.owned_);
  mapping_ = other.mapping_;
  size_ = other.size_;
  data_ = mapping_ != nullptr ? static_cast<const uint8_t*>(mapping_) : owned_.data();
  other.mapping_ = nullptr;
  other.data_ = nullptr;
  other.size_ = 0;
  return *this;
}

void Blob::Reset() {
  if (mapping_ != nullptr) {
    munmap(mapping_, size_);
    mapping_ = nullptr;
  }
  owned_.clear();
  owned_.shrink_to_fit();
  data_ = nullptr;
  size_ = 0;
}

Blob Blob::FromOwned(std::vector<uint8_t> bytes) {
  Blob blob;
  blob.owned_ = std::move(bytes);
  blob.data_ = blob.owned_.data();
  blob.size_ = blob.owned_.size();
  return blob;
}

Blob Blob::FromMapping(void* mapping, size_t length) {
  Blob blob;
  blob.mapping_ = mapping;
  blob.data_ = static_cast<const uint8_t*>(mapping);
  blob.size_ = length;
  return blob;
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

class Server::Impl {
 public:
  Impl(ServerOptions options, pk_engine_t* engine, Server* owner)
      : options_(std::move(options)), engine_(engine), owner_(owner) {}

  ~Impl() { Stop(); }

  bool Open(std::string* error) {
    if (options_.max_value_bytes == 0) {
      options_.max_value_bytes = pk_engine_max_value_bytes(engine_);
    }
    bool any = false;
    if (options_.tcp_port > 0) {
      tcp_fd_ = OpenTcp(error);
      any = any || tcp_fd_ >= 0;
    }
    if (!options_.unix_path.empty()) {
      unix_fd_ = OpenUnix(error);
      any = any || unix_fd_ >= 0;
    }
    if (!any && error && error->empty()) *error = "no bulk listener was configured";
    return any;
  }

  void Start() {
    if (tcp_fd_ >= 0) {
      accept_threads_.emplace_back([this] { AcceptLoop(tcp_fd_); });
    }
    if (unix_fd_ >= 0) {
      accept_threads_.emplace_back([this] { AcceptLoop(unix_fd_); });
    }
  }

  void Stop() {
    if (stopping_.exchange(true)) return;
    // Shutting the listeners down unblocks accept() rather than relying on a
    // timeout; closing alone can leave a blocked accept in place.
    if (tcp_fd_ >= 0) { shutdown(tcp_fd_, SHUT_RDWR); close(tcp_fd_); tcp_fd_ = -1; }
    if (unix_fd_ >= 0) { shutdown(unix_fd_, SHUT_RDWR); close(unix_fd_); unix_fd_ = -1; }
    for (auto& thread : accept_threads_) {
      if (thread.joinable()) thread.join();
    }
    accept_threads_.clear();

    // Connection threads are detached; wait, bounded, for them to drain so a
    // caller can destroy the engine afterwards without racing a handler.
    for (int i = 0; i < 300 && live_connections_.load() > 0; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!options_.unix_path.empty()) unlink(options_.unix_path.c_str());
  }

  int bound_tcp_port() const { return bound_tcp_port_; }

 private:
  int OpenTcp(std::string* error) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      if (error) *error = std::string("bulk tcp socket: ") + strerror(errno);
      return -1;
    }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(options_.tcp_port));
    if (inet_pton(AF_INET, options_.host.c_str(), &address.sin_addr) != 1) {
      if (error) *error = "bulk host is not an IPv4 address: " + options_.host;
      close(fd);
      return -1;
    }
    if (bind(fd, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) != 0) {
      if (error) {
        *error = "bulk tcp bind " + options_.host + ":" + std::to_string(options_.tcp_port) +
                 ": " + strerror(errno);
      }
      close(fd);
      return -1;
    }
    if (listen(fd, 64) != 0) {
      if (error) *error = std::string("bulk tcp listen: ") + strerror(errno);
      close(fd);
      return -1;
    }
    socklen_t length = sizeof(address);
    if (getsockname(fd, reinterpret_cast<struct sockaddr*>(&address), &length) == 0) {
      bound_tcp_port_ = ntohs(address.sin_port);
    }
    return fd;
  }

  int OpenUnix(std::string* error) {
    const std::string& path = options_.unix_path;
    if (path.size() + 1 > sizeof(((struct sockaddr_un*)nullptr)->sun_path)) {
      if (error) *error = "bulk unix socket path is too long: " + path;
      return -1;
    }
    // A stale socket from a crashed previous incarnation would make bind fail
    // forever. The path is per-node by construction, so removing it is safe.
    unlink(path.c_str());

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
      if (error) *error = std::string("bulk unix socket: ") + strerror(errno);
      return -1;
    }
    struct sockaddr_un address;
    memset(&address, 0, sizeof(address));
    address.sun_family = AF_UNIX;
    memcpy(address.sun_path, path.c_str(), path.size());
    if (bind(fd, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) != 0) {
      if (error) *error = "bulk unix bind " + path + ": " + strerror(errno);
      close(fd);
      return -1;
    }
    if (listen(fd, 64) != 0) {
      if (error) *error = std::string("bulk unix listen: ") + strerror(errno);
      close(fd);
      unlink(path.c_str());
      return -1;
    }
    return fd;
  }

  void AcceptLoop(int listener) {
    for (;;) {
      int fd = accept(listener, nullptr, nullptr);
      if (fd < 0) {
        if (stopping_.load()) return;
        if (errno == EINTR) continue;
        return;
      }
      if (stopping_.load()) { close(fd); return; }

      if (live_connections_.load() >= static_cast<int>(options_.max_connections)) {
        owner_->stats_.rejected_connections.fetch_add(1, std::memory_order_relaxed);
        close(fd);
        continue;
      }
      // Whether this is a Unix connection decides whether a memfd handoff is
      // even possible: an fd is only meaningful to a process on this host.
      bool is_unix = listener == unix_fd_;
      live_connections_.fetch_add(1, std::memory_order_relaxed);
      std::thread([this, fd, is_unix] {
        SetTimeouts(fd, options_.io_timeout_ms);
        if (!is_unix) SetTcpTuning(fd);
        ServeConnection(fd, is_unix);
        close(fd);
        live_connections_.fetch_sub(1, std::memory_order_relaxed);
      }).detach();
    }
  }

  void ServeConnection(int fd, bool is_unix) {
    // One connection carries many requests; peers keep them cached exactly the
    // way Phase 4 caches gRPC stubs.
    while (!stopping_.load()) {
      uint8_t raw[kHeaderBytes];
      if (!ReadFully(fd, raw, kHeaderBytes)) return;
      Header request;
      if (!DecodeHeader(raw, &request)) return;

      switch (static_cast<Op>(request.code)) {
        case Op::kPing:
          if (!HandlePing(fd)) return;
          break;
        case Op::kGet:
          if (!HandleGet(fd, request, is_unix)) return;
          break;
        case Op::kPut:
          if (!HandlePut(fd, request)) return;
          break;
        default:
          return;  // unknown opcode: the stream is no longer trustworthy
      }
    }
  }

  bool SendStatus(int fd, Status status, const std::string& detail) {
    Header response;
    response.code = static_cast<uint8_t>(status);
    response.aux_len = static_cast<uint32_t>(detail.size());
    uint8_t raw[kHeaderBytes];
    EncodeHeader(response, raw);
    if (detail.empty()) return WriteFully(fd, raw, kHeaderBytes);
    return WriteVectored(fd, raw, kHeaderBytes, detail.data(), detail.size());
  }

  bool HandlePing(int fd) { return SendStatus(fd, Status::kOk, options_.node_id); }

  bool ReadKey(int fd, const Header& request, std::string* key) {
    if (request.aux_len == 0 || request.aux_len > kMaxKeyLen) return false;
    key->resize(request.aux_len);
    return ReadFully(fd, key->data(), key->size());
  }

  bool HandleGet(int fd, const Header& request, bool is_unix) {
    std::string key;
    if (!ReadKey(fd, request, &key)) return false;
    owner_->stats_.gets.fetch_add(1, std::memory_order_relaxed);

    uint8_t* value = nullptr;
    uint64_t length = 0;
    pk_engine_result_t rc = pk_engine_get(
        engine_, reinterpret_cast<const uint8_t*>(key.data()),
        static_cast<uint32_t>(key.size()), &value, &length);

    if (rc == PK_ENGINE_NOT_FOUND) return SendStatus(fd, Status::kNotFound, std::string());
    if (rc != PK_ENGINE_OK) {
      owner_->stats_.errors.fetch_add(1, std::memory_order_relaxed);
      return SendStatus(fd, Status::kError, pk_engine_strerror(rc));
    }

    const bool wants_memfd = (request.flags & kFlagAcceptMemfd) != 0;
    bool ok;
    if (is_unix && wants_memfd && options_.allow_memfd && length > 0 && MemfdSupported()) {
      ok = SendMemfd(fd, value, static_cast<size_t>(length));
      if (ok) {
        owner_->stats_.memfd_handoffs.fetch_add(1, std::memory_order_relaxed);
        pk_engine_free_value(value);
        return true;
      }
      // Staging failed. The header has not been sent yet, so falling back to an
      // inline transfer on the same connection is still safe.
    }

    Header response;
    response.code = static_cast<uint8_t>(Status::kOk);
    response.value_len = length;
    uint8_t raw[kHeaderBytes];
    EncodeHeader(response, raw);
    ok = SendInlineValue(fd, raw, value, static_cast<size_t>(length), options_.send_mode);
    owner_->stats_.inline_sends.fetch_add(1, std::memory_order_relaxed);
    pk_engine_free_value(value);
    return ok;
  }

  bool SendMemfd(int fd, const uint8_t* value, size_t length) {
    int payload = StageInMemfd(value, length);
    if (payload < 0) return false;

    Header response;
    response.code = static_cast<uint8_t>(Status::kOkMemfd);
    response.value_len = length;
    uint8_t raw[kHeaderBytes];
    EncodeHeader(response, raw);
    bool ok = SendFdWithHeader(fd, raw, payload);
    // Our reference goes away immediately; the receiver's dup keeps the region
    // alive for exactly as long as it maps it.
    close(payload);
    return ok;
  }

  bool HandlePut(int fd, const Header& request) {
    std::string key;
    if (!ReadKey(fd, request, &key)) return false;

    // Checked BEFORE a byte is buffered, the same discipline PutChunked uses:
    // a hostile or corrupt length must not become an allocation.
    if (request.value_len > options_.max_value_bytes) {
      owner_->stats_.errors.fetch_add(1, std::memory_order_relaxed);
      // The peer is about to send value_len bytes we will not read, so the
      // stream cannot continue. Answer, then let the connection close.
      SendStatus(fd, Status::kError, "value exceeds this node's max-value-bytes");
      return false;
    }

    std::vector<uint8_t> value(static_cast<size_t>(request.value_len));
    if (!value.empty() && !ReadFully(fd, value.data(), value.size())) return false;
    owner_->stats_.puts.fetch_add(1, std::memory_order_relaxed);

    pk_engine_result_t rc = pk_engine_put(
        engine_, reinterpret_cast<const uint8_t*>(key.data()),
        static_cast<uint32_t>(key.size()), value.empty() ? nullptr : value.data(),
        value.size());
    if (rc != PK_ENGINE_OK) {
      owner_->stats_.errors.fetch_add(1, std::memory_order_relaxed);
      return SendStatus(fd, Status::kError, pk_engine_strerror(rc));
    }
    return SendStatus(fd, Status::kOk, std::string());
  }

  static constexpr uint32_t kMaxKeyLen = 64 * 1024;

  ServerOptions options_;
  pk_engine_t* const engine_;
  Server* const owner_;
  int tcp_fd_ = -1;
  int unix_fd_ = -1;
  int bound_tcp_port_ = 0;
  std::atomic<bool> stopping_{false};
  std::atomic<int> live_connections_{0};
  std::vector<std::thread> accept_threads_;
};

Server::Server(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

Server::~Server() { Stop(); }

void Server::Stop() {
  if (impl_) impl_->Stop();
}

std::unique_ptr<Server> Server::Start(ServerOptions options, pk_engine_t* engine,
                                      std::string* error) {
  if (engine == nullptr) {
    if (error) *error = "bulk server requires an engine";
    return nullptr;
  }
  // SIGPIPE would kill the process when a peer disappears mid-transfer. Every
  // send here uses MSG_NOSIGNAL, but sendfile/splice do not take that flag.
  signal(SIGPIPE, SIG_IGN);

  auto server = std::unique_ptr<Server>(new Server(nullptr));
  server->options_ = options;
  auto impl = std::make_unique<Impl>(std::move(options), engine, server.get());
  std::string local_error;
  if (!impl->Open(&local_error)) {
    if (error) *error = local_error;
    return nullptr;
  }
  server->bound_tcp_port_ = impl->bound_tcp_port();
  impl->Start();
  server->impl_ = std::move(impl);
  return server;
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

Client::~Client() {
  if (fd_ >= 0) close(fd_);
}

Client::Client(Client&& other) noexcept { *this = std::move(other); }

Client& Client::operator=(Client&& other) noexcept {
  if (this == &other) return *this;
  if (fd_ >= 0) close(fd_);
  fd_ = other.fd_;
  over_unix_socket_ = other.over_unix_socket_;
  accept_memfd_ = other.accept_memfd_;
  peer_node_id_ = std::move(other.peer_node_id_);
  address_ = std::move(other.address_);
  other.fd_ = -1;
  return *this;
}

std::unique_ptr<Client> Client::Connect(const Endpoint& endpoint, ClientOptions options,
                                        std::string* error) {
  auto client = std::unique_ptr<Client>(new Client());
  client->accept_memfd_ = options.accept_memfd;

  // The Unix socket first, always. It is the only path that can hand over a
  // memfd, and its absence is also the cleanest same-host test there is: a
  // remote peer's socket path simply does not exist on this filesystem.
  if (!endpoint.unix_path.empty()) {
    std::string local_error;
    int fd = ConnectUnix(endpoint.unix_path, options.io_timeout_ms, &local_error);
    if (fd >= 0) {
      client->fd_ = fd;
      client->over_unix_socket_ = true;
      client->address_ = endpoint.unix_path;
    }
  }
  if (client->fd_ < 0 && !endpoint.tcp.empty()) {
    std::string local_error;
    int fd = ConnectTcp(endpoint.tcp, options.io_timeout_ms, &local_error);
    if (fd < 0) {
      if (error) *error = local_error;
      return nullptr;
    }
    client->fd_ = fd;
    client->over_unix_socket_ = false;
    client->address_ = endpoint.tcp;
  }
  if (client->fd_ < 0) {
    if (error) *error = "no reachable bulk endpoint";
    return nullptr;
  }

  // Handshake. This is what makes a convention-derived endpoint safe to use:
  // if the socket we found belongs to some other node, we find out here rather
  // than by reading its data.
  Header ping;
  ping.code = static_cast<uint8_t>(Op::kPing);
  uint8_t raw[kHeaderBytes];
  EncodeHeader(ping, raw);
  if (!WriteFully(client->fd_, raw, kHeaderBytes) ||
      !ReadFully(client->fd_, raw, kHeaderBytes)) {
    if (error) *error = "bulk handshake failed on " + client->address_;
    return nullptr;
  }
  Header response;
  if (!DecodeHeader(raw, &response) ||
      static_cast<Status>(response.code) != Status::kOk) {
    if (error) *error = "bulk handshake was rejected by " + client->address_;
    return nullptr;
  }
  client->peer_node_id_.resize(response.aux_len);
  if (response.aux_len > 0 &&
      !ReadFully(client->fd_, client->peer_node_id_.data(), client->peer_node_id_.size())) {
    if (error) *error = "bulk handshake identity was truncated";
    return nullptr;
  }
  if (!options.expect_node_id.empty() && client->peer_node_id_ != options.expect_node_id) {
    if (error) {
      *error = "bulk endpoint " + client->address_ + " is node " + client->peer_node_id_ +
               ", expected " + options.expect_node_id;
    }
    return nullptr;
  }
  return client;
}

bool Client::Get(const std::string& key, Blob* out, bool* found, std::string* error) {
  if (fd_ < 0 || key.empty() || key.size() > 64 * 1024) {
    if (error) *error = "invalid bulk get";
    return false;
  }
  *found = false;

  Header request;
  request.code = static_cast<uint8_t>(Op::kGet);
  request.aux_len = static_cast<uint32_t>(key.size());
  if (accept_memfd_ && over_unix_socket_) request.flags |= kFlagAcceptMemfd;
  uint8_t raw[kHeaderBytes];
  EncodeHeader(request, raw);
  if (!WriteVectored(fd_, raw, kHeaderBytes, key.data(), key.size())) {
    if (error) *error = "bulk get request failed";
    return false;
  }

  Header response;
  int payload_fd = -1;
  if (over_unix_socket_) {
    // The response may or may not carry an fd; recvmsg handles both.
    if (!ReceiveFdWithHeader(fd_, raw, &payload_fd)) {
      if (error) *error = "bulk get response header failed";
      return false;
    }
  } else if (!ReadFully(fd_, raw, kHeaderBytes)) {
    if (error) *error = "bulk get response header failed";
    return false;
  }
  if (!DecodeHeader(raw, &response)) {
    if (payload_fd >= 0) close(payload_fd);
    if (error) *error = "bulk get response was malformed";
    return false;
  }

  switch (static_cast<Status>(response.code)) {
    case Status::kNotFound:
      if (payload_fd >= 0) close(payload_fd);
      *found = false;
      return true;

    case Status::kOkMemfd: {
      if (payload_fd < 0) {
        if (error) *error = "bulk peer promised a memfd and sent none";
        return false;
      }
      size_t length = static_cast<size_t>(response.value_len);
      if (length == 0) {
        close(payload_fd);
        *out = Blob::FromOwned({});
        *found = true;
        return true;
      }
      void* mapping = mmap(nullptr, length, PROT_READ, MAP_SHARED, payload_fd, 0);
      close(payload_fd);  // the mapping keeps the region alive
      if (mapping == MAP_FAILED) {
        if (error) *error = std::string("map bulk memfd: ") + strerror(errno);
        return false;
      }
      *out = Blob::FromMapping(mapping, length);
      *found = true;
      return true;
    }

    case Status::kOk: {
      if (payload_fd >= 0) close(payload_fd);
      std::vector<uint8_t> bytes(static_cast<size_t>(response.value_len));
      if (!bytes.empty() && !ReadFully(fd_, bytes.data(), bytes.size())) {
        if (error) *error = "bulk get value was truncated";
        return false;
      }
      *out = Blob::FromOwned(std::move(bytes));
      *found = true;
      return true;
    }

    case Status::kError:
    default: {
      if (payload_fd >= 0) close(payload_fd);
      std::string detail(response.aux_len, '\0');
      if (response.aux_len > 0) ReadFully(fd_, detail.data(), detail.size());
      if (error) *error = "bulk peer error: " + detail;
      return false;
    }
  }
}

bool Client::Put(const std::string& key, const uint8_t* value, size_t length,
                 std::string* error) {
  if (fd_ < 0 || key.empty() || key.size() > 64 * 1024) {
    if (error) *error = "invalid bulk put";
    return false;
  }
  Header request;
  request.code = static_cast<uint8_t>(Op::kPut);
  request.aux_len = static_cast<uint32_t>(key.size());
  request.value_len = length;
  uint8_t raw[kHeaderBytes];
  EncodeHeader(request, raw);

  if (!WriteVectored(fd_, raw, kHeaderBytes, key.data(), key.size())) {
    if (error) *error = "bulk put header failed";
    return false;
  }
  if (length > 0 && !WriteFully(fd_, value, length)) {
    if (error) *error = "bulk put payload failed";
    return false;
  }

  if (!ReadFully(fd_, raw, kHeaderBytes)) {
    if (error) *error = "bulk put response failed";
    return false;
  }
  Header response;
  if (!DecodeHeader(raw, &response)) {
    if (error) *error = "bulk put response was malformed";
    return false;
  }
  if (static_cast<Status>(response.code) == Status::kOk) return true;

  std::string detail(response.aux_len, '\0');
  if (response.aux_len > 0) ReadFully(fd_, detail.data(), detail.size());
  if (error) *error = "bulk put rejected: " + detail;
  return false;
}

}  // namespace bulk
}  // namespace pulsekv
