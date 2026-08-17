// PulseKV v2 data-plane node -- gRPC shim.
//
// This process is the network face of one data-plane node. It implements
// NodeService (proto/node.proto) using gRPC's C++ API and, from Phase 1
// onward, forwards every real request across an `extern "C"` boundary into the
// pure-C storage engine under node/engine/. See node/README.md for why the
// boundary sits here.
//
// Phase 0 scope, verbatim: HealthCheck returns real data (ok, this node's id,
// actual uptime). Get / Put / PrefixMatch / Capacity return UNIMPLEMENTED.
// They must not return an empty success -- a stub that fakes a cache miss is
// indistinguishable from a working engine with nothing in it, and that
// ambiguity is exactly what deploy/smoke-test.sh exists to rule out.

#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
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

namespace nodev1 = pulsekv::node::v1;

namespace {

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

// Every RPC that Phase 1 will implement funnels through here, so there is
// exactly one place that decides what "not yet" looks like on the wire.
grpc::Status NotImplementedYet(const char* rpc, const char* phase) {
  std::string message = std::string("NodeService.") + rpc +
                        " is not implemented in Phase 0; it arrives in " +
                        phase + " once node/engine/ exists";
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, message);
}

class NodeServiceImpl final : public nodev1::NodeService::Service {
 public:
  explicit NodeServiceImpl(std::string node_id)
      : node_id_(std::move(node_id)),
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
                   const nodev1::GetRequest* /*request*/,
                   nodev1::GetResponse* /*response*/) override {
    return NotImplementedYet("Get", "Phase 1.4");
  }

  grpc::Status Put(grpc::ServerContext* /*context*/,
                   const nodev1::PutRequest* /*request*/,
                   nodev1::PutResponse* /*response*/) override {
    return NotImplementedYet("Put", "Phase 1.4");
  }

  grpc::Status PrefixMatch(
      grpc::ServerContext* /*context*/,
      const nodev1::PrefixMatchRequest* /*request*/,
      grpc::ServerWriter<nodev1::PrefixMatchResponse>* /*writer*/) override {
    // Returning UNIMPLEMENTED without writing anything means the client sees
    // the status on its first Recv(), not a clean end-of-stream.
    return NotImplementedYet("PrefixMatch", "Phase 1.4");
  }

  grpc::Status Capacity(grpc::ServerContext* /*context*/,
                        const nodev1::CapacityRequest* /*request*/,
                        nodev1::CapacityResponse* /*response*/) override {
    return NotImplementedYet("Capacity", "Phase 1.4");
  }

 private:
  int64_t UptimeSeconds() const {
    const auto elapsed = std::chrono::steady_clock::now() - started_;
    return std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
  }

  const std::string node_id_;
  const std::chrono::steady_clock::time_point started_;
};

struct Options {
  std::string node_id;
  std::string host = "127.0.0.1";
  int port = 0;
};

void PrintUsage(const char* argv0) {
  std::fprintf(stderr,
               "usage: %s --node-id ID --port PORT [--host HOST]\n"
               "\n"
               "  --node-id ID    identity this node reports from HealthCheck\n"
               "  --port PORT     TCP port to serve NodeService on\n"
               "  --host HOST     bind address (default 127.0.0.1)\n",
               argv0);
}

// Hand-rolled rather than pulling in gflags/absl::flags: three options do not
// justify a dependency in a process whose entire job is to be thin.
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
    } else if (name == "--port") {
      if (!next_value("--port")) return false;
      char* end = nullptr;
      const long parsed = std::strtol(value.c_str(), &end, 10);
      if (end == value.c_str() || *end != '\0' || parsed <= 0 || parsed > 65535) {
        std::fprintf(stderr, "error: --port %s is not in 1..65535\n", value.c_str());
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

  const std::string address = options.host + ":" + std::to_string(options.port);

  NodeServiceImpl service(options.node_id);

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

  int bound_port = 0;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials(), &bound_port);
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (!server || bound_port == 0) {
    // BuildAndStart reports a bind failure by leaving bound_port at 0 rather
    // than by throwing, so this check is the only thing standing between a
    // port collision and a node that looks started but answers nothing.
    std::fprintf(stderr, "[%s] fatal: could not bind %s (port in use?)\n",
                 options.node_id.c_str(), address.c_str());
    return 1;
  }

  std::printf("[%s] NodeService listening on %s (pid %d)\n",
              options.node_id.c_str(), address.c_str(),
              static_cast<int>(getpid()));
  std::printf("[%s] Phase 0: HealthCheck is live; Get/Put/PrefixMatch/Capacity return UNIMPLEMENTED\n",
              options.node_id.c_str());

  std::thread serving([&server] { server->Wait(); });

  // Block until a signal arrives on the self-pipe.
  unsigned char sig = 0;
  ssize_t n;
  do {
    n = read(g_shutdown_pipe[0], &sig, 1);
  } while (n < 0 && errno == EINTR);

  std::printf("[%s] received signal %d, shutting down\n", options.node_id.c_str(),
              static_cast<int>(sig));
  server->Shutdown();
  serving.join();
  std::printf("[%s] stopped\n", options.node_id.c_str());
  return 0;
}
