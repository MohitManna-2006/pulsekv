# PulseKV v2 polyglot build/run environment.
#
# NEW FILE. The root Dockerfile is v1's and is not touched by v2.
#
# v2 needs three toolchains at once -- C/C++ with gRPC, Go, and Python -- and
# the C++ side does not build on macOS without a fight. Same reasoning as v1:
# everything is built and run in here.
#
#   docker build -f deploy/Dockerfile -t pulsekv-v2-dev .        # from the repo root
#   docker run --rm -it -v "$PWD:/src" -w /src pulsekv-v2-dev bash
#
# Then, inside:
#   deploy/run-local-cluster.sh
#   deploy/smoke-test.sh
#   deploy/stop-local-cluster.sh
#
# Note the build context is the repo root, not deploy/ -- the image pre-warms
# the Go module cache from control/go.mod, so a container never needs network
# access to build the control plane.

FROM debian:bookworm-slim

# ---------------------------------------------------------------------------
# Pinned tool versions.
#
# Everything that this project controls is pinned exactly here. The three
# things that come from Debian's archive -- protoc, libprotobuf, libgrpc++ --
# are pinned by base image tag rather than by `apt-get install pkg=version`,
# because Debian's binNMU suffixes (`1.51.1-3+b1`) differ per architecture and
# a hard `=` pin makes the image un-buildable on the other arch. The resolved
# versions are recorded into /etc/pulsekv-v2-toolchain at build time and can be
# read back with `pulsekv-v2-versions`.
#
# Verified against, on both linux/amd64 and linux/arm64:
#   protoc              3.21.12   (Debian protobuf-compiler)
#   libprotobuf         3.21.12   (Debian libprotobuf-dev)
#   gRPC C++            1.51.1    (Debian libgrpc++-dev, protobuf-compiler-grpc)
#   grpc_cpp_plugin     1.51.1
#
# The C++ side has to use Debian's protoc: generated .pb.cc is ABI-coupled to
# the libprotobuf it links against, so mixing a newer standalone protoc with
# Debian's runtime would be a link-time or, worse, a run-time surprise. Go and
# Python have no such coupling and use their own pinned generators.
# ---------------------------------------------------------------------------
ARG GO_VERSION=1.25.6
ARG PROTOC_GEN_GO_VERSION=v1.36.12
ARG PROTOC_GEN_GO_GRPC_VERSION=v1.6.2
ARG GRPCURL_VERSION=v1.9.3
ARG GRPCIO_VERSION=1.83.0
ARG PROTOBUF_PY_VERSION=7.35.1

# Populated automatically by BuildKit; falls back to the base image's own arch
# when the legacy builder is used.
ARG TARGETARCH

ENV DEBIAN_FRONTEND=noninteractive

# Package groups, in order below:
#   ca-certificates, curl, git  -- fetching the Go toolchain and Go tools
#   gcc, make, libc6-dev        -- C; Phase 1 extracts v1's engine into node/engine/
#   g++, cmake, pkg-config,
#   protobuf-*, libgrpc++-dev   -- C++ and gRPC, for node/grpc_shim/
#   python3, python3-venv       -- adapters/
#   procps                      -- pgrep/ps, used by the deploy scripts
#   valgrind                    -- deploy/test-engine.sh --valgrind. Same reason
#                                  v1's Dockerfile carries it: the engine holds
#                                  long-lived heap state whose ownership moves
#                                  between RAM, disk, and the caller, and "the
#                                  tests pass" is not evidence about lifetimes.
#
# The pkg-config and grpc_cpp_plugin checks at the end fail the image build
# loudly here rather than at cmake-configure time on someone else's machine.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        gcc \
        make \
        libc6-dev \
        g++ \
        cmake \
        pkg-config \
        protobuf-compiler \
        libprotobuf-dev \
        protobuf-compiler-grpc \
        libgrpc++-dev \
        python3 \
        python3-venv \
        procps \
        valgrind \
 && rm -rf /var/lib/apt/lists/* \
 && pkg-config --exists grpc++ protobuf \
 && test -x /usr/bin/grpc_cpp_plugin

# ---------------------------------------------------------------------------
# Go toolchain.
# ---------------------------------------------------------------------------
RUN set -eux; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${arch}.tar.gz" -o /tmp/go.tar.gz; \
    tar -C /usr/local -xzf /tmp/go.tar.gz; \
    rm /tmp/go.tar.gz; \
    /usr/local/go/bin/go version

# GOTOOLCHAIN=local makes Go refuse to silently download a different toolchain
# version than the one pinned above. The pin is the pin.
ENV GOPATH=/go \
    GOCACHE=/go/cache \
    GOBIN=/usr/local/bin \
    GOTOOLCHAIN=local \
    PATH=/usr/local/go/bin:/usr/local/bin:$PATH

# ---------------------------------------------------------------------------
# Codegen plugins and grpcurl.
#
# grpcurl is not required by any script -- deploy/smoke-test.sh deliberately
# asserts through the generated Go stubs instead, so it checks the same
# contract the control plane compiled against. It is here because both servers
# enable gRPC server reflection, and being able to poke a live cluster by hand
# is worth the ~20 MB.
# ---------------------------------------------------------------------------
RUN set -eux; \
    go install "google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}"; \
    go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}"; \
    go install "github.com/fullstorydev/grpcurl/cmd/grpcurl@${GRPCURL_VERSION}"; \
    protoc-gen-go --version; \
    protoc-gen-go-grpc --version

# ---------------------------------------------------------------------------
# Python.
#
# A venv rather than --break-system-packages: Debian marks its Python
# externally-managed (PEP 668), and overriding that to install grpcio next to
# the system packages is the kind of shortcut that produces an unreproducible
# image six months later.
# ---------------------------------------------------------------------------
ENV VIRTUAL_ENV=/opt/pulsekv-venv
RUN python3 -m venv "$VIRTUAL_ENV"
ENV PATH=$VIRTUAL_ENV/bin:$PATH

RUN set -eux; \
    pip install --no-cache-dir --upgrade pip setuptools; \
    pip install --no-cache-dir \
        "grpcio==${GRPCIO_VERSION}" \
        "grpcio-tools==${GRPCIO_VERSION}" \
        "protobuf==${PROTOBUF_PY_VERSION}"

# ---------------------------------------------------------------------------
# Pre-warm the Go module cache.
#
# This is the only place the build context is read from. It means `go build`
# inside a running container needs no network, which is what makes the
# "clean machine with only Docker" claim in the Phase 0 exit criteria true.
# ---------------------------------------------------------------------------
COPY control/go.mod control/go.sum /tmp/gomodcache-warm/
RUN cd /tmp/gomodcache-warm \
 && go mod download \
 && rm -rf /tmp/gomodcache-warm

# The repo is bind-mounted from the host, which may run the container as a
# non-root uid. Keep the Go caches writable either way.
RUN mkdir -p /go/cache && chmod -R 0777 /go

# ---------------------------------------------------------------------------
# Record what actually got installed, so the image can answer for itself.
# ---------------------------------------------------------------------------
RUN set -eux; \
    { \
      echo "# PulseKV v2 dev image -- resolved toolchain versions"; \
      echo "base:              debian:bookworm-slim ($(dpkg --print-architecture))"; \
      echo "gcc:               $(gcc -dumpfullversion)"; \
      echo "g++:               $(g++ -dumpfullversion)"; \
      echo "cmake:             $(cmake --version | head -1 | awk '{print $3}')"; \
      echo "protoc:            $(protoc --version | awk '{print $2}')"; \
      echo "libprotobuf:       $(pkg-config --modversion protobuf)"; \
      echo "grpc++:            $(pkg-config --modversion grpc++)"; \
      echo "grpc_cpp_plugin:   $(dpkg-query -W protobuf-compiler-grpc | awk '{print $2}')"; \
      echo "go:                $(go version | awk '{print $3}')"; \
      echo "protoc-gen-go:     $(protoc-gen-go --version 2>&1 | awk '{print $2}')"; \
      echo "protoc-gen-go-grpc: $(protoc-gen-go-grpc --version 2>&1 | awk '{print $2}')"; \
      echo "grpcurl:           ${GRPCURL_VERSION} (binary self-reports 'dev'; go install does not stamp it)"; \
      echo "python:            $(python3 --version | awk '{print $2}')"; \
      echo "grpcio:            $(pip show grpcio | awk '/^Version:/{print $2}')"; \
      echo "grpcio-tools:      $(pip show grpcio-tools | awk '/^Version:/{print $2}')"; \
      echo "protobuf (python): $(pip show protobuf | awk '/^Version:/{print $2}')"; \
    } > /etc/pulsekv-v2-toolchain; \
    printf '#!/bin/sh\nexec cat /etc/pulsekv-v2-toolchain\n' > /usr/local/bin/pulsekv-v2-versions; \
    chmod +x /usr/local/bin/pulsekv-v2-versions; \
    cat /etc/pulsekv-v2-toolchain

WORKDIR /src
CMD ["bash"]
