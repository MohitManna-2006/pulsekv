# Linux build/test environment for pulsekv.
#
# The design targets Linux (epoll, and pthreads from step 4 on), so the build
# does not run natively on a macOS host. Everything is built and tested in here
# instead:
#
#   docker build -t pulsekv-dev .
#   docker run --rm -v "$PWD:/src" -w /src pulsekv-dev make
#
# Debian rather than Alpine on purpose: glibc, not musl, since the threading and
# allocator behaviour from step 4 onward should match the deployment target.
# valgrind is here because from step 3 the store holds long-lived heap state --
# nodes, keys and values that outlive the request that created them -- and
# "the tests pass" stops being sufficient evidence of correctness.
FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        gcc \
        make \
        libc6-dev \
        valgrind \
        procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
