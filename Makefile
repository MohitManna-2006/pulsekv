# PulseKV Makefile — Ergonomic CLI for v2 Development & Testing
#
# Supports automatic container wrapping (Option A):
# - When run on macOS/host, commands run seamlessly inside `pulsekv-v2-dev`.
# - When run inside the container, commands execute natively.

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Environment Detection & Execution Wrapper
# ---------------------------------------------------------------------------
IN_CONTAINER := $(shell [ -f /.dockerenv ] && echo 1 || echo 0)
DOCKER_IMAGE := pulsekv-v2-dev

ifeq ($(IN_CONTAINER),1)
  RUN_CMD  := bash -c
  INTERACT := bash
else
  RUN_CMD  := docker run --rm -v "$(CURDIR):/src" -w /src $(DOCKER_IMAGE) bash -c
  INTERACT := docker run --rm -it -v "$(CURDIR):/src" -w /src $(DOCKER_IMAGE) bash
endif

# ---------------------------------------------------------------------------
# Terminal Colors & Help
# ---------------------------------------------------------------------------
CYAN  := \033[36m
GREEN := \033[32m
BOLD  := \033[1m
RESET := \033[0m

.PHONY: help
help:
	@printf "\n$(BOLD)PulseKV v2 Development CLI$(RESET)\n"
	@printf "===============================================================\n"
	@printf "$(CYAN)Cluster Management:$(RESET)\n"
	@printf "  $(GREEN)make start$(RESET) / $(GREEN)make up$(RESET)       Boot 4-node cluster with 3-replica Raft control plane\n"
	@printf "  $(GREEN)make stop$(RESET) / $(GREEN)make down$(RESET)      Stop all cluster processes and clean PIDs\n"
	@printf "  $(GREEN)make restart$(RESET)             Gracefully restart the cluster\n"
	@printf "  $(GREEN)make status$(RESET)              Show cluster leader and live service status\n"
	@printf "\n$(CYAN)Testing & Verification:$(RESET)\n"
	@printf "  $(GREEN)make test$(RESET)                Run complete test suite (smoke + engine + adapters)\n"
	@printf "  $(GREEN)make test-adapter$(RESET)        Run Python Client SDK & SGLang HiCache tests\n"
	@printf "  $(GREEN)make test-engine$(RESET)         Run pure-C storage engine tiering & stress tests\n"
	@printf "  $(GREEN)make test-smoke$(RESET)          Run Go control plane + gRPC contract smoke test\n"
	@printf "\n$(CYAN)Demos & Benchmarks:$(RESET)\n"
	@printf "  $(GREEN)make demo$(RESET)                Run SGLang cross-replica prefix cache hit demo\n"
	@printf "  $(GREEN)make bench$(RESET)               Run bulk transport and cluster benchmarks\n"
	@printf "  $(GREEN)make chaos$(RESET)               Run node crash/restart fault injection chaos tests\n"
	@printf "\n$(CYAN)Development Environment:$(RESET)\n"
	@printf "  $(GREEN)make shell$(RESET)               Open an interactive bash shell in the dev container\n"
	@printf "  $(GREEN)make dev-image$(RESET)           Build or update the pulsekv-v2-dev Docker image\n"
	@printf "  $(GREEN)make clean$(RESET)               Clean build artifacts, run logs, and sockets\n"
	@printf "===============================================================\n\n"

# ---------------------------------------------------------------------------
# Development Environment
# ---------------------------------------------------------------------------
.PHONY: dev-image image
dev-image image:
	docker build -t $(DOCKER_IMAGE) -f deploy/Dockerfile .

.PHONY: shell
shell:
	$(INTERACT)

# ---------------------------------------------------------------------------
# Cluster Lifecycle
# ---------------------------------------------------------------------------
.PHONY: start up
start up:
	$(RUN_CMD) "deploy/run-local-cluster.sh"

.PHONY: stop down
stop down:
	$(RUN_CMD) "deploy/stop-local-cluster.sh"

.PHONY: restart
restart:
	$(RUN_CMD) "deploy/run-local-cluster.sh --restart"

.PHONY: status
status:
	$(RUN_CMD) "deploy/build/bin/pulsekv-smoke --config deploy/cluster.config.yaml --mode=leader 2>/dev/null || true; grpcurl -plaintext 127.0.0.1:7000 list 2>/dev/null || echo 'Cluster is not running (start with make start)'"

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
.PHONY: test
test:
	$(RUN_CMD) "\
		deploy/run-local-cluster.sh && \
		deploy/smoke-test.sh && \
		deploy/test-engine.sh && \
		PYTHONPATH=adapters python3 -m unittest discover -s adapters/tests && \
		deploy/stop-local-cluster.sh \
	"

.PHONY: test-adapter
test-adapter:
	$(RUN_CMD) "\
		deploy/run-local-cluster.sh && \
		PYTHONPATH=adapters python3 -m unittest discover -s adapters/tests && \
		deploy/stop-local-cluster.sh \
	"

.PHONY: test-engine
test-engine:
	$(RUN_CMD) "deploy/test-engine.sh"

.PHONY: test-smoke
test-smoke:
	$(RUN_CMD) "\
		deploy/run-local-cluster.sh && \
		deploy/smoke-test.sh && \
		deploy/stop-local-cluster.sh \
	"

# ---------------------------------------------------------------------------
# Demos, Benchmarks & Chaos
# ---------------------------------------------------------------------------
.PHONY: demo demo-sglang
demo demo-sglang:
	$(RUN_CMD) "\
		deploy/run-local-cluster.sh && \
		deploy/demo-cross-replica-sglang.sh --trials 10 --prefix-tokens 512 && \
		deploy/stop-local-cluster.sh \
	"

.PHONY: bench
bench:
	$(RUN_CMD) "\
		deploy/run-local-cluster.sh && \
		deploy/bench-bulk.sh && \
		deploy/stop-local-cluster.sh \
	"

.PHONY: bench-bulk
bench-bulk:
	$(RUN_CMD) "deploy/bench-bulk.sh"

.PHONY: chaos
chaos:
	$(RUN_CMD) "\
		deploy/run-local-cluster.sh && \
		deploy/chaos-test.sh --target node-1 --cycles 3 --seed 7 && \
		deploy/stop-local-cluster.sh \
	"

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------
.PHONY: clean
clean:
	$(RUN_CMD) "deploy/stop-local-cluster.sh 2>/dev/null || true; rm -rf deploy/run deploy/build build .pytest_cache"

# ---------------------------------------------------------------------------
# Legacy v1 Single-Node Targets (Preserved)
# ---------------------------------------------------------------------------
CC            ?= cc
COMMON_CFLAGS := -Wall -Wextra -std=c11 -Iinclude
CFLAGS        := $(COMMON_CFLAGS) -O2 -pthread
LDFLAGS       := -pthread

BUILD         := build
PROTO         := src/protocol.c
TABLE         := src/hashtable.c
WAL           := src/wal.c
HDRS          := include/protocol.h include/hashtable.h include/wal.h

SERVER        := $(BUILD)/pulsekv
SERVER_LT     := $(BUILD)/pulsekv_lt

.PHONY: v1-all v1-bench
v1-all: $(SERVER) $(SERVER_LT)

$(SERVER): src/main.c $(PROTO) $(TABLE) $(WAL) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ src/main.c $(PROTO) $(TABLE) $(WAL) $(LDFLAGS)

$(SERVER_LT): src/main.c $(PROTO) $(TABLE) $(WAL) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -DPULSEKV_LEVEL_TRIGGERED -o $@ src/main.c $(PROTO) $(TABLE) $(WAL) $(LDFLAGS)

$(BUILD):
	mkdir -p $@

v1-bench: v1-all
	tests/run_benchmarks.sh $(SERVER)
