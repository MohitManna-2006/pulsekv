#!/usr/bin/env bash
#
# End-to-end smoke test for the running Phase 2 dev cluster.
#
#   deploy/run-local-cluster.sh
#   deploy/smoke-test.sh [--config PATH] [--no-install] [--skip-build]
#
# Three legs, all against the live cluster:
#
#   1. Go   -- pulsekv-smoke asserts the contract through the checked-in
#              generated stubs, independently reproduces the HRW shard map,
#              writes through the public SDK, and directly proves each sample
#              key hit its predicted owner and missed a different node.
#   2. Python -- the adapters package health-checks the Go control plane and a
#              C++ node, proving both cross-language boundaries.
#   3. grpcurl -- optional; only runs if grpcurl is on PATH. Verifies server
#              reflection advertises the expected services.
#
# Exits non-zero, loudly, if any check fails.
#
# The Go leg compiles against the same generated stubs and router package as
# the control plane, so it checks the actual contract and exact assignment
# function rather than approximating either in shell.

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

NO_INSTALL=0
SKIP_BUILD=0

while [ $# -gt 0 ]; do
    case "$1" in
        --config)     PULSEKV_CONFIG="$2"; shift 2 ;;
        --config=*)   PULSEKV_CONFIG="${1#*=}"; shift ;;
        --no-install) NO_INSTALL=1; shift ;;
        --skip-build) SKIP_BUILD=1; shift ;;
        -h|--help)    sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)            pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

PASS=0
FAIL=0
record_pass() { PASS=$((PASS + 1)); pk_ok "$*"; }
record_fail() { FAIL=$((FAIL + 1)); pk_err "$*"; }

# ---------------------------------------------------------------------------
# Preconditions.
# ---------------------------------------------------------------------------
if ! pk_cluster_running; then
    pk_die "no cluster is running. Start one first: deploy/run-local-cluster.sh"
fi
[ -f "$PULSEKV_CONFIG" ] || pk_die "config not found: $PULSEKV_CONFIG"

if [ ! -x "$PULSEKV_SMOKE_BIN" ] && [ "$SKIP_BUILD" -eq 0 ]; then
    pk_step "Building pulsekv-smoke"
    ( cd "$PULSEKV_REPO_ROOT/control" && \
      go build -o "$PULSEKV_SMOKE_BIN" ./cmd/pulsekv-smoke )
fi
[ -x "$PULSEKV_SMOKE_BIN" ] || pk_die "$(pk_relpath "$PULSEKV_SMOKE_BIN") is missing"

IFS=$'\t' read -r CP_HOST CP_PORT < <(pk_config_read --print-control-plane)
CP_ADDRESS="${CP_HOST}:${CP_PORT}"

IFS=$'\t' read -r FIRST_NODE_ID FIRST_NODE_HOST FIRST_NODE_PORT \
    < <(pk_config_read --print-nodes | head -1)
FIRST_NODE_ADDRESS="${FIRST_NODE_HOST}:${FIRST_NODE_PORT}"

# ---------------------------------------------------------------------------
# Leg 1 -- Go, the full contract.
# ---------------------------------------------------------------------------
pk_step "Leg 1/3: Go contract + routing assertions (control plane + every node)"

go_output=""
go_rc=0
go_output="$("$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" --mode=smoke 2>&1)" || go_rc=$?
printf '%s\n' "$go_output" | sed 's/^/    /'

if [ "$go_rc" -eq 0 ]; then
    record_pass "Go contract + routing assertions passed"
else
    record_fail "Go contract + routing assertions failed (exit $go_rc)"
fi

# ---------------------------------------------------------------------------
# Leg 2 -- Python, both language boundaries.
# ---------------------------------------------------------------------------
pk_step "Leg 2/3: Python adapters client"

# Installing into the ambient Python is a side effect, so only do it where that
# is clearly safe and clearly intended: inside a virtualenv (which is what the
# v2 dev image gives you), or on an explicit opt-in. Otherwise fall back to
# PYTHONPATH, which proves the same gRPC path without touching anything.
PY_HEALTH=()
if [ "$NO_INSTALL" -eq 0 ] && { [ -n "${VIRTUAL_ENV:-}" ] || [ "${PULSEKV_ALLOW_PIP_INSTALL:-0}" = "1" ]; }; then
    pk_dim "installing adapters/ into ${VIRTUAL_ENV:-the active Python} (--no-deps)"
    if pip install --quiet --no-deps --no-input "$PULSEKV_REPO_ROOT/adapters"; then
        record_pass "adapters/ installs as a Python package"
        PY_HEALTH=(pulsekv-health)
    else
        record_fail "pip install ./adapters failed"
        PY_HEALTH=(python3 -m pulsekv_adapters.health_client)
        export PYTHONPATH="$PULSEKV_REPO_ROOT/adapters${PYTHONPATH:+:$PYTHONPATH}"
    fi
else
    pk_dim "not in a virtualenv (or --no-install given); using PYTHONPATH instead of installing"
    pk_dim "set PULSEKV_ALLOW_PIP_INSTALL=1 to exercise the install path too"
    PY_HEALTH=(python3 -m pulsekv_adapters.health_client)
    export PYTHONPATH="$PULSEKV_REPO_ROOT/adapters${PYTHONPATH:+:$PYTHONPATH}"
fi

run_py_health() {
    # run_py_health SERVICE ADDRESS -> writes output to $py_out, sets $py_rc
    py_rc=0
    py_out="$("${PY_HEALTH[@]}" --service "$1" --address "$2" 2>&1)" || py_rc=$?
}

# 2a. Python -> Go. This is the Phase 0 cross-language proof.
run_py_health metadata "$CP_ADDRESS"
printf '%s\n' "$py_out" | sed 's/^/    /'
if [ "$py_rc" -eq 0 ]; then
    record_pass "Python -> Go: ClusterMetadataService.HealthCheck on $CP_ADDRESS"
else
    record_fail "Python -> Go: ClusterMetadataService.HealthCheck failed (exit $py_rc)"
fi

# 2b. Python -> C++.
run_py_health node "$FIRST_NODE_ADDRESS"
printf '%s\n' "$py_out" | sed 's/^/    /'
if [ "$py_rc" -eq 0 ]; then
    record_pass "Python -> C++: NodeService.HealthCheck on $FIRST_NODE_ID ($FIRST_NODE_ADDRESS)"
else
    record_fail "Python -> C++: NodeService.HealthCheck failed (exit $py_rc)"
fi

# 2c. AdapterService has no server in Phase 0 -- it arrives in Phase 7. Assert
#     that it says so, rather than assuming it.
run_py_health adapter "$CP_ADDRESS"
# The client correctly renders this as FAIL -- it is a failed health check.
# Mark it as the expected outcome so the transcript does not read like a bug.
printf '%s\n' "$py_out" | sed 's/^/    expected -> /'
if [ "$py_rc" -ne 0 ] && printf '%s' "$py_out" | grep -q 'UNIMPLEMENTED'; then
    record_pass "AdapterService.HealthCheck reports UNIMPLEMENTED (Phase 7 implements it)"
elif [ "$py_rc" -eq 0 ]; then
    record_fail "AdapterService.HealthCheck succeeded, but nothing should implement it in Phase 0"
else
    record_fail "AdapterService.HealthCheck failed, but not with UNIMPLEMENTED"
fi

# ---------------------------------------------------------------------------
# Leg 3 -- grpcurl reflection check (optional).
# ---------------------------------------------------------------------------
pk_step "Leg 3/3: server reflection (grpcurl)"

if ! command -v grpcurl >/dev/null 2>&1; then
    pk_dim "grpcurl not on PATH, skipping (not a failure -- the Go leg already"
    pk_dim "covers every contract assertion; this leg only checks reflection)"
else
    reflect_rc=0
    cp_services="$(grpcurl -plaintext "$CP_ADDRESS" list 2>&1)" || reflect_rc=$?
    if [ "$reflect_rc" -eq 0 ] && \
       printf '%s' "$cp_services" | grep -q 'pulsekv.metadata.v1.ClusterMetadataService'; then
        record_pass "control plane reflection lists ClusterMetadataService"
    else
        record_fail "control plane reflection did not list ClusterMetadataService: $cp_services"
    fi

    reflect_rc=0
    node_services="$(grpcurl -plaintext "$FIRST_NODE_ADDRESS" list 2>&1)" || reflect_rc=$?
    if [ "$reflect_rc" -eq 0 ] && \
       printf '%s' "$node_services" | grep -q 'pulsekv.node.v1.NodeService'; then
        record_pass "$FIRST_NODE_ID reflection lists NodeService"
    else
        record_fail "$FIRST_NODE_ID reflection did not list NodeService: $node_services"
    fi
fi

# ---------------------------------------------------------------------------
# Verdict.
# ---------------------------------------------------------------------------
echo
if [ "$FAIL" -eq 0 ]; then
    printf '%s==> smoke test passed%s  (%d check(s))\n\n' \
        "$PK_BOLD$PK_GREEN" "$PK_RESET" "$PASS"
    exit 0
fi

printf '%s==> SMOKE TEST FAILED%s  (%d passed, %d failed)\n\n' \
    "$PK_BOLD$PK_RED" "$PK_RESET" "$PASS" "$FAIL"
pk_info "process logs: $(pk_relpath "$PULSEKV_LOG_DIR")"
exit 1
