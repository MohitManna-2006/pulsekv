#!/usr/bin/env bash
#
# Build and run the storage engine's test suite.
#
#   deploy/test-engine.sh                 release build, all four suites
#   deploy/test-engine.sh --tsan          ... plus a ThreadSanitizer run
#   deploy/test-engine.sh --valgrind      ... plus a Valgrind memcheck run
#   deploy/test-engine.sh --all           release + tsan + valgrind
#
# The engine builds as a standalone pure-C CMake project, so none of this needs
# gRPC, a running cluster, or a network stack. That is the point of the
# boundary described in node/README.md.
#
# WHY THREE MODES. v1 kept a parallel ThreadSanitizer build of its whole suite
# and ran the store's tests under Valgrind, because for a concurrent data
# structure holding long-lived heap state "the tests pass" is not evidence of
# correctness. v2's engine has strictly more of both: an intrusive LRU list and
# four counters per shard on top of the chains, plus values whose ownership
# moves between RAM, disk, and the caller. Same discipline applies.
#
#   release   fast; the assertions themselves
#   tsan      races the assertions would only catch on an unlucky day
#   valgrind  leaks and invalid accesses; the ownership rules
#
# RUNNING --tsan INSIDE DOCKER: ThreadSanitizer disables ASLR by calling
# personality(ADDR_NO_RANDOMIZE), which Docker's default seccomp profile blocks.
# The container needs to be started with:
#
#   docker run --security-opt seccomp=unconfined ...
#
# Without it TSan aborts at startup with a tsan_platform_linux.cpp CHECK
# failure. This script detects that specific failure and says so rather than
# reporting it as a test failure.

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

RUN_RELEASE=1
RUN_TSAN=0
RUN_VALGRIND=0

while [ $# -gt 0 ]; do
    case "$1" in
        --tsan)     RUN_TSAN=1; shift ;;
        --valgrind) RUN_VALGRIND=1; shift ;;
        --all)      RUN_TSAN=1; RUN_VALGRIND=1; shift ;;
        --only-tsan)     RUN_RELEASE=0; RUN_TSAN=1; shift ;;
        --only-valgrind) RUN_RELEASE=0; RUN_VALGRIND=1; shift ;;
        -h|--help)  sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)          pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

pk_require cmake "Install the v2 dev image (deploy/Dockerfile)."

ENGINE_SRC="${PULSEKV_REPO_ROOT}/node/engine"
FAILED=0

# build_and_run BUILDDIR SANITIZER LABEL [runner...]
build_and_run() {
    local builddir="$1" sanitize="$2" label="$3"; shift 3

    pk_step "Building the engine (${label})"
    local cfg_log="${builddir}.configure.log"
    local build_log="${builddir}.build.log"
    mkdir -p "$(dirname "$builddir")"

    if ! cmake -S "$ENGINE_SRC" -B "$builddir" \
               -DCMAKE_BUILD_TYPE="$([ "$sanitize" = none ] && echo Release || echo Debug)" \
               -DPULSEKV_SANITIZE="$sanitize" >"$cfg_log" 2>&1; then
        pk_err "cmake configure failed; last 30 lines of $(pk_relpath "$cfg_log"):"
        tail -n 30 "$cfg_log" >&2
        FAILED=1
        return
    fi
    if ! cmake --build "$builddir" -j "$(nproc 2>/dev/null || echo 4)" >"$build_log" 2>&1; then
        pk_err "build failed; last 40 lines of $(pk_relpath "$build_log"):"
        tail -n 40 "$build_log" >&2
        FAILED=1
        return
    fi

    pk_step "Running the engine tests (${label})"
    local suite
    for suite in test_engine_basic test_engine_chunked test_engine_tiering test_engine_stress; do
        local bin="${builddir}/${suite}"
        [ -x "$bin" ] || { pk_err "$suite was not built"; FAILED=1; continue; }

        local out rc=0
        out="$("$@" "$bin" 2>&1)" || rc=$?

        # Docker's seccomp profile blocks the personality() call TSan uses to
        # turn off ASLR. That is an environment problem, not a test failure,
        # and reporting it as one would send someone hunting a race that does
        # not exist.
        if printf '%s' "$out" | grep -q "tsan_platform_linux.cpp.*ADDR_NO_RANDOMIZE"; then
            pk_err "ThreadSanitizer cannot start: Docker's seccomp profile blocks personality(ADDR_NO_RANDOMIZE)."
            pk_err "Re-run the container with:  docker run --security-opt seccomp=unconfined ..."
            FAILED=1
            return
        fi

        local summary
        summary="$(printf '%s\n' "$out" | grep -E "check\(s\), .* failed" | tail -1)"
        if [ "$rc" -eq 0 ]; then
            pk_ok "$(printf '%-22s %s' "$suite" "${summary:-passed}")"
        else
            pk_err "$(printf '%-22s exit %d' "$suite" "$rc")"
            printf '%s\n' "$out" | grep -E "FAIL|ERROR SUMMARY|WARNING: ThreadSanitizer|definitely lost" | head -20 >&2
            FAILED=1
        fi
    done
}

if [ "$RUN_RELEASE" -eq 1 ]; then
    build_and_run "${PULSEKV_BUILD_DIR}/engine-release" none "release"
fi

if [ "$RUN_TSAN" -eq 1 ]; then
    build_and_run "${PULSEKV_BUILD_DIR}/engine-tsan" thread "ThreadSanitizer"
fi

if [ "$RUN_VALGRIND" -eq 1 ]; then
    if ! command -v valgrind >/dev/null 2>&1; then
        pk_die "valgrind is not installed. Rebuild the v2 dev image: docker build -f deploy/Dockerfile -t pulsekv-v2-dev ."
    fi
    # --error-exitcode makes a leak or an invalid access fail the suite rather
    # than merely printing something nobody reads.
    build_and_run "${PULSEKV_BUILD_DIR}/engine-valgrind" none "Valgrind memcheck" \
        valgrind --error-exitcode=42 --leak-check=full --errors-for-leak-kinds=definite \
                 --track-origins=yes --quiet
fi

echo
if [ "$FAILED" -eq 0 ]; then
    printf '%s==> engine tests passed%s\n\n' "$PK_BOLD$PK_GREEN" "$PK_RESET"
    exit 0
fi
printf '%s==> ENGINE TESTS FAILED%s\n\n' "$PK_BOLD$PK_RED" "$PK_RESET"
exit 1
