#!/usr/bin/env bash
#
# Regression tests for the process-lifecycle registry in common.sh.
#
#   deploy/test-lifecycle.sh
#
# These exist because of a real incident. On 2026-08-19 a soak run drove the
# cluster into a permanent zero-data-node state 3m18s in and stayed there for
# the remaining 80 minutes, with every client operation failing instantly and
# the benchmark process happily continuing. The cause was not in the data plane
# or the control plane: it was this registry. Concurrent lifecycle operations
# raced on deploy/run/cluster.pids, entries were silently lost, and after that
# the harness could neither stop the processes it thought it had stopped nor
# start the ones it thought it had started.
#
# Each test below is one link of that chain, checked in isolation and without
# needing a cluster. See docs/pulsekv-v2-soak-collapse-analysis.md.
#

set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

FAILURES=0
CASES=0

check() {
    local name="$1" condition="$2"
    CASES=$((CASES + 1))
    if [ "$condition" = "pass" ]; then
        pk_ok "$name"
    else
        pk_err "$name"
        FAILURES=$((FAILURES + 1))
    fi
}

# Every test runs against a scratch registry, never the real one.
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/pulsekv-lifecycle-test.XXXXXX")"
SPAWNED=()
cleanup() {
    # Kill any stand-in still running, so the suite never leaves strays behind.
    # Tracked explicitly: spawn_fake_replica runs inside a command substitution,
    # so its child is not a job of this shell and `jobs` would not list it.
    local pid
    for pid in "${SPAWNED[@]:-}"; do
        [ -n "$pid" ] || continue
        kill -KILL "$pid" 2>/dev/null || true
    done
    rm -rf -- "$TEST_ROOT"
}
trap cleanup EXIT INT TERM

PULSEKV_RUN_DIR="$TEST_ROOT/run"
# A real executable under the name the label matcher expects. pk_pid_matches_label
# insists argv[0] IS the expected binary, so a shell script with a #! line would
# show up as its interpreter and match nothing.
PULSEKV_CONTROLPLANE_BIN="$TEST_ROOT/bin/pulsekv-controlplane"
mkdir -p "$TEST_ROOT/bin"
cp -- "$(command -v bash)" "$PULSEKV_CONTROLPLANE_BIN"

# That copy cannot also answer --print-control-plane, and pk_controlplane_ids
# goes through the real binary's parser to get the replica list. Stub the config
# read -- and only the config read -- so the liveness logic under test stays
# exactly the code that runs in production.
pk_config_read() {
    case "${1:-}" in
        --print-control-plane)
            printf 'cp-0\t127.0.0.1\t7000\ncp-1\t127.0.0.1\t7001\ncp-2\t127.0.0.1\t7002\n' ;;
        --print-nodes)
            printf 'node-0\t127.0.0.1\t7100\t7201\nnode-1\t127.0.0.1\t7101\t7202\n' ;;
        *) return 1 ;;
    esac
}
PULSEKV_PID_FILE="$PULSEKV_RUN_DIR/cluster.pids"
PULSEKV_LOG_DIR="$PULSEKV_RUN_DIR/logs"
mkdir -p "$PULSEKV_RUN_DIR" "$PULSEKV_LOG_DIR"

# ---------------------------------------------------------------------------
# 1. Concurrent writers must not lose each other's entries.
#
# This is the root cause. pk_pid_set is a read-modify-write: it copies the file
# minus its own label, appends its record, and renames over the original. The
# rename is atomic; the read-modify-write is not. Two writers that read the same
# starting state both write a file missing the other's record, and whichever
# renames last wins.
#
# The soak harness creates this concurrency by design -- the chaos injector
# cycles a data node and, every fourth cycle, a control-plane replica, and
# nothing serialises the two -- and by accident, when an injector from an
# earlier run survives cleanup and keeps operating on the same directory.
# ---------------------------------------------------------------------------
test_concurrent_writes_keep_every_entry() {
    : >"$PULSEKV_PID_FILE"

    local writers=24 i
    for ((i = 0; i < writers; i++)); do
        (
            # A short jitter widens the window without making the test flaky:
            # the failure is a lost update, so any interleaving at all loses
            # entries, and this only makes it lose more of them.
            pk_pid_set "data:node-$i" "$((10000 + i))" "127.0.0.1:$((7100 + i))"
        ) &
    done
    wait

    local present=0
    for ((i = 0; i < writers; i++)); do
        if pk_pid_record_for "data:node-$i" >/dev/null 2>&1; then
            present=$((present + 1))
        fi
    done

    if [ "$present" -eq "$writers" ]; then
        check "concurrent pk_pid_set keeps every entry ($present/$writers)" pass
    else
        check "concurrent pk_pid_set keeps every entry ($present/$writers survived)" fail
    fi
}

# ---------------------------------------------------------------------------
# 2. A concurrent remove must not resurrect or drop unrelated entries.
# ---------------------------------------------------------------------------
test_concurrent_remove_is_surgical() {
    : >"$PULSEKV_PID_FILE"

    local total=12 i
    for ((i = 0; i < total; i++)); do
        pk_pid_set "data:node-$i" "$((20000 + i))" "127.0.0.1:$((7100 + i))"
    done

    # Remove the even ones while re-writing the odd ones, concurrently.
    for ((i = 0; i < total; i++)); do
        if [ $((i % 2)) -eq 0 ]; then
            ( pk_pid_remove "data:node-$i" ) &
        else
            ( pk_pid_set "data:node-$i" "$((30000 + i))" "127.0.0.1:$((7100 + i))" ) &
        fi
    done
    wait

    local wrong=0
    for ((i = 0; i < total; i++)); do
        if [ $((i % 2)) -eq 0 ]; then
            pk_pid_record_for "data:node-$i" >/dev/null 2>&1 && wrong=$((wrong + 1))
        else
            pk_pid_record_for "data:node-$i" >/dev/null 2>&1 || wrong=$((wrong + 1))
        fi
    done

    if [ "$wrong" -eq 0 ]; then
        check "concurrent remove/set leaves exactly the intended entries" pass
    else
        check "concurrent remove/set leaves exactly the intended entries ($wrong wrong)" fail
    fi
}

# ---------------------------------------------------------------------------
# 3. A live process must never be reported dead because its record was lost.
#
# pk_any_controlplane_alive consulted only the registry. When the registry lost
# the control-plane entries, it answered "no control-plane replica is running"
# while all three were serving happily -- and local-node.sh refuses to start a
# data node on exactly that answer. That is what made the outage permanent:
# every subsequent restart attempt was rejected before it began.
# ---------------------------------------------------------------------------
#
# The stand-in for a running replica is a copy of bash under the binary name the
# label matcher expects, carrying the same --node-id argument a real replica
# does. That exercises pk_pid_matches_label's real argv[0] and --node-id checks
# rather than stubbing around them; a `sleep` would be found by nothing.
spawn_fake_replica() {
    local node_id="$1"
    # Redirected: a child holding this script's stdout keeps `docker run` from
    # returning long after the tests are done.
    "$PULSEKV_CONTROLPLANE_BIN" -c 'sleep 30; exit 0' --node-id "$node_id" >/dev/null 2>&1 &
    printf '%s\n' "$!"
}

test_liveness_survives_a_lost_record() {
    : >"$PULSEKV_PID_FILE"

    local victim
    victim="$(spawn_fake_replica cp-0)"; SPAWNED+=("$victim")
    pk_pid_set "controlplane:cp-0" "$victim" "127.0.0.1:7000"

    local before="fail"
    pk_recorded_alive "controlplane:cp-0" && before="pass"

    # The lost update: the record vanishes while the process keeps running.
    pk_pid_remove "controlplane:cp-0"

    local found="fail"
    pk_process_alive_for_label "controlplane:cp-0" && found="pass"

    # The question local-node.sh actually asks before starting a data node.
    local usable="fail"
    pk_any_controlplane_alive && usable="pass"

    kill -KILL "$victim" 2>/dev/null || true
    wait "$victim" 2>/dev/null || true

    check "a recorded live process reads as alive" "$before"
    check "a live process is still found after its record is lost" "$found"
    check "pk_any_controlplane_alive survives a lost record" "$usable"
}

# ---------------------------------------------------------------------------
# 4. Stopping must not report success while the process is still running.
#
# pk_signal_managed returns 2 ("already gone") when the registry has no record,
# and pk_stop_managed turns that into success. With a lost record that is a lie:
# the process keeps running and keeps its ports. In the incident this left the
# old cp-0 holding gossip port 7240, so every replacement died at startup with
# "bind: address already in use" -- seven times, to the end of the run.
# ---------------------------------------------------------------------------
test_stop_does_not_lie_about_a_surviving_process() {
    : >"$PULSEKV_PID_FILE"

    local victim
    victim="$(spawn_fake_replica cp-0)"; SPAWNED+=("$victim")
    # No registry record at all -- exactly the post-race state.
    pk_stop_managed "controlplane:cp-0" 2 >/dev/null 2>&1 || true

    local stopped="fail"
    pk_pid_alive "$victim" || stopped="pass"

    kill -KILL "$victim" 2>/dev/null || true
    wait "$victim" 2>/dev/null || true

    check "stop with a lost record actually stops the process" "$stopped"
}

# ---------------------------------------------------------------------------
# 5. Starting must adopt a survivor rather than launch a doomed rival.
#
# With the record lost and the process alive, pk_start_managed used to spawn a
# replacement that could not possibly work -- the survivor holds the port -- and
# then recorded the replacement's pid as it died. Repeat that on all three
# replicas and the registry believes the entire control plane is dead.
# ---------------------------------------------------------------------------
test_start_adopts_an_unrecorded_survivor() {
    : >"$PULSEKV_PID_FILE"

    local victim
    victim="$(spawn_fake_replica cp-1)"; SPAWNED+=("$victim")

    local log="$PULSEKV_LOG_DIR/adopt.log"
    pk_start_managed "controlplane:cp-1" "127.0.0.1:7001" "$log" \
        "$PULSEKV_CONTROLPLANE_BIN" -c 'sleep 30; exit 0' --node-id cp-1 >/dev/null 2>&1 || true

    local adopted="fail" recorded_pid=""
    if record="$(pk_pid_record_for "controlplane:cp-1" 2>/dev/null)"; then
        IFS=$'\t' read -r _ recorded_pid _ <<< "$record"
        [ "$recorded_pid" = "$victim" ] && adopted="pass"
    fi

    kill -KILL "$victim" 2>/dev/null || true
    wait "$victim" 2>/dev/null || true

    check "start adopts the running process instead of spawning a rival" "$adopted"
}

# ---------------------------------------------------------------------------
# 6. Adoption must not paper over a DIFFERENT process.
#
# Adopting a survivor is right when it is the process the caller asked for. A
# leftover started from another config is not, and silently adopting it would
# trade a loud port-bind failure for a cluster that disagrees with its own
# config file.
# ---------------------------------------------------------------------------
test_start_refuses_to_adopt_a_different_command() {
    : >"$PULSEKV_PID_FILE"

    local victim
    victim="$(spawn_fake_replica cp-2)"; SPAWNED+=("$victim")

    # Same label, same binary, DIFFERENT arguments than the survivor is running.
    local log="$PULSEKV_LOG_DIR/adopt-mismatch.log"
    pk_start_managed "controlplane:cp-2" "127.0.0.1:7002" "$log" \
        "$PULSEKV_CONTROLPLANE_BIN" -c 'sleep 31; exit 0' --node-id cp-2 >/dev/null 2>&1 || true

    local refused="pass" recorded_pid=""
    if record="$(pk_pid_record_for "controlplane:cp-2" 2>/dev/null)"; then
        IFS=$'\t' read -r _ recorded_pid _ <<< "$record"
        [ "$recorded_pid" = "$victim" ] && refused="fail"
        SPAWNED+=("$recorded_pid")
    fi

    # Reap the rival here too, so bash does not print a job-control notice
    # about it after the suite summary.
    if [ -n "$recorded_pid" ] && [ "$recorded_pid" != "$victim" ]; then
        kill -KILL "$recorded_pid" 2>/dev/null || true
        wait "$recorded_pid" 2>/dev/null || true
    fi
    kill -KILL "$victim" 2>/dev/null || true
    wait "$victim" 2>/dev/null || true

    check "start refuses to adopt a process running a different command" "$refused"
}

pk_step "Lifecycle registry regression tests"
test_concurrent_writes_keep_every_entry
test_concurrent_remove_is_surgical
test_liveness_survives_a_lost_record
test_stop_does_not_lie_about_a_surviving_process
test_start_adopts_an_unrecorded_survivor
test_start_refuses_to_adopt_a_different_command

echo
if [ "$FAILURES" -eq 0 ]; then
    pk_ok "$CASES lifecycle check(s) passed"
    exit 0
fi
pk_err "$FAILURES of $CASES lifecycle check(s) failed"
exit 1
