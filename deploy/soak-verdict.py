#!/usr/bin/env python3
"""Judge a soak run and annotate its report.

    deploy/soak-verdict.py REPORT.json [--chaos-interval SECONDS]
                                       [--chaos-enabled 0|1]
                                       [--chaos-log PATH]

Exit status is the verdict: 0 healthy, 1 degraded.

Why this exists
---------------
On 2026-08-19 a soak run spent 75 minutes with zero live data nodes. Every
client operation failed instantly, nothing recovered, and the harness reported
success -- because ``--continue-on-error=true`` keeps the load generator alive
through anything, and nothing downstream ever asked whether the operations had
actually worked. A run whose cluster is dead is not a passing run, and the only
reason that needed saying out loud is that for one run it did not get said.

The report also never recorded how it was faulted. ``pulsekv-v2-progress-report.md``
narrates "node crash/restarts every 15s" for a Phase 9.4 run whose artifact does
not state any chaos interval, so the claim cannot be checked against the file it
supposedly came from. This script writes that configuration into the report, so
a future reader does not have to take a doc's word for it.

See docs/pulsekv-v2-soak-collapse-analysis.md.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


def dead_windows(intervals: list[dict]) -> list[dict]:
    """Intervals where operations were attempted and none succeeded.

    That combination is the signature of the failure this guard exists for: the
    workers are running at full speed and every single operation is failing, so
    throughput can look busy while the cluster serves nothing.
    """
    dead = []
    for row in intervals:
        operations = row.get("operations", 0) or 0
        verified = row.get("verified", 0) or 0
        if operations > 0 and verified == 0:
            dead.append(row)
    return dead


def longest_dead_run(intervals: list[dict]) -> int:
    longest = current = 0
    for row in intervals:
        operations = row.get("operations", 0) or 0
        verified = row.get("verified", 0) or 0
        if operations > 0 and verified == 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def count_chaos_cycles(chaos_log: Path | None) -> dict | None:
    """Cycle count and injector count from the chaos log.

    More than one injector is itself a finding: the 2026-08-19 run had three,
    from three different soak invocations, interleaving crash/restart cycles on
    one cluster. Counting them here means a future run says so in its report
    instead of leaving it to be discovered in a log months later.
    """
    if not chaos_log or not chaos_log.exists():
        return None
    cycles = 0
    injectors = 0
    seen_cycle_numbers: set[int] = set()
    pattern = re.compile(r"\[chaos-cycle (\d+)\]")
    for line in chaos_log.read_text(errors="replace").splitlines():
        if "[injector] started" in line:
            injectors += 1
        match = pattern.search(line)
        if match and "Crashing" in line:
            cycles += 1
            seen_cycle_numbers.add(int(match.group(1)))
    return {
        "crash_cycles": cycles,
        "injectors_started": injectors,
        "distinct_cycle_numbers": len(seen_cycle_numbers),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--chaos-interval", type=int, default=None)
    parser.add_argument("--chaos-enabled", type=int, default=None)
    parser.add_argument("--chaos-log", type=Path, default=None)
    parser.add_argument("--max-error-rate", type=float, default=0.5,
                        help="fraction of operations that may fail before the run is degraded")
    args = parser.parse_args()

    if not args.report.exists():
        print(f"soak-verdict: no report at {args.report}", file=sys.stderr)
        return 1

    report = json.loads(args.report.read_text())
    intervals = report.get("intervals", []) or []

    operations = report.get("operations", 0) or 0
    errors = report.get("rpc_errors", 0) or 0
    verified = report.get("verified", 0) or 0
    mismatches = report.get("mismatches", 0) or 0
    error_rate = (errors / operations) if operations else 0.0

    dead = dead_windows(intervals)
    longest_dead = longest_dead_run(intervals)

    problems: list[str] = []
    # Ordered by how badly each one invalidates the run.
    if mismatches:
        problems.append(
            f"{mismatches} value mismatch(es): a read returned bytes that were never written. "
            "This invalidates the run outright and is not a availability finding.")
    if dead:
        problems.append(
            f"{len(dead)} reporting interval(s) served zero verified reads while operations "
            f"were still being attempted (longest unbroken stretch: {longest_dead}). "
            "The cluster was not serving; the load generator only looked busy.")
    if verified == 0 and operations > 0:
        problems.append("the run verified nothing at all.")
    if error_rate > args.max_error_rate:
        problems.append(
            f"error rate {error_rate:.1%} exceeds the {args.max_error_rate:.0%} ceiling.")

    verdict = "healthy" if not problems else "degraded"

    report["fault_injection"] = {
        "chaos_enabled": bool(args.chaos_enabled) if args.chaos_enabled is not None else None,
        "chaos_interval_seconds": args.chaos_interval,
        **(count_chaos_cycles(args.chaos_log) or {}),
    }
    report["verdict"] = {
        "result": verdict,
        "problems": problems,
        "error_rate": error_rate,
        "dead_intervals": len(dead),
        "longest_dead_interval_run": longest_dead,
        "intervals_evaluated": len(intervals),
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n")

    if problems:
        print(f"soak verdict: DEGRADED ({args.report})", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    print(f"soak verdict: healthy — {operations} operations, {verified} verified, "
          f"{errors} error(s) ({error_rate:.2%}), {len(intervals)} interval(s), no dead windows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
