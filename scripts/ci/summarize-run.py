#!/usr/bin/env python3
"""Summarize `gh run view RUN --json createdAt,updatedAt,status,conclusion,jobs`."""
import argparse
from datetime import datetime
import json
import sys


def timestamp(value):
    if not value or value.startswith("0001-"):
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def duration(start, end):
    start, end = timestamp(start), timestamp(end)
    return (end - start).total_seconds() if start and end else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", nargs="?", help="JSON file; defaults to stdin")
    parser.add_argument("--baseline-seconds", type=float)
    args = parser.parse_args()
    if args.baseline_seconds is not None and args.baseline_seconds <= 0:
        parser.error("baseline seconds must be positive")
    if args.file:
        with open(args.file) as source:
            run = json.load(source)
    else:
        run = json.load(sys.stdin)
    complete = run["status"] == "completed"
    print(f"Status: {run['status']} / {run.get('conclusion') or 'pending'}")
    jobs = [j for j in run.get("jobs", []) if timestamp(j.get("startedAt"))]
    if not complete:
        print("Run incomplete: no final elapsed time or speedup reported.")
    elif jobs:
        finished = [timestamp(j.get("completedAt")) for j in jobs]
        finished = [t for t in finished if t]
        # Job completion, rather than updatedAt, excludes later metadata edits.
        end = max(finished)
        created = timestamp(run["createdAt"])
        started = min(timestamp(j["startedAt"]) for j in jobs)
        elapsed = (end - created).total_seconds()
        print(f"End-to-end: {elapsed:.0f}s; initial queue: {(started - created).total_seconds():.0f}s")
        print(f"First job start to last finish: {(end - started).total_seconds():.0f}s")
        if args.baseline_seconds and elapsed > 0:
            print(f"Elapsed-time ratio against supplied baseline: {args.baseline_seconds / elapsed:.2f}x")
            if run.get("conclusion") != "success":
                print("Not a passing-suite speedup: this run did not succeed.")
    measured = [(duration(j.get("startedAt"), j.get("completedAt")), j) for j in jobs]
    measured = [(d, j) for d, j in measured if d is not None]
    print(f"Completed job time: {sum(d for d, _ in measured) / 60:.1f} runner-minutes")
    for seconds, job in sorted(measured, key=lambda pair: -pair[0]):
        print(f"{seconds:6.0f}s  {job['conclusion']:10} {job['name']}")


if __name__ == "__main__":
    main()
