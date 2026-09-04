#!/usr/bin/env python3
"""Validate raw rf3-sqlbench JSON (or gzip JSON) and print an honest comparison.

Usage: summarize-crdb-sql.py vibedb.json[.gz] cockroachdb.json[.gz]
Missing, failed or unverified trials never contribute to a throughput median.
"""
import argparse
import gzip
import json
import math
from pathlib import Path
from statistics import median

WORKLOADS = ("point_hit", "point_miss", "range_64", "group_16", "update_existing")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path, engine):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as stream:
        report = json.load(stream)
    config = report["config"]
    require(config["Engine"] == engine, "unexpected engine")
    clients = [int(n) for n in config["Clients"].split(",")]
    expected = [(w, c, r) for w in WORKLOADS for c in clients
                for r in range(1, config["Repetitions"] + 1)]
    results = report["results"]
    actual = [(r["workload"], r["clients"], r["repetition"]) for r in results]
    require(actual == expected[:len(actual)], "trials are missing, repeated or reordered")
    require(len(actual) == len(expected) or report.get("verification_error"),
            "incomplete run has no recorded failure")
    samples_checked = 0
    for trial in results:
        count = trial["operations"]
        expected_count = config["ScanOperations"] if trial["workload"] in ("range_64", "group_16") else config["Operations"]
        require(trial["engine"] == engine and count == expected_count and count > 0,
                "trial operation count/engine mismatch")
        samples = trial["samples"]
        require(len(samples) == count, "sample count mismatch")
        for ordinal, sample in enumerate(samples):
            require(sample["ordinal"] == ordinal and sample["client"] == ordinal % trial["clients"]
                    and sample["ns"] > 0, "invalid sample identity or latency")
        errors = sum(bool(s.get("error")) for s in samples)
        require(errors == trial["errors"], "error count mismatch")
        require(not trial["verified"] or errors == 0, "failed trial marked verified")
        elapsed = trial["elapsed_ns"]
        require(elapsed > 0, "invalid elapsed time")
        throughput = (count-errors) * 1e9 / elapsed
        require(math.isclose(throughput, trial["successful_ops_per_second"], rel_tol=1e-12),
                "throughput does not match samples and elapsed time")
        latencies = sorted(s["ns"] for s in samples)
        for percent in (50, 95, 99):
            require(trial[f"p{percent}_ns"] == latencies[math.ceil(count * percent / 100)-1],
                    "percentile does not match samples")
        samples_checked += count
    return report, samples_checked


def complete_trials(report, workload, clients):
    trials = [r for r in report["results"] if (r["workload"], r["clients"]) == (workload, clients)]
    if len(trials) != report["config"]["Repetitions"] or any(not r["verified"] or r["errors"] for r in trials):
        return None
    return trials


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("vibedb", type=Path)
    parser.add_argument("cockroachdb", type=Path)
    args = parser.parse_args()
    vibe, vc = load(args.vibedb, "vibedb")
    crdb, cc = load(args.cockroachdb, "cockroachdb")
    require({k: v for k, v in vibe["config"].items() if k != "Engine"} ==
            {k: v for k, v in crdb["config"].items() if k != "Engine"}, "workload configurations differ")
    print(f"Validated {vc + cc:,} raw latency samples; medians require all repetitions to pass.\n")
    print("| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |")
    print("|---|---:|---:|---:|---:|")
    for workload in WORKLOADS:
        for clients in map(int, vibe["config"]["Clients"].split(",")):
            trials = [complete_trials(r, workload, clients) for r in (vibe, crdb)]
            rates = [median(t["successful_ops_per_second"] for t in group) if group else None for group in trials]
            cells = [f"{rate:,.1f}" if rate is not None else "Incomplete/failed" for rate in rates]
            ratio = f"{rates[0]/rates[1]:.3f}×" if all(rate is not None and rate > 0 for rate in rates) else "N/A"
            print(f"| {workload} | {clients} | {cells[0]} | {cells[1]} | {ratio} |")
    for engine, report in (("VibeDB", vibe), ("CRDB", crdb)):
        if report.get("verification_error"):
            print(f"\n{engine} recorded failure: {report['verification_error']}")


if __name__ == "__main__":
    main()
