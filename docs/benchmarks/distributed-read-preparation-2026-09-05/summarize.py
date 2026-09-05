#!/usr/bin/env python3
"""Summarize the ABBA prepared-read benchmark campaign.

The campaign uses two baseline (before) and two candidate (after) logs.  Each
log contains three Go benchmark samples for the workloads below.  This script
keeps every parsed sample under its order, computes per-order medians, and
computes aggregate medians and ratios without treating this local driver
microbenchmark as an RF3 or CockroachDB latency measurement.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import sys
from pathlib import Path


WORKLOADS = ("point_hit", "point_miss", "range_32", "range_64", "range_256")
ORDERS = ("before-1", "after-1", "after-2", "before-2")
METRICS = ("ns/op", "B/op", "allocs/op")
NUMBER = r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
METRIC_PATTERNS = {
    metric: re.compile(rf"(?P<value>{NUMBER})\s+{re.escape(metric)}(?:\s|$)")
    for metric in METRICS
}


def fail(message: str) -> "NoReturn":
    raise SystemExit(f"summarize.py: error: {message}")


def parse_benchmark_line(line: str, path: Path, line_number: int):
    fields = line.split()
    if not fields or not fields[0].startswith("Benchmark"):
        return None
    token = fields[0]
    suffix = token.rsplit("-", 1)
    if len(suffix) != 2 or not suffix[1].isdigit():
        return None
    benchmark, cpu_text = suffix
    parts = benchmark.split("/")
    lane = None
    workload = None
    if len(parts) == 3 and parts[0] == "BenchmarkReplicatedReadExecutor":
        workload, lane = parts[1], parts[2]
    elif len(parts) == 3 and parts[0] == "BenchmarkReplicatedReadExecutorPreparedReuse":
        workload, lane = parts[1], parts[2]
    else:
        return None
    if workload not in WORKLOADS or lane not in ("fresh", "prepared_reuse"):
        return None
    if len(fields) < 2 or not fields[1].isdigit():
        fail(f"{path}:{line_number}: benchmark line has no iteration count")
    metrics = {}
    for metric, pattern in METRIC_PATTERNS.items():
        match = pattern.search(line)
        if match is None:
            fail(f"{path}:{line_number}: benchmark line lacks {metric}: {line.rstrip()}")
        value = float(match.group("value"))
        if not math.isfinite(value) or value < 0:
            fail(f"{path}:{line_number}: invalid {metric} value")
        metrics[metric] = value
    return {
        "benchmark": benchmark,
        "cpu": int(cpu_text),
        "iterations": int(fields[1]),
        "workload": workload,
        "lane": lane,
        "metrics": metrics,
        "line": line.rstrip("\n"),
        "line_number": line_number,
    }


def read_log(path: Path):
    if not path.is_file():
        fail(f"missing input log {path}")
    observations = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(True), 1):
        parsed = parse_benchmark_line(line, path, line_number)
        if parsed is not None:
            observations.append(parsed)
    if not observations:
        fail(f"{path}: no recognized benchmark lines")
    return observations


def canonical_lane(order: str, parsed_lane: str) -> str:
    if order.startswith("before-"):
        if parsed_lane != "fresh":
            fail(f"{order}: baseline log contains unexpected {parsed_lane} lane")
        return "baseline_fresh"
    if not order.startswith("after-"):
        fail(f"unknown ABBA order {order}")
    if parsed_lane == "fresh":
        return "candidate_fresh"
    return "candidate_prepared_reuse"


def median_metrics(observations):
    return {
        metric: statistics.median([sample["metrics"][metric] for sample in observations])
        for metric in METRICS
    }


def fmt_number(value):
    if value is None:
        return ""
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def ratio_metrics(numerator, denominator):
    result = {}
    for metric in METRICS:
        divisor = denominator[metric]
        result[metric] = None if divisor == 0 else numerator[metric] / divisor
    return result


def digest(path: Path):
    raw = path.read_bytes()
    return {"path": path.name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def validate_and_build(log_dir: Path):
    parsed_orders = {}
    source_files = []
    for order in ORDERS:
        path = log_dir / f"{order}.log"
        observations = read_log(path)
        source_files.append(digest(path))
        lanes = {}
        for sample in observations:
            canonical = canonical_lane(order, sample["lane"])
            lanes.setdefault(canonical, {}).setdefault(sample["workload"], []).append(sample)
        parsed_orders[order] = {"arm": "before" if order.startswith("before-") else "after", "lanes": lanes}

    expected = {
        "before-1": {"baseline_fresh"},
        "before-2": {"baseline_fresh"},
        "after-1": {"candidate_fresh", "candidate_prepared_reuse"},
        "after-2": {"candidate_fresh", "candidate_prepared_reuse"},
    }
    for order, required_lanes in expected.items():
        got = set(parsed_orders[order]["lanes"])
        if got != required_lanes:
            fail(f"{order}: expected lanes {sorted(required_lanes)}, got {sorted(got)}")
        for lane in required_lanes:
            for workload in WORKLOADS:
                samples = parsed_orders[order]["lanes"].get(lane, {}).get(workload, [])
                if len(samples) != 3:
                    fail(f"{order}/{lane}/{workload}: expected 3 samples, got {len(samples)}")

    aggregate = {}
    for workload in WORKLOADS:
        medians = {}
        for lane in ("baseline_fresh", "candidate_fresh", "candidate_prepared_reuse"):
            samples = []
            for order in ORDERS:
                samples.extend(parsed_orders[order]["lanes"].get(lane, {}).get(workload, []))
            medians[lane] = median_metrics(samples)
        aggregate[workload] = {
            "medians": medians,
            "ratios": {
                "candidate_fresh_over_baseline_fresh": ratio_metrics(
                    medians["candidate_fresh"], medians["baseline_fresh"]
                ),
                "candidate_prepared_reuse_over_baseline_fresh": ratio_metrics(
                    medians["candidate_prepared_reuse"], medians["baseline_fresh"]
                ),
                "candidate_prepared_reuse_over_candidate_fresh": ratio_metrics(
                    medians["candidate_prepared_reuse"], medians["candidate_fresh"]
                ),
            },
        }

    order_summary = {}
    for order in ORDERS:
        order_summary[order] = {"arm": parsed_orders[order]["arm"], "lanes": {}}
        for lane, workloads in parsed_orders[order]["lanes"].items():
            order_summary[order]["lanes"][lane] = {}
            for workload in WORKLOADS:
                samples = workloads[workload]
                order_summary[order]["lanes"][lane][workload] = {
                    "samples": samples,
                    "median": median_metrics(samples),
                }

    return {
        "schema": "vibedb.replicated_read_reuse_summary.v1",
        "scope": "Local sql/driver benchmark micromeasurement only; not CRDB, RF3, or end-to-end SQL latency.",
        "workloads": list(WORKLOADS),
        "metrics": list(METRICS),
        "source_files": source_files,
        "orders": order_summary,
        "aggregate": aggregate,
    }


def write_table(summary, path: Path):
    headers = [
        "workload",
        "metric",
        "baseline_fresh",
        "candidate_fresh",
        "candidate_prepared_reuse",
        "candidate_fresh_over_baseline_fresh",
        "candidate_prepared_reuse_over_baseline_fresh",
        "candidate_prepared_reuse_over_candidate_fresh",
    ]
    rows = ["\t".join(headers)]
    for workload in WORKLOADS:
        medians = summary["aggregate"][workload]["medians"]
        ratios = summary["aggregate"][workload]["ratios"]
        for metric in METRICS:
            rows.append(
                "\t".join(
                    [
                        workload,
                        metric,
                        fmt_number(medians["baseline_fresh"][metric]),
                        fmt_number(medians["candidate_fresh"][metric]),
                        fmt_number(medians["candidate_prepared_reuse"][metric]),
                        fmt_number(ratios["candidate_fresh_over_baseline_fresh"][metric]),
                        fmt_number(ratios["candidate_prepared_reuse_over_baseline_fresh"][metric]),
                        fmt_number(ratios["candidate_prepared_reuse_over_candidate_fresh"][metric]),
                    ]
                )
            )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="directory containing before-1.log, after-1.log, after-2.log, before-2.log",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="write summary.json and summary.tsv here (default: input directory)",
    )
    args = parser.parse_args()
    output_dir = args.output_dir or args.input_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = validate_and_build(args.input_dir)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_table(summary, output_dir / "summary.tsv")
    print((output_dir / "summary.tsv").read_text(encoding="utf-8"), end="")


if __name__ == "__main__":
    main()
