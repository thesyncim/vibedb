#!/usr/bin/env python3
"""Summarize the completed distributed integer-group campaign.

This reads only the retained rf3-sqlbench JSON reports.  It checks the report
completion/verification flags and sample counts before writing a wide TSV, a
human-readable Markdown table, and a JSON copy with integer percentile fields.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import median


WORKLOADS = ("point_hit", "point_miss", "range_64", "group_16", "update_existing")
CLIENTS = (1, 8)
ORDERS = ("before-first", "after-first")
ARMS = ("before", "after", "crdb")


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def load_report(path: Path) -> dict:
    try:
        report = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")
    if report.get("status") != "complete":
        fail(f"{path} is not complete")
    if len(report.get("results", [])) != 30:
        fail(f"{path} has {len(report.get('results', []))} results, expected 30")
    for result in report["results"]:
        if not result.get("verified") or result.get("errors") != 0:
            fail(f"{path} contains an unverified or failed trial")
        if len(result.get("samples", [])) != result.get("operations"):
            fail(f"{path} contains a sample-count mismatch")
    return report


def arm_metrics(report: dict, workload: str, clients: int) -> dict:
    trials = [
        result
        for result in report["results"]
        if result["workload"] == workload and result["clients"] == clients
    ]
    repetitions = report["config"]["Repetitions"]
    if len(trials) != repetitions:
        fail(f"{workload}/c{clients}: expected {repetitions} repetitions, got {len(trials)}")
    if [result["repetition"] for result in trials] != list(range(1, repetitions + 1)):
        fail(f"{workload}/c{clients}: repetitions are not ordered")
    return {
        "ops_per_sec": median(result["successful_ops_per_second"] for result in trials),
        "p50_ns": median(result["p50_ns"] for result in trials),
        "p95_ns": median(result["p95_ns"] for result in trials),
        "p99_ns": median(result["p99_ns"] for result in trials),
        "errors": sum(result["errors"] for result in trials),
        "samples": sum(len(result["samples"]) for result in trials),
        "verified_operations": sum(result["operations"] - result["errors"] for result in trials),
    }


def report_path(campaign: Path, order: str, arm: str) -> Path:
    return campaign / "read-3n-4g" / order / arm / "report.json"


def make_rows(campaign: Path) -> tuple[dict, list[dict]]:
    manifest = json.loads((campaign / "manifest.json").read_text())
    reports = {
        order: {
            arm: load_report(report_path(campaign, order, arm))
            for arm in ARMS
        }
        for order in ORDERS
    }
    rows = []
    for order in ORDERS:
        for workload in WORKLOADS:
            for clients in CLIENTS:
                row = {"order": order, "workload": workload, "clients": clients}
                metrics = {
                    arm: arm_metrics(reports[order][arm], workload, clients)
                    for arm in ARMS
                }
                for arm in ARMS:
                    for key, value in metrics[arm].items():
                        row[f"{arm}_{key}"] = value
                row["candidate_over_baseline"] = (
                    metrics["after"]["ops_per_sec"] / metrics["before"]["ops_per_sec"]
                )
                row["candidate_over_crdb"] = (
                    metrics["after"]["ops_per_sec"] / metrics["crdb"]["ops_per_sec"]
                )
                rows.append(row)
    for order in ORDERS:
        total = sum(row["after_verified_operations"] for row in rows if row["order"] == order)
        if total != 48000:
            fail(f"{order}: candidate verified-operation sum is {total}, expected 48000")
    for arm in ARMS:
        total = sum(row[f"{arm}_verified_operations"] for row in rows)
        if total != 96000:
            fail(f"{arm} verified-operation total is {total}, expected 96000")
    return manifest, rows


TSV_COLUMNS = [
    "order", "workload", "clients",
    "before_ops_per_sec", "before_p50_ns", "before_p95_ns", "before_p99_ns", "before_errors", "before_samples", "before_verified_operations",
    "after_ops_per_sec", "after_p50_ns", "after_p95_ns", "after_p99_ns", "after_errors", "after_samples", "after_verified_operations",
    "crdb_ops_per_sec", "crdb_p50_ns", "crdb_p95_ns", "crdb_p99_ns", "crdb_errors", "crdb_samples", "crdb_verified_operations",
    "candidate_over_baseline", "candidate_over_crdb",
]


def format_number(value: float | int) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    return f"{value:.6f}"


def write_tsv(path: Path, rows: list[dict]) -> None:
    with path.open("w") as stream:
        stream.write("\t".join(TSV_COLUMNS) + "\n")
        for row in rows:
            stream.write("\t".join(format_number(row[column]) for column in TSV_COLUMNS) + "\n")


def markdown_metrics(row: dict, arm: str) -> str:
    return (
        f"{row[f'{arm}_ops_per_sec']:,.1f} / "
        f"{row[f'{arm}_p50_ns'] / 1e6:.3f} / "
        f"{row[f'{arm}_p95_ns'] / 1e6:.3f} / "
        f"{row[f'{arm}_p99_ns'] / 1e6:.3f}"
    )


def write_markdown(path: Path, rows: list[dict]) -> None:
    with path.open("w") as stream:
        stream.write("# Full 20-cell summary\n\n")
        stream.write("Each cell is the median of three repetitions. Latencies are milliseconds; errors and samples are summed over the three trials. Throughput ratios use medians within the same execution order.\n\n")
        stream.write("| Order | Workload | Clients | Baseline ops/s / p50 / p95 / p99 | Candidate ops/s / p50 / p95 / p99 | CRDB ops/s / p50 / p95 / p99 | Candidate / baseline | Candidate / CRDB | Errors (B/C/R) | Samples (B/C/R) |\n")
        stream.write("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in rows:
            stream.write(
                f"| {row['order']} | {row['workload']} | {row['clients']} | "
                f"{markdown_metrics(row, 'before')} | {markdown_metrics(row, 'after')} | {markdown_metrics(row, 'crdb')} | "
                f"{row['candidate_over_baseline']:.3f}x | {row['candidate_over_crdb']:.3f}x | "
                f"{row['before_errors']}/{row['after_errors']}/{row['crdb_errors']} | "
                f"{row['before_samples']}/{row['after_samples']}/{row['crdb_samples']} |\n"
            )


def write_grouped(path: Path, rows: list[dict]) -> None:
    grouped = [row for row in rows if row["workload"] == "group_16"]
    with path.open("w") as stream:
        stream.write("| Order | Clients | Baseline ops/s | Candidate ops/s | CRDB ops/s | Candidate / baseline | Candidate / CRDB |\n")
        stream.write("|---|---:|---:|---:|---:|---:|---:|\n")
        for row in grouped:
            stream.write(
                f"| {row['order']} | {row['clients']} | {row['before_ops_per_sec']:,.1f} | "
                f"{row['after_ops_per_sec']:,.1f} | {row['crdb_ops_per_sec']:,.1f} | "
                f"{row['candidate_over_baseline']:.3f}x | {row['candidate_over_crdb']:.3f}x |\n"
            )


def write_grouped_tsv(path: Path, rows: list[dict]) -> None:
    grouped = [row for row in rows if row["workload"] == "group_16"]
    columns = ["order", "clients", "before_ops_per_sec", "after_ops_per_sec", "crdb_ops_per_sec", "candidate_over_baseline", "candidate_over_crdb"]
    with path.open("w") as stream:
        stream.write("\t".join(columns) + "\n")
        for row in grouped:
            stream.write("\t".join(format_number(row[column]) for column in columns) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("campaign", type=Path, help="completed campaign directory")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).parent)
    args = parser.parse_args()
    manifest, rows = make_rows(args.campaign)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_tsv(args.output_dir / "summary.tsv", rows)
    write_markdown(args.output_dir / "summary.md", rows)
    write_grouped(args.output_dir / "grouped-summary.md", rows)
    write_grouped_tsv(args.output_dir / "grouped-summary.tsv", rows)
    config = json.loads((args.campaign / "read-3n-4g" / "before-first" / "after" / "report.json").read_text())["config"]
    output = {
        "campaign": args.campaign.name,
        "manifest_status": manifest.get("status"),
        "refs": manifest.get("refs"),
        "config": config,
        "orders": manifest.get("orders"),
        "rows": rows,
        "verified_operations_by_arm": {
            arm: sum(row[f"{arm}_verified_operations"] for row in rows)
            for arm in ARMS
        },
    }
    (args.output_dir / "summary.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
