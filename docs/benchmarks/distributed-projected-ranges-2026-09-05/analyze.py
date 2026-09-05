#!/usr/bin/env python3
"""Summarize the completed distributed projected-range campaign.

This reads only the retained rf3-sqlbench JSON reports.  It checks the report
completion/verification flags and sample counts before writing a wide TSV, a
human-readable Markdown table, and a JSON copy with integer percentile fields.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import median


WORKLOADS = (
    "point_hit",
    "point_miss",
    "range_32",
    "range_64",
    "range_256",
    "group_16",
    "update_existing",
)
CLIENTS = (1, 8)
ORDERS = ("before-first", "after-first")
ARMS = ("before", "after", "crdb")
EXPECTED_RESULTS_PER_REPORT = len(WORKLOADS) * len(CLIENTS) * 3
EXPECTED_OPERATIONS_PER_REPORT = 60000
EXPECTED_COMMON_CONFIG = {
    "KeySelection": "splitmix64-independent-with-replacement-v1",
    "SeedBatch": 64,
    "VerifyEveryTrial": True,
    "Rows": 8192,
    "PayloadBytes": 256,
    "Operations": 2000,
    "ScanOperations": 1000,
    "Warmup": 500,
    "Repetitions": 3,
    "Clients": "1,8",
    "Protocol": "extended unnamed parse/bind/execute; text parameters/results; one autocommit statement per operation",
    "Tables": [
        "rf3_sql_group",
        "rf3_sql_group_01",
        "rf3_sql_group_02",
        "rf3_sql_group_03",
    ],
    "Workloads": list(WORKLOADS),
    "GroupDistribution": "uniform",
    "SkewPercent": 80,
    "PhysicalNodes": 3,
    "EndpointCount": 1,
    "EndpointRouting": "round-robin-per-client",
}


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def load_report(path: Path, arm: str) -> dict:
    try:
        report = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")
    if report.get("status") != "complete":
        fail(f"{path} is not complete")
    if len(report.get("results", [])) != EXPECTED_RESULTS_PER_REPORT:
        fail(
            f"{path} has {len(report.get('results', []))} results, "
            f"expected {EXPECTED_RESULTS_PER_REPORT}"
        )
    config = report.get("config", {})
    for key, expected in EXPECTED_COMMON_CONFIG.items():
        if config.get(key) != expected:
            fail(f"{path} config {key}={config.get(key)!r}, expected {expected!r}")
    expected_engine = "cockroachdb" if arm == "crdb" else "vibedb"
    expected_diagnostic_mode = "none" if arm == "crdb" else "signal-acknowledged-snapshots"
    if config.get("Engine") != expected_engine:
        fail(f"{path} config Engine={config.get('Engine')!r}, expected {expected_engine!r}")
    if config.get("DiagnosticMode") != expected_diagnostic_mode:
        fail(
            f"{path} config DiagnosticMode={config.get('DiagnosticMode')!r}, "
            f"expected {expected_diagnostic_mode!r}"
        )
    for result in report["results"]:
        if not result.get("verified") or result.get("errors") != 0:
            fail(f"{path} contains an unverified or failed trial")
        if len(result.get("samples", [])) != result.get("operations"):
            fail(f"{path} contains a sample-count mismatch")
        if result.get("engine") != expected_engine:
            fail(f"{path} contains a result for the wrong engine")
        clients = result["clients"]
        if clients not in CLIENTS:
            fail(f"{path} contains an unsupported client count {clients}")
        seen = set()
        for sample in result["samples"]:
            client = sample.get("client")
            ordinal = sample.get("ordinal")
            group = sample.get("group")
            if not isinstance(client, int) or not 0 <= client < clients:
                fail(f"{path} contains an out-of-range sample client")
            if not isinstance(ordinal, int) or not 0 <= ordinal < result["operations"]:
                fail(f"{path} contains an out-of-range sample ordinal")
            pair = (client, ordinal)
            if pair in seen:
                fail(f"{path} contains duplicate sample identity {pair}")
            seen.add(pair)
            if not isinstance(group, int) or not 0 <= group < len(config["Tables"]):
                fail(f"{path} contains an out-of-range sample group")
            if sample.get("table") != config["Tables"][group]:
                fail(f"{path} contains a sample/table mapping mismatch")
            if sample.get("endpoint") != 0:
                fail(f"{path} contains a nonzero endpoint sample")
            if sample.get("operation") != result["workload"]:
                fail(f"{path} contains a sample for the wrong workload")
    operations = sum(result["operations"] for result in report["results"])
    samples = sum(len(result["samples"]) for result in report["results"])
    if operations != EXPECTED_OPERATIONS_PER_REPORT or samples != EXPECTED_OPERATIONS_PER_REPORT:
        fail(
            f"{path} has {operations} operations/{samples} samples, "
            f"expected {EXPECTED_OPERATIONS_PER_REPORT}/{EXPECTED_OPERATIONS_PER_REPORT}"
        )
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
        "operations": sum(result["operations"] for result in trials),
        "samples": sum(len(result["samples"]) for result in trials),
        "verified_operations": sum(result["operations"] - result["errors"] for result in trials),
    }


def report_path(campaign: Path, order: str, arm: str) -> Path:
    return campaign / "read-3n-4g" / order / arm / "report.json"


def make_rows(campaign: Path) -> tuple[dict, list[dict]]:
    manifest = json.loads((campaign / "manifest.json").read_text())
    reports = {
        order: {
            arm: load_report(report_path(campaign, order, arm), arm)
            for arm in ARMS
        }
        for order in ORDERS
    }
    reference_config = None
    for order in ORDERS:
        for arm in ARMS:
            config = reports[order][arm]["config"]
            common_config = {
                key: value
                for key, value in config.items()
                if key not in {"Engine", "DiagnosticMode"}
            }
            if reference_config is None:
                reference_config = common_config
            elif common_config != reference_config:
                fail(f"{order}/{arm}: common benchmark config differs from the other reports")
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
        if total != EXPECTED_OPERATIONS_PER_REPORT:
            fail(
                f"{order}: candidate verified-operation sum is {total}, "
                f"expected {EXPECTED_OPERATIONS_PER_REPORT}"
            )
    for arm in ARMS:
        total = sum(row[f"{arm}_verified_operations"] for row in rows)
        if total != EXPECTED_OPERATIONS_PER_REPORT * len(ORDERS):
            fail(
                f"{arm} verified-operation total is {total}, "
                f"expected {EXPECTED_OPERATIONS_PER_REPORT * len(ORDERS)}"
            )
    return manifest, rows


TSV_COLUMNS = [
    "order", "workload", "clients",
    "before_ops_per_sec", "before_p50_ns", "before_p95_ns", "before_p99_ns", "before_errors", "before_operations", "before_samples", "before_verified_operations",
    "after_ops_per_sec", "after_p50_ns", "after_p95_ns", "after_p99_ns", "after_errors", "after_operations", "after_samples", "after_verified_operations",
    "crdb_ops_per_sec", "crdb_p50_ns", "crdb_p95_ns", "crdb_p99_ns", "crdb_errors", "crdb_operations", "crdb_samples", "crdb_verified_operations",
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
        stream.write("# Full 28-cell summary\n\n")
        stream.write("Each cell is the median of three repetitions. Latencies are milliseconds; errors, operations and samples are summed over the three trials. Throughput ratios use medians within the same execution order.\n\n")
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


def write_range_summary(path: Path, rows: list[dict]) -> None:
    workloads = ("range_32", "range_64", "range_256")
    with path.open("w") as stream:
        stream.write("| Order | Workload | Clients | Baseline ops/s | Candidate ops/s | CRDB ops/s | Candidate / baseline | Candidate / CRDB |\n")
        stream.write("|---|---|---:|---:|---:|---:|---:|---:|\n")
        for row in rows:
            if row["workload"] not in workloads:
                continue
            stream.write(
                f"| {row['order']} | {row['workload']} | {row['clients']} | "
                f"{row['before_ops_per_sec']:,.1f} | {row['after_ops_per_sec']:,.1f} | "
                f"{row['crdb_ops_per_sec']:,.1f} | {row['candidate_over_baseline']:.3f}x | "
                f"{row['candidate_over_crdb']:.3f}x |\n"
            )


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
    write_range_summary(args.output_dir / "range-summary.md", rows)
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
