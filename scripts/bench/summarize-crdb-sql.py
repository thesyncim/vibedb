#!/usr/bin/env python3
"""Validate raw rf3-sqlbench JSON (or gzip JSON) and print an honest comparison.

Usage: summarize-crdb-sql.py vibedb.json[.gz] cockroachdb.json[.gz]
Missing, failed or unverified trials never contribute to a throughput median.
"""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import median

WORKLOADS = ("point_hit", "point_miss", "range_64", "group_16", "update_existing")
DEFAULT_TABLES = ("rf3_sql_bench",)
MIXED_WORKLOAD = "mixed_read_update"
ALL_WORKLOADS = WORKLOADS + (MIXED_WORKLOAD,)
UINT64_MASK = (1 << 64) - 1
DIAGNOSTIC_COUNTERS = (
    "ready_waves", "ready_durable_waves", "observed_append_barriers", "multi_group_waves", "failed_waves",
    "checkpoint_queue_submissions", "checkpoint_queue_rejected", "checkpoint_queue_wait_ns", "checkpoint_service_ns",
    "native_accepted", "native_rejected", "native_failed", "native_semantic_dispatches",
    "gateway_local_calls", "gateway_remote_calls", "gateway_semantic_sql_calls", "gateway_legacy_calls",
    "gateway_sql_request_encodings", "gateway_sql_request_encoded_bytes", "remote_dials", "remote_reuses",
    "remote_poisoned", "remote_rejected", "remote_handshake_failures",
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def validate_anchor(value):
    require(isinstance(value, str) and re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z", value),
            "measurement anchor is not an RFC3339 UTC string")
    body = value[:-1]
    if "." in body:
        whole, fraction = body.rsplit(".", 1)
        require(fraction.isdigit() and 1 <= len(fraction) <= 9,
                "measurement anchor has invalid fractional precision")
        body = whole
    try:
        # datetime.fromisoformat on older supported Python versions accepts at
        # most microseconds; the discarded fraction has already been checked
        # for RFC3339Nano's one-to-nine digit range above.
        parsed = datetime.fromisoformat(body + "+00:00")
    except ValueError as exc:
        raise ValueError("measurement anchor is not RFC3339") from exc
    require(parsed.tzinfo == timezone.utc, "measurement anchor is not UTC")


def tables_for(config):
    tables = config.get("Tables", list(DEFAULT_TABLES))
    require(isinstance(tables, list) and tables and all(isinstance(t, str) and t for t in tables),
            "invalid table list")
    require(all(len(table) <= 63 and all(char == "_" or "a" <= char <= "z" or
                                        index > 0 and "0" <= char <= "9"
                                        for index, char in enumerate(table))
                for table in tables), "invalid table list")
    require(len(set(tables)) == len(tables), "duplicate tables")
    return tables


def workloads_for(config):
    workloads = config.get("Workloads", list(WORKLOADS))
    require(isinstance(workloads, list) and workloads and all(isinstance(w, str) for w in workloads),
            "invalid workload list")
    require(len(set(workloads)) == len(workloads), "duplicate workloads")
    require(all(w in ALL_WORKLOADS for w in workloads), "unknown workload")
    return workloads


def mix_ordinal(value):
    """Match rf3-sqlbench's uint64 splitmix stream exactly."""
    value = (value + 0x9E3779B97F4A7C15) & UINT64_MASK
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & UINT64_MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & UINT64_MASK
    return (value ^ (value >> 31)) & UINT64_MASK


def group_for(config, groups, ordinal):
    if groups <= 1:
        return 0
    value = mix_ordinal((ordinal + 0x9E3779B97F4A7C15) & UINT64_MASK)
    if config.get("GroupDistribution", "uniform") == "skewed":
        skew = config.get("SkewPercent", 80)
        require(is_integer(skew) and 51 <= skew <= 99, "invalid skew percentage")
        if value % 100 < skew:
            return 0
        return 1 + (value // 100) % (groups - 1)
    require(config.get("GroupDistribution", "uniform") == "uniform",
            "unknown group distribution")
    return value % groups


def comparable_config(config):
    """Fill fields introduced after the original report format for comparison."""
    normalized = dict(config)
    normalized.setdefault("Tables", list(DEFAULT_TABLES))
    normalized.setdefault("Workloads", list(WORKLOADS))
    normalized.setdefault("GroupDistribution", "uniform")
    normalized.setdefault("SkewPercent", 80)
    normalized.setdefault("PhysicalNodes", 0)
    normalized.setdefault("EndpointCount", 1)
    normalized.setdefault("EndpointRouting", "round-robin-per-client")
    normalized.setdefault("DiagnosticMode", "none")
    normalized.setdefault("KeySelection", "stride-7919-v1")
    return normalized


def workload_config(config):
    # PhysicalNodes describes the server topology, not client work. The
    # legacy parent has nine role-node identities; the candidate has three.
    return {key: value for key, value in comparable_config(config).items()
            if key not in {"Engine", "PhysicalNodes", "DiagnosticMode"}}


def validate_diagnostics(path, trial, nodes):
    bracket = trial.get("diagnostics")
    require(isinstance(bracket, dict), "missing diagnostic bracket")
    require(is_integer(bracket.get("before_completed_offset_ns")) and bracket["before_completed_offset_ns"] <= 0,
            "diagnostic snapshot completed after the timer started")
    require(is_integer(bracket.get("after_started_offset_ns")) and bracket["after_started_offset_ns"] >= trial["elapsed_ns"],
            "diagnostic snapshot started before the timer ended")
    phases = []
    for phase in ("before", "after"):
        records = bracket.get(phase) or []
        require(isinstance(records, list) and len(records) <= nodes, "invalid diagnostic snapshot list")
        if trial["verified"]:
            require(len(records) == nodes, "verified trial is missing a physical-node snapshot")
        identities = set()
        pids = set()
        for record in records:
            node = record.get("node_id")
            pid, serial = record.get("pid"), record.get("serial")
            require(isinstance(node, str) and re.fullmatch(r"[0-9a-f]{32}", node) and node not in identities,
                    "invalid or duplicate diagnostic node")
            require(is_integer(pid) and pid > 1 and pid not in pids and is_integer(serial) and 0 < serial <= UINT64_MASK,
                    "invalid or duplicate diagnostic process/serial")
            identities.add(node)
            pids.add(pid)
            snapshot = record.get("snapshot")
            require(isinstance(snapshot, dict) and is_integer(snapshot.get("pid")) and is_integer(snapshot.get("serial")) and
                    snapshot.get("node_id") == node and snapshot.get("pid") == pid and
                    snapshot.get("serial") == serial and snapshot.get("event") == "snapshot", "diagnostic snapshot identity mismatch")
            validate_anchor(snapshot.get("utc"))
            relative = record.get("file")
            require(isinstance(relative, str) and len(Path(relative).parts) == 2 and Path(relative).parts[0] == "diagnostics" and
                    Path(relative).parts[1] not in {".", ".."}, "invalid diagnostic archive path")
            archived = (path.parent / relative).read_bytes()
            require(hashlib.sha256(archived).hexdigest() == record.get("sha256") and json.loads(archived) == snapshot,
                    "archived diagnostic bytes/hash differ from the report")
        phases.append(records)
    before, after = phases
    deltas = bracket.get("deltas") or []
    require(isinstance(deltas, list), "invalid diagnostic deltas")
    if trial["verified"]:
        require(len(deltas) == nodes, "verified trial is missing diagnostic deltas")
    if not deltas:
        return
    require(len(before) == len(after) == len(deltas), "incomplete diagnostic delta bracket")
    for first, last, delta in zip(before, after, deltas):
        require(first["node_id"] == last["node_id"] == delta.get("node_id") and first["pid"] == last["pid"] and
                first["serial"] < last["serial"], "diagnostic process identity or serial changed")
        counters = {}
        for key in DIAGNOSTIC_COUNTERS:
            low, high = first["snapshot"].get(key), last["snapshot"].get(key)
            require(is_integer(low) and is_integer(high) and 0 <= low <= high <= UINT64_MASK,
                    "diagnostic counter missing or decreased")
            counters[key] = high - low
        reported = delta.get("counters")
        require(isinstance(reported, dict) and all(is_integer(value) for value in reported.values()) and
                reported == counters, "diagnostic counter delta mismatch")
        low, high = [record["snapshot"].get("ready_wave_group_histogram") for record in (first, last)]
        require(isinstance(low, list) and isinstance(high, list) and len(low) == len(high) and len(low) >= 2 and
                all(is_integer(a) and is_integer(b) and 0 <= a <= b <= UINT64_MASK for a, b in zip(low, high)),
                "invalid diagnostic histogram")
        reported = delta.get("ready_wave_group_histogram")
        require(isinstance(reported, list) and all(is_integer(value) for value in reported) and
                reported == [b-a for a, b in zip(low, high)], "diagnostic histogram delta mismatch")


def endpoint_count_for(config):
    count = config.get("EndpointCount", 1)
    require(is_integer(count) and count >= 1, "invalid endpoint count")
    routing = config.get("EndpointRouting", "round-robin-per-client")
    require(routing == "round-robin-per-client", "unknown endpoint routing")
    return count


def load(path, engine):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as stream:
        report = json.load(stream)
    config = report["config"]
    schema_version = report.get("schema_version", 1)
    require(is_integer(schema_version) and schema_version in (1, 2), "unknown report schema")
    if schema_version == 2:
        require(config.get("KeySelection") == "splitmix64-independent-with-replacement-v1", "missing or unknown read-key selection")
        require(report.get("status") in {"complete", "failed", "incomplete"}, "invalid report status")
        validate_anchor(report.get("started_utc"))
        if report["status"] != "complete":
            require(isinstance(report.get("verification_error"), str) and report["verification_error"],
                    "incomplete report has no recorded failure")
        else:
            require(not report.get("verification_error") and not report.get("active_trial"),
                    "completed report retains a failure or unfinished trial")
    require(config["Engine"] == engine, "unexpected engine")
    for field, lower, upper in (("Rows", 64, 1000000), ("PayloadBytes", 1, 1000000),
                                ("Operations", 1, 1000000), ("ScanOperations", 1, 100000),
                                ("Warmup", 0, 100000), ("Repetitions", 1, 20)):
        require(is_integer(config.get(field)) and lower <= config[field] <= upper,
                f"invalid {field}")
    tables = tables_for(config)
    workloads = workloads_for(config)
    require(isinstance(config.get("Clients"), str) and config["Clients"], "invalid clients")
    try:
        clients = [int(n) for n in config["Clients"].split(",")]
    except ValueError as exc:
        raise ValueError("invalid clients") from exc
    require(clients and all(1 <= n <= 15 for n in clients) and len(set(clients)) == len(clients),
            "invalid clients")
    distribution = config.get("GroupDistribution", "uniform")
    require(distribution in {"uniform", "skewed"}, "invalid group distribution")
    require(distribution != "skewed" or len(tables) > 1, "skewed distribution needs multiple tables")
    skew = config.get("SkewPercent", 80)
    require(is_integer(skew) and 51 <= skew <= 99, "invalid skew percentage")
    nodes = config.get("PhysicalNodes", 0)
    require(is_integer(nodes) and 0 <= nodes <= 64, "invalid physical-node count")
    diagnostic_mode = config.get("DiagnosticMode", "none")
    require(diagnostic_mode in {"none", "signal-acknowledged-snapshots"}, "invalid diagnostic mode")
    if diagnostic_mode != "none":
        require(engine == "vibedb" and nodes in (3, 6), "invalid diagnostic physical-node count/engine")
    expected = [(w, c, r) for w in workloads for c in clients
                for r in range(1, config["Repetitions"] + 1)]
    results = report["results"]
    require(isinstance(results, list), "invalid result list")
    require(all(is_integer(r.get("clients")) and is_integer(r.get("repetition")) for r in results),
            "invalid trial identity")
    actual = [(r["workload"], r["clients"], r["repetition"]) for r in results]
    require(actual == expected[:len(actual)], "trials are missing, repeated or reordered")
    require(len(actual) == len(expected) or report.get("verification_error"),
            "incomplete run has no recorded failure")
    has_timing_anchors = schema_version == 2 or "Tables" in config or any("measurement_started_utc" in trial or
                             any("start_offset_ns" in sample for sample in trial.get("samples", []))
                             for trial in results)
    has_groups = schema_version == 2 or "Tables" in config or any("group" in sample or "table" in sample
                     for trial in results for sample in trial.get("samples", []))
    endpoint_count = endpoint_count_for(config)
    has_endpoints = schema_version == 2 or any("endpoint" in sample
                        for trial in results for sample in trial.get("samples", []))
    if endpoint_count != 1 and results:
        require(has_endpoints, "multi-endpoint report is missing endpoint identities")
    samples_checked = 0
    for trial in results:
        if has_timing_anchors:
            require("measurement_started_utc" in trial, "missing measurement anchor")
            validate_anchor(trial["measurement_started_utc"])
        count = trial.get("operations")
        require(is_integer(count) and count > 0, "invalid trial operation count")
        expected_count = config["ScanOperations"] if trial["workload"] in ("range_64", "group_16") else config["Operations"]
        require(trial["engine"] == engine and count == expected_count and count > 0,
                "trial operation count/engine mismatch")
        samples = trial["samples"]
        require(len(samples) == count, "sample count mismatch")
        elapsed = trial["elapsed_ns"]
        require(is_integer(elapsed) and elapsed > 0, "invalid elapsed time")
        last_end_by_client = {}
        for ordinal, sample in enumerate(samples):
            require(is_integer(sample.get("ordinal")) and is_integer(sample.get("client"))
                    and is_integer(sample.get("ns")) and sample["ordinal"] == ordinal
                    and sample["client"] == ordinal % trial["clients"]
                    and sample["ns"] > 0, "invalid sample identity or latency")
            if has_timing_anchors:
                require("start_offset_ns" in sample and is_integer(sample["start_offset_ns"]),
                        "missing or invalid sample start offset")
                require(0 <= sample["start_offset_ns"] <= elapsed,
                        "sample start offset is outside the measured window")
                end = sample["start_offset_ns"] + sample["ns"]
                require(end <= elapsed, "sample ends outside the measured window")
                require(sample["start_offset_ns"] >= last_end_by_client.get(sample["client"], 0),
                        "closed-loop operations overlap within one client")
                last_end_by_client[sample["client"]] = end
            if has_groups:
                require("group" in sample and "table" in sample and is_integer(sample["group"]),
                        "missing or invalid sample group identity")
                group = group_for(config, len(tables), ordinal)
                require(0 <= sample["group"] < len(tables) and sample["group"] == group and
                        sample["table"] == tables[group], "sample group identity mismatch")
            if has_endpoints:
                require("endpoint" in sample and is_integer(sample["endpoint"]),
                        "missing or invalid sample endpoint identity")
                require(0 <= sample["endpoint"] < endpoint_count and
                        sample["endpoint"] == sample["client"] % endpoint_count,
                        "sample endpoint identity mismatch")
            if "error" in sample:
                require(isinstance(sample["error"], str), "invalid sample error")
            if schema_version == 2 or "operation" in sample:
                operation = trial["workload"]
                if operation == MIXED_WORKLOAD:
                    operation = "point_hit" if mix_ordinal(ordinal + 0xD1B54A32D192ED03) & 1 == 0 else "update_existing"
                require(sample.get("operation") == operation, "sample operation identity mismatch")
        errors = sum(bool(s.get("error")) for s in samples)
        require(is_integer(trial.get("errors")) and errors == trial["errors"],
                "error count mismatch")
        require(isinstance(trial.get("verified"), bool), "invalid verification flag")
        require(not trial["verified"] or errors == 0, "failed trial marked verified")
        if schema_version == 2 and report["status"] == "complete":
            require(trial["verified"] and errors == 0, "completed report contains an unsuccessful trial")
        if diagnostic_mode != "none":
            validate_diagnostics(path, trial, nodes)
        else:
            require("diagnostics" not in trial, "unexpected diagnostic data")
        throughput = (count-errors) * 1e9 / elapsed
        reported_throughput = trial.get("successful_ops_per_second")
        require(isinstance(reported_throughput, (int, float)) and
                not isinstance(reported_throughput, bool) and math.isfinite(reported_throughput) and
                math.isclose(throughput, reported_throughput, rel_tol=1e-12),
                "throughput does not match samples and elapsed time")
        latencies = sorted(s["ns"] for s in samples)
        for percent in (50, 95, 99):
            percentile = trial.get(f"p{percent}_ns")
            require(is_integer(percentile) and percentile == latencies[math.ceil(count * percent / 100)-1],
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
    require(workload_config(vibe["config"]) == workload_config(crdb["config"]), "workload configurations differ")
    print(f"Validated {vc + cc:,} raw latency samples; medians require all repetitions to pass.\n")
    print("| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |")
    print("|---|---:|---:|---:|---:|")
    for workload in workloads_for(vibe["config"]):
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
