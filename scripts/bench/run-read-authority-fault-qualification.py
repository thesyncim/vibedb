#!/usr/bin/env python3
"""Run a bounded RF3 read-authority fault qualification.

This command is a correctness diagnostic for one Linux, single-host Docker
fixture.  It retains strict SQL/oracle evidence while exercising a process
pause beyond the configured grant and a same-root process restart.  The
``--no-fault`` mode preserves the same setup and workload while collecting
bounded per-group Raft cuts without injecting a pause or restart.  It never
produces a throughput comparison or a CRDB result.

The qualification requires the explicit ``--laboratory-read-authority`` opt-in
and builds every candidate executable with the labelled laboratory build tag.
"""

import argparse
import calendar
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = ROOT / "scripts/bench/run-fused-node-comparison.py"
VALIDATOR_PATH = ROOT / "scripts/bench/summarize-crdb-sql.py"
RUNTIME = (
    "golang:1.27-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b"
)
GOCACHE = Path(os.environ.get("VIBEDB_GO_CACHE", "/Users/thesyncim/Library/Caches/go-build"))
LAB_BUILD_TAG = "vibedb_rf3_read_authority_lab"
QUARANTINE_SECONDS = 6.11
GRANT_SECONDS = 5.0
REQUIRED_AUTHORITY_COUNTERS = (
    "authority_read_hits",
    "authority_read_index_fallbacks",
    "authority_read_validation_retries",
    "authority_read_validation_failures",
    "authority_round_attempts",
    "read_authority_rounds_started",
    "read_authority_requests_created",
    "read_authority_grants_accepted",
)


def load_fixture():
    spec = importlib.util.spec_from_file_location("vibedb_fault_fixture", FIXTURE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load fixture {FIXTURE_PATH}")
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    return fixture


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def source_snapshot(fixture, repo, destination, label):
    """Retain all changed bytes, including untracked files, for provenance."""
    info = fixture.git_info(repo)
    summary = {key: value for key, value in info.items() if key != "patch"}
    fixture.write_git_evidence(destination, label, dict(info))
    # `fixture.text_output` strips leading whitespace, which is significant in
    # porcelain-v1 status (` M path` versus `M  path`).  Keep the raw bytes so
    # the retained path and status are exact even when the first entry is
    # modified rather than untracked.
    status_result = fixture.run([
        "git", "-C", str(repo), "status", "--porcelain=v1", "--untracked-files=all",
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    status_lines = status_result.stdout.decode(errors="replace").splitlines()
    statuses = {line[3:]: line[:2] for line in status_lines if len(line) >= 4}
    changed = sorted(statuses)
    retained = destination / f"source-{label}-files"
    records = []
    for relative in changed:
        path = (repo / relative).resolve()
        record = {"path": relative, "status": statuses[relative]}
        try:
            path.relative_to(repo.resolve())
        except ValueError:
            record["error"] = "changed path resolves outside repository"
            records.append(record)
            continue
        if not path.is_file():
            record["error"] = "changed path is not a regular file"
            records.append(record)
            continue
        content = path.read_bytes()
        record["size"] = len(content)
        record["sha256"] = hashlib.sha256(content).hexdigest()
        target = retained / relative
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        target.write_bytes(content)
        records.append(record)
    summary["files"] = records
    summary["file_sha256"] = {
        record["path"]: record["sha256"]
        for record in records if "sha256" in record
    }
    return summary


def source_identity(snapshot):
    return {
        "revision": snapshot.get("revision"),
        "status": snapshot.get("status"),
        "patch_sha256": snapshot.get("patch_sha256"),
        "file_sha256": snapshot.get("file_sha256", {}),
    }


def build_candidate(fixture, repo, destination, arch):
    binaries = destination / "bin"
    binaries.mkdir(mode=0o700)
    env = dict(os.environ, GOOS="linux", GOARCH=arch, CGO_ENABLED="0",
               GOEXPERIMENT="simd", GOFLAGS="-tags=" + LAB_BUILD_TAG,
               GOCACHE=str(GOCACHE))
    packages = (
        ("candidate-vibedb", repo, "./cmd/vibedb"),
        ("candidate-vibedb-shard", repo, "./cmd/vibedb-shard"),
        ("candidate-vibedb-gateway", repo, "./cmd/vibedb-gateway"),
        ("rf3-diagnostic", repo / "bench/competitive", "./cmd/rf3-diagnostic"),
        ("rf3-sqlbench", repo / "integration/pgclient", "./cmd/rf3-sqlbench"),
    )
    for name, source, package in packages:
        fixture.build_binary(source, package, binaries / name, env)
    metadata = {}
    for executable in sorted(binaries.iterdir()):
        value = fixture.text_output(["go", "version", "-m", executable])
        for setting in ("GOEXPERIMENT=simd", "GOOS=linux", "GOARCH=" + arch):
            if "\tbuild\t" + setting not in value:
                raise fixture.RunnerError(
                    f"{executable.name} is missing required build setting {setting}")
        if "\tbuild\t-tags=" + LAB_BUILD_TAG not in value:
            raise fixture.RunnerError(
                f"{executable.name} is missing required laboratory build tag {LAB_BUILD_TAG}")
        metadata[executable.name] = value
    return binaries, metadata, env


def json_sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_snapshot(path, target):
    value = json.loads(path.read_text())
    if value.get("event") != "snapshot" or value.get("pid") != target["pid"] or value.get("node_id") != target["node_id"]:
        raise RuntimeError(f"diagnostic identity mismatch for node {target['node_id']}")
    if not isinstance(value.get("serial"), int) or value["serial"] < 1:
        raise RuntimeError("diagnostic snapshot has no positive serial")
    return value


def counter(value, name):
    result = value.get(name)
    if not isinstance(result, int):
        raise RuntimeError(f"diagnostic snapshot is missing integer {name}")
    return result


def diagnostics_summary(path):
    records = []
    for candidate in sorted(path.rglob("*.json")) if path.exists() else ():
        try:
            value = json.loads(candidate.read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(value, dict) or "read_authority_rounds_started" not in value:
            continue
        records.append({
            "path": str(candidate.relative_to(path)),
            "node_id": value.get("node_id"),
            "pid": value.get("pid"),
            "authority_read_hits": value.get("authority_read_hits"),
            "authority_read_index_fallbacks": value.get("authority_read_index_fallbacks"),
            "read_authority_rounds_started": value.get("read_authority_rounds_started"),
            "read_authority_requests_created": value.get("read_authority_requests_created"),
            "read_authority_grants_accepted": value.get("read_authority_grants_accepted"),
        })
    return records


def per_group_diagnostic_summary(path, expected_groups=7, expected_members=3):
    """Summarize bounded JSONL cuts without turning them into a performance claim."""
    records = []
    parse_errors = []
    if not path.is_file():
        return {"records": 0, "parse_errors": ["missing output"], "complete_shape": False}
    try:
        lines = path.read_text().splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        return {"records": 0, "parse_errors": [str(exc)], "complete_shape": False}
    for line_number, line in enumerate(lines, 1):
        try:
            value = json.loads(line)
            groups = value.get("groups")
            if value.get("schema") != "vibedb.rf3-diagnostic/1" or not isinstance(groups, list):
                raise ValueError("invalid cycle header")
            if len(groups) != expected_groups or any(
                    not isinstance(group.get("members"), list) or
                    len(group["members"]) != expected_members
                    for group in groups):
                raise ValueError("incomplete group/member cut")
            records.append(value)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            parse_errors.append(f"line {line_number}: {exc}")
    elapsed = [value.get("elapsed_ns") for value in records
               if isinstance(value.get("elapsed_ns"), int)]
    status_cuts = sum(
        1 for value in records for group in value["groups"]
        for member in group["members"] if isinstance(member.get("status"), dict))
    metrics_cuts = sum(
        1 for value in records for group in value["groups"]
        for member in group["members"] if isinstance(member.get("metrics"), dict))
    valid_cuts = sum(
        1 for value in records for group in value["groups"]
        for member in group["members"]
        if isinstance(member.get("status"), dict) and isinstance(member.get("metrics"), dict))
    return {
        "records": len(records),
        "first_sequence": records[0].get("sequence") if records else None,
        "last_sequence": records[-1].get("sequence") if records else None,
        "max_cycle_elapsed_ns": max(elapsed, default=0),
        "sampling_errors": sum(value.get("sampling_errors", 0) for value in records
                                if isinstance(value.get("sampling_errors"), int)),
        "status_cuts": status_cuts,
        "metrics_cuts": metrics_cuts,
        "valid_cuts": valid_cuts,
        "has_valid_cuts": valid_cuts > 0,
        "preflight_ready_records": sum(
            1 for value in records if value.get("preflight_ready") is True and
            value.get("valid_cuts") == expected_groups * expected_members),
        "parse_errors": parse_errors,
        "complete_shape": bool(records) and not parse_errors and any(
            value.get("preflight_ready") is True and
            value.get("valid_cuts") == expected_groups * expected_members
            for value in records),
    }


def diagnostic_output_path(run_dir):
    """Locate the copied diagnostic stream, including a partial-run raw copy."""
    direct = run_dir / "per-group-snapshots.jsonl"
    if direct.is_file():
        return direct
    return run_dir / "raw" / "per-group-snapshots.jsonl"


def write_group_timeline(path, output, table):
    """Retain one table group's member cuts without cross-group aggregation."""
    prefix = f"table-{table}-"
    rows = []
    parse_errors = []
    group_ids = set()
    try:
        lines = path.read_text().splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        lines = []
        parse_errors.append(f"read output: {exc}")
    for line_number, line in enumerate(lines, 1):
        try:
            value = json.loads(line)
            if value.get("schema") != "vibedb.rf3-diagnostic/1":
                raise ValueError("invalid cycle schema")
            groups = value.get("groups")
            if not isinstance(groups, list):
                raise ValueError("cycle has no groups")
            matches = [group for group in groups
                       if isinstance(group, dict) and
                       isinstance(group.get("distribution"), str) and
                       group["distribution"].startswith(prefix)]
            if len(matches) != 1:
                raise ValueError(
                    f"expected one group distribution with prefix {prefix!r}, found {len(matches)}")
            group = matches[0]
            members = group.get("members")
            if not isinstance(members, list):
                raise ValueError("selected group has no member cuts")
            group_id = group.get("group_id")
            if isinstance(group_id, str):
                group_ids.add(group_id)
            node_process_metrics = {}
            for node in value.get("node_metrics", []):
                if not isinstance(node, dict) or not isinstance(node.get("node_id"), str):
                    continue
                node_process_metrics[node["node_id"]] = {
                    "scope": node.get("scope"),
                    "source": node.get("source"),
                    "utc": node.get("utc"),
                    "serial": node.get("serial"),
                    "pid": node.get("pid"),
                    "authority_available": node.get("authority_available"),
                    "authority_error": node.get("authority_error"),
                    "metrics": node.get("metrics"),
                    "error": node.get("error"),
                    "elapsed_ns": node.get("elapsed_ns"),
                }
            timeline_members = []
            for member in members:
                if not isinstance(member, dict):
                    raise ValueError("selected group has a non-object member cut")
                status = member.get("status") if isinstance(member.get("status"), dict) else {}
                metrics = member.get("metrics") if isinstance(member.get("metrics"), dict) else None
                progress = member.get("progress") if isinstance(member.get("progress"), dict) else None
                authority = node_process_metrics.get(member.get("node_id"))
                timeline_members.append({
                    "member_id": member.get("member_id"),
                    "node_id": member.get("node_id"),
                    "term": status.get("term"),
                    "leader_id": status.get("leader_id"),
                    "commit": status.get("commit"),
                    "applied": status.get("applied"),
                    "checkpoint_applied": status.get("checkpoint_applied"),
                    "raft_state": status.get("raft_state"),
                    "raft_state_name": status.get("raft_state_name"),
                    "state_applied": status.get("state_applied"),
                    "progress": progress,
                    "metrics": metrics,
                    "ready_metrics": None if metrics is None else {
                        "ready_persisted": metrics.get("ready_persisted"),
                        "applied_entries": metrics.get("applied_entries"),
                        "commit_advancements": metrics.get("commit_advancements"),
                        "committed_entries": metrics.get("committed_entries"),
                    },
                    # These counters are process-wide on the fixed metrics
                    # endpoint. The explicit scope keeps them from being read
                    # as group-local authority counters.
                    "authority_metrics": authority,
                    "observe_error": member.get("observe_error"),
                    "metrics_error": member.get("metrics_error"),
                    "error": member.get("error"),
                })
            rows.append({
                "sequence": value.get("sequence"),
                "utc": value.get("utc"),
                "elapsed_ns": value.get("elapsed_ns"),
                "expected_cuts": value.get("expected_cuts"),
                "valid_cuts": value.get("valid_cuts"),
                "preflight_ready": value.get("preflight_ready"),
                "preflight_reason": value.get("preflight_reason"),
                "sampling_errors": value.get("sampling_errors"),
                "latch": value.get("latch"),
                "group_id": group_id,
                "distribution": group.get("distribution"),
                "shard": group.get("shard"),
                "members": timeline_members,
                "node_process_metrics": node_process_metrics,
            })
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            parse_errors.append(f"line {line_number}: {exc}")
    payload = {
        "schema": "vibedb.rf3-diagnostic-group-timeline/1",
        "table": table,
        "distribution_prefix": prefix,
        "group_ids": sorted(group_ids),
        "terms_scope": "term, leader, commit, and applied are retained per member of this one group; no cross-group term aggregation",
        "authority_metrics_scope": "node_process",
        "records": rows,
        "parse_errors": parse_errors,
        "complete": bool(rows) and not parse_errors,
    }
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n")
    return {
        "schema": payload["schema"],
        "table": table,
        "group_ids": payload["group_ids"],
        "records": len(rows),
        "parse_errors": parse_errors,
        "complete": payload["complete"],
        "path": output.name,
    }


_RFC3339_NANO_RE = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2})T"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})$")


def _parse_utc(value, name):
    """Parse an RFC3339/RFC3339Nano instant to exact epoch nanoseconds.

    ``datetime.fromisoformat`` on the supported Python versions accepts only
    microseconds, while Go's ``time.RFC3339Nano`` emits one to nine fractional
    digits.  Keep the comparison value as an integer so a one-nanosecond
    ordering cannot be rounded away or changed by floating-point conversion.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} is missing UTC")
    match = _RFC3339_NANO_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"{name} has invalid UTC")
    try:
        year, month, day = (int(part) for part in match.group("date").split("-"))
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        second = int(match.group("second"))
        # Validate the civil time without converting through a platform
        # timestamp, which may lose precision or have a limited year range.
        datetime(year, month, day, hour, minute, second)
        offset = match.group("offset")
        if offset == "Z":
            offset_seconds = 0
        else:
            offset_hour = int(offset[1:3])
            offset_minute = int(offset[4:6])
            if offset_hour > 23 or offset_minute > 59:
                raise ValueError("timezone offset is out of range")
            offset_seconds = offset_hour * 60 * 60 + offset_minute * 60
            if offset[0] == "-":
                offset_seconds = -offset_seconds
        epoch_seconds = calendar.timegm(
            (year, month, day, hour, minute, second)) - offset_seconds
        fraction = match.group("fraction") or ""
        fraction_nanoseconds = int(fraction.ljust(9, "0")) if fraction else 0
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} has invalid UTC") from exc
    return epoch_seconds * 1_000_000_000 + fraction_nanoseconds


def _positive_int(value, name, minimum=1):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _is_secondary_result_error(error):
    value = str(error)
    return value.startswith((
        "post-CONT diagnostic latch",
        "per-group diagnostic output",
        "retaining stopped fixture evidence:",
        "fixture container cleanup failed",
        "fixture volume cleanup failed:",
    ))


def validate_post_cont_latch(path, expected_nodes=3, post_pause_diagnostics=None):
    """Validate the immutable post-CONT cut and every required owner snapshot."""
    _positive_int(expected_nodes, "expected node count")
    try:
        value = json.loads(path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read post-CONT latch: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError("post-CONT latch is not an object")
    if value.get("schema") != "vibedb.rf3-diagnostic-latch/1":
        raise ValueError("post-CONT latch has an invalid schema")
    if value.get("event") != "post-cont":
        raise ValueError("post-CONT latch has an invalid event")
    cycle = value.get("cycle")
    if not isinstance(cycle, dict):
        raise ValueError("post-CONT latch has no cycle")
    latch = cycle.get("latch")
    if not isinstance(latch, dict) or latch.get("complete") is not True:
        raise ValueError("post-CONT latch cycle is incomplete")
    _positive_int(value.get("sequence"), "latch sequence")
    _positive_int(cycle.get("sequence"), "cycle sequence")
    requested = _parse_utc(value.get("requested_utc"), "latch requested")
    armed = _parse_utc(value.get("armed_utc"), "latch armed")
    captured = _parse_utc(value.get("captured_utc"), "latch captured")
    cycle_utc = _parse_utc(cycle.get("utc"), "latch cycle")
    if armed < requested:
        raise ValueError("post-CONT latch armed before controller handoff")
    latch_requested = _parse_utc(latch.get("requested_utc"), "cycle latch requested")
    latch_armed = _parse_utc(latch.get("armed_utc"), "cycle latch armed")
    latch_captured = _parse_utc(latch.get("captured_utc"), "cycle latch captured")
    if latch_requested != requested or latch_armed != armed:
        raise ValueError("post-CONT cycle latch timestamps do not match artifact")
    if latch_captured < cycle_utc:
        raise ValueError("post-CONT cycle latch capture predates its cycle")
    if cycle_utc < requested:
        raise ValueError("post-CONT latch cycle predates controller handoff")
    if cycle_utc < armed:
        raise ValueError("post-CONT latch cycle predates sidecar arm handoff")
    if captured < cycle_utc:
        raise ValueError("post-CONT latch capture predates its cycle")
    if value.get("sequence") != cycle.get("sequence"):
        raise ValueError("post-CONT latch sequence does not match its cycle")
    if latch.get("sequence") != cycle.get("sequence"):
        raise ValueError("post-CONT latch sequence does not match its cycle")
    if cycle.get("preflight_ready") is not True:
        raise ValueError("post-CONT latch cycle did not pass group preflight")
    expected_cuts = _positive_int(cycle.get("expected_cuts"), "latch expected cuts")
    if cycle.get("valid_cuts") != expected_cuts:
        raise ValueError("post-CONT latch cycle has incomplete group cuts")
    if not isinstance(post_pause_diagnostics, dict) or len(post_pause_diagnostics) != expected_nodes:
        raise ValueError(
            "post-CONT latch requires retained acknowledged diagnostics for every node")
    acknowledged = {}
    for key, snapshot in post_pause_diagnostics.items():
        if not isinstance(key, str) or not isinstance(snapshot, dict):
            raise ValueError("retained post-CONT diagnostics have invalid node identity")
        node_id = snapshot.get("node_id")
        if node_id != key:
            raise ValueError(f"retained post-CONT diagnostic identity mismatch for {key}")
        serial = _positive_int(snapshot.get("serial"), f"acknowledged node {node_id} serial")
        pid = _positive_int(snapshot.get("pid"), f"acknowledged node {node_id} pid", minimum=2)
        snapshot_utc = _parse_utc(snapshot.get("utc"), f"acknowledged node {node_id}")
        if snapshot_utc < requested:
            raise ValueError(
                f"post-CONT acknowledged node {node_id} predates controller handoff")
        acknowledged[node_id] = {
            "serial": serial, "pid": pid, "utc": snapshot_utc,
            "utc_text": snapshot["utc"],
        }
    nodes = cycle.get("node_metrics")
    if not isinstance(nodes, list) or len(nodes) != expected_nodes:
        raise ValueError(
            f"post-CONT latch requires {expected_nodes} node authority snapshots")
    authority_nodes = []
    seen = set()
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise ValueError(f"post-CONT node snapshot {index} is not an object")
        node_id = node.get("node_id")
        if not isinstance(node_id, str) or not node_id or node_id in seen:
            raise ValueError(f"post-CONT node snapshot {index} has duplicate identity")
        seen.add(node_id)
        acknowledged_snapshot = acknowledged.get(node_id)
        if acknowledged_snapshot is None:
            raise ValueError(f"post-CONT latch is missing expected node {node_id}")
        if node.get("source") != "rf3-diagnostics-file":
            raise ValueError(f"post-CONT node {node_id} lacks diagnostic-file provenance")
        if node.get("authority_available") is not True:
            raise ValueError(f"post-CONT node {node_id} lacks authority availability")
        node_utc_value = node.get("utc")
        node_utc = _parse_utc(node_utc_value, f"post-CONT node {node_id}")
        serial = _positive_int(node.get("serial"), f"post-CONT node {node_id} serial")
        pid = _positive_int(node.get("pid"), f"post-CONT node {node_id} pid", minimum=2)
        if pid != acknowledged_snapshot["pid"]:
            raise ValueError(f"post-CONT node {node_id} PID differs from acknowledged process")
        if serial < acknowledged_snapshot["serial"]:
            raise ValueError(f"post-CONT node {node_id} serial regressed")
        if node_utc < acknowledged_snapshot["utc"]:
            raise ValueError(f"post-CONT node {node_id} timestamp regressed")
        metrics = node.get("metrics")
        if not isinstance(metrics, dict):
            raise ValueError(f"post-CONT node {node_id} lacks authority metrics")
        for counter_name in REQUIRED_AUTHORITY_COUNTERS:
            counter_value = metrics.get(counter_name)
            if isinstance(counter_value, bool) or not isinstance(counter_value, int):
                raise ValueError(
                    f"post-CONT node {node_id} lacks integer {counter_name}")
        authority_nodes.append({
            "node_id": node_id,
            "source": node["source"],
            "utc": node_utc_value,
            "serial": serial,
            "pid": pid,
            "acknowledged_serial": acknowledged_snapshot["serial"],
            "acknowledged_utc": acknowledged_snapshot["utc_text"],
        })
    return {
        "schema": value["schema"],
        "event": value["event"],
        "sequence": value["sequence"],
        "captured_utc": value["captured_utc"],
        "cycle_utc": cycle["utc"],
        "authority_nodes": authority_nodes,
        "authority_snapshot_count": len(authority_nodes),
        "complete": True,
    }


def primary_result_failure(result, run_dir):
    """Retain the client failure when later qualification bookkeeping fails."""
    if not isinstance(result, dict):
        return None
    client_exit_code = result.get("client_exit_code")
    client_failed = (
        isinstance(client_exit_code, int) and
        not isinstance(client_exit_code, bool) and client_exit_code != 0)
    validation = result.get("validation")
    validation_failed = isinstance(validation, dict) and validation.get("complete") is False
    if not client_failed and not validation_failed:
        return None
    errors = result.get("errors")
    errors = [str(error) for error in errors] if isinstance(errors, list) else []
    errors = [error for error in errors if not _is_secondary_result_error(error)]
    if not errors:
        errors = ["client exited nonzero" if client_failed else
                  "client report is incomplete or failed"]
    failure = {
        "status": result.get("status"),
        "client_exit_code": client_exit_code,
        "validation_failed": validation_failed,
        "errors": errors,
        "client_log": "candidate/client.log",
    }
    client_log = run_dir / "client.log"
    if client_log.is_file():
        try:
            lines = [line.strip() for line in client_log.read_text(errors="replace").splitlines()
                     if line.strip()]
            if lines:
                failure["client_error_tail"] = lines[-1]
        except OSError as exc:
            failure["client_log_error"] = str(exc)
    return failure


def fault_qualification_complete(no_fault, pause, restart, latch_complete=True):
    """Evaluate the fault contract without treating an absent restart as a dict."""
    pause = pause if isinstance(pause, dict) else {}
    restart = restart if isinstance(restart, dict) else {}
    return no_fault or (
        pause.get("status") == "verified-signals" and
        restart.get("status") == "verified" and
        latch_complete is True)


def authority_proof(records):
    latest = {}
    for record in records:
        node = record.get("node_id") or record.get("path")
        hits = record.get("authority_read_hits")
        rounds = record.get("read_authority_rounds_started")
        if isinstance(hits, int) and isinstance(rounds, int):
            previous = latest.get(node, (0, 0))
            latest[node] = (max(previous[0], hits), max(previous[1], rounds))
    hits = sum(value[0] for value in latest.values())
    rounds = sum(value[1] for value in latest.values())
    return {
        "nodes": len(latest),
        "lifetime_authority_read_hits": hits,
        "lifetime_read_authority_rounds_started": rounds,
        "rounds_amortized": hits > 0 and 0 < rounds < hits,
        "several_rounds": rounds >= 3,
        "basis": "per-node maximum acknowledged diagnostic counters; setup counters may precede workload brackets",
    }


def remote_snapshot(fixture, container, target, destination, label, prior_serial=None):
    """Signal and archive one acknowledged diagnostic snapshot."""
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if prior_serial is None:
        prior_serial = 0
    fixture.run(["docker", "exec", container, "kill", "-USR1", str(target["pid"])])
    deadline = time.monotonic() + 10
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    last = None
    while time.monotonic() < deadline:
        copied = fixture.run([
            "docker", "cp", container + ":" + target["path"], temporary,
        ], check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if copied.returncode == 0:
            try:
                value = parse_snapshot(temporary, target)
                last = value
                if value["serial"] > prior_serial:
                    os.replace(temporary, destination)
                    return value
            except (OSError, json.JSONDecodeError, RuntimeError):
                pass
        time.sleep(0.05)
    temporary.unlink(missing_ok=True)
    raise RuntimeError(
        f"diagnostic acknowledgement did not advance for node {target['node_id']} "
        f"(prior={prior_serial}, last={last})")


def all_snapshots(fixture, container, targets, destination, label, prior=None):
    prior = prior or {}
    values = {}
    for index, target in enumerate(targets):
        path = destination / f"{label}-node{index}.json"
        values[target["node_id"]] = remote_snapshot(
            fixture, container, target, path, label,
            prior.get(target["node_id"], 0))
    return values


def current_serials(fixture, container, targets, destination):
    values = {}
    for index, target in enumerate(targets):
        path = destination / f"current-node{index}.json"
        copied = fixture.run([
            "docker", "cp", container + ":" + target["path"], path,
        ], check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if copied.returncode == 0:
            try:
                value = parse_snapshot(path, target)
                values[target["node_id"]] = value["serial"]
            except (OSError, json.JSONDecodeError, RuntimeError):
                pass
        path.unlink(missing_ok=True)
    return values


def node_group_incarnations(destination, prefix="ready"):
    result = {}
    root = destination / "published" / prefix
    if not root.exists():
        return result
    for path in sorted(root.rglob("serve-rf3.vibejson")):
        try:
            value = json.loads(path.read_text())
            route = value.get("route", {})
            group = route.get("group_id")
            incarnation = route.get("shard_incarnation")
            parts = path.parts
            node = next((part for part in parts if part.startswith("node-") and part[5:].isdigit()), None)
            if node and isinstance(group, str) and isinstance(incarnation, str):
                result.setdefault(node, {})[group] = incarnation
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
    return result


def target_identity(inventory, target, group_incarnations):
    process = None
    for line in inventory.get("processes", {}).get("text", "").splitlines():
        fields = line.split(None, 3)
        if fields and fields[0].isdigit() and int(fields[0]) == target["pid"]:
            process = line
            break
    return {
        "pid": target["pid"],
        "node_id": target["node_id"],
        "executable": target.get("executable"),
        "process_line": process,
        "group_incarnations": group_incarnations,
    }


def exact_candidate_pids(fixture, container, inventory):
    pids = []
    for line in inventory.get("executables", {}).get("text", "").splitlines():
        fields = line.split("\t", 1)
        if len(fields) == 2 and fields[0].isdigit() and fields[1] in {
            "/bench/candidate-vibedb", "/bench/candidate-vibedb-shard",
        }:
            pids.append(fields[0])
    if len(pids) != 4 or not all(pid.isdigit() for pid in pids):
        raise fixture.RunnerError(
            "restart fault requires one candidate supervisor and three candidate shard processes")
    return sorted(pids, key=int)


def copy_markers(fixture, container, destination, label, nodes=3, groups=4):
    records = []
    for node in range(1, nodes + 1):
        for group in range(groups):
            remote = f"/data/vibe/node-{node}/group-{group}/read-authority.state.vibejson"
            local = destination / f"{label}-node{node}-group{group}-read-authority.state.vibejson"
            copied = fixture.run(["docker", "cp", container + ":" + remote, local],
                                 check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            record = {"remote": remote, "path": str(local), "exit_code": copied.returncode}
            if copied.returncode == 0:
                try:
                    value = json.loads(local.read_text())
                    record.update({
                        "enabled": value.get("enabled"),
                        "feature_version": value.get("feature_version"),
                        "policy_version": value.get("policy_version"),
                        "policy_digest": value.get("policy_digest"),
                        "sha256": json_sha256(local),
                    })
                except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                    record["error"] = str(exc)
            records.append(record)
    return records


def capture_restart_diagnostics(fixture, container, targets, destination, label):
    values = {}
    for index, target in enumerate(targets):
        path = destination / f"{label}-node{index}.json"
        # The new process has a fresh counter stream, so the serial can start
        # at one.  Poll for the first acknowledged snapshot without assuming a
        # serial relationship to the pre-crash incarnation.
        values[target["node_id"]] = remote_snapshot(
            fixture, container, target, path, label, prior_serial=0)
    return values


def wait_processes(processes):
    for process in processes:
        if process.poll() is None:
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new absolute evidence directory")
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--operations", type=int, default=50000)
    parser.add_argument("--scans", type=int, default=500)
    parser.add_argument("--warmup", type=int, default=1000)
    parser.add_argument("--workloads", default="point_hit,point_miss,mixed_read_update,mixed_uniform")
    parser.add_argument("--pause-seconds", type=float, default=7.0)
    parser.add_argument("--ready-timeout", type=int, default=180)
    parser.add_argument("--no-fault", action="store_true",
                        help="run the original workload without SIGSTOP/SIGKILL fault injection")
    parser.add_argument("--prepare-only", action="store_true",
                        help="build and retain the lab variant provenance without starting Docker")
    parser.add_argument("--laboratory-read-authority", action="store_true",
                        help="required explicit opt-in for the lab-tagged read-authority binary")
    selected = parser.parse_args(argv)
    if not selected.laboratory_read_authority:
        parser.error("--laboratory-read-authority is required for this qualification")
    repo = selected.repo.resolve()
    fixture = load_fixture()
    destination = fixture.require_new_directory(selected.output)
    fixture.COMMAND_LOG = destination / "control-commands.jsonl"
    fault_dir = destination / "fault"
    fault_dir.mkdir(mode=0o700)
    controls = destination / "controls"
    controls.mkdir(mode=0o700)
    for path in (Path(__file__), FIXTURE_PATH, VALIDATOR_PATH):
        shutil.copy2(path, controls / path.name)
    manifest = {
        "schema": "vibedb.read-authority-fault-qualification/1",
        "status": "preparing",
        "diagnostic_only": True,
        "timed_performance_claim": False,
        "throughput_comparison": False,
        "crdb_comparison": False,
        "qualification_variant": "laboratory-read-authority",
        "laboratory_read_authority": True,
        "build_tag": LAB_BUILD_TAG,
        "runtime_image": RUNTIME,
        "environment": {
            "GOEXPERIMENT": "simd", "GOFLAGS": "-tags=" + LAB_BUILD_TAG,
            "GOCACHE": str(GOCACHE), "build_tags": [LAB_BUILD_TAG],
            "docker_resource_limits": {"cpus": "12", "memory": "24g", "memory_swap": "24g"},
        },
        "workload": {
            "physical_nodes": 3, "groups": 4,
            "tables": fixture.table_names(4, "rf3_sql_group"),
            "workloads": [value.strip() for value in selected.workloads.split(",") if value.strip()],
            "clients": "1,8", "rows": 64,
            "operations": selected.operations, "scans": selected.scans,
            "warmup": selected.warmup, "repetitions": 1,
        },
        "fault_contract": {
            "enabled": not selected.no_fault,
            "mode": "none" if selected.no_fault else "pause-and-restart",
            "grant_max_seconds": GRANT_SECONDS,
            "pause_seconds": selected.pause_seconds if not selected.no_fault else 0,
            "pause_is_process_signal": not selected.no_fault,
            "pause_is_not_network_partition": not selected.no_fault,
            "restart_quarantine_seconds": QUARANTINE_SECONDS if not selected.no_fault else 0,
            "same_volume_restart": not selected.no_fault,
        },
        "source": {},
        "binary_sha256": {},
        "events": [],
        "diagnostic_errors": [],
    }
    fixture.write_json(destination / "manifest.json", manifest)
    if selected.prepare_only:
        source_before = None
        try:
            source_before = source_snapshot(fixture, repo, destination, "before")
            manifest["source"] = source_before
            arch = fixture.docker_architecture()
            manifest["docker_architecture"] = arch
            image = fixture.ensure_image(RUNTIME)
            manifest["runtime_image_inspection"] = image
            binaries, metadata, _ = build_candidate(fixture, repo, destination, arch)
            manifest["binary_sha256"] = fixture.binary_hashes(binaries)
            manifest["build_metadata"] = metadata
            manifest["preparation"] = {
                "variant": "laboratory-read-authority",
                "build_tag": LAB_BUILD_TAG,
                "source_revision": source_before.get("revision"),
                "workload_started": False,
                "docker_fixture_started": False,
            }
            manifest["status"] = "prepared-only"
        except BaseException as exc:
            manifest["status"] = "incomplete-or-failed"
            manifest["failure"] = str(exc) or exc.__class__.__name__
        finally:
            if source_before is not None:
                try:
                    source_after = source_snapshot(fixture, repo, destination, "after")
                    manifest["source_after"] = source_after
                    manifest["source_stable"] = (
                        source_identity(source_before) == source_identity(source_after)
                    )
                    if not manifest["source_stable"]:
                        manifest["status"] = "incomplete-or-failed"
                        manifest["failure"] = "source changed during preparation"
                except BaseException as exc:
                    manifest["status"] = "incomplete-or-failed"
                    manifest["failure"] = "could not snapshot source after preparation: " + str(exc)
            fixture.write_json(destination / "manifest.json", manifest)
        print(destination)
        print(json.dumps({
            "status": manifest.get("status"),
            "binary_sha256": manifest.get("binary_sha256"),
            "source_revision": manifest.get("source", {}).get("revision"),
            "source_stable": manifest.get("source_stable"),
            "failure": manifest.get("failure"),
        }, sort_keys=True))
        return 0 if manifest.get("status") == "prepared-only" else 1
    source_before = None
    original_run = fixture.run
    original_popen = subprocess.Popen
    original_subprocess_run = subprocess.run
    original_stop = fixture.stop_processes
    captured = {}
    state = {"pause": None, "pause_error": None}
    lock = threading.Lock()

    def logged_run(command, *args, **kwargs):
        values = [str(value) for value in command]
        if values[:2] == ["docker", "run"] and "--name" in values:
            with lock:
                captured["container"] = values[values.index("--name") + 1]
                captured["volume"] = values[values.index("--mount") + 1].split(",", 1)[0].split("=", 1)[1]
                manifest["container"] = captured["container"]
                manifest["volume"] = captured["volume"]
                fixture.write_json(destination / "manifest.json", manifest)
        return original_run(command, *args, **kwargs)

    def captured_popen(command, *args, **kwargs):
        values = list(command) if isinstance(command, (list, tuple)) else command
        if isinstance(values, list) and values[:2] == ["docker", "exec"]:
            if values[3:5] == ["/bench/candidate-vibedb", "cluster"]:
                with lock:
                    captured["server"] = list(values)
                    manifest["server_argv"] = list(values[3:])
                    fixture.write_json(destination / "manifest.json", manifest)
            if (len(values) > 3 and values[3] == "/bench/rf3-sqlbench" and
                    "-phase" in values and values[values.index("-phase") + 1] == "run"):
                values = list(values)
                if "-recovery-oracle" not in values:
                    values.extend(["-recovery-oracle", "/evidence/client-oracle.json"])
                with lock:
                    captured["client"] = list(values)
                    manifest["client_argv"] = list(values[3:])
                    fixture.write_json(destination / "manifest.json", manifest)
        return original_popen(values, *args, **kwargs)

    def captured_run(command, *args, **kwargs):
        values = list(command) if isinstance(command, (list, tuple)) else command
        if (isinstance(values, list) and values[:2] == ["docker", "exec"] and
                len(values) > 3 and values[3] == "/bench/rf3-sqlbench" and
                "-phase" in values and values[values.index("-phase") + 1] == "run"):
            values = list(values)
            if "-recovery-oracle" not in values:
                values.extend(["-recovery-oracle", "/evidence/client-oracle.json"])
            with lock:
                captured["client"] = list(values)
                manifest["client_argv"] = list(values[3:])
                fixture.write_json(destination / "manifest.json", manifest)
        return original_subprocess_run(values, *args, **kwargs)

    def record_event(event):
        # Keep each manifest event as an immutable checkpoint; later updates to
        # the live event dictionary must not rewrite the earlier raw record.
        manifest["events"].append(json.loads(json.dumps(event)))
        fixture.write_json(destination / "manifest.json", manifest)

    def do_pause():
        """Inject SIGSTOP/SIGCONT after a fresh fast-path observation."""
        # Four-group development setup includes deterministic preparation and
        # catalog settlement; it can exceed one minute before the measuring
        # checkpoint.  Bound this wait by readiness plus a separate setup
        # allowance so a slow fixture is retained as a startup failure rather
        # than silently skipping the requested fault.
        deadline = time.monotonic() + selected.ready_timeout + 180
        run_dir = destination / "candidate"
        while time.monotonic() < deadline:
            with lock:
                container = captured.get("container")
            run_json = run_dir / "run.json"
            if container and run_json.exists():
                try:
                    current = json.loads(run_json.read_text())
                except (OSError, json.JSONDecodeError):
                    current = {}
                if current.get("status") == "measuring":
                    break
            time.sleep(0.1)
        else:
            state["pause_error"] = "candidate did not reach measuring state"
            return
        try:
            targets = json.loads((run_dir / "diagnostic-targets.json").read_text())
            inventory = fixture.process_inventory(container)
            fixture.save_inventory(fault_dir, inventory, "pre-pause")
            group_incarnations = node_group_incarnations(run_dir, "ready")
            record_event({
                "event": "pause-prepared", "utc": utc_now(),
                "container": container, "targets": targets,
                "process_identities": [
                    target_identity(inventory, target, group_incarnations)
                    for target in targets
                ],
            })
            prior = current_serials(fixture, container, targets, fault_dir)
            before = all_snapshots(fixture, container, targets, fault_dir,
                                   "selection-before", prior)
            time.sleep(1.5)
            prior = {node: value["serial"] for node, value in before.items()}
            after = all_snapshots(fixture, container, targets, fault_dir,
                                  "selection-after", prior)
            deltas = {
                node: counter(after[node], "authority_read_hits") -
                counter(before[node], "authority_read_hits")
                for node in before
            }
            candidates = [target for target in targets if deltas.get(target["node_id"], 0) > 0]
            # Keep the SQL endpoint's process alive where possible so the
            # client retains its session while another physical node pauses.
            endpoint_targets = [
                target for target in targets if "/node-1/" in target.get("path", "")
            ]
            non_endpoint = [target for target in candidates if target not in endpoint_targets]
            chosen = max(non_endpoint or candidates,
                         key=lambda target: deltas[target["node_id"]],
                         default=None)
            selection = {
                "basis": "recent acknowledged diagnostic authority_read_hits delta",
                "native_current_group_leader_probed": False,
                "current_holder_claim": False,
                "node_pause_only": True,
                "deltas": deltas,
                "before": before, "after": after,
                "selected": chosen,
            }
            if chosen is None:
                raise RuntimeError(
                    "no physical process recorded a recent authority fast-path hit")
            pid = str(chosen["pid"])
            pause = {
                "event": "process-pause",
                "status": "stopping",
                "utc_stop_requested": utc_now(),
                "container": container,
                "node_id": chosen["node_id"], "pid": chosen["pid"],
                "selection": selection,
                "signal_stop": ["docker", "exec", container, "kill", "-STOP", pid],
                "signal_continue": ["docker", "exec", container, "kill", "-CONT", pid],
                "grant_max_seconds": GRANT_SECONDS,
                "pause_seconds_requested": selected.pause_seconds,
                "clock_bound_note": "7 s wall pause exceeds 5 s grant, including documented 5.56 s slow-clock bound",
            }
            state["pause"] = pause
            record_event(pause)
            fixture.run(["docker", "exec", container, "kill", "-STOP", pid])
            started = time.monotonic()
            pause["utc_stop_sent"] = utc_now()
            continue_result = None
            try:
                time.sleep(selected.pause_seconds)
            finally:
                continue_result = fixture.run(
                    ["docker", "exec", container, "kill", "-CONT", pid], check=False)
            elapsed = time.monotonic() - started
            pause["continue_exit_code"] = continue_result.returncode
            pause["utc_continue_sent"] = utc_now()
            if continue_result.returncode:
                raise RuntimeError("SIGCONT was not delivered; post-CONT latch is invalid")
            pause["pause_seconds_observed"] = elapsed
            pause["status"] = "continued"
            after_inventory = fixture.process_inventory(container)
            fixture.save_inventory(fault_dir, after_inventory, "post-pause")
            pause["post_pause_process_identity"] = target_identity(
                after_inventory, chosen, group_incarnations)
            post = all_snapshots(fixture, container, targets, fault_dir,
                                 "pause-after", current_serials(
                                     fixture, container, targets, fault_dir))
            # Refresh each node's SIGUSR1-backed owner counters before arming
            # the sidecar. The requested timestamp remains the exact CONT
            # handoff; delaying the request until these cuts are retained makes
            # the first latched cycle carry post-CONT authority evidence.
            latch_file = getattr(args, "rf3_diagnostic_latch_file", "")
            if not latch_file:
                raise RuntimeError("RF3 diagnostic latch request path was not configured")
            latch_request = fault_dir / "post-cont-latch-request.json"
            fixture.write_json(latch_request, {
                "schema": "vibedb.rf3-diagnostic-latch-request/1",
                "event": "post-cont",
                "requested_utc": pause["utc_continue_sent"],
                "node_id": chosen["node_id"],
                "pid": chosen["pid"],
                "signal": "SIGCONT",
                "source": "fault-controller",
            })
            # Publish into the container in two steps.  docker cp may expose a
            # partially written destination while the sidecar is polling; a
            # same-filesystem rename makes the request an atomic handoff.
            latch_temporary = latch_file + ".tmp"
            copied_latch_request = fixture.run([
                "docker", "cp", latch_request, container + ":" + latch_temporary,
            ], check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            published_latch_request = None
            if copied_latch_request.returncode == 0:
                published_latch_request = fixture.run([
                    "docker", "exec", container, "mv", "-f",
                    latch_temporary, latch_file,
                ], check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            pause["post_cont_latch_request"] = {
                "path": str(latch_request.relative_to(destination)),
                "temporary_remote_path": latch_temporary,
                "remote_path": latch_file,
                "exit_code": copied_latch_request.returncode,
                "publish_exit_code": None if published_latch_request is None else published_latch_request.returncode,
                "requested_utc": pause["utc_continue_sent"],
                "armed_after_post_pause_diagnostic": True,
            }
            if (copied_latch_request.returncode or
                    published_latch_request is None or
                    published_latch_request.returncode):
                raise RuntimeError("post-CONT latch request was not atomically published into the container")
            pause["post_pause_diagnostics"] = post
            pause["unavailable_interval_seconds"] = elapsed
            pause["post_pause_counter_deltas"] = {
                node: {
                    "authority_read_hits": counter(post[node], "authority_read_hits") -
                    counter(before[node], "authority_read_hits"),
                    "authority_read_index_fallbacks": counter(post[node], "authority_read_index_fallbacks") -
                    counter(before[node], "authority_read_index_fallbacks"),
                    "read_authority_rounds_started": counter(post[node], "read_authority_rounds_started") -
                    counter(before[node], "read_authority_rounds_started"),
                }
                for node in before if node in post
            }
            pause["strict_sql_oracle_deferred_to_run"] = True
            pause["status"] = "verified-signals"
            record_event(pause)
        except BaseException as exc:
            state["pause_error"] = str(exc) or exc.__class__.__name__
            if state.get("pause") is not None:
                state["pause"]["status"] = "failed"
                state["pause"]["error"] = state["pause_error"]
                record_event(state["pause"])

    def stop_with_restart(processes, container, destination_run):
        """Retain strict pre-crash oracle, kill serving processes, restart root."""
        logs = []
        restart = {
            "event": "sigkill-restart",
            "status": "starting",
            "utc_started": utc_now(),
            "same_volume": True,
            "quarantine_seconds": QUARANTINE_SECONDS,
        }
        try:
            try:
                report = json.loads((destination_run / "report.json").read_text())
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                restart["status"] = "skipped"
                restart["reason"] = "pre-crash client report unavailable: " + str(exc)
                record_event(restart)
                return []
            if report.get("status") != "complete":
                restart["status"] = "skipped"
                restart["reason"] = "pre-crash client report was not complete"
                record_event(restart)
                return []
            if "client" not in captured or "server" not in captured:
                restart["status"] = "skipped"
                restart["reason"] = "server/client argv was not captured"
                record_event(restart)
                return []
            copied = fixture.run([
                "docker", "cp", container + ":/evidence/client-oracle.json",
                destination / "client-oracle.json",
            ], check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if copied.returncode:
                raise fixture.RunnerError("pre-crash recovery oracle was not retained")
            before = fixture.process_inventory(container)
            fixture.save_inventory(fault_dir, before, "pre-sigkill")
            pids = exact_candidate_pids(fixture, container, before)
            restart["killed_pids"] = pids
            restart["pre_sigkill_group_incarnations"] = node_group_incarnations(destination_run, "ready")
            restart["utc_sigkill"] = utc_now()
            restart["signal"] = "SIGKILL"
            record_event(restart)
            fixture.run(["docker", "exec", container, "kill", "-KILL", *pids])
            wait_processes(processes)
            restart["utc_processes_stopped"] = utc_now()
            log_path = destination / "restart-server.log"
            log = log_path.open("wb")
            logs.append(log)
            started = time.monotonic()
            replacement = original_popen(captured["server"], stdout=log,
                                          stderr=subprocess.STDOUT)
            processes.append(replacement)
            fixture.wait_for_marker(
                replacement, log_path,
                ["VibeDB development RF3 physical cluster ready:"],
                args.ready_timeout,
            )
            fixture.wait_for_tcp_ports(container, [5432], args.ready_timeout)
            restart["restart_ready_seconds"] = time.monotonic() - started
            restart["utc_ready"] = utc_now()
            after = fixture.process_inventory(container)
            fixture.save_inventory(fault_dir, after, "post-restart")
            restart["post_restart_group_incarnations"] = node_group_incarnations(destination_run, "ready")
            restart["marker_files"] = copy_markers(
                fixture, container, fault_dir, "post-restart", nodes=3, groups=4)
            targets = fixture.candidate_diagnostic_targets(
                destination_run, after, 3)
            fixture.write_json(fault_dir / "post-restart-diagnostic-targets.json", targets)
            early = capture_restart_diagnostics(
                fixture, container, targets, fault_dir, "quarantine-early")
            restart["quarantine_early_diagnostics"] = early
            restart["quarantine_early_elapsed_seconds"] = time.monotonic() - started
            restart["quarantine_observable"] = bool(
                restart["quarantine_early_elapsed_seconds"] < QUARANTINE_SECONDS and
                restart["marker_files"] and
                all(record.get("exit_code") == 0 and record.get("enabled") is True
                    for record in restart["marker_files"]) and
                all(counter(value, "read_authority_rounds_started") == 0 and
                    counter(value, "read_authority_grants_accepted") == 0
                    for value in early.values())
            )
            restart["quarantine_observation_basis"] = (
                "retained enabled marker plus first acknowledged diagnostic snapshot "
                "before the configured restart fence; diagnostics have no direct boolean quarantine field"
            )
            remaining = max(0.0, QUARANTINE_SECONDS - restart["quarantine_early_elapsed_seconds"] + 0.5)
            restart["quarantine_wait_seconds"] = remaining
            if remaining:
                time.sleep(remaining)
            client = list(captured["client"])
            client[client.index("-phase") + 1] = "recovery"
            client[client.index("-output") + 1] = "/evidence/recovery.json"
            if "-diagnostic-targets" in client:
                index = client.index("-diagnostic-targets")
                del client[index:index + 2]
            restart["recovery_argv"] = client[3:]
            recovery_log = (destination / "recovery.log").open("wb")
            try:
                recovery_started = time.monotonic()
                recovered = fixture.run(client, stdout=recovery_log,
                                        stderr=subprocess.STDOUT, check=False)
                restart["recovery_elapsed_seconds"] = time.monotonic() - recovery_started
            finally:
                recovery_log.close()
            copied = fixture.run([
                "docker", "cp", container + ":/evidence/recovery.json",
                destination / "recovery.json",
            ], check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if copied.returncode:
                raise fixture.RunnerError("recovery report was not retained")
            recovery_report = json.loads((destination / "recovery.json").read_text())
            restart["recovery_exit_code"] = recovered.returncode
            restart["recovery_status"] = recovery_report.get("status")
            restart["acknowledged_state_retained"] = (
                recovered.returncode == 0 and recovery_report.get("status") == "complete")
            restart["utc_recovery_complete"] = utc_now()
            final = fixture.process_inventory(container)
            fixture.save_inventory(fault_dir, final, "post-recovery")
            restart["status"] = "verified" if (
                restart["acknowledged_state_retained"] and restart["quarantine_observable"]
            ) else "incomplete-or-failed"
            record_event(restart)
        except BaseException as exc:
            restart["status"] = "failed"
            restart["error"] = str(exc) or exc.__class__.__name__
            record_event(restart)
            raise
        finally:
            for log in logs:
                log.close()
            original_stop(processes, container, destination_run)
        return []

    try:
        source_before = source_snapshot(fixture, repo, destination, "before")
        manifest["source"] = source_before
        arch = fixture.docker_architecture()
        manifest["docker_architecture"] = arch
        image = fixture.ensure_image(RUNTIME)
        manifest["runtime_image_inspection"] = image
        binaries, metadata, env = build_candidate(fixture, repo, destination, arch)
        manifest["binary_sha256"] = fixture.binary_hashes(binaries)
        manifest["build_metadata"] = metadata
        args = type("QualificationArgs", (), {})()
        args.cpus = "12"
        args.memory = "24g"
        args.runtime_image_id = image["Id"]
        args.vibedb_sql_ports = "5432"
        args.rows = 64
        args.operations = selected.operations
        args.scans = selected.scans
        args.warmup = selected.warmup
        args.repetitions = 1
        args.skew_percent = 80
        args.ready_marker = "VibeDB development cluster ready:"
        args.ready_timeout = selected.ready_timeout
        args.parent_arg = []
        args.candidate_arg = ["--read-authority"]
        args.rf3_diagnostic = True
        args.rf3_diagnostic_latch_file = "/evidence/post-cont-latch.json"
        args.rf3_diagnostic_latch_output = "/evidence/post-cont-cut.json"
        args.rf3_diagnostic_latch_required = not selected.no_fault
        args.validator_path = VALIDATOR_PATH
        tables = fixture.table_names(4, "rf3_sql_group")
        cell = {
            "id": "read-authority-fault-3n-4g",
            "kind": "distributed-read-fault-qualification",
            "physical_nodes": 3, "tables": tables,
            "workloads": selected.workloads,
            "group_distribution": "uniform", "clients": "1,8",
            "groups": 4, "endpoint_mode": "single",
        }
        run_dir = destination / "candidate"
        schema = fixture.schema_files(run_dir, tables)
        manifest["cell"] = cell
        manifest["status"] = "measuring-diagnostic-only"
        fixture.write_json(destination / "manifest.json", manifest)
        fixture.run = logged_run
        subprocess.Popen = captured_popen
        subprocess.run = captured_run
        if not selected.no_fault:
            fixture.stop_processes = stop_with_restart
        result_holder = {}

        def run_fixture():
            started = time.monotonic()
            try:
                result_holder["result"] = fixture.run_engine(
                    args, cell, "candidate", "fault-qualification", binaries,
                    run_dir, schema, arch)
            except BaseException as exc:
                result_holder["exception"] = exc
            finally:
                result_holder["wall_elapsed_seconds"] = time.monotonic() - started

        thread = threading.Thread(target=run_fixture, name="rf3-fault-fixture")
        thread.start()
        if not selected.no_fault:
            do_pause()
        thread.join()
        if "exception" in result_holder:
            raise result_holder["exception"]
        result = result_holder.get("result", {})
        manifest["result"] = result
        result_errors = result.get("errors") if isinstance(result, dict) else None
        if isinstance(result_errors, list):
            manifest["diagnostic_errors"].extend(str(error) for error in result_errors)
        manifest["run_wall_elapsed_seconds"] = result_holder.get("wall_elapsed_seconds")
        manifest["diagnostic_records"] = diagnostics_summary(run_dir / "diagnostics")
        manifest["authority_proof"] = authority_proof(manifest["diagnostic_records"])
        manifest["per_group_diagnostic"] = per_group_diagnostic_summary(
            diagnostic_output_path(run_dir))
        timeline_path = run_dir / "failed-table-g0-timeline.json"
        manifest["failed_table_g0_timeline"] = write_group_timeline(
            diagnostic_output_path(run_dir), timeline_path, tables[0])
        if not selected.no_fault:
            latch_path = run_dir / "post-cont-cut.json"
            try:
                pause_event = state.get("pause")
                post_pause_diagnostics = (
                    pause_event.get("post_pause_diagnostics")
                    if isinstance(pause_event, dict) else None)
                latch_summary = validate_post_cont_latch(
                    latch_path, expected_nodes=cell["physical_nodes"],
                    post_pause_diagnostics=post_pause_diagnostics)
                latch_summary["path"] = "candidate/post-cont-cut.json"
                manifest["post_cont_latch"] = latch_summary
            except ValueError as exc:
                manifest["post_cont_latch"] = {
                    "path": "candidate/post-cont-cut.json",
                    "complete": False,
                    "error": str(exc),
                }
                manifest["diagnostic_errors"].append(
                    "post-CONT diagnostic latch was not retained: " + str(exc))
        else:
            manifest["post_cont_latch"] = {
                "path": "candidate/post-cont-cut.json",
                "required": False,
                "complete": True,
            }
        manifest["pause"] = state.get("pause")
        manifest["pause_error"] = state.get("pause_error")
        primary = primary_result_failure(result, run_dir)
        if primary is not None:
            manifest["primary_failure"] = primary
            manifest["failure"] = primary.get("client_error_tail") or "; ".join(
                primary["errors"])
        manifest["restart"] = next((event for event in reversed(manifest["events"])
                                     if event.get("event") == "sigkill-restart"), None)
        latch_summary = manifest.get("post_cont_latch")
        latch_complete = (
            selected.no_fault or
            (isinstance(latch_summary, dict) and
             latch_summary.get("complete") is True))
        fault_complete = fault_qualification_complete(
            selected.no_fault, state.get("pause"), manifest.get("restart"),
            latch_complete)
        manifest["status"] = "complete" if (
            result.get("status") == "completed" and
            result.get("client_exit_code") == 0 and
            result.get("validation", {}).get("complete") and
            manifest["per_group_diagnostic"].get("complete_shape") and
            fault_complete
        ) else "incomplete-or-failed"
        if manifest["diagnostic_errors"] and not manifest.get("primary_failure"):
            manifest.setdefault("failure", "diagnostic: " + "; ".join(
                manifest["diagnostic_errors"]))
    except BaseException as exc:
        manifest["status"] = "incomplete-or-failed"
        detail = str(exc) or exc.__class__.__name__
        manifest["diagnostic_errors"].append("qualification finalization: " + detail)
        if not manifest.get("primary_failure") and not manifest.get("failure"):
            manifest["failure"] = detail
    finally:
        fixture.run = original_run
        subprocess.Popen = original_popen
        subprocess.run = original_subprocess_run
        fixture.stop_processes = original_stop
        if source_before is not None:
            try:
                source_after = source_snapshot(fixture, repo, destination, "after")
                manifest["source_after"] = source_after
                manifest["source_stable"] = (
                    source_identity(source_before) == source_identity(source_after)
                )
                if not manifest["source_stable"]:
                    manifest["status"] = "incomplete-or-failed"
                    manifest["diagnostic_errors"].append(
                        "source changed during qualification")
                    if not manifest.get("primary_failure") and not manifest.get("failure"):
                        manifest["failure"] = "diagnostic: source changed during qualification"
            except BaseException as exc:
                manifest["status"] = "incomplete-or-failed"
                detail = "could not snapshot source after qualification: " + str(exc)
                manifest["diagnostic_errors"].append(detail)
                if not manifest.get("primary_failure") and not manifest.get("failure"):
                    manifest["failure"] = "diagnostic: " + detail
        fixture.write_json(destination / "manifest.json", manifest)
    print(destination)
    print(json.dumps({
        "status": manifest.get("status"),
        "result_status": manifest.get("result", {}).get("status"),
        "validation": manifest.get("result", {}).get("validation"),
        "pause": manifest.get("pause"),
        "restart": manifest.get("restart"),
        "diagnostic_errors": manifest.get("diagnostic_errors"),
        "failure": manifest.get("failure"),
    }, sort_keys=True))
    return 0 if manifest.get("status") == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
