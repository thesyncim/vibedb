#!/usr/bin/env python3
"""Run a bounded, alternating whole-main Batch64 comparison.

Each benchmark process performs its complete 131072-row workload and final
fold once. The output directory is an immutable evidence bundle: it must be
new or empty, and every process result is flushed before the next one starts.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone


WORKLOADS = {
    "sequential-insert": "BenchmarkReplicatedApplyBatch64SequentialInsert",
    "compact-shared-payload": "BenchmarkReplicatedApplyBatch64CompactSharedPayload",
}
REQUIRED_METRICS = {
    "rows",
    "ns/op",
    "B/op",
    "allocs/op",
    "final-allocated-file-B",
    "final-apparent-file-B",
    "final-fold-ns/row",
}
BENCHMARK_TIMEOUT_SECONDS = 20 * 60
WORKLOAD_SCOPE = {
    "rows_per_operation": 131072,
    "batches_per_operation": 2048,
    "rows_per_batch": 64,
    "path": "Local ReplicatedApply over three durable collections; members are not physical replicas.",
    "excluded": ["Raft", "network transport", "proposal admission", "RF3 runtime"],
    "final_full_fold_included_in_benchmark_time": True,
    "timing_boundary": (
        "Database/schema setup, final row oracle, and disk sampling are outside Go benchmark time; "
        "per-batch row generation, command preparation, apply, and final full fold are included."
    ),
}
RESOURCE_SCOPE = {
    "whole_process_rss_includes_setup_and_oracles": True,
    "rss_is_a_hard_memory_bound": False,
    "disk_peak_is_sampled_not_continuously_observed": True,
    "disk_peak_is_a_hard_space_bound": False,
    "notes": (
        "GNU time maximum RSS is KiB for the entire process, including setup and row oracles; "
        "it is diagnostic, not a memory bound. Disk peak values are sampled by the benchmark "
        "and are not a continuous maximum or hard space bound."
    ),
}
PROFILE_GROUPS = {
    "compact-planner-encoding": (
        "encodeShape|measureDictionary|measureAlphabet|finishAlphabet"
    ),
    "tail-leaf-work": (
        "buildPrimaryBatchLeaf|stagePrimaryBatchUnifiedOverlayLocked|"
        "preparePrimaryTailBatch(Split|Update)Locked|publishPrimaryTailBatchGateHeld"
    ),
    "journal-marker": "AppendPreparedConditionalBatch|EntryCurrent|AppendDecision",
    "garbage-collection": r"runtime\.(gcBgMarkWorker|gcDrain|mallocgc|scanobject)",
}
PPROF_NO_MATCH_EXIT_STATUS = 1
PPROF_NO_MATCH_DIAGNOSTIC = "no matches found for regexp"
PROFILE_SCOPE = {
    "processes": 4,
    "workloads": list(WORKLOADS),
    "arms": ["baseline", "candidate"],
    "iterations_per_process": 1,
    "diagnostic_only": True,
    "comparison_samples_are_not_timing_samples": True,
    "cpu_profile_includes_benchmark_setup_and_final_row_oracle": True,
    "notes": (
        "Each profile runs the same one-shot benchmark in a separate process. CPU samples "
        "include benchmark setup and the final row oracle, which are outside ns/op timing. "
        "Profiled ns/op values are diagnostic and are excluded from throughput comparisons."
    ),
}


class RunnerError(ValueError):
    """Invalid benchmark output, setup, process result, or evidence state."""


def parse_benchmark_output(output: str, expected_benchmark: str) -> dict:
    """Parse exactly one Go benchmark row and validate its full workload shape."""
    rows = []
    wanted = re.compile(re.escape(expected_benchmark) + r"(?:-[0-9]+)?\Z")
    for line in output.splitlines():
        fields = line.split()
        if not fields or not fields[0].startswith("Benchmark"):
            continue
        if not wanted.fullmatch(fields[0]):
            raise RunnerError(f"unexpected benchmark row {fields[0]!r}")
        if len(fields) < 4 or (len(fields) - 2) % 2:
            raise RunnerError(f"malformed benchmark row: {line}")
        try:
            iterations = int(fields[1])
        except ValueError as exc:
            raise RunnerError(f"invalid benchmark iteration count: {line}") from exc
        metrics = {}
        for offset in range(2, len(fields), 2):
            try:
                value = float(fields[offset])
            except ValueError as exc:
                raise RunnerError(f"invalid benchmark metric: {line}") from exc
            unit = fields[offset + 1]
            if not math.isfinite(value):
                raise RunnerError(f"non-finite benchmark metric {unit!r}")
            if unit in metrics:
                raise RunnerError(f"duplicate benchmark metric {unit!r}")
            metrics[unit] = value
        rows.append((fields[0], iterations, metrics))

    if len(rows) != 1:
        raise RunnerError(f"expected exactly one benchmark row, found {len(rows)}")
    benchmark, iterations, metrics = rows[0]
    if iterations != 1:
        raise RunnerError(f"expected one benchmark iteration, found {iterations}")
    missing = sorted(REQUIRED_METRICS - metrics.keys())
    if missing:
        raise RunnerError("benchmark row is missing required metrics: " + ", ".join(missing))
    if metrics["rows"] != 131072:
        raise RunnerError(f"expected rows=131072, found {metrics['rows']:g}")
    for unit in ("ns/op", "B/op", "allocs/op", "final-allocated-file-B",
                 "final-apparent-file-B", "final-fold-ns/row"):
        if metrics[unit] < 0 or (unit == "ns/op" and metrics[unit] == 0):
            message = "must be positive" if unit == "ns/op" else "must not be negative"
            raise RunnerError(f"benchmark metric {unit!r} {message}")
    return {"benchmark": benchmark, "iterations": iterations, "metrics": metrics}


def planned_samples() -> list[dict]:
    """Return the immutable twelve-process, alternating sample schedule."""
    samples = []
    ordinal = 0
    pair_orders = (
        ("baseline", "candidate"),
        ("candidate", "baseline"),
        ("baseline", "candidate"),
    )
    for workload, benchmark in WORKLOADS.items():
        for pair, order in enumerate(pair_orders, 1):
            for arm in order:
                ordinal += 1
                samples.append({
                    "ordinal": ordinal,
                    "workload": workload,
                    "benchmark": benchmark,
                    "pair": pair,
                    "arm": arm,
                })
    return samples


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _as_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _time_metrics(stderr: str) -> dict:
    metrics = {}
    rss = re.search(r"^\s*Maximum resident set size \(kbytes\):\s*(\d+)\s*$",
                    stderr, re.MULTILINE)
    elapsed = re.search(r"^\s*Elapsed \(wall clock\) time \([^)]*\):\s*(.*?)\s*$",
                        stderr, re.MULTILINE)
    if rss:
        # GNU time labels this "kbytes"; report the conventional binary unit.
        metrics["maximum_resident_set_kib"] = int(rss.group(1))
    if elapsed:
        metrics["elapsed_wall_clock"] = elapsed.group(1)
    for key, pattern in (
        ("user_seconds", r"^\s*User time \(seconds\):\s*([0-9]+(?:\.[0-9]+)?)\s*$"),
        ("system_seconds", r"^\s*System time \(seconds\):\s*([0-9]+(?:\.[0-9]+)?)\s*$"),
        ("cpu_percent", r"^\s*Percent of CPU this job got:\s*(\d+)%\s*$"),
    ):
        match = re.search(pattern, stderr, re.MULTILINE)
        if match:
            metrics[key] = float(match.group(1))
    return metrics


def resolve_commit_revision(
    revision: str,
    default_revision: str = "",
    repository: Path = Path.cwd(),
    process_runner=subprocess.run,
) -> str:
    """Resolve an optional user revision to a full commit SHA.

    An empty optional candidate selects the workflow revision. An empty
    required baseline is an error. The Git invocation is injectable for tests.
    """
    selected = (revision or "").strip() or (default_revision or "").strip()
    if not selected:
        raise RunnerError("a revision is required")
    command = [
        "git", "-C", str(Path(repository).resolve()), "rev-parse", "--verify",
        "--end-of-options", f"{selected}^{{commit}}",
    ]
    try:
        result = process_runner(command, capture_output=True, text=True, check=False)
    except Exception as exc:
        raise RunnerError(f"could not resolve revision {selected!r}: {exc}") from exc
    if result.returncode != 0:
        raise RunnerError(f"revision {selected!r} does not resolve to a commit")
    resolved = _as_text(result.stdout).strip().splitlines()
    if len(resolved) != 1 or not re.fullmatch(r"[0-9a-fA-F]{40}", resolved[0]):
        raise RunnerError(f"revision {selected!r} did not resolve to a full commit SHA")
    return resolved[0].lower()


def planned_profile_samples() -> list[dict]:
    """Return the four serial diagnostic processes, separate from timed samples."""
    samples = []
    ordinal = 0
    for workload, benchmark in WORKLOADS.items():
        for arm in ("baseline", "candidate"):
            ordinal += 1
            samples.append({
                "ordinal": ordinal,
                "workload": workload,
                "benchmark": benchmark,
                "arm": arm,
            })
    return samples


def _ratio(numerator: float, denominator: float):
    if denominator == 0:
        return None
    value = numerator / denominator
    if not math.isfinite(value):
        raise RunnerError("benchmark ratio is non-finite")
    return value


def _store_capture(output: Path, filename: str, value):
    if value is None:
        return None, "unavailable"
    (output / filename).write_text(_as_text(value), encoding="utf-8")
    return filename, "captured"


def _finish_sample(item: dict, started_monotonic: float) -> None:
    item["completed_utc"] = datetime.now(timezone.utc).isoformat()
    item["wall_seconds"] = round(max(0.0, time.monotonic() - started_monotonic), 6)


def _summarize(runs: list[dict]) -> dict:
    completed = {(run["workload"], run["pair"], run["arm"]): run for run in runs}
    workloads = {}
    for workload, benchmark in WORKLOADS.items():
        baseline_runs = [completed[(workload, pair, "baseline")] for pair in range(1, 4)]
        candidate_runs = [completed[(workload, pair, "candidate")] for pair in range(1, 4)]
        metric_names = set(baseline_runs[0]["metrics"])
        if any(set(run["metrics"]) != metric_names for run in baseline_runs + candidate_runs):
            raise RunnerError(f"metric set changed across {workload} samples")
        metrics = {}
        for name in sorted(metric_names):
            baseline = [run["metrics"][name] for run in baseline_runs]
            candidate = [run["metrics"][name] for run in candidate_runs]
            base_median = statistics.median(baseline)
            candidate_median = statistics.median(candidate)
            ratio_of_medians = _ratio(base_median, candidate_median)
            paired = [_ratio(baseline[index], candidate[index]) for index in range(3)]
            metrics[name] = {
                "baseline_median": base_median,
                "candidate_median": candidate_median,
                "baseline_over_candidate_ratio_of_medians": ratio_of_medians,
                "paired_baseline_over_candidate_ratios": paired,
            }
        workloads[workload] = {
            "benchmark": benchmark,
            "pairs": [
                {
                    "pair": pair,
                    "baseline": completed[(workload, pair, "baseline")]["metrics"],
                    "candidate": completed[(workload, pair, "candidate")]["metrics"],
                }
                for pair in range(1, 4)
            ],
            "metrics": metrics,
        }
    return {
        "status": "complete",
        "ratio_definition": "baseline divided by candidate; medians are across three runs per arm",
        "workload_scope": WORKLOAD_SCOPE,
        "resource_scope": RESOURCE_SCOPE,
        "workloads": workloads,
    }


def _check_output_directory(output: Path) -> None:
    if output.exists():
        if not output.is_dir():
            raise RunnerError(f"output path is not a directory: {output}")
        if any(output.iterdir()):
            raise RunnerError(f"output directory must be new or empty: {output}")
    else:
        output.mkdir(parents=True)


def run_comparison(
    binaries: dict,
    output: Path,
    gomaxprocs: int,
    process_runner=subprocess.run,
    time_binary: str = "/usr/bin/time",
    timeout_seconds: int = BENCHMARK_TIMEOUT_SECONDS,
) -> dict:
    """Run all samples serially; injectable runner supports failure tests."""
    output = Path(output)
    if not isinstance(gomaxprocs, int) or gomaxprocs < 1:
        raise RunnerError("gomaxprocs must be a positive integer")
    for arm in ("baseline", "candidate"):
        if arm not in binaries:
            raise RunnerError(f"missing {arm} benchmark binary")
        binaries[arm] = Path(binaries[arm]).resolve()
        if not binaries[arm].is_file():
            raise RunnerError(f"{arm} benchmark binary does not exist: {binaries[arm]}")
    _check_output_directory(output)

    now = datetime.now(timezone.utc).isoformat()
    manifest = {
        "schema": "vibedb.whole-main-batch64-write-runs.v1",
        "status": "running",
        "started_utc": now,
        "updated_utc": now,
        "gomaxprocs": gomaxprocs,
        "goexperiment": os.environ.get("GOEXPERIMENT", "simd"),
        "locale": "C",
        "tmpdir": os.environ.get("TMPDIR", ""),
        "timeout_seconds": timeout_seconds,
        "time_binary": str(time_binary),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "workloads": WORKLOADS,
        "workload_scope": WORKLOAD_SCOPE,
        "resource_scope": RESOURCE_SCOPE,
        "binary_sha256": {arm: _sha256(path) for arm, path in binaries.items()},
        "runs": [],
    }
    runs_path = output / "runs.json"
    _atomic_json(runs_path, manifest)
    environment = dict(os.environ)
    environment["GOMAXPROCS"] = str(gomaxprocs)
    environment.setdefault("GOEXPERIMENT", "simd")
    environment["LC_ALL"] = "C"

    try:
        for sample in planned_samples():
            stem = (f"{sample['ordinal']:02d}-{sample['workload']}-pair{sample['pair']}-"
                    f"{sample['arm']}")
            stdout_name = stem + ".stdout.txt"
            stderr_name = stem + ".stderr.txt"
            command = [
                str(time_binary), "-v", str(binaries[sample["arm"]]),
                "-test.run=^$",
                "-test.bench=^" + sample["benchmark"] + "$",
                "-test.benchtime=1x",
                "-test.count=1",
                "-test.benchmem",
                "-test.timeout=20m",
            ]
            item = {
                **sample,
                "status": "running",
                "command": command,
                "exit_code": None,
                "stdout_file": stdout_name,
                "stderr_file": stderr_name,
                "stdout_capture": "pending",
                "stderr_capture": "pending",
                "started_utc": datetime.now(timezone.utc).isoformat(),
            }
            started_monotonic = time.monotonic()
            try:
                result = process_runner(
                    command,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds,
                    check=False,
                )
                stdout = _as_text(result.stdout)
                stderr = _as_text(result.stderr)
                item["exit_code"] = result.returncode
                item["stdout_file"], item["stdout_capture"] = _store_capture(
                    output, stdout_name, result.stdout)
                item["stderr_file"], item["stderr_capture"] = _store_capture(
                    output, stderr_name, result.stderr)
                item["time_metrics"] = _time_metrics(stderr)
                _finish_sample(item, started_monotonic)
                if result.returncode != 0:
                    item["status"] = "failed"
                    item["error"] = f"process exited with status {result.returncode}"
                else:
                    try:
                        parsed = parse_benchmark_output(stdout, sample["benchmark"])
                        if item["time_metrics"].get("maximum_resident_set_kib", 0) <= 0:
                            raise RunnerError("GNU time did not report a usable maximum RSS")
                    except ValueError as exc:
                        item["status"] = "failed"
                        item["error"] = str(exc)
                    else:
                        item["status"] = "complete"
                        item["benchmark_row"] = parsed["benchmark"]
                        item["iterations"] = parsed["iterations"]
                        item["metrics"] = parsed["metrics"]
            except KeyboardInterrupt as exc:
                _finish_sample(item, started_monotonic)
                item["status"] = "interrupted"
                item["error"] = "benchmark process interrupted"
                item["stdout_file"], item["stdout_capture"] = _store_capture(
                    output, stdout_name, getattr(exc, "stdout", None))
                item["stderr_file"], item["stderr_capture"] = _store_capture(
                    output, stderr_name, getattr(exc, "stderr", None))
                manifest["runs"].append(item)
                manifest["status"] = "interrupted"
                manifest["updated_utc"] = datetime.now(timezone.utc).isoformat()
                _atomic_json(runs_path, manifest)
                raise
            except Exception as exc:
                _finish_sample(item, started_monotonic)
                item["status"] = "failed"
                item["error"] = f"{type(exc).__name__}: {exc}"
                item["stdout_file"], item["stdout_capture"] = _store_capture(
                    output, stdout_name, getattr(exc, "stdout", None))
                item["stderr_file"], item["stderr_capture"] = _store_capture(
                    output, stderr_name, getattr(exc, "stderr", None))

            manifest["runs"].append(item)
            if item["status"] == "failed":
                manifest["status"] = "failed"
            manifest["updated_utc"] = datetime.now(timezone.utc).isoformat()
            _atomic_json(runs_path, manifest)
            if item["status"] != "complete":
                raise RunnerError(item.get("error", "benchmark run failed"))
    except KeyboardInterrupt:
        manifest["status"] = "interrupted"
        manifest["updated_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic_json(runs_path, manifest)
        raise

    if len(manifest["runs"]) != 12 or any(run["status"] != "complete" for run in manifest["runs"]):
        manifest["status"] = "failed"
        manifest["updated_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic_json(runs_path, manifest)
        return manifest
    comparison_tmp = output / "comparison.json.tmp"
    comparison_path = output / "comparison.json"
    try:
        comparison = _summarize(manifest["runs"])
        comparison["schema"] = "vibedb.whole-main-batch64-write-comparison.v1"
        comparison["tmpdir"] = manifest["tmpdir"]
        comparison["gomaxprocs"] = manifest["gomaxprocs"]
        comparison["goexperiment"] = manifest["goexperiment"]
        comparison_tmp.write_text(
            json.dumps(comparison, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8")
        os.replace(comparison_tmp, comparison_path)
        manifest["status"] = "complete"
        manifest["updated_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic_json(runs_path, manifest)
    except KeyboardInterrupt:
        for path in (comparison_tmp, comparison_path):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        manifest["status"] = "interrupted"
        manifest["updated_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic_json(runs_path, manifest)
        raise
    except Exception as exc:
        for path in (comparison_tmp, comparison_path):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        manifest["updated_utc"] = datetime.now(timezone.utc).isoformat()
        try:
            _atomic_json(runs_path, manifest)
        except OSError:
            pass
        if isinstance(exc, RunnerError):
            raise
        raise RunnerError(f"could not finalize comparison evidence: {exc}") from exc
    return manifest


def run_profiles(
    binaries: dict,
    output: Path,
    gomaxprocs: int,
    timing_output: Path,
    build_metadata: Path,
    source_directories: dict,
    process_runner=subprocess.run,
    time_binary: str = "/usr/bin/time",
    go_command: str = "go",
    timeout_seconds: int = BENCHMARK_TIMEOUT_SECONDS,
) -> dict:
    """Capture four serial one-shot CPU profiles and pprof exports.

    This writes only to a fresh diagnostics directory and requires a completed
    twelve-run comparison. It never edits the timing manifests or summary.
    """
    output = Path(output)
    timing_output = Path(timing_output)
    build_metadata = Path(build_metadata)
    if not isinstance(gomaxprocs, int) or gomaxprocs < 1:
        raise RunnerError("gomaxprocs must be a positive integer")
    for arm in ("baseline", "candidate"):
        if arm not in binaries:
            raise RunnerError(f"missing {arm} benchmark binary")
        binaries[arm] = Path(binaries[arm]).resolve()
        if not binaries[arm].is_file():
            raise RunnerError(f"{arm} benchmark binary does not exist: {binaries[arm]}")
        if arm not in source_directories:
            raise RunnerError(f"missing {arm} source directory")
        source_directories[arm] = Path(source_directories[arm]).resolve()
        if not source_directories[arm].is_dir():
            raise RunnerError(f"{arm} source directory does not exist: {source_directories[arm]}")
    if not build_metadata.is_file():
        raise RunnerError(f"build metadata does not exist: {build_metadata}")

    runs_path = timing_output / "runs.json"
    comparison_path = timing_output / "comparison.json"
    try:
        timing_manifest = json.loads(runs_path.read_text(encoding="utf-8"))
        timing_comparison = json.loads(comparison_path.read_text(encoding="utf-8"))
        build_info = json.loads(build_metadata.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"completed timing/build evidence is unavailable: {exc}") from exc
    if timing_manifest.get("status") != "complete" or len(timing_manifest.get("runs", [])) != 12:
        raise RunnerError("CPU profiles require twelve completed timing samples")
    if timing_comparison.get("status") != "complete":
        raise RunnerError("CPU profiles require a completed timing comparison")
    actual_binary_hashes = {arm: _sha256(path) for arm, path in binaries.items()}
    if timing_manifest.get("binary_sha256") != actual_binary_hashes:
        raise RunnerError("profile binaries do not match the completed timing binaries")
    profile_goexperiment = os.environ.get("GOEXPERIMENT", "simd")
    profile_gomaxprocs = str(gomaxprocs)
    current_tmpdir = os.environ.get("TMPDIR", "")
    profile_settings = {
        "goexperiment": profile_goexperiment,
        "gomaxprocs": profile_gomaxprocs,
        "tmpdir": current_tmpdir,
    }
    for setting, profile_value in profile_settings.items():
        if setting not in timing_manifest:
            raise RunnerError(f"completed timing manifest is missing {setting}")
        if setting not in build_info:
            raise RunnerError(f"build metadata is missing {setting}")
        timing_value = str(timing_manifest[setting])
        build_value = str(build_info[setting])
        if timing_value != profile_value:
            raise RunnerError(f"profile {setting.upper()} differs from the completed timing run")
        if build_value != profile_value:
            raise RunnerError(f"profile {setting.upper()} differs from the timed build")
    _check_output_directory(output)
    now = datetime.now(timezone.utc).isoformat()
    environment = dict(os.environ)
    environment["GOMAXPROCS"] = str(gomaxprocs)
    environment.setdefault("GOEXPERIMENT", "simd")
    environment["LC_ALL"] = "C"
    manifest = {
        "schema": "vibedb.whole-main-batch64-write-profiles.v1",
        "status": "running",
        "started_utc": now,
        "updated_utc": now,
        "workflow_revision": build_info.get("workflow_revision", ""),
        "baseline_revision": build_info.get("baseline_revision", ""),
        "candidate_revision": build_info.get("candidate_revision", ""),
        "harness_revision": build_info.get("harness_revision", ""),
        "harness_sha256": build_info.get("harness_sha256", ""),
        "go_version": build_info.get("go_version", ""),
        "go_settings": build_info.get("go_settings", {}),
        "go_command": str(go_command),
        "time_binary": str(time_binary),
        "timeout_seconds": timeout_seconds,
        "goexperiment": environment.get("GOEXPERIMENT", ""),
        "gomaxprocs": gomaxprocs,
        "locale": environment.get("LC_ALL", ""),
        "tmpdir": environment.get("TMPDIR", ""),
        "profile_scope": PROFILE_SCOPE,
        "resource_scope": RESOURCE_SCOPE,
        "workload_scope": WORKLOAD_SCOPE,
        "binary_sha256": actual_binary_hashes,
        "build_metadata_sha256": _sha256(build_metadata),
        "timing_evidence": {
            "runs_sha256": _sha256(runs_path),
            "comparison_sha256": _sha256(comparison_path),
            "samples": 12,
        },
        "runs": [],
    }
    manifest_path = output / "profiles.json"
    _atomic_json(manifest_path, manifest)

    def write_manifest():
        manifest["updated_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic_json(manifest_path, manifest)

    def capture(value, filename):
        return _store_capture(output, filename, value)

    def fail(item, error, interrupted=False):
        item["status"] = "interrupted" if interrupted else "failed"
        item["error"] = str(error)
        manifest["status"] = item["status"]
        write_manifest()

    try:
        for sample in planned_profile_samples():
            stem = (f"{sample['ordinal']:02d}-{sample['workload']}-"
                    f"{sample['arm']}")
            profile_name = stem + ".pprof"
            stdout_name = stem + ".stdout.txt"
            stderr_name = stem + ".stderr.txt"
            binary = binaries[sample["arm"]]
            profile_path = output / profile_name
            command = [
                str(time_binary), "-v", str(binary),
                "-test.run=^$",
                "-test.bench=^" + sample["benchmark"] + "$",
                "-test.benchtime=1x",
                "-test.count=1",
                "-test.benchmem",
                "-test.timeout=20m",
                "-test.cpuprofile=" + str(profile_path),
            ]
            item = {
                **sample,
                "status": "running",
                "command": command,
                "working_directory": str(source_directories[sample["arm"]]),
                "binary_sha256": manifest["binary_sha256"][sample["arm"]],
                "exit_code": None,
                "profile_file": profile_name,
                "profile_capture": "pending",
                "stdout_file": stdout_name,
                "stderr_file": stderr_name,
                "stdout_capture": "pending",
                "stderr_capture": "pending",
                "exports": [],
                "started_utc": datetime.now(timezone.utc).isoformat(),
            }
            manifest["runs"].append(item)
            write_manifest()
            started_monotonic = time.monotonic()
            try:
                result = process_runner(
                    command,
                    cwd=str(source_directories[sample["arm"]]),
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds,
                    check=False,
                )
                stdout = _as_text(result.stdout)
                stderr = _as_text(result.stderr)
                item["exit_code"] = result.returncode
                item["stdout_file"], item["stdout_capture"] = capture(result.stdout, stdout_name)
                item["stderr_file"], item["stderr_capture"] = capture(result.stderr, stderr_name)
                item["time_metrics"] = _time_metrics(stderr)
                _finish_sample(item, started_monotonic)
                if result.returncode != 0:
                    fail(item, f"profile benchmark exited with status {result.returncode}")
                    raise RunnerError(item["error"])
                parsed = parse_benchmark_output(stdout, sample["benchmark"])
                if item["time_metrics"].get("maximum_resident_set_kib", 0) <= 0:
                    raise RunnerError("GNU time did not report a usable maximum RSS")
                if not profile_path.is_file() or profile_path.stat().st_size == 0:
                    raise RunnerError("profile process did not produce a nonempty CPU profile")
                item["profile_capture"] = "captured"
                item["benchmark_row"] = parsed["benchmark"]
                item["iterations"] = parsed["iterations"]
                item["metrics"] = parsed["metrics"]
                item["status"] = "exporting"
                write_manifest()
            except KeyboardInterrupt as exc:
                _finish_sample(item, started_monotonic)
                item["stdout_file"], item["stdout_capture"] = capture(
                    getattr(exc, "stdout", None), stdout_name)
                item["stderr_file"], item["stderr_capture"] = capture(
                    getattr(exc, "stderr", None), stderr_name)
                item["profile_capture"] = "captured" if profile_path.is_file() else "unavailable"
                fail(item, "profile process interrupted", interrupted=True)
                raise
            except Exception as exc:
                if item.get("status") == "running":
                    _finish_sample(item, started_monotonic)
                    if item["stdout_capture"] == "pending":
                        item["stdout_file"], item["stdout_capture"] = capture(
                            getattr(exc, "stdout", None), stdout_name)
                    if item["stderr_capture"] == "pending":
                        item["stderr_file"], item["stderr_capture"] = capture(
                            getattr(exc, "stderr", None), stderr_name)
                    item["profile_capture"] = "captured" if profile_path.is_file() else "unavailable"
                    fail(item, exc)
                if isinstance(exc, RunnerError):
                    raise
                raise RunnerError(f"profile process failed: {exc}") from exc

            export_specs = [
                ("flat-top", ["-top", "-nodecount=40"]),
                ("cumulative-top", ["-top", "-cum", "-nodecount=40"]),
                *[("source-" + label, ["-list=" + pattern])
                  for label, pattern in PROFILE_GROUPS.items()],
            ]
            export_failed = False
            for label, flags in export_specs:
                export_stem = stem + "." + label
                stdout_export = export_stem + ".txt"
                stderr_export = export_stem + ".stderr.txt"
                export_command = [
                    str(go_command), "tool", "pprof", *flags,
                    str(binary), str(profile_path),
                ]
                export = {
                    "name": label,
                    "command": export_command,
                    "working_directory": str(source_directories[sample["arm"]]),
                    "stdout_file": stdout_export,
                    "stderr_file": stderr_export,
                    "stdout_capture": "pending",
                    "stderr_capture": "pending",
                    "exit_code": None,
                    "status": "running",
                    "started_utc": datetime.now(timezone.utc).isoformat(),
                }
                item["exports"].append(export)
                write_manifest()
                export_started = time.monotonic()
                try:
                    result = process_runner(
                        export_command,
                        cwd=str(source_directories[sample["arm"]]),
                        env=environment,
                        capture_output=True,
                        text=True,
                        timeout=timeout_seconds,
                        check=False,
                    )
                    export["exit_code"] = result.returncode
                    export["stdout_file"], export["stdout_capture"] = capture(
                        result.stdout, stdout_export)
                    export["stderr_file"], export["stderr_capture"] = capture(
                        result.stderr, stderr_export)
                    _finish_sample(export, export_started)
                    output_text = _as_text(result.stdout) + _as_text(result.stderr)
                    no_match_diagnostic = (
                        label.startswith("source-")
                        and PPROF_NO_MATCH_DIAGNOSTIC in output_text.lower()
                    )
                    empty_source = no_match_diagnostic
                    if empty_source and result.returncode != PPROF_NO_MATCH_EXIT_STATUS:
                        export["status"] = "failed"
                        export["error"] = (
                            "go tool pprof no-match diagnostic had unexpected exit status "
                            f"{result.returncode}"
                        )
                        export_failed = True
                    elif result.returncode != 0 and not empty_source:
                        export["status"] = "failed"
                        export["error"] = f"go tool pprof exited with status {result.returncode}"
                        export_failed = True
                    elif label in ("flat-top", "cumulative-top") and not re.search(
                        r"\btotal\b", output_text, re.IGNORECASE
                    ):
                        export["status"] = "failed"
                        export["error"] = "go tool pprof produced no valid sample summary"
                        export_failed = True
                    elif empty_source:
                        export["status"] = "empty"
                        export["empty_reason"] = "profile contains no matching sampled source"
                    elif not output_text.strip():
                        export["status"] = "failed"
                        export["error"] = "go tool pprof produced no export output"
                        export_failed = True
                    else:
                        export["status"] = "complete"
                except KeyboardInterrupt as exc:
                    _finish_sample(export, export_started)
                    export["stdout_file"], export["stdout_capture"] = capture(
                        getattr(exc, "stdout", None), stdout_export)
                    export["stderr_file"], export["stderr_capture"] = capture(
                        getattr(exc, "stderr", None), stderr_export)
                    export["status"] = "interrupted"
                    export["error"] = "pprof export interrupted"
                    fail(item, export["error"], interrupted=True)
                    raise
                except Exception as exc:
                    _finish_sample(export, export_started)
                    export["stdout_file"], export["stdout_capture"] = capture(
                        getattr(exc, "stdout", None), stdout_export)
                    export["stderr_file"], export["stderr_capture"] = capture(
                        getattr(exc, "stderr", None), stderr_export)
                    export["status"] = "failed"
                    export["error"] = f"{type(exc).__name__}: {exc}"
                    export_failed = True
                write_manifest()
                if export_failed:
                    fail(item, export.get("error", "pprof export failed"))
                    raise RunnerError(item["error"])
            item["status"] = "complete"
            write_manifest()
    except KeyboardInterrupt:
        manifest["status"] = "interrupted"
        write_manifest()
        raise

    if len(manifest["runs"]) != 4 or any(run["status"] != "complete" for run in manifest["runs"]):
        manifest["status"] = "failed"
        manifest["error"] = "incomplete diagnostic profile set"
        write_manifest()
        raise RunnerError(manifest["error"])
    manifest["status"] = "complete"
    write_manifest()
    return manifest


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--gomaxprocs", type=int)
    parser.add_argument("--time-binary", default="/usr/bin/time")
    parser.add_argument("--profiles", action="store_true",
                        help="capture four post-timing CPU profiles and pprof exports")
    parser.add_argument("--timing-output", type=Path,
                        help="completed comparison directory required for --profiles")
    parser.add_argument("--build-metadata", type=Path,
                        help="build metadata required for --profiles")
    parser.add_argument("--baseline-source-dir", type=Path)
    parser.add_argument("--candidate-source-dir", type=Path)
    parser.add_argument("--go-command", default="go")
    parser.add_argument("--timeout-seconds", type=int, default=BENCHMARK_TIMEOUT_SECONDS)
    parser.add_argument("--resolve-commit", default=None,
                        help="resolve a revision input to a full commit SHA and exit")
    parser.add_argument("--default-commit", default="",
                        help="fallback commit when --resolve-commit is empty")
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    if args.resolve_commit is not None:
        try:
            print(resolve_commit_revision(
                args.resolve_commit, args.default_commit, args.repository))
        except (OSError, RunnerError) as exc:
            print(f"whole-main revision resolution failed: {exc}", file=sys.stderr)
            return 2
        return 0
    try:
        if args.profiles:
            required = {
                "--baseline": args.baseline,
                "--candidate": args.candidate,
                "--output": args.output,
                "--gomaxprocs": args.gomaxprocs,
                "--timing-output": args.timing_output,
                "--build-metadata": args.build_metadata,
                "--baseline-source-dir": args.baseline_source_dir,
                "--candidate-source-dir": args.candidate_source_dir,
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise RunnerError("--profiles requires " + ", ".join(missing))
            manifest = run_profiles(
                {"baseline": args.baseline, "candidate": args.candidate},
                args.output,
                args.gomaxprocs,
                args.timing_output,
                args.build_metadata,
                {"baseline": args.baseline_source_dir,
                 "candidate": args.candidate_source_dir},
                time_binary=args.time_binary,
                go_command=args.go_command,
                timeout_seconds=args.timeout_seconds,
            )
        else:
            required = {
                "--baseline": args.baseline,
                "--candidate": args.candidate,
                "--output": args.output,
                "--gomaxprocs": args.gomaxprocs,
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise RunnerError("comparison requires " + ", ".join(missing))
            manifest = run_comparison(
                {"baseline": args.baseline, "candidate": args.candidate},
                args.output,
                args.gomaxprocs,
                time_binary=args.time_binary,
            )
    except (OSError, RunnerError) as exc:
        print(f"whole-main write comparison setup failed: {exc}", file=sys.stderr)
        return 2
    if manifest["status"] != "complete":
        filename = "profiles.json" if args.profiles else "runs.json"
        label = "write profiles" if args.profiles else "write comparison"
        print(f"whole-main {label} {manifest['status']}; see {args.output / filename}",
              file=sys.stderr)
        return 1
    if args.profiles:
        print(f"whole-main write profiles complete: {args.output / 'profiles.json'}")
    else:
        print(f"whole-main write comparison complete: {args.output / 'comparison.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
