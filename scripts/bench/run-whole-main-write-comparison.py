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
    return metrics


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


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--gomaxprocs", type=int, required=True)
    parser.add_argument("--time-binary", default="/usr/bin/time")
    args = parser.parse_args(argv)
    try:
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
        print(f"whole-main write comparison {manifest['status']}; see {args.output / 'runs.json'}",
              file=sys.stderr)
        return 1
    print(f"whole-main write comparison complete: {args.output / 'comparison.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
