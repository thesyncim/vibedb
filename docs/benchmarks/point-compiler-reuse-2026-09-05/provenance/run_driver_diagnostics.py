#!/usr/bin/env python3
"""Build and run bounded Linux/ARM64 point-read CPU diagnostics.

The runner is deliberately fail-closed: it requires a new output directory,
uses only the pinned local image with --pull=never and --network=none, records
every command/result, and stops at the first failure. It does not run when
imported.
    """

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile
import re
from typing import Any, Mapping, Sequence


PINNED_IMAGE = (
    "golang:1.27-bookworm@sha256:"
    "648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b"
)
BEFORE_REVISION = "3a7d3b15dfc82579c7080b4acf95a12383718737"
BENCHMARK_NAME = "replicated_point_cpu_bench_test.go"
BENCHMARK_RELATIVE = Path("sql/driver") / BENCHMARK_NAME
NATIVE_TEST_REGEX = r"Test(ReplicatedPointRaw|ReplicatedPointReadSession|ReplicatedReadReuse)"
CPU_BENCH_REGEX = r"^BenchmarkReplicatedPointReadCPU/(point_hit|point_miss)$"
GO_VERSION = "go1.27.1"
TEST_TMP_VOLUME_DEFAULT = "vibedb-point-round-test-scratch"
PROJECT_ENV_DEFAULT = Path("/Users/thesyncim/.codex/bin/project-env")
NATIVE_REQUIRED_AFTER_PREFIXES = (
    "TestReplicatedPointRaw",
    "TestReplicatedPointReadSession",
    "TestReplicatedReadReuse",
)
NATIVE_REQUIRED_BEFORE_PREFIXES = (
    "TestReplicatedPointRaw",
    "TestReplicatedPointReadSession",
    "TestReplicatedReadReuse",
)
BENCHMARK_LINE = re.compile(
    r"^BenchmarkReplicatedPointReadCPU/(point_hit|point_miss)(?:-\d+)?\s+"
    r"(\d+)\s+([0-9]+(?:\.[0-9]+)?)\s+ns/op\s+"
    r"([0-9]+(?:\.[0-9]+)?)\s+B/op\s+"
    r"([0-9]+(?:\.[0-9]+)?)\s+allocs/op(?:\s|$)"
)


class RunnerError(RuntimeError):
    pass


@dataclass
class CommandResult:
    argv: list[str]
    returncode: int
    stdout_path: Path
    stderr_path: Path
    started_utc: str
    finished_utc: str

    def stdout_text(self) -> str:
        return self.stdout_path.read_text(encoding="utf-8", errors="replace")

    def stderr_text(self) -> str:
        return self.stderr_path.read_text(encoding="utf-8", errors="replace")


class Runner:
    def __init__(self, output: Path):
        self.output = output
        self.logs = output / "logs"
        self.logs.mkdir(parents=True, exist_ok=False)
        self.commands_path = output / "commands.jsonl"
        self.sequence = 0
        self.records: list[dict[str, Any]] = []

    @staticmethod
    def now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    def run(
        self,
        argv: Sequence[str | Path],
        name: str,
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        check: bool = True,
    ) -> CommandResult:
        args = [str(value) for value in argv]
        sequence = self.sequence
        self.sequence += 1
        safe_name = "".join(
            character if character.isalnum() or character in "-_" else "_"
            for character in name
        )
        prefix = f"{sequence:03d}-{safe_name}"
        stdout_path = self.logs / f"{prefix}.stdout"
        stderr_path = self.logs / f"{prefix}.stderr"
        started = self.now()
        print(f"[{sequence:03d}] {shlex.join(args)}", flush=True)
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            command_env = os.environ.copy()
            if env is not None:
                command_env.update(env)
            completed = subprocess.run(
                args,
                cwd=str(cwd) if cwd is not None else None,
                env=command_env,
                stdout=stdout,
                stderr=stderr,
                check=False,
            )
        finished = self.now()
        result = CommandResult(
            argv=args,
            returncode=completed.returncode,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            started_utc=started,
            finished_utc=finished,
        )
        record = {
            "sequence": sequence,
            "name": name,
            "argv": args,
            "command": shlex.join(args),
            "cwd": str(cwd) if cwd is not None else None,
            "started_utc": started,
            "finished_utc": finished,
            "exit_code": completed.returncode,
            "env_overrides": dict(env) if env is not None else {},
            "stdout": {
                "path": str(stdout_path.relative_to(self.output)),
                "sha256": self.sha256(stdout_path),
                "bytes": stdout_path.stat().st_size,
            },
            "stderr": {
                "path": str(stderr_path.relative_to(self.output)),
                "sha256": self.sha256(stderr_path),
                "bytes": stderr_path.stat().st_size,
            },
        }
        self.records.append(record)
        with self.commands_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
        if check and completed.returncode != 0:
            raise RunnerError(
                f"command {name!r} failed with exit {completed.returncode}; "
                f"see {stdout_path} and {stderr_path}"
            )
        return result

    def write_json(self, path: Path, value: Any) -> None:
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        temporary.replace(path)

    def write_manifest(self, manifest: dict[str, Any]) -> None:
        manifest["command_count"] = len(self.records)
        self.write_json(self.output / "manifest.json", manifest)


@dataclass
class SourceInfo:
    label: str
    path: Path
    revision: str
    tree: str
    benchmark_sha256: str
    status_lines: list[str]
    expected_revision: str | None
    staged_path: Path | None = None
    archive_path: Path | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "path": str(self.path),
            "staged_path": str(self.staged_path) if self.staged_path else None,
            "archive_path": str(self.archive_path) if self.archive_path else None,
            "revision": self.revision,
            "tree": self.tree,
            "benchmark": {
                "path": str(BENCHMARK_RELATIVE),
                "sha256": self.benchmark_sha256,
            },
            "status_lines": self.status_lines,
            "expected_revision": self.expected_revision,
        }


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-source", required=True, type=Path)
    parser.add_argument("--after-source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--before-revision", default=BEFORE_REVISION)
    parser.add_argument(
        "--after-revision",
        help="optional exact candidate revision; the runner always records the actual revision",
    )
    parser.add_argument(
        "--benchmark-source",
        type=Path,
        default=Path(__file__).with_name(BENCHMARK_NAME),
    )
    parser.add_argument(
        "--project-env", type=Path, default=PROJECT_ENV_DEFAULT,
        help="Codex project Go environment wrapper",
    )
    parser.add_argument(
        "--agent-id", default="luna-point-validation",
        help="stable temporary-directory identity for project-env",
    )
    parser.add_argument(
        "--project-cache-root", type=Path,
        help="optional CODEX_PROJECT_CACHE_ROOT override for project-env",
    )
    parser.add_argument("--image", default=PINNED_IMAGE)
    parser.add_argument("--module-cache", type=Path)
    parser.add_argument("--go-cache-directory", type=Path)
    parser.add_argument("--go-tmp-directory", type=Path)
    parser.add_argument("--test-tmp-volume", default=TEST_TMP_VOLUME_DEFAULT)
    parser.add_argument(
        "--reuse-binaries-from", type=Path,
        help="reuse the before/after binaries from a completed diagnostic output",
    )
    parser.add_argument(
        "--build-only", action="store_true",
        help="compile and record binaries, then stop before native tests and timings",
    )
    return parser.parse_args(argv)


def source_info(
    runner: Runner,
    label: str,
    source: Path,
    expected_revision: str | None,
    benchmark_source_sha256: str,
) -> SourceInfo:
    source = source.resolve()
    if not source.is_dir():
        raise RunnerError(f"{label} source is not a directory: {source}")
    revision_result = runner.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"], f"{label}-git-revision"
    )
    revision = revision_result.stdout_text().strip()
    tree_result = runner.run(
        ["git", "-C", str(source), "rev-parse", "HEAD^{tree}"], f"{label}-git-tree"
    )
    tree = tree_result.stdout_text().strip()
    status_result = runner.run(
        [
            "git", "-C", str(source), "status", "--porcelain=v1",
            "--untracked-files=all",
        ],
        f"{label}-git-status",
    )
    status_lines = [line for line in status_result.stdout_text().splitlines() if line]
    unexpected = status_lines
    if unexpected:
        raise RunnerError(
            f"{label} must be clean before staging; found worktree changes: {unexpected!r}"
        )
    runner.run(
        ["git", "-C", str(source), "diff", "--quiet"], f"{label}-git-diff"
    )
    runner.run(
        ["git", "-C", str(source), "diff", "--cached", "--quiet"],
        f"{label}-git-diff-cached",
    )
    if expected_revision is not None and revision != expected_revision:
        raise RunnerError(
            f"{label} revision {revision} does not match expected {expected_revision}"
        )
    if revision == "":
        raise RunnerError(f"{label} git revision was empty")
    return SourceInfo(
        label=label,
        path=source,
        revision=revision,
        tree=tree,
        benchmark_sha256=benchmark_source_sha256,
        status_lines=status_lines,
        expected_revision=expected_revision,
    )


def project_env_values(
    runner: Runner,
    project_env: Path,
    agent_id: str,
    project_cache_root: Path | None,
    source: Path,
) -> dict[str, Path]:
    if not project_env.is_file():
        raise RunnerError(f"project-env wrapper is missing: {project_env}")
    env = {"CODEX_AGENT_ID": agent_id}
    if project_cache_root is not None:
        env["CODEX_PROJECT_CACHE_ROOT"] = str(project_cache_root)
    result = runner.run(
        [project_env, "go", "env", "GOMODCACHE", "GOCACHE", "GOTMPDIR"],
        "project-env-go-cache",
        cwd=source,
        env=env,
    )
    values = [line.strip() for line in result.stdout_text().splitlines() if line.strip()]
    if len(values) != 3:
        raise RunnerError(
            "project-env go env returned unexpected values; "
            f"want GOMODCACHE/GOCACHE/GOTMPDIR, got {values!r}"
        )
    names = ("module_cache", "go_cache_directory", "go_tmp_directory")
    paths = {name: Path(value).resolve() for name, value in zip(names, values)}
    for name, path in paths.items():
        if not path.is_dir():
            raise RunnerError(f"project-env {name} is not a directory: {path}")
    return paths


def stage_source(
    runner: Runner,
    info: SourceInfo,
    benchmark_source: Path,
    output: Path,
) -> None:
    """Stage a clean committed source tree and inject the diagnostic benchmark."""
    if info.staged_path is not None:
        raise RunnerError(f"{info.label} source was staged more than once")
    staged = output / "sources" / info.label
    staged.mkdir(parents=True, exist_ok=False)
    archive = output / "sources" / f"{info.label}.tar"
    runner.run(
        [
            "git", "-C", str(info.path), "archive", "--format=tar",
            f"--output={archive}", info.revision,
        ],
        f"{info.label}-git-archive",
    )
    with tarfile.open(archive, mode="r:") as stream:
        members = stream.getmembers()
        for member in members:
            target = (staged / member.name).resolve()
            try:
                target.relative_to(staged.resolve())
            except ValueError as exc:
                raise RunnerError(
                    f"{info.label} git archive member escapes staging directory: {member.name!r}"
                ) from exc
        stream.extractall(staged)
    benchmark = staged / BENCHMARK_RELATIVE
    benchmark.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(benchmark_source, benchmark)
    digest = sha256(benchmark)
    if digest != info.benchmark_sha256:
        raise RunnerError(
            f"{info.label} staged benchmark hash {digest} differs from "
            f"standalone source {info.benchmark_sha256}"
        )
    info.staged_path = staged
    info.archive_path = archive


def docker_base(
    image: str,
    module_cache: Path,
    go_cache_directory: Path,
    go_tmp_directory: Path,
    *,
    output: Path | None = None,
    source: Path | None = None,
    test_tmp_volume: str | None = None,
) -> list[str]:
    command = [
        "docker", "run", "--rm", "--pull=never", "--network=none",
        "--platform", "linux/arm64",
    ]
    if source is not None:
        command += ["--mount", f"type=bind,src={source},dst=/src,readonly"]
    if output is not None:
        command += ["--mount", f"type=bind,src={output},dst=/out"]
    if test_tmp_volume is not None:
        command += ["--mount", f"type=volume,src={test_tmp_volume},dst=/testtmp"]
    command += [
        "--mount", f"type=bind,src={module_cache},dst=/go/pkg/mod,readonly",
        "--mount", f"type=bind,src={go_cache_directory},dst=/go/cache",
        "--mount", f"type=bind,src={go_tmp_directory},dst=/go/tmp",
    ]
    return command


def build_command(
    image: str,
    module_cache: Path,
    go_cache_directory: Path,
    go_tmp_directory: Path,
    source: SourceInfo,
    output: Path,
    binary_name: str,
) -> list[str]:
    if source.staged_path is None:
        raise RunnerError(f"{source.label} source has not been staged")
    command = docker_base(
        image, module_cache, go_cache_directory, go_tmp_directory,
        output=output, source=source.staged_path,
    )
    command += [
        "--workdir", "/src",
        "--env", "GOOS=linux",
        "--env", "GOARCH=arm64",
        "--env", "CGO_ENABLED=0",
        "--env", "GOEXPERIMENT=simd",
        "--env", "GOTOOLCHAIN=local",
        "--env", "GOMODCACHE=/go/pkg/mod",
        "--env", "GOCACHE=/go/cache",
        "--env", "GOTMPDIR=/go/tmp",
        image,
        "go", "test", "-c", "-mod=readonly", "-trimpath",
        "-o", f"/out/bin/{binary_name}", "./sql/driver",
    ]
    return command


def binary_metadata(
    runner: Runner,
    project_env: Path,
    project_env_overrides: Mapping[str, str],
    cwd: Path,
    image: str,
    module_cache: Path,
    go_cache_directory: Path,
    go_tmp_directory: Path,
    output: Path,
    binary_name: str,
) -> dict[str, Any]:
    binary = output / "bin" / binary_name
    if not binary.is_file():
        raise RunnerError(f"compiled binary is missing: {binary}")
    version = runner.run(
        project_command(
            project_env,
            docker_base(
                image, module_cache, go_cache_directory, go_tmp_directory, output=output
            ) + [image, "go", "version", "-m", f"/out/bin/{binary_name}"],
        ),
        f"{binary_name}-version-m",
        cwd=cwd,
        env=project_env_overrides,
    )
    return {
        "path": str(binary.relative_to(output)),
        "sha256": sha256(binary),
        "bytes": binary.stat().st_size,
        "go_version_metadata_log": str(version.stdout_path.relative_to(output)),
    }


def binary_run_command(
    image: str,
    module_cache: Path,
    go_cache_directory: Path,
    go_tmp_directory: Path,
    test_tmp_volume: str,
    output: Path,
    binary_name: str,
    args: Sequence[str],
) -> list[str]:
    command = docker_base(
        image, module_cache, go_cache_directory, go_tmp_directory, output=output,
        test_tmp_volume=test_tmp_volume,
    )
    command += [
        "--env", "TMPDIR=/testtmp",
        "--workdir", "/out", image, f"/out/bin/{binary_name}",
    ]
    command += list(args)
    return command


def native_regression_evidence(
    result: CommandResult, required_prefixes: Sequence[str]
) -> dict[str, Any]:
    text = result.stdout_text()
    started = re.findall(r"^\s*=== RUN\s+(\S+)", text, flags=re.MULTILINE)
    passed = re.findall(r"^\s*--- PASS:\s+(\S+)", text, flags=re.MULTILINE)
    skipped = re.findall(r"^\s*--- SKIP:\s+(\S+)", text, flags=re.MULTILINE)
    if skipped:
        raise RunnerError(f"native regression skipped tests: {skipped!r}")
    if not started:
        raise RunnerError("native regression ran no tests")
    missing = [name for name in started if name not in passed]
    if missing:
        raise RunnerError(f"native regression tests did not pass: {missing!r}")
    missing_prefixes = [
        prefix for prefix in required_prefixes
        if not any(name == prefix or name.startswith(prefix) for name in passed)
    ]
    if missing_prefixes:
        raise RunnerError(
            f"native regression missed required test families: {missing_prefixes!r}"
        )
    return {
        "started": started,
        "passed": passed,
        "skipped": skipped,
    }


def benchmark_evidence(
    result: CommandResult,
    expected_names: Sequence[str],
    expected_count: int,
) -> list[dict[str, Any]]:
    rows = []
    for line in result.stdout_text().splitlines():
        match = BENCHMARK_LINE.match(line)
        if match is None:
            continue
        name, iterations, ns_per_op, bytes_per_op, allocs_per_op = match.groups()
        rows.append({
            "name": name,
            "iterations": int(iterations),
            "ns_per_op": float(ns_per_op),
            "bytes_per_op": float(bytes_per_op),
            "allocs_per_op": float(allocs_per_op),
        })
    expected = set(expected_names)
    counts = {name: sum(row["name"] == name for row in rows) for name in expected}
    if set(row["name"] for row in rows) != expected or any(
        count != expected_count for count in counts.values()
    ):
        raise RunnerError(
            f"benchmark output rows {rows!r} do not contain exactly "
            f"{expected_count} rows for each {sorted(expected)!r}"
        )
    if any(
        row["iterations"] <= 0
        or row["ns_per_op"] <= 0
        or row["bytes_per_op"] < 0
        or row["allocs_per_op"] < 0
        for row in rows
    ):
        raise RunnerError(f"benchmark output contains non-positive measurements: {rows!r}")
    return rows


def load_reuse_manifest(path: Path) -> dict[str, Any]:
    path = path.resolve()
    if not path.is_dir():
        raise RunnerError(f"reuse output is not a directory: {path}")
    manifest_path = path / "manifest.json"
    if not manifest_path.is_file():
        raise RunnerError(f"reuse output has no manifest: {manifest_path}")
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"cannot read reuse manifest {manifest_path}: {exc}") from exc
    if not isinstance(document, dict):
        raise RunnerError(f"reuse manifest is not an object: {manifest_path}")
    return document


def validate_reuse_manifest(
    reuse_dir: Path,
    document: dict[str, Any],
    before_info: SourceInfo,
    after_info: SourceInfo,
    benchmark_digest: str,
) -> dict[str, dict[str, Any]]:
    if document.get("status") not in {"built", "complete"}:
        raise RunnerError("reuse manifest is not built or complete")
    if document.get("runtime_image") != PINNED_IMAGE:
        raise RunnerError("reuse manifest does not use the pinned runtime image")
    if GO_VERSION not in str(document.get("go_version", "")):
        raise RunnerError("reuse manifest does not record Go 1.27.1")
    standalone = document.get("standalone_benchmark")
    if not isinstance(standalone, dict) or standalone.get("sha256") != benchmark_digest:
        raise RunnerError("reuse manifest benchmark hash does not match the standalone source")
    sources = document.get("sources")
    if not isinstance(sources, dict):
        raise RunnerError("reuse manifest has no source provenance")
    for label, info in (("before", before_info), ("after", after_info)):
        entry = sources.get(label)
        if not isinstance(entry, dict) or entry.get("revision") != info.revision:
            raise RunnerError(f"reuse manifest {label} source revision does not match")
        benchmark = entry.get("benchmark")
        if not isinstance(benchmark, dict) or benchmark.get("sha256") != benchmark_digest:
            raise RunnerError(f"reuse manifest {label} benchmark hash does not match")
    binaries = document.get("binaries")
    if not isinstance(binaries, dict):
        raise RunnerError("reuse manifest has no binary provenance")
    result = {}
    for label in ("before", "after"):
        metadata = binaries.get(label)
        if not isinstance(metadata, dict):
            raise RunnerError(f"reuse manifest has no {label} binary metadata")
        relative = metadata.get("path")
        expected_hash = metadata.get("sha256")
        expected_bytes = metadata.get("bytes")
        if not isinstance(relative, str) or not isinstance(expected_hash, str):
            raise RunnerError(f"reuse manifest {label} binary metadata is incomplete")
        source = (reuse_dir / relative).resolve()
        try:
            source.relative_to(reuse_dir)
        except ValueError as exc:
            raise RunnerError(f"reuse manifest {label} binary escapes its output") from exc
        if not source.is_file() or sha256(source) != expected_hash:
            raise RunnerError(f"reuse manifest {label} binary hash does not match")
        if expected_bytes is not None and source.stat().st_size != expected_bytes:
            raise RunnerError(f"reuse manifest {label} binary size does not match")
        result[label] = {
            "source": source,
            "sha256": expected_hash,
            "bytes": source.stat().st_size,
            "reused_from": str(source),
        }
    return result


def copy_reuse_binaries(
    output: Path, metadata: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    destination_dir = output / "bin"
    destination_dir.mkdir()
    result = {}
    for label, entry in metadata.items():
        destination = destination_dir / f"{label}-driver.test"
        shutil.copy2(entry["source"], destination)
        digest = sha256(destination)
        if digest != entry["sha256"]:
            raise RunnerError(f"copied {label} binary hash changed")
        result[label] = {
            "path": str(destination.relative_to(output)),
            "sha256": digest,
            "bytes": destination.stat().st_size,
            "reused_from": entry["reused_from"],
        }
    return result


def project_command(project_env: Path, argv: Sequence[str | Path]) -> list[str]:
    """Run host tools through the shared project environment wrapper."""
    return [str(project_env), *[str(value) for value in argv]]


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    before = args.before_source.resolve()
    after = args.after_source.resolve()
    output = args.output.resolve()
    benchmark_source = args.benchmark_source.resolve()
    project_env = args.project_env.resolve()
    project_cache_root = (
        args.project_cache_root.resolve()
        if args.project_cache_root is not None else None
    )
    reuse_dir = (
        args.reuse_binaries_from.resolve()
        if args.reuse_binaries_from is not None else None
    )

    if not output.is_absolute():
        raise RunnerError("--output must be an absolute path")
    if before == after:
        raise RunnerError("before and after sources must be different paths")
    if args.build_only and reuse_dir is not None:
        raise RunnerError("--build-only cannot be combined with --reuse-binaries-from")
    if output.exists():
        raise RunnerError(f"refusing to overwrite existing output: {output}")
    if not benchmark_source.is_file():
        raise RunnerError(f"standalone benchmark source is missing: {benchmark_source}")
    if args.image != PINNED_IMAGE:
        raise RunnerError(
            "image must be the pinned Go 1.27 bookworm digest: "
            f"{PINNED_IMAGE}; got {args.image}"
        )

    output.mkdir(parents=True)
    runner = Runner(output)
    project_env_overrides = {"CODEX_AGENT_ID": args.agent_id}
    if project_cache_root is not None:
        project_env_overrides["CODEX_PROJECT_CACHE_ROOT"] = str(project_cache_root)
    project_paths = project_env_values(
        runner, project_env, args.agent_id, project_cache_root, before,
    )
    module_cache = (
        args.module_cache.resolve()
        if args.module_cache is not None else project_paths["module_cache"]
    )
    go_cache_directory = (
        args.go_cache_directory.resolve()
        if args.go_cache_directory is not None
        else project_paths["go_cache_directory"]
    )
    go_tmp_directory = (
        args.go_tmp_directory.resolve()
        if args.go_tmp_directory is not None
        else project_paths["go_tmp_directory"]
    )
    if not module_cache.is_dir():
        raise RunnerError(f"readonly module cache is missing: {module_cache}")
    if not go_cache_directory.is_dir():
        raise RunnerError(f"shared Go cache directory is missing: {go_cache_directory}")
    if not go_tmp_directory.is_dir():
        raise RunnerError(f"agent Go temporary directory is missing: {go_tmp_directory}")
    manifest: dict[str, Any] = {
        "schema": "vibedb.point-read-driver-diagnostics/2",
        "status": "preparing",
        "started_utc": runner.now(),
        "diagnostic_only": True,
        "timing_scope": (
            "CPU benchmark timer covers Acquire, QueryCandidateKeysInto, complete "
            "cursor JSON verification, cursor Close, and lease Finish; setup is outside it."
        ),
        "no_wire_encoding_claim": True,
        "no_network_downloads": True,
        "runtime_image": args.image,
        "project_env": str(project_env),
        "agent_id": args.agent_id,
        "project_cache_root": str(project_cache_root) if project_cache_root else None,
        "module_cache": str(module_cache),
        "go_cache_directory": str(go_cache_directory),
        "go_tmp_directory": str(go_tmp_directory),
        "test_tmp_volume": args.test_tmp_volume,
        "reuse_binaries_from": str(reuse_dir) if reuse_dir is not None else None,
        "build_only": args.build_only,
        "sources": {},
        "binaries": {},
        "runs": [],
    }
    try:
        benchmark_digest = sha256(benchmark_source)
        manifest["standalone_benchmark"] = {
            "path": str(benchmark_source),
            "sha256": benchmark_digest,
            "relative_destination": str(BENCHMARK_RELATIVE),
        }
        before_info = source_info(
            runner, "before", before, args.before_revision, benchmark_digest
        )
        after_info = source_info(
            runner, "after", after, args.after_revision, benchmark_digest
        )
        if before_info.revision == after_info.revision:
            raise RunnerError("before and after sources resolve to the same revision")
        stage_source(runner, before_info, benchmark_source, output)
        stage_source(runner, after_info, benchmark_source, output)
        manifest["sources"] = {
            "before": before_info.as_dict(),
            "after": after_info.as_dict(),
        }
        reuse_manifest = None
        reuse_metadata = None
        if reuse_dir is not None:
            reuse_manifest = load_reuse_manifest(reuse_dir)
            reuse_metadata = validate_reuse_manifest(
                reuse_dir, reuse_manifest, before_info, after_info, benchmark_digest,
            )
            manifest["reused_binary_manifest"] = {
                "path": str(reuse_dir / "manifest.json"),
                "sha256": sha256(reuse_dir / "manifest.json"),
            }
        runner.write_manifest(manifest)

        runner.run(
            project_command(
                project_env,
                ["docker", "image", "inspect", args.image, "--format", "{{json .RepoDigests}}"],
            ),
            "docker-image-inspect",
            cwd=before,
            env=project_env_overrides,
        )
        manifest["go_cache_mount"] = {
            "host": str(go_cache_directory),
            "container": "/go/cache",
            "mode": "bind",
        }
        manifest["go_tmp_mount"] = {
            "host": str(go_tmp_directory),
            "container": "/go/tmp",
            "mode": "bind",
            "agent_specific": True,
        }
        runner.run(
            project_command(
                project_env,
                [
                    "docker", "volume", "create",
                    "--label", "vibedb.point-round=diagnostic",
                    args.test_tmp_volume,
                ],
            ),
            "docker-test-tmp-volume-create",
            cwd=before,
            env=project_env_overrides,
        )
        test_tmp_info = runner.run(
            project_command(
                project_env, ["docker", "volume", "inspect", args.test_tmp_volume]
            ),
            "docker-test-tmp-volume-inspect",
            cwd=before,
            env=project_env_overrides,
        )
        try:
            test_tmp_records = json.loads(test_tmp_info.stdout_text())
            test_tmp_labels = test_tmp_records[0].get("Labels") or {}
        except (ValueError, IndexError, TypeError, AttributeError) as exc:
            raise RunnerError("could not parse dedicated Docker test scratch metadata") from exc
        if test_tmp_labels.get("vibedb.point-round") != "diagnostic":
            raise RunnerError(
                f"Docker test scratch volume {args.test_tmp_volume!r} is not labeled as owned diagnostic storage"
            )
        manifest["docker_test_tmp_volume_inspect_log"] = str(
            test_tmp_info.stdout_path.relative_to(output)
        )
        go_version = runner.run(
            project_command(
                project_env,
                docker_base(
                    args.image, module_cache, go_cache_directory, go_tmp_directory
                ) + [args.image, "go", "version"],
            ),
            "docker-go-version",
            cwd=before,
            env=project_env_overrides,
        )
        version_text = go_version.stdout_text().strip()
        if GO_VERSION not in version_text:
            raise RunnerError(f"pinned image reported {version_text!r}, expected {GO_VERSION}")
        manifest["go_version"] = version_text
        runner.write_manifest(manifest)

        if reuse_metadata is not None:
            manifest["binaries"] = copy_reuse_binaries(output, reuse_metadata)
            runner.write_manifest(manifest)
        else:
            (output / "bin").mkdir()
            for label, info in (("before", before_info), ("after", after_info)):
                binary_name = f"{label}-driver.test"
                runner.run(
                    project_command(
                        project_env,
                        build_command(
                            args.image, module_cache, go_cache_directory,
                            go_tmp_directory, info, output, binary_name,
                        ),
                    ),
                    f"{label}-driver-test-compile",
                    cwd=info.path,
                    env=project_env_overrides,
                )
                manifest["binaries"][label] = binary_metadata(
                    runner, project_env, project_env_overrides, info.path,
                    args.image, module_cache, go_cache_directory,
                    go_tmp_directory, output, binary_name,
                )
                runner.write_manifest(manifest)

        if args.build_only:
            manifest["status"] = "built"
            manifest["finished_utc"] = runner.now()
            runner.write_manifest(manifest)
            print(json.dumps({"status": manifest["status"], "output": str(output)}, indent=2))
            return 0

        for label in ("before", "after"):
            binary_name = f"{label}-driver.test"
            native_result = runner.run(
                project_command(
                    project_env,
                    binary_run_command(
                        args.image, module_cache, go_cache_directory,
                        go_tmp_directory, args.test_tmp_volume, output,
                        binary_name,
                        ["-test.v", "-test.count=1", "-test.run", NATIVE_TEST_REGEX],
                    ),
                ),
                f"{label}-native-regression",
                cwd=before,
                env=project_env_overrides,
            )
            native_prefixes = (
                NATIVE_REQUIRED_AFTER_PREFIXES
                if label == "after" else NATIVE_REQUIRED_BEFORE_PREFIXES
            )
            native_evidence = native_regression_evidence(native_result, native_prefixes)
            manifest["runs"].append({
                "kind": "native-regression",
                "arm": label,
                "regex": NATIVE_TEST_REGEX,
                "evidence": native_evidence,
            })
            runner.write_manifest(manifest)

        for arm in ("before", "after", "after", "before"):
            ordinal = sum(1 for run in manifest["runs"]
                          if run.get("kind") == "cpu-abba" and run.get("arm") == arm) + 1
            binary_name = f"{arm}-driver.test"
            benchmark_result = runner.run(
                project_command(
                    project_env,
                    binary_run_command(
                        args.image, module_cache, go_cache_directory,
                        go_tmp_directory, args.test_tmp_volume, output,
                        binary_name,
                        [
                            "-test.run", "^$",
                            "-test.bench", CPU_BENCH_REGEX,
                            "-test.benchtime", "5000x",
                            "-test.count", "3",
                            "-test.cpu", "1",
                        ],
                    ),
                ),
                f"cpu-abba-{arm}-{ordinal}",
                cwd=before,
                env=project_env_overrides,
            )
            benchmark_rows = benchmark_evidence(
                benchmark_result, ("point_hit", "point_miss"), 3,
            )
            manifest["runs"].append({
                "kind": "cpu-abba",
                "arm": arm,
                "ordinal": ordinal,
                "benchmark": CPU_BENCH_REGEX,
                "benchtime": "5000x",
                "count": 3,
                "cpu": 1,
                "rows": benchmark_rows,
            })
            runner.write_manifest(manifest)

        (output / "profiles").mkdir()
        profile_path = "/out/profiles/after-point-miss.cpu.pprof"
        profile_result = runner.run(
            project_command(
                project_env,
                binary_run_command(
                    args.image, module_cache, go_cache_directory,
                    go_tmp_directory, args.test_tmp_volume, output,
                    "after-driver.test",
                    [
                        "-test.run", "^$",
                        "-test.bench", r"^BenchmarkReplicatedPointReadCPU/point_miss$",
                        "-test.benchtime", "5s",
                        "-test.count", "1",
                        "-test.cpu", "1",
                        "-test.cpuprofile", profile_path,
                    ],
                ),
            ),
            "candidate-point-miss-cpuprofile",
            cwd=before,
            env=project_env_overrides,
        )
        profile_rows = benchmark_evidence(profile_result, ("point_miss",), 1)
        profile = output / "profiles" / "after-point-miss.cpu.pprof"
        if not profile.is_file() or profile.stat().st_size == 0:
            raise RunnerError(f"candidate CPU profile was not produced: {profile}")
        manifest["runs"].append({
            "kind": "candidate-cpuprofile",
            "arm": "after",
            "benchmark": r"^BenchmarkReplicatedPointReadCPU/point_miss$",
            "benchtime": "5s",
            "count": 1,
            "cpu": 1,
            "profile": {
                "path": str(profile.relative_to(output)),
                "sha256": sha256(profile),
                "bytes": profile.stat().st_size,
            },
            "rows": profile_rows,
        })
        manifest["status"] = "complete"
        manifest["finished_utc"] = runner.now()
        runner.write_manifest(manifest)
        print(json.dumps({"status": manifest["status"], "output": str(output)}, indent=2))
        return 0
    except BaseException as exc:
        manifest["status"] = "failed"
        manifest["finished_utc"] = runner.now()
        manifest["fatal_error"] = f"{type(exc).__name__}: {exc}"
        runner.write_manifest(manifest)
        print(f"run_driver_diagnostics: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
