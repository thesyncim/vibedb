#!/usr/bin/env python3
"""Capture one current-source RF3 point-read CPU/trace diagnostic.

This deliberately reuses the reviewed fused-node fixture.  It runs only the
candidate arm internally, because this artifact is a materialization profile,
not a before/after timing campaign.  The fixture still performs its normal
setup, topology, report, and oracle validation.
"""

from datetime import datetime, timezone
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys


ROOT = Path(__file__).resolve().parent
FIXTURE_RELATIVE = Path("scripts/bench/run-fused-node-comparison.py")
VALIDATOR_RELATIVE = Path("scripts/bench/summarize-crdb-sql.py")
PROFILE_ENV = {
    "VIBEDB_PROFILE_DIRECTORY": "/evidence",
    "VIBEDB_PROFILE_DURATION": "90s",
}
WORKLOADS = ("point_hit", "point_miss")


def load_fixture(source):
    fixture_path = source / FIXTURE_RELATIVE
    spec = importlib.util.spec_from_file_location("point_range_profile_fixture", fixture_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load fixture {fixture_path}")
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    return fixture


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def source_info(fixture, source, revision):
    info = fixture.git_info(str(source))
    actual = info["revision"].strip()
    if actual != revision:
        raise fixture.RunnerError(
            f"source revision {actual} does not match requested {revision}"
        )
    if info["dirty"] or info["status"]:
        raise fixture.RunnerError(
            f"source checkout is not clean: {info['status'].splitlines()}"
        )
    patch = info.pop("patch")
    info["patch_sha256"] = hashlib.sha256(patch).hexdigest()
    return info


def binary_hashes(binary_dir):
    if not binary_dir.is_dir():
        return {}
    return {
        path.name: {"sha256": sha256(path), "bytes": path.stat().st_size}
        for path in sorted(binary_dir.iterdir()) if path.is_file()
    }


REQUIRED_BINARIES = (
    "candidate-vibedb",
    "candidate-vibedb-shard",
    "candidate-vibedb-gateway",
    "rf3-sqlbench",
)
COMMON_CLIENT_BINARY = "rf3-sqlbench"
SERVER_BINARIES = REQUIRED_BINARIES[:-1]


def _mapping(document, key, label):
    value = document.get(key)
    if not isinstance(value, dict):
        raise RuntimeError(f"comparison manifest {label} is missing object {key!r}")
    return value


def _clean_source_entry(source, role):
    entry = source.get(role)
    if not isinstance(entry, dict):
        raise RuntimeError(f"comparison manifest is missing source.{role}")
    if entry.get("dirty") is not False or entry.get("status") not in ("", None):
        raise RuntimeError(f"comparison manifest source.{role} is not clean")
    revision = entry.get("revision")
    if not isinstance(revision, str) or not revision:
        raise RuntimeError(f"comparison manifest source.{role}.revision is missing")
    return entry


def _comparison_binary_hashes(document, requested_revision):
    """Validate the completed matched-campaign after arm and shared client.

    The comparison runner copies both VibeDB revisions into the same candidate
    fixture names and builds one shared rf3-sqlbench.  The profile must consume
    the retained after-bin plus that exact client; accepting a manifest that
    merely mentions the requested revision would allow a mixed-arm profile.
    """
    if document.get("status") != "complete":
        raise RuntimeError("comparison binary manifest is not complete")
    source = _mapping(document, "source", "source")
    refs = _mapping(document, "refs", "refs")
    after = _clean_source_entry(source, "after")
    _clean_source_entry(source, "before")
    client = _clean_source_entry(source, "client")
    if after.get("revision") != requested_revision:
        raise RuntimeError(
            "comparison manifest after revision does not match requested source "
            f"{requested_revision}"
        )
    if refs.get("after") != requested_revision:
        raise RuntimeError(
            "comparison manifest refs.after does not match requested source "
            f"{requested_revision}"
        )
    for role in ("before", "after"):
        if refs.get(role) != source[role].get("revision"):
            raise RuntimeError(
                f"comparison manifest refs.{role} does not match source.{role}.revision"
            )
    client_revision = refs.get("client_source_revision")
    if client_revision != client.get("revision"):
        raise RuntimeError("comparison manifest shared client revision is inconsistent")

    fixture_hashes = _mapping(document, "fixture_binary_sha256", "fixture_binary_sha256")
    before_hashes = _mapping(fixture_hashes, "before", "fixture_binary_sha256")
    after_hashes = _mapping(fixture_hashes, "after", "fixture_binary_sha256")
    build_hashes = _mapping(document, "build_binary_sha256", "build_binary_sha256")
    for name in REQUIRED_BINARIES:
        if not isinstance(after_hashes.get(name), str) or not after_hashes[name]:
            raise RuntimeError(f"comparison manifest after hash is missing for {name}")
        if build_hashes.get(name) != after_hashes[name]:
            raise RuntimeError(f"comparison manifest build/after hash differs for {name}")
    if before_hashes.get(COMMON_CLIENT_BINARY) != after_hashes.get(COMMON_CLIENT_BINARY):
        raise RuntimeError("before and after do not use one common client binary")
    if build_hashes.get(COMMON_CLIENT_BINARY) != after_hashes.get(COMMON_CLIENT_BINARY):
        raise RuntimeError("shared client build hash does not match the after arm")

    return {
        "format": "vibedb.distributed-read-comparison",
        "path": None,
        "source_revisions": {
            "before": source["before"]["revision"],
            "after": after["revision"],
            "client": client["revision"],
        },
        "selected_arm": "after",
        "selected_revision": requested_revision,
        "common_client": {
            "binary": COMMON_CLIENT_BINARY,
            "revision": client_revision,
            "sha256": after_hashes[COMMON_CLIENT_BINARY],
        },
        "binaries": {name: after_hashes[name] for name in REQUIRED_BINARIES},
    }


def binary_provenance(binary_dir, requested_revision, manifest_path):
    if not binary_dir.is_dir():
        raise RuntimeError(f"binary directory is missing: {binary_dir}")
    manifest_path = manifest_path.resolve()
    if not manifest_path.is_file():
        raise RuntimeError(
            f"frozen binary manifest is missing; expected {manifest_path}"
        )
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"cannot read binary manifest {manifest_path}: {exc}") from exc
    if not isinstance(document, dict):
        raise RuntimeError(f"binary manifest is not an object: {manifest_path}")
    comparison = _comparison_binary_hashes(document, requested_revision)
    actual = binary_hashes(binary_dir)
    missing = [name for name in REQUIRED_BINARIES if name not in actual]
    if missing:
        raise RuntimeError(f"binary directory is missing required files: {missing}")
    for name in REQUIRED_BINARIES:
        expected_hash = comparison["binaries"][name]
        if expected_hash != actual[name]["sha256"]:
            raise RuntimeError(f"binary hash mismatch for {name}")
    comparison["path"] = str(manifest_path)
    comparison["sha256"] = sha256(manifest_path)
    return {
        **comparison,
        "binaries": {name: actual[name] for name in REQUIRED_BINARIES},
    }


def profile_artifacts(run_dir):
    artifacts = []
    raw = run_dir / "raw"
    if not raw.is_dir():
        return artifacts
    for path in sorted(raw.rglob("*")):
        if path.is_file() and (path.name.endswith(".cpu.pprof") or path.name.endswith(".trace")):
            artifacts.append({
                "path": str(path.relative_to(run_dir)),
                "sha256": sha256(path),
                "bytes": path.stat().st_size,
            })
    return artifacts


def now():
    return datetime.now(timezone.utc).isoformat()


def build_fixture_args(fixture, config, output, source, revision):
    return fixture.parser().parse_args([
        str(output),
        "--repo", str(source),
        "--client-source", str(source),
        "--candidate-ref", revision,
        "--matrix", "all",
        "--groups", str(config.groups),
        "--physical-nodes", str(config.physical_nodes),
        "--distributions", "uniform",
        "--endpoint-modes", "single",
        "--multigroup-workloads", config.workload,
        "--multigroup-clients", config.clients,
        "--rows", str(config.rows),
        "--operations", str(config.operations),
        "--scans", str(config.scans),
        "--warmup", str(config.warmup),
        "--repetitions", str(config.repetitions),
        "--cpus", config.cpus,
        "--memory", config.memory,
        "--order", "candidate-first",
        "--no-include-crdb",
        "--vibedb-sql-ports", "5432",
    ])


def parser():
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("output", type=Path,
                       help="new absolute evidence directory")
    value.add_argument("--source", required=True, type=Path,
                       help="clean source checkout whose exact revision is profiled")
    value.add_argument("--revision", required=True,
                       help="exact source HEAD revision expected by the binary manifest")
    value.add_argument("--bin", required=True, dest="binary_dir", type=Path,
                       help="directory containing the candidate server and client binaries")
    value.add_argument("--binary-manifest", "--manifest", dest="binary_manifest",
                       required=True, type=Path,
                       help="completed matched before/after comparison manifest")
    value.add_argument("--workload", choices=WORKLOADS, default="point_miss")
    value.add_argument("--clients", default="8")
    value.add_argument("--rows", type=int, default=8192)
    value.add_argument("--operations", type=int, default=20000)
    value.add_argument("--scans", type=int, default=1)
    value.add_argument("--warmup", type=int, default=1000)
    value.add_argument("--repetitions", type=int, default=1)
    value.add_argument("--groups", type=int, default=4)
    value.add_argument("--physical-nodes", type=int, default=3)
    value.add_argument("--cpus", default="12")
    value.add_argument("--memory", default="24g")
    return value


def manifest_for(config, output, fixture, source, revision, binary_dir,
                 binary_manifest, arch=None, docker_host=None, image=None):
    return {
        "schema": "vibedb.point-profile/2",
        "started_utc": now(),
        "status": "preparing",
        "diagnostic_only": True,
        "timings_are_not_benchmark_results": True,
        "startup_included": True,
        "startup_exclusion": (
            "The existing processprofile hook starts with the server and has no delayed-start option; "
            "startup/setup remains in CPU and trace artifacts."
        ),
        "source": {"revision": revision, "worktree": str(source),
                    "git": source_info(fixture, source, revision)},
        "binaries": binary_hashes(binary_dir),
        "binary_provenance": binary_manifest,
        "workload": {
            "workload": config.workload,
            "clients": config.clients,
            "rows_per_table": config.rows,
            "operations": config.operations,
            "scans": config.scans,
            "warmup": config.warmup,
            "repetitions": config.repetitions,
            "groups": config.groups,
            "physical_nodes": config.physical_nodes,
        },
        "topology": {
            "replication_factor": 3,
            "single_host_fixture": True,
            "internal_fixture_engine": "candidate",
            "comparison_arm": "none; candidate-only diagnostic",
            "publication": False,
            "horizontal_multimachine_scaling": False,
        },
        "limits": {"cpus": config.cpus, "memory": config.memory, "memory_swap": config.memory},
        "profile": {
            **PROFILE_ENV,
            "recording": "CPU pprof plus execution trace from inherited VibeDB server processes",
            "heap_or_allocation_profile": False,
            "allocation_evidence": "source/CPU targets only; no allocation counts are emitted by this hook",
        },
        "docker": {
            "architecture": arch,
            "host": docker_host,
            "runtime_image": image or fixture.RUNTIME,
        },
        "controls": {},
    }


def main(argv=None):
    outer = parser().parse_args(argv)
    source = outer.source.resolve()
    revision = outer.revision.strip()
    if not revision:
        raise RuntimeError("--revision must not be empty")
    binary_dir = outer.binary_dir.resolve()
    fixture = load_fixture(source)
    source_info_value = source_info(fixture, source, revision)
    binary_manifest = binary_provenance(
        binary_dir, revision, outer.binary_manifest,
    )
    output = fixture.require_new_directory(outer.output)
    fixture.COMMAND_LOG = output / "control-commands.jsonl"
    controls = output / "controls"
    controls.mkdir(mode=0o700)
    config = outer
    fixture_args = build_fixture_args(fixture, config, output, source, revision)
    validator_path = source / VALIDATOR_RELATIVE
    if not validator_path.is_file():
        raise RuntimeError(f"source is missing validator {validator_path}")
    fixture_args.validator_path = validator_path

    manifest = manifest_for(
        config, output, fixture, source, revision, binary_dir, binary_manifest,
    )
    control_paths = [
        (Path(__file__), Path(__file__).name),
        (source / FIXTURE_RELATIVE, "run-fused-node-comparison.py"),
        (validator_path, "summarize-crdb-sql.py"),
        (Path(binary_manifest["path"]), "binary-provenance-manifest.json"),
    ]
    for path, control_name in control_paths:
        copied = controls / control_name
        shutil.copy2(path, copied)
        manifest["controls"][control_name] = {
            "path": str(copied.relative_to(output)),
            "sha256": sha256(copied),
        }
    fixture.write_json(output / "manifest.json", manifest)

    run_dir = None
    try:
        arch = fixture.docker_architecture()
        if arch != "arm64":
            raise fixture.RunnerError(f"diagnostic profile requires Docker arm64, got {arch}")
        image = fixture.ensure_image(fixture.RUNTIME)
        fixture_args.runtime_image_id = image["Id"]
        manifest["docker"].update({
            "architecture": arch,
            "host": fixture.docker_json(["docker", "info", "--format", "{{json .}}"]),
            "runtime_image": image,
        })
        manifest["status"] = "ready-to-run"
        manifest["binaries"] = binary_hashes(binary_dir)
        fixture.write_json(output / "manifest.json", manifest)

        cells = [cell for cell in fixture.cell_matrix(fixture_args) if cell["kind"] == "multigroup"]
        if len(cells) != 1:
            raise fixture.RunnerError(f"expected one multigroup profile cell, got {len(cells)}")
        cell = cells[0]
        run_dir = output / cell["id"] / "profile" / "candidate"
        schema = fixture.schema_files(run_dir, cell["tables"])
        manifest["cell"] = cell
        manifest["active_run"] = {
            "fixture_engine": "candidate",
            "evidence_path": str(run_dir.relative_to(output)),
        }
        fixture.write_json(output / "manifest.json", manifest)

        original_popen = fixture.subprocess.Popen

        def profile_popen(command, *positional, **keyword):
            if (isinstance(command, list) and len(command) > 4 and
                    command[:2] == ["docker", "exec"] and
                    command[3] == "/bench/candidate-vibedb" and command[4] == "cluster"):
                command = command[:2] + [
                    "-e", f"VIBEDB_PROFILE_DIRECTORY={PROFILE_ENV['VIBEDB_PROFILE_DIRECTORY']}",
                    "-e", f"VIBEDB_PROFILE_DURATION={PROFILE_ENV['VIBEDB_PROFILE_DURATION']}",
                ] + command[2:]
                manifest["server_command"] = command
                manifest["status"] = "profiling"
                fixture.write_json(output / "manifest.json", manifest)
            return original_popen(command, *positional, **keyword)

        fixture.subprocess.Popen = profile_popen
        try:
            result = fixture.run_engine(fixture_args, cell, "candidate", "profile", binary_dir,
                                        run_dir, schema, arch)
        finally:
            fixture.subprocess.Popen = original_popen

        result["diagnostic_only"] = True
        result["timings_are_not_benchmark_results"] = True
        result["startup_included"] = True
        manifest["run"] = result
        manifest["profile_artifacts"] = profile_artifacts(run_dir)
        manifest.pop("active_run", None)
        manifest["status"] = "complete" if result.get("status") == "completed" else "failed"
        manifest["finished_utc"] = now()
        fixture.write_json(run_dir / "run.json", result)
        fixture.write_json(output / "manifest.json", manifest)
        print(json.dumps({"status": manifest["status"],
                          "profile_artifacts": manifest["profile_artifacts"]}, indent=2))
        return 0 if manifest["status"] == "complete" else 1
    except BaseException as exc:
        # Keep the preparation/build/image/cell/server command metadata on every
        # late failure.  A diagnostic failure must remain reviewable.
        manifest["status"] = "failed"
        manifest["fatal_error"] = f"{type(exc).__name__}: {exc}"
        if run_dir is not None:
            manifest["profile_artifacts"] = profile_artifacts(run_dir)
        manifest["finished_utc"] = now()
        fixture.write_json(output / "manifest.json", manifest)
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except Exception as exc:
        print(f"run_profile: {exc}", file=sys.stderr)
        raise SystemExit(2)
