#!/usr/bin/env python3
"""Run a matched before/after RF3 read matrix on the fused-node fixture.

The existing fused-node fixture is intentionally reused without changing its
server or client/oracle code.  Both VibeDB arms are copied into the fixture's
``candidate`` binary names, so they have the same process layout, diagnostics,
resource ceiling, and RF3 topology.  ``arm`` and ``revision`` in the retained
manifest are the authoritative identities for the before and after binaries.

This is a single-host RF3 comparison harness.  It does not publish a result,
measure horizontal multi-machine scaling, or enable profiling.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile


REPO = Path(__file__).resolve().parents[2]
CLIENT = REPO
CONTROL = REPO / "scripts" / "bench" / "run-fused-node-comparison.py"
VALIDATOR = REPO / "scripts" / "bench" / "summarize-crdb-sql.py"

DEFAULT_WORKLOADS = "point_hit,point_miss,range_64,group_16"
# ``update_existing`` is retained as an optional control workload.  The
# default remains the four requested reads, and the control is useful when a
# campaign needs to separate read-path changes from write admission effects.
ALLOWED_WORKLOADS = frozenset((
    *DEFAULT_WORKLOADS.split(","),
    "range_32",
    "range_256",
    "update_existing",
))
ALLOWED_CLIENTS = frozenset((1, 8))
ALLOWED_GROUPS = frozenset((4, 16))
ALLOWED_PHYSICAL_NODES = frozenset((3, 6))
DEFAULT_ROWS = 8192
DEFAULT_OPERATIONS = 20000
DEFAULT_SCANS = 2000
DEFAULT_WARMUP = 1000
DEFAULT_REPETITIONS = 3
DEFAULT_CPUS = "12"
DEFAULT_MEMORY = "24g"

# Keep the fixture's balanced order contract explicit.  Every entry is run
# sequentially in a fresh container and volume by run_engine.
ORDER_ENGINES = {
    "before-first": ("before", "after", "crdb"),
    "after-first": ("crdb", "after", "before"),
}


def load_fixture(path=CONTROL):
    """Load the validated fixture as a module without importing the package."""
    spec = importlib.util.spec_from_file_location("vibedb_fused_fixture", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load fixture {path}")
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    return fixture


def parser():
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("output", type=Path,
                       help="new absolute evidence directory")
    value.add_argument("--baseline-ref", required=True,
                       help="immutable ancestor commit, tag, or ref for before")
    value.add_argument("--candidate-ref", required=True,
                       help="immutable descendant commit, tag, or ref for after")
    value.add_argument("--repo", type=Path, default=REPO,
                       help="repository containing both revisions")
    value.add_argument("--client-source", type=Path, default=CLIENT,
                       help="clean repository used once for the shared client")
    value.add_argument("--workloads", default=DEFAULT_WORKLOADS,
                       help="comma-separated read workloads")
    value.add_argument("--clients", default="1,8",
                       help="comma-separated client counts; only 1 and 8 are supported")
    value.add_argument("--groups", type=int, choices=sorted(ALLOWED_GROUPS), default=4,
                       help="logical tables/groups (4 or 16)")
    value.add_argument("--physical-nodes", type=int,
                       choices=sorted(ALLOWED_PHYSICAL_NODES), default=3,
                       help="fused physical-node count (3 or 6)")
    value.add_argument("--rows", type=int, default=DEFAULT_ROWS,
                       help="rows seeded per logical table")
    value.add_argument("--operations", type=int, default=DEFAULT_OPERATIONS,
                       help="point operations per workload trial")
    value.add_argument("--scans", type=int, default=DEFAULT_SCANS,
                       help="range/group operations per workload trial")
    value.add_argument("--warmup", type=int, default=DEFAULT_WARMUP,
                       help="unmeasured operations before each trial")
    value.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS,
                       help="repetitions per workload and client count")
    value.add_argument("--cpus", default=DEFAULT_CPUS,
                       help="aggregate Docker CPU ceiling shared by the fixture")
    value.add_argument("--memory", default=DEFAULT_MEMORY,
                       help="aggregate Docker memory ceiling shared by the fixture")
    value.add_argument("--vibedb-sql-ports", default="5432",
                       help="single loopback SQL frontend port for both arms")
    value.add_argument("--ready-timeout", type=int, default=120,
                       help="server readiness timeout in seconds")
    return value


def parse_workloads(raw, fixture):
    """Return canonical workloads and reject unbounded workload additions."""
    workloads = fixture.parse_workloads(raw, "workloads")
    if any(workload not in ALLOWED_WORKLOADS for workload in workloads):
        raise fixture.RunnerError(
            "workloads must be a unique subset of point_hit, point_miss, range_32, range_64, range_256, group_16, update_existing")
    return workloads


def parse_clients(raw, fixture):
    clients = fixture.parse_positive_csv(raw, "clients")
    if any(client not in ALLOWED_CLIENTS for client in clients):
        raise fixture.RunnerError("clients must be a unique subset of 1 and 8")
    return clients


def validate_options(selected, fixture):
    """Validate the bounded read campaign before creating an output directory."""
    workloads = parse_workloads(selected.workloads, fixture)
    clients = parse_clients(selected.clients, fixture)
    if selected.groups not in ALLOWED_GROUPS:
        raise fixture.RunnerError("groups must be 4 or 16")
    if selected.physical_nodes not in ALLOWED_PHYSICAL_NODES:
        raise fixture.RunnerError("physical-nodes must be 3 or 6")
    if not 64 <= selected.rows <= 1_000_000:
        raise fixture.RunnerError("rows must be in the range 64..1000000")
    if not 1 <= selected.operations <= 1_000_000:
        raise fixture.RunnerError("operations must be in the range 1..1000000")
    if not 1 <= selected.scans <= 100_000:
        raise fixture.RunnerError("scans must be in the range 1..100000")
    if not 0 <= selected.warmup <= 100_000:
        raise fixture.RunnerError("warmup must be in the range 0..100000")
    if not 1 <= selected.repetitions <= 20:
        raise fixture.RunnerError("repetitions must be in the range 1..20")
    if selected.operations < max(clients) or selected.scans < max(clients):
        raise fixture.RunnerError(
            "operations and scans must cover every configured client")
    if selected.ready_timeout < 1:
        raise fixture.RunnerError("ready-timeout must be positive")
    return {"workloads": workloads, "clients": clients}


def make_fixture_args(selected, fixture, workloads):
    """Build the fixture Namespace used by run_engine and validate_client_report."""
    return fixture.parser().parse_args([
        str(selected.output),
        "--repo", str(selected.repo),
        "--client-source", str(selected.client_source),
        # The fixture requires a candidate ref at parser level.  Its main
        # comparison path is not used; both source trees are passed explicitly
        # to build_all below and both measured VibeDB arms use engine=candidate.
        "--candidate-ref", selected.candidate_ref,
        "--matrix", "all",
        "--clients", selected.clients,
        "--multigroup-clients", selected.clients,
        "--groups", str(selected.groups),
        "--physical-nodes", str(selected.physical_nodes),
        "--distributions", "uniform",
        "--endpoint-modes", "single",
        "--multigroup-workloads", ",".join(workloads),
        "--rows", str(selected.rows),
        "--operations", str(selected.operations),
        "--scans", str(selected.scans),
        "--warmup", str(selected.warmup),
        "--repetitions", str(selected.repetitions),
        "--cpus", selected.cpus,
        "--memory", selected.memory,
        "--order", "both",
        "--include-crdb",
        "--vibedb-sql-ports", selected.vibedb_sql_ports,
        "--ready-timeout", str(selected.ready_timeout),
    ])


def read_cells(selected, fixture, workloads, clients):
    """Create the one matched read cell for the selected topology and dataset."""
    tables = fixture.table_names(selected.groups, "rf3_sql_group")
    return [{
        "id": f"read-{selected.physical_nodes}n-{selected.groups}g",
        "kind": "distributed-read",
        "physical_nodes": selected.physical_nodes,
        "tables": tables,
        "workloads": ",".join(workloads),
        "group_distribution": "uniform",
        "clients": ",".join(str(client) for client in clients),
        "groups": selected.groups,
        "endpoint_mode": "single",
    }]


def arm_binary_directories(destination, binaries, fixture):
    """Copy each revision into identical candidate-named fixture directories."""
    arm_binaries = {}
    for arm, prefix in (("before", "parent"), ("after", "candidate")):
        target = destination / f"{arm}-bin"
        target.mkdir(mode=0o700, parents=True)
        for suffix in ("vibedb", "vibedb-shard", "vibedb-gateway"):
            shutil.copy2(binaries / f"{prefix}-{suffix}", target / f"candidate-{suffix}")
        shutil.copy2(binaries / "rf3-sqlbench", target / "rf3-sqlbench")
        arm_binaries[arm] = target
    arm_binaries["crdb"] = binaries
    return arm_binaries


def source_manifest(destination, sources, fixture):
    """Retain source patches and return JSON-safe source metadata."""
    result = {}
    for name, info in sources.items():
        copy = dict(info)
        fixture.write_git_evidence(destination, name, copy)
        copy.pop("patch", None)
        result[name] = copy
    return result


def verify_build_metadata(binaries, arch, fixture):
    metadata = {}
    for executable in sorted(binaries.iterdir()):
        if executable.name == "cockroach":
            continue
        text = fixture.text_output(["go", "version", "-m", executable])
        for setting in ("GOEXPERIMENT=simd", "GOOS=linux", "GOARCH=" + arch):
            if "\tbuild\t" + setting not in text:
                raise fixture.RunnerError(
                    f"{executable.name} is missing required build setting {setting}")
        metadata[executable.name] = text
    return metadata


def resolve_refs(selected, fixture, repo, client_info):
    """Resolve both refs and prove that the before commit is an ancestor."""
    baseline = fixture.text_output([
        "git", "rev-parse", selected.baseline_ref + "^{commit}"
    ], cwd=repo)
    candidate = fixture.text_output([
        "git", "rev-parse", selected.candidate_ref + "^{commit}"
    ], cwd=repo)
    if baseline == candidate:
        raise fixture.RunnerError("baseline and candidate commits must be distinct")
    fixture.run(["git", "merge-base", "--is-ancestor", baseline, candidate], cwd=repo)
    return {
        "before": baseline,
        "after": candidate,
        "requested_before": selected.baseline_ref,
        "requested_after": selected.candidate_ref,
        "client_source_revision": client_info["revision"],
        "baseline_is_ancestor": True,
    }


def retain_before_after_diff(repo, refs, destination, fixture):
    """Retain the exact binary diff and its digest beside the manifest."""
    path = destination / "before-after.patch"
    with path.open("wb") as patch:
        fixture.run(["git", "diff", "--binary", refs["before"], refs["after"]],
                    cwd=repo, stdout=patch)
    refs["before_after_patch_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()


def run_campaign(selected, fixture, destination, fixture_args, cells, refs, arch):
    """Build once, then run all fresh AB/BA fixtures sequentially."""
    manifest = {
        "schema": "vibedb.distributed-read-comparison/1",
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "status": "preparing",
        "profiling": False,
        "arm_contract": (
            "before and after use the existing fused candidate fixture; "
            "arm and revision identify the immutable source"
        ),
        "fixture_engine": "candidate for both VibeDB arms; crdb for the oracle arm",
        "topology": {
            "replication_factor": 3,
            "physical_nodes": selected.physical_nodes,
            "single_host_fixture": True,
            "identical_fused_vibedb_topology": True,
            "publication": False,
            "horizontal_multimachine_scaling": False,
        },
        "workload_contract": {
            "rows_per_table": selected.rows,
            "groups": selected.groups,
            "workloads": [cell["workloads"] for cell in cells],
            "clients": selected.clients,
            "operations": selected.operations,
            "scans": selected.scans,
            "warmup": selected.warmup,
            "repetitions": selected.repetitions,
        },
        "limits": fixture.resource_limits(selected.cpus, selected.memory),
        "orders": {name: list(engines) for name, engines in ORDER_ENGINES.items()},
        "refs": refs,
        "cells": cells,
        "runs": [],
    }

    controls = destination / "controls"
    controls.mkdir(mode=0o700)
    for source in (Path(__file__), CONTROL, VALIDATOR):
        shutil.copy2(source, controls / source.name)
    manifest["control_sha256"] = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(controls.iterdir())
    }
    fixture_args.validator_path = controls / VALIDATOR.name
    fixture_args.runtime_image_id = None

    manifest["docker_architecture"] = arch
    manifest["vibedb_build_environment"] = {
        "GOOS": "linux", "GOARCH": arch, "CGO_ENABLED": "0",
        "GOEXPERIMENT": "simd",
        "note": "the benchmark executables run in Linux containers",
    }
    manifest["docker_host"] = fixture.docker_json(["docker", "info", "--format", "{{json .}}"])
    # Persist the control contract before worktrees, builds, or images can
    # fail.  Later checkpoints add source/build/run evidence atomically.
    fixture.write_json(destination / "manifest.json", manifest)

    repo = selected.repo.resolve()
    client_source = selected.client_source.resolve()
    baseline_revision = refs["before"]
    candidate_revision = refs["after"]
    parent = candidate = client_tree = None
    try:
        with tempfile.TemporaryDirectory(prefix="vibedb-distributed-read-") as temp:
            work = Path(temp)
            parent, candidate = fixture.prepare_worktrees(
                repo, baseline_revision, candidate_revision, work)
            client_tree = work / "client"
            try:
                fixture.run([
                    "git", "worktree", "add", "--detach", client_tree,
                    refs["client_source_revision"],
                ], cwd=client_source)
                sources = {
                    "before": fixture.git_info(parent),
                    "after": fixture.git_info(candidate),
                    "client": fixture.git_info(client_tree),
                }
                manifest["source"] = source_manifest(destination, sources, fixture)
                fixture.write_json(destination / "manifest.json", manifest)
                binaries, go_version = fixture.build_all(
                    fixture_args, destination, parent, candidate, client_tree, arch)
                manifest["go_version"] = go_version
                manifest["images"] = {
                    "runtime": fixture.ensure_image(fixture.RUNTIME),
                    "crdb": fixture.ensure_image(fixture.CRDB),
                }
                fixture_args.runtime_image_id = manifest["images"]["runtime"]["Id"]
                fixture.extract_crdb_binary(binaries)
                manifest["build_binary_sha256"] = fixture.binary_hashes(binaries)
                manifest["vibedb_build_metadata"] = verify_build_metadata(
                    binaries, arch, fixture)
                manifest["executable_formats"] = fixture.text_output([
                    "file", binaries / "parent-vibedb-shard",
                    binaries / "candidate-vibedb-shard", binaries / "cockroach",
                    binaries / "rf3-sqlbench",
                ])
                arm_binaries = arm_binary_directories(destination, binaries, fixture)
                manifest["fixture_binary_sha256"] = {
                    name: fixture.binary_hashes(path)
                    for name, path in arm_binaries.items()
                }
                manifest["status"] = "measuring"
                manifest["planned_runs"] = [
                    {"cell": cell["id"], "order": order, "arm": arm,
                     "fixture_engine": "crdb" if arm == "crdb" else "candidate",
                     "revision": refs.get(arm, fixture.CRDB)}
                    for order, arms in ORDER_ENGINES.items()
                    for cell in cells for arm in arms
                ]
                fixture.write_json(destination / "manifest.json", manifest)

                for order, arms in ORDER_ENGINES.items():
                    for cell in cells:
                        for arm in arms:
                            run_dir = destination / cell["id"] / order / arm
                            schema = fixture.schema_files(run_dir, cell["tables"])
                            fixture_engine = "crdb" if arm == "crdb" else "candidate"
                            manifest["active_run"] = {
                                "arm": arm,
                                "revision": refs.get(arm, fixture.CRDB),
                                "fixture_engine": fixture_engine,
                                "order": order,
                                "cell": cell,
                                "evidence_path": str(run_dir.relative_to(destination)),
                            }
                            fixture.write_json(destination / "manifest.json", manifest)
                            print(f"{order}: {arm} starting", flush=True)
                            result = fixture.run_engine(
                                fixture_args, cell, fixture_engine, order,
                                arm_binaries[arm], run_dir, schema, arch)
                            result["arm"] = arm
                            result["revision"] = refs.get(arm, fixture.CRDB)
                            result["fixture_engine"] = fixture_engine
                            result["dataset"] = {
                                "rows_per_table": selected.rows,
                                "table_count": len(cell["tables"]),
                                "total_rows": selected.rows * len(cell["tables"]),
                                "payload_bytes_per_row": fixture.DEFAULT_PAYLOAD_BYTES,
                                "total_payload_bytes": (
                                    selected.rows * len(cell["tables"])
                                    * fixture.DEFAULT_PAYLOAD_BYTES
                                ),
                            }
                            manifest["runs"].append(result)
                            if result.get("status") == "completed" and result.get("client_exit_code") == 0:
                                manifest.pop("active_run", None)
                            else:
                                # Keep the failed cell visible in the last
                                # checkpoint so an interrupted campaign can
                                # be resumed or diagnosed without guessing.
                                manifest["active_run"]["result_status"] = result.get("status")
                                manifest["active_run"]["errors"] = result.get("errors", [])
                            fixture.write_json(run_dir / "run.json", result)
                            fixture.write_json(destination / "manifest.json", manifest)
                            print(f"{order}: {arm} {result.get('status')} {result.get('errors', [])}",
                                  flush=True)
                            if result.get("status") != "completed" or result.get("client_exit_code") != 0:
                                raise fixture.RunnerError(
                                    "stopped after incomplete fixture; retained evidence")
            finally:
                if client_tree is not None and client_tree.exists():
                    fixture.run([
                        "git", "worktree", "remove", "--force", client_tree,
                    ], cwd=client_source, check=False)
                if parent is not None and candidate is not None:
                    fixture.cleanup_worktrees(repo, work, parent, candidate)
    finally:
        # The temporary worktree context owns the actual paths.  These names
        # are retained only to make cleanup intent explicit to reviewers.
        parent = candidate = client_tree = None

    manifest["status"] = "complete"
    manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
    return manifest


def main(argv=None):
    selected = parser().parse_args(argv)
    fixture = load_fixture()
    validated = validate_options(selected, fixture)
    fixture_args = make_fixture_args(selected, fixture, validated["workloads"])
    # Let the validated fixture reject any future matrix-contract drift before
    # creating the output directory; the runner then keeps only its matched
    # multigroup read cell below.
    fixture.cell_matrix(fixture_args)
    cells = read_cells(selected, fixture, validated["workloads"], validated["clients"])
    destination = fixture.require_new_directory(selected.output)
    fixture.COMMAND_LOG = destination / "control-commands.jsonl"

    repo = selected.repo.resolve()
    client_source = selected.client_source.resolve()
    destination_refs = {
        "requested_before": selected.baseline_ref,
        "requested_after": selected.candidate_ref,
    }
    try:
        if not repo.is_dir() or not client_source.is_dir():
            raise fixture.RunnerError("repo and client-source must be directories")
        client_info = fixture.git_info(client_source)
        if client_info["dirty"]:
            raise fixture.RunnerError(
                "shared client source must be clean so one immutable client is used by every arm")
        destination_refs = resolve_refs(selected, fixture, repo, client_info)
        retain_before_after_diff(repo, destination_refs, destination, fixture)
        arch = fixture.docker_architecture()
        manifest = run_campaign(
            selected, fixture, destination, fixture_args, cells,
            destination_refs, arch)
        fixture.write_json(destination / "manifest.json", manifest)
        return 0
    except (Exception, KeyboardInterrupt) as exc:
        manifest_path = destination / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text())
            except (OSError, json.JSONDecodeError):
                manifest = {}
        else:
            manifest = {}
        manifest.setdefault("schema", "vibedb.distributed-read-comparison/1")
        manifest.setdefault("profiling", False)
        manifest.setdefault("cells", cells)
        manifest["refs"] = destination_refs
        manifest["status"] = "incomplete-or-failed"
        manifest["fatal_error"] = str(exc) or "runner interrupted"
        fixture.write_json(destination / "manifest.json", manifest)
        print(manifest["fatal_error"], flush=True)
        return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"run-distributed-read-comparison: {exc}", file=sys.stderr)
        raise SystemExit(2)
