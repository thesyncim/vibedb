#!/usr/bin/env python3
"""Run a clean parent/candidate RF3 SQL matrix with retained evidence.

The runner creates detached worktrees for the two VibeDB revisions, builds all
server and client binaries before starting any timed fixture, and gives every
engine/order/cell a fresh Docker volume.  It records failures and incomplete
cells instead of turning them into throughput results.  The default matrix is
the existing C1/C8 five-workload comparison; ``--matrix all`` adds explicit
multigroup uniform/skewed mixed-read/update cells at the requested physical
node counts.

This is a control runner, not a claim generator.  A six-process Docker fixture
is a six-process single-host diagnostic, not six independent machines.
"""

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[2]
CRDB = "cockroachdb/cockroach:v26.3.1@sha256:204f131510c78393adb02345f289a8dbb32e1491e26cc92b6c7751f3b97be3c5"
RUNTIME = "golang:1.27-bookworm"
PARENT_DEFAULT_REF = "82ea6abfcf51de01745a99609d5ffb0cbbb828d0"
DEFAULT_WORKLOADS = "point_hit,point_miss,range_64,group_16,update_existing"
DEFAULT_PAYLOAD_BYTES = 256
ALLOWED_WORKLOADS = {
    "point_hit", "point_miss", "range_32", "range_64", "range_256", "group_16", "update_existing",
    "mixed_read_update", "update_uniform", "mixed_uniform",
}
COMMAND_LOG = None


def redact(value):
    if isinstance(value, str):
        value = re.sub(r"(postgres(?:ql)?://)[^/\s]*@", r"\1[redacted]@", value)
        return re.sub(r"([?&](?:password|sslpassword)=)[^&\s]+", r"\1[redacted]", value)
    if isinstance(value, dict):
        return {key: redact(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(child) for child in value]
    return value


class RunnerError(RuntimeError):
    """A setup or evidence-control failure that must remain visible."""


def write_json(path, value):
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".evidence-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(redact(value), stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def run(argv, *, cwd=None, env=None, check=True, stdout=None, stderr=None):
    command = [str(value) for value in argv]
    completed = subprocess.run(command, cwd=cwd, env=env, check=False,
                               stdout=subprocess.PIPE if stdout is None else stdout,
                               stderr=subprocess.PIPE if stderr is None else stderr)
    if COMMAND_LOG is not None:
        record = {"argv": command, "cwd": str(cwd) if cwd is not None else None,
                  "exit_code": completed.returncode}
        for name in ("stdout", "stderr"):
            value = getattr(completed, name, None)
            if isinstance(value, bytes):
                record[name] = value.decode(errors="replace")
        with COMMAND_LOG.open("a") as stream:
            stream.write(json.dumps(redact(record)) + "\n")
    if check:
        completed.check_returncode()
    return completed


def wait_for_diagnostic_preflight(process, container, ready_file, timeout=15):
    """Wait for every configured group/member to pass status and metrics cuts."""
    started = time.monotonic()
    deadline = started + timeout
    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RunnerError(
                f"per-group diagnostic preflight failed with exit code {exit_code}")
        ready = run(["docker", "exec", container, "test", "-s", ready_file],
                    check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if ready.returncode == 0:
            return time.monotonic() - started
        time.sleep(0.1)
    exit_code = process.poll()
    if exit_code is not None:
        raise RunnerError(
            f"per-group diagnostic preflight failed with exit code {exit_code}")
    raise RunnerError("per-group diagnostic preflight did not become ready")


def text_output(argv, *, cwd=None, env=None, check=True):
    completed = run(argv, cwd=cwd, env=env, check=check,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return completed.stdout.decode().strip()


def bytes_output(argv, *, cwd=None, check=True):
    completed = run(argv, cwd=cwd, check=check,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return completed.stdout


def require_new_directory(path):
    if not path.is_absolute():
        raise RunnerError("output must be an absolute path that does not exist")
    path = path.resolve()
    if not path.is_absolute() or path.exists():
        raise RunnerError("output must be an absolute path that does not exist")
    path.mkdir(mode=0o700)
    return path


def parse_positive_csv(raw, name):
    values = []
    for value in raw.split(","):
        try:
            parsed = int(value)
        except ValueError as exc:
            raise RunnerError(f"{name} must contain positive integers") from exc
        if parsed < 1 or parsed in values:
            raise RunnerError(f"{name} must contain distinct positive integers")
        values.append(parsed)
    if not values:
        raise RunnerError(f"{name} must not be empty")
    return values


def parse_port_csv(raw, name):
    ports = parse_positive_csv(raw, name)
    if any(port > 65535 for port in ports):
        raise RunnerError(f"{name} must contain TCP ports in the range 1..65535")
    return ports


def resource_limits(cpus, memory):
    try:
        quota = Decimal(cpus) * 1000000000
        if not quota.is_finite() or quota <= 0 or quota != int(quota):
            raise ValueError
    except (InvalidOperation, ValueError, OverflowError) as exc:
        raise RunnerError("cpus must be a positive finite Docker CPU quota") from exc
    match = re.fullmatch(r"([1-9][0-9]*)([bkmg]?)", memory.lower())
    if not match:
        raise RunnerError("memory must be a positive whole number of bytes or use b/k/m/g")
    factor = {"": 1, "b": 1, "k": 1024, "m": 1024**2, "g": 1024**3}[match[2]]
    return {"NanoCpus": int(quota), "Memory": int(match[1]) * factor, "MemorySwap": int(match[1]) * factor}


def validate_resource_limits(inspected, cpus, memory):
    expected = resource_limits(cpus, memory)
    actual = {key: inspected.get("HostConfig", {}).get(key) for key in expected}
    if actual != expected:
        raise RunnerError(f"Docker resource ceiling differs from requested aggregate limits: {actual} != {expected}")
    return actual


def parse_csv(raw, name):
    values = [value.strip() for value in raw.split(",") if value.strip()]
    if not values:
        raise RunnerError(f"{name} must not be empty")
    return values


def valid_identifier(value):
    if not value or len(value) > 63:
        return False
    for index, char in enumerate(value):
        if "a" <= char <= "z" or char == "_" or index > 0 and "0" <= char <= "9":
            continue
        return False
    return True


def parse_tables(raw):
    tables = parse_csv(raw, "tables")
    if len(tables) > 63 or any(not valid_identifier(table) for table in tables):
        raise RunnerError("tables must be lowercase SQL identifiers (maximum 63)")
    if len(set(tables)) != len(tables):
        raise RunnerError("tables must be unique")
    return tables


def parse_workloads(raw, name):
    workloads = parse_csv(raw, name)
    canonical = ["mixed_read_update" if workload == "mixed" else workload
                 for workload in workloads]
    if any(workload not in ALLOWED_WORKLOADS for workload in canonical):
        raise RunnerError(f"{name} contains an unsupported workload")
    if len(set(canonical)) != len(canonical):
        raise RunnerError(f"{name} must not repeat workloads")
    return canonical


def engines_for_order(order, include_crdb):
    """Return the exact engine sequence for one recorded parent/candidate order."""
    if order == "parent-first":
        sequence = ["parent", "candidate"]
        return sequence + (["crdb"] if include_crdb else [])
    if order == "candidate-first":
        sequence = ["candidate", "parent"]
        return (["crdb"] if include_crdb else []) + sequence
    raise RunnerError(f"unsupported engine order {order!r}")


def table_names(count, prefix):
    if not valid_identifier(prefix):
        raise RunnerError(f"invalid table prefix {prefix!r}")
    if count == 1:
        return [prefix]
    names = [prefix] + [f"{prefix}_{index:02d}" for index in range(1, count)]
    if any(not valid_identifier(name) for name in names):
        raise RunnerError("generated table name exceeds the SQL identifier limit")
    return names


def git_info(source):
    revision = text_output(["git", "-C", source, "rev-parse", "HEAD"])
    status = text_output(["git", "-C", source, "status", "--porcelain=v1", "--untracked-files=normal"])
    patch = bytes_output(["git", "-C", source, "diff", "HEAD"])
    return {"revision": revision, "status": status, "dirty": bool(status),
            "patch_sha256": hashlib.sha256(patch).hexdigest(), "patch": patch}


def write_git_evidence(destination, name, info):
    (destination / f"source-{name}.patch").write_bytes(info.pop("patch"))
    (destination / f"source-{name}.json").write_text(json.dumps(info, indent=2) + "\n")


def binary_hashes(directory):
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(directory.iterdir()) if path.is_file()}


def docker_architecture():
    value = text_output(["docker", "info", "--format", "{{.Architecture}}"])
    try:
        return {"arm64": "arm64", "aarch64": "arm64", "amd64": "amd64", "x86_64": "amd64"}[value]
    except KeyError as exc:
        raise RunnerError(f"unsupported Docker architecture {value!r}") from exc


def docker_json(argv):
    return json.loads(text_output(argv))


def ensure_image(image):
    if run(["docker", "image", "inspect", image], check=False,
           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        run(["docker", "pull", image])
    inspected = docker_json(["docker", "image", "inspect", image])[0]
    if "@sha256:" in image:
        digest = image.rsplit("@", 1)[1]
        if not any(str(entry).endswith(digest) for entry in inspected.get("RepoDigests", [])):
            raise RunnerError(f"Docker image {image!r} did not resolve to its pinned digest")
    return inspected


def extract_crdb_binary(destination):
    name = "vibedb-fused-crdb-extract-" + uuid.uuid4().hex[:12]
    created = False
    try:
        run(["docker", "create", "--name", name, CRDB])
        created = True
        output = destination / "cockroach"
        run(["docker", "cp", name + ":/cockroach/cockroach", output])
        output.chmod(0o700)
        return output
    finally:
        if created:
            run(["docker", "rm", "-f", name], check=False,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def render_arg(value, substitutions):
    rendered = value
    for source, replacement in substitutions.items():
        rendered = rendered.replace(source, replacement)
    return rendered


def schema_files(destination, tables):
    schema = destination / "schema"
    schema.mkdir(mode=0o700, parents=True)
    for table in tables:
        (schema / f"{table}.sql").write_text(
            f"CREATE TABLE {table} (id TEXT PRIMARY KEY, bucket INTEGER NOT NULL, "
            "score INTEGER NOT NULL, payload TEXT NOT NULL)\n")
    return schema


def cell_matrix(args):
    cells = [{
        "id": "baseline-c1-c8",
        "kind": "baseline",
        "physical_nodes": 3,
        "tables": ["rf3_sql_bench"],
        "workloads": DEFAULT_WORKLOADS,
        "group_distribution": "uniform",
        "clients": args.clients,
        "groups": 1,
        "endpoint_mode": "single",
    }]
    if args.matrix == "base":
        return cells
    nodes = parse_positive_csv(args.physical_nodes, "physical-nodes")
    if any(node not in (3, 6) for node in nodes):
        raise RunnerError("physical-nodes must contain only 3 or 6")
    tables = parse_tables(args.tables) if args.tables else table_names(args.groups, args.table_prefix)
    if len(tables) < 2:
        raise RunnerError("multigroup cells require at least two tables")
    distributions = parse_csv(args.distributions, "distributions")
    endpoint_modes = parse_csv(args.endpoint_modes, "endpoint-modes")
    if len(set(distributions)) != len(distributions) or len(set(endpoint_modes)) != len(endpoint_modes):
        raise RunnerError("distribution and endpoint-mode entries must be unique")
    if any(mode not in {"single", "per-node"} for mode in endpoint_modes):
        raise RunnerError("endpoint-modes must be single or per-node")
    if any(value not in {"uniform", "skewed"} for value in distributions):
        raise RunnerError("distributions must be uniform or skewed")
    if "skewed" in distributions and len(tables) < 2:
        raise RunnerError("skewed multigroup cells require at least two tables")
    for physical_nodes in nodes:
        for distribution in distributions:
            for endpoint_mode in endpoint_modes:
                cells.append({
                    "id": f"multigroup-{physical_nodes}n-{distribution}" + ("-frontends" if endpoint_mode == "per-node" else ""),
                    "kind": "multigroup",
                    "physical_nodes": physical_nodes,
                    "tables": tables,
                    "workloads": ",".join(parse_workloads(args.multigroup_workloads, "multigroup-workloads")),
                    "group_distribution": distribution,
                    "clients": args.multigroup_clients,
                    "groups": len(tables),
                    "endpoint_mode": endpoint_mode,
                })
    return cells


def process_inventory(container):
    records = {}
    for name, argv in {
        "processes": ["ps", "-eo", "pid,ppid,comm,args"],
        # /proc/PID/exe is authoritative. Linux comm is truncated to 15 bytes;
        # candidate-vibedb-shard and candidate-vibedb-gateway share that prefix.
        "executables": ["sh", "-c", 'for proc in /proc/[0-9]*; do exe=$(readlink "$proc/exe") || continue; printf "%s\\t%s\\n" "${proc##*/}" "$exe"; done'],
        "endpoints": ["ss", "-ltnp"],
        "tcp": ["cat", "/proc/net/tcp"],
        "manifests": ["find", "/data", "-type", "f", "(", "-name", "*.vibejson",
                      "-o", "-iname", "*manifest*", "-o", "-iname", "*range*", "-o", "-name", "rf3-diagnostics.json", ")", "-print"],
        "platform": ["uname", "-a"],
        "memory": ["cat", "/proc/meminfo"],
        "logs": ["find", "/data", "-type", "f", "(", "-name", "*.log", "-o", "-path", "*/logs/*", ")", "-print"],
        "cgroup": ["sh", "-c", 'for item in cpu.max cpu.stat cpuset.cpus.effective memory.max memory.swap.max memory.current memory.events io.stat; do printf "%s\\n" "$item"; cat "/sys/fs/cgroup/$item"; done'],
    }.items():
        completed = run(["docker", "exec", container, *argv], check=False,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        records[name] = {"exit_code": completed.returncode,
                         "text": completed.stdout.decode(errors="replace")}
    return records


def save_inventory(destination, inventory, prefix):
    write_json(destination / f"{prefix}-inventory.json", inventory)
    for name, record in inventory.items():
        (destination / f"{prefix}-{name}.txt").write_text(redact(record["text"]))


def process_counts(inventory):
    record = inventory.get("executables", {})
    if record.get("exit_code") != 0:
        raise RunnerError("authoritative /proc executable inventory is unavailable")
    executables = {}
    for line in record.get("text", "").splitlines():
        fields = line.split("\t", 1)
        if len(fields) != 2 or not fields[0].isdigit() or not fields[1].startswith("/"):
            raise RunnerError("invalid /proc executable inventory")
        pid = int(fields[0])
        if pid in executables:
            raise RunnerError("duplicate PID in executable inventory")
        executables[pid] = fields[1]
    comms = [Path(executable).name for executable in executables.values()]
    return {
        "vibedb": sum(comm in {"vibedb", "parent-vibedb", "candidate-vibedb"} for comm in comms),
        "vibedb_shard": sum(comm in {"vibedb-shard", "parent-vibedb-shard", "candidate-vibedb-shard"} for comm in comms),
        "vibedb_gateway": sum(comm in {"vibedb-gateway", "parent-vibedb-gateway", "candidate-vibedb-gateway"} for comm in comms),
        "cockroach": comms.count("cockroach"),
    }


def sql_ports_for(args, cell, engine):
    if engine == "crdb":
        return [26257 + node for node in range(cell["physical_nodes"])] if cell.get("endpoint_mode") == "per-node" else [26257]
    ports = parse_port_csv(args.vibedb_sql_ports, "vibedb-sql-ports")
    if cell.get("endpoint_mode") == "per-node" and len(ports) < cell["physical_nodes"]:
        raise RunnerError(
            f"missing dependency: {cell['id']} requires one explicit VibeDB SQL frontend "
            f"per physical node ({cell['physical_nodes']} ports), got {len(ports)}; "
            "provide --vibedb-sql-ports and matching exact launcher endpoint flags")
    return ports[:cell["physical_nodes"]] if cell.get("endpoint_mode") == "per-node" else ports[:1]


def unsupported_reason(engine, cell):
    if engine == "parent" and cell["physical_nodes"] != 3:
        return "Pinned parent has no six-physical-node RF3 placement fixture; replicas=6 would not implement it."
    if engine == "parent" and cell.get("endpoint_mode") == "per-node":
        return "Pinned parent has one standalone SQL frontend; use the matched single-entrypoint multigroup diagnostic."
    return None


def endpoint_urls(engine, ports):
    if engine == "crdb":
        database = "defaultdb"
        user = "bench"
    else:
        database = "vibedb"
        user = "local"
    return [f"postgresql://{user}@127.0.0.1:{port}/{database}?sslmode=disable" for port in ports]


def endpoint_labels(ports):
    return [f"127.0.0.1:{port}" for port in ports]


def client_endpoint_choices(cell, labels):
    clients = parse_positive_csv(cell["clients"], "clients")
    if any(count > 15 for count in clients):
        raise RunnerError("clients must be no greater than 15")
    return {str(count): [labels[index % len(labels)] for index in range(count)]
            for count in clients}


def copy_published_inventories(container, destination, inventory, prefix, field="manifests"):
    """Copy published topology/leader files while retaining the raw listing."""
    copied = []
    failed = []
    for line in inventory.get(field, {}).get("text", "").splitlines():
        remote = line.strip()
        if not remote.startswith("/"):
            continue
        local = destination / "published" / prefix / remote.lstrip("/")
        local.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        completed = run(["docker", "cp", container + ":" + remote, local], check=False,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if completed.returncode == 0:
            copied.append(remote)
        else:
            failed.append({"path": remote, "output": completed.stdout.decode(errors="replace")})
    return {"copied": copied, "failed": failed}


def published_identity_inventory(destination, prefix, copied):
    """Summarize identity fields while retaining each original manifest byte."""
    storage = set()
    gateways = set()
    node_logs = set()
    group_rosters = {}
    table_groups = {}
    parse_errors = []

    def visit(value):
        if isinstance(value, dict):
            node_log = value.get("node_log")
            if isinstance(node_log, dict) and isinstance(node_log.get("path"), str):
                node_logs.add(node_log["path"])
            route = value.get("route")
            group = value.get("group_id") or (route.get("group_id") if isinstance(route, dict) else None)
            table = value.get("table")
            if isinstance(group, str) and isinstance(table, str):
                table_groups.setdefault(table, set()).add(group)
            for field in ("members", "ledger_members", "data_members"):
                roster = value.get(field, [])
                if not isinstance(roster, list):
                    continue
                nodes = []
                for member in roster:
                    if not isinstance(member, dict) or not any(key in member for key in ("member", "member_id")):
                        continue
                    node = member.get("node_id", member.get("node"))
                    if isinstance(node, str):
                        storage.add(node)
                        nodes.append(node)
                if field == "members" and isinstance(group, str) and nodes:
                    group_rosters.setdefault(group, set()).add(tuple(sorted(nodes)))
            for key, child in value.items():
                normalized = key.lower().replace("-", "_")
                if isinstance(child, str):
                    if normalized in {"gateway_node", "gateway_node_id"}:
                        gateways.add(child)
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    for remote in copied.get("copied", []):
        local = destination / "published" / prefix / remote.lstrip("/")
        try:
            visit(json.loads(local.read_text()))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            parse_errors.append({"path": remote, "error": str(exc)})
    return {
        "storage_node_ids": sorted(storage),
        "gateway_node_ids": sorted(gateways),
        "storage_node_count": len(storage),
        "gateway_node_count": len(gateways),
        "node_log_paths": sorted(node_logs),
        "node_log_count": len(node_logs),
        "group_rosters": {key: [list(roster) for roster in sorted(rosters)] for key, rosters in sorted(group_rosters.items())},
        "table_groups": {key: sorted(groups) for key, groups in sorted(table_groups.items())},
        "parse_errors": parse_errors,
    }


def validate_published_topology(engine, cell, identities):
    if engine == "crdb":
        return
    expected_nodes = 9 if engine == "parent" else cell["physical_nodes"]
    for field in ("storage_node_count", "node_log_count"):
        if identities.get(field) != expected_nodes:
            raise RunnerError(f"{engine} published {field}={identities.get(field)}, expected {expected_nodes}")
    seen_groups = set()
    for table in cell["tables"]:
        groups = identities["table_groups"].get(table, [])
        if len(groups) != 1 or groups[0] in seen_groups:
            raise RunnerError(f"no distinct single data-group inventory for table {table}")
        seen_groups.add(groups[0])
        rosters = identities["group_rosters"].get(groups[0], [])
        if len(rosters) != 1 or len(rosters[0]) != 3 or len(set(rosters[0])) != 3:
            raise RunnerError(f"table {table} lacks one coherent three-node RF3 roster")


def candidate_diagnostic_targets(destination, inventory, nodes):
    pids = {int(line.split("\t", 1)[0]) for line in inventory["executables"]["text"].splitlines()
            if line.split("\t", 1)[1] == "/bench/candidate-vibedb-shard"}
    targets = []
    for line in inventory["processes"]["text"].splitlines():
        fields = line.split(None, 3)
        if len(fields) < 4 or not fields[0].isdigit() or int(fields[0]) not in pids:
            continue
        argv = shlex.split(fields[3])
        if len(argv) < 2 or argv[1] != "serve-node":
            raise RunnerError("diagnostic target is not a ready serve-node process")
        manifest_path = None
        for index, token in enumerate(argv):
            if token in {"-manifest", "--manifest"} and index + 1 < len(argv):
                manifest_path = argv[index + 1]
            elif token.startswith(("-manifest=", "--manifest=")):
                manifest_path = token.split("=", 1)[1]
        if not manifest_path or not manifest_path.startswith("/data/") or ".." in Path(manifest_path).parts:
            raise RunnerError("candidate diagnostic process has no retained serving manifest")
        manifest = json.loads((destination / "published/ready" / manifest_path.lstrip("/")).read_text())
        local_nodes = set()
        for group in manifest.get("groups", []):
            member_id = group.get("route", {}).get("member_id")
            for member in group.get("members", []):
                if member.get("member_id") == member_id:
                    local_nodes.add(member.get("node_id"))
        if len(local_nodes) != 1 or not all(isinstance(node, str) and re.fullmatch(r"[0-9a-f]{32}", node) for node in local_nodes):
            raise RunnerError("candidate diagnostic manifest has no unambiguous local node identity")
        log = manifest.get("node_log", {}).get("path")
        if not isinstance(log, str) or not log.startswith("/data/"):
            raise RunnerError("candidate diagnostic manifest has no node-log path")
        targets.append({"pid": int(fields[0]), "node_id": next(iter(local_nodes)),
                        "path": str(Path(log).parent / "rf3-diagnostics.json"),
                        "executable": "/bench/candidate-vibedb-shard"})
    targets.sort(key=lambda value: value["node_id"])
    if len(targets) != nodes or len({target["node_id"] for target in targets}) != nodes or len({target["pid"] for target in targets}) != nodes:
        raise RunnerError("candidate diagnostic bindings do not cover every ready physical node exactly once")
    return targets


def serving_topology_expectation(engine, cell):
    if engine == "parent" and not unsupported_reason(engine, cell):
        return {"vibedb_shard": 9, "vibedb_gateway": 1}
    if engine == "candidate":
        return {"vibedb_shard": cell["physical_nodes"], "vibedb_gateway": 0}
    if engine == "crdb":
        return {"cockroach": cell["physical_nodes"]}
    return None


def validate_serving_topology(engine, cell, counts):
    expected = serving_topology_expectation(engine, cell)
    if expected is None:
        return
    missing = {name: {"expected": value, "observed": counts.get(name, 0)}
               for name, value in expected.items() if counts.get(name, 0) != value}
    if missing:
        raise RunnerError(f"serving topology mismatch for {engine}: {missing}")


def docker_stats(container):
    completed = run(["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
                    check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    text = completed.stdout.decode(errors="replace").strip()
    if not text:
        return {"exit_code": completed.returncode, "raw": text}
    try:
        return {"exit_code": completed.returncode, "raw": text, "parsed": json.loads(text.splitlines()[-1])}
    except json.JSONDecodeError:
        return {"exit_code": completed.returncode, "raw": text}


def wait_for_marker(process, log_path, markers, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RunnerError(f"server exited with status {process.returncode}; see {log_path}")
        try:
            text = log_path.read_text(errors="replace")
        except FileNotFoundError:
            text = ""
        if any(marker and marker in text for marker in markers):
            return text
        time.sleep(0.25)
    raise RunnerError(f"readiness marker {markers!r} was not published; see {log_path}")


def wait_for_tcp_ports(container, ports, timeout):
    """Wait until every disclosed SQL listener has a TCP LISTEN socket."""
    deadline = time.monotonic() + timeout
    expected = set(ports)
    listening = set()
    while time.monotonic() < deadline:
        completed = run(["docker", "exec", container, "cat", "/proc/net/tcp"], check=False,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        listening = set()
        if completed.returncode == 0:
            for line in completed.stdout.decode(errors="replace").splitlines()[1:]:
                fields = line.split()
                if len(fields) < 4 or fields[3] != "0A":
                    continue
                try:
                    listening.add(int(fields[1].rsplit(":", 1)[1], 16))
                except (IndexError, ValueError):
                    continue
        if expected.issubset(listening):
            return
        time.sleep(0.25)
    missing = sorted(expected - listening)
    raise RunnerError(f"SQL frontend readiness missing TCP listeners: {missing}")


def stop_processes(processes, container, destination):
    # Signal the real serving executables, not their docker-exec wrappers and
    # not Linux's truncated comm names. Every PID is inside this unique fixture.
    command = ['sh', '-c', 'for proc in /proc/[0-9]*; do exe=$(readlink "$proc/exe") || continue; case "$exe" in /bench/parent-vibedb|/bench/candidate-vibedb|/bench/parent-vibedb-shard|/bench/candidate-vibedb-shard|/bench/parent-vibedb-gateway|/bench/candidate-vibedb-gateway|/bench/cockroach) printf "%s\\n" "${proc##*/}";; esac; done']

    def live_pids():
        completed = run(["docker", "exec", container, *command], check=False,
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        return [value for value in completed.stdout.decode().splitlines() if value.isdigit()]

    pids = live_pids()
    if pids:
        run(["docker", "exec", container, "kill", "-TERM", *pids], check=False)
    deadline = time.monotonic() + 20
    while pids and time.monotonic() < deadline:
        time.sleep(0.25)
        pids = live_pids()
    forced = pids
    if forced:
        run(["docker", "exec", container, "kill", "-KILL", *forced], check=False)
        (destination / "shutdown-notes.txt").write_text("Forced post-measurement shutdown of fixture PIDs: " + ",".join(forced) + "\n")
    for process in processes:
        if process.poll() is None:
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
    return forced


def run_engine(args, cell, engine, order, binaries, destination, schema, arch):
    # schema_files has already created this unique engine directory.
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)
    container = "vibedb-fused-" + uuid.uuid4().hex[:12]
    volume = container + "-data"
    processes = []
    open_logs = []
    diagnostic_process = None
    diagnostic_log = None
    container_created = False
    result = {
        "engine": engine,
        "order": order,
        "cell": cell,
        "container": container,
        "volume": volume,
        "limits": {"cpus": args.cpus, "memory": args.memory},
        "status": "not_started",
        "exit_code": None,
        "errors": [],
        "topology": {
            "requested_candidate_physical_nodes": cell["physical_nodes"],
            "logical_table_count": len(cell["tables"]),
            "replication_factor": 3,
            "processes_are_not_replicas": True,
            "single_host_fixture": True,
            "rf3_subset_proof": {
                "status": "inventory_required",
                "requirement": "each logical data group has an independent three-member RF3 subset",
                "raw_manifests_retained": True,
            },
        },
    }
    reason = unsupported_reason(engine, cell)
    if reason:
        result.update(status="unsupported", errors=[reason])
        write_json(destination / "run.json", result)
        return result
    try:
        if engine == "parent":
            result["topology"].update({
                "mode": "legacy-node-log-role-processes",
                "expected_physical_node_identity_count": 9,
                "expected_serving_layout": {"shard_role_processes": 9, "standalone_gateway_processes": 1},
            })
        elif engine == "candidate":
            result["topology"].update({
                "mode": "fused-physical-node-processes",
                "expected_physical_node_identity_count": cell["physical_nodes"],
                "expected_serving_layout": {"fused_shard_processes": cell["physical_nodes"], "standalone_gateway_processes": 0},
            })
        else:
            result["topology"].update({
                "mode": "cockroachdb-process-per-node",
                "expected_physical_node_identity_count": cell["physical_nodes"],
                "expected_serving_layout": {"cockroach_processes": cell["physical_nodes"]},
            })
        ports = sql_ports_for(args, cell, engine)
        labels = endpoint_labels(ports)
        urls = endpoint_urls(engine, ports)
        result["routing"] = {
            "endpoint_mode": "ordinary_loopback_round_robin_per_client",
            "endpoint_count": len(labels),
            "endpoint_labels": labels,
            "client_endpoint_identity": "sample.endpoint == sample.client % endpoint_count",
            "client_endpoint_labels": client_endpoint_choices(cell, labels),
            "leader_placement": "retained in published manifests/range inventory when available",
        }
        run(["docker", "volume", "create", volume])
        run(["docker", "run", "-d", "--name", container, "--cpus", args.cpus,
             "--memory", args.memory, "--memory-swap", args.memory, "--mount", f"source={volume},target=/data",
             "--entrypoint", "sleep", args.runtime_image_id, "infinity"])
        container_created = True
        run(["docker", "cp", str(binaries), container + ":/bench"])
        run(["docker", "cp", str(schema), container + ":/bench/schema"])
        run(["docker", "exec", container, "mkdir", "-p", "/evidence", "/data/certs"])
        save_inventory(destination, process_inventory(container), "before")
        inspection = docker_json(["docker", "inspect", container])
        write_json(destination / "docker-inspect-before.json", inspection)
        result["observed_limits"] = validate_resource_limits(inspection[0], args.cpus, args.memory)
        (destination / "stats-before.json").write_text(json.dumps(docker_stats(container), indent=2) + "\n")

        if engine == "crdb":
            crdb_commands = []
            for cert in (("create-ca",), ("create-node", "localhost", "127.0.0.1"),
                         ("create-client", "root")):
                run(["docker", "exec", container, "/bench/cockroach", "cert", *cert,
                     "--certs-dir=/data/certs", "--ca-key=/data/ca.key"])
            for node in range(cell["physical_nodes"]):
                port = 26257 + node
                http = 8080 + node
                log = (destination / f"crdb-{node}.log").open("wb")
                open_logs.append(log)
                start_command = ["/bench/cockroach", "start",
                     "--certs-dir=/data/certs", "--accept-sql-without-tls",
                     "--store", f"/data/crdb-{node}", "--listen-addr", f"127.0.0.1:{port}",
                     "--http-addr", f"127.0.0.1:{http}", "--join",
                     ",".join(f"127.0.0.1:{26257 + peer}" for peer in range(cell["physical_nodes"])),
                     "--cache=512MiB", "--max-sql-memory=512MiB"]
                crdb_commands.append(start_command)
                process = subprocess.Popen(
                    ["docker", "exec", container, *start_command],
                    stdout=log, stderr=subprocess.STDOUT)
                processes.append(process)
                result.setdefault("log_files", []).append(str(log_path_name(log)))
            result["server_argv"] = crdb_commands
            deadline = time.monotonic() + 90
            while True:
                init = run(["docker", "exec", container, "/bench/cockroach", "init",
                            "--certs-dir=/data/certs", "--host=127.0.0.1:26257"], check=False,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if init.returncode == 0:
                    break
                if time.monotonic() > deadline:
                    raise RunnerError("CockroachDB init did not become ready")
                time.sleep(0.5)
            for statement in (
                    "SET CLUSTER SETTING server.host_based_authentication.configuration = 'host all all 127.0.0.1/32 trust'",
                    "CREATE USER bench", "GRANT admin TO bench"):
                run(["docker", "exec", container, "/bench/cockroach", "sql",
                     "--certs-dir=/data/certs", "--host=127.0.0.1:26257", "-e", statement])
            wait_for_tcp_ports(container, ports, args.ready_timeout)
            client_engine = "cockroachdb"
        else:
            substitutions = {
                "{ROOT}": "/data/vibe",
                "{PG}": labels[0],
                "{SQL_PORTS}": ",".join(labels),
                "{NODES}": str(cell["physical_nodes"]),
                "{GROUPS}": str(cell["groups"]),
                "{SCHEMA_DIR}": "/bench/schema",
                "{SHARD}": f"/bench/{engine}-vibedb-shard",
                "{GATEWAY}": f"/bench/{engine}-vibedb-gateway",
            }
            if engine == "parent":
                command = [f"/bench/{engine}-vibedb", "cluster", "dev", "--root", "/data/vibe",
                           "--replicas", "3", "--node-log",
                           "--pg-listen", labels[0], "--shard-binary", substitutions["{SHARD}"],
                           "--gateway-binary", substitutions["{GATEWAY}"], "--diagnostics-on-exit"]
                extra = args.parent_arg
                mode = "cluster-dev"
            else:
                command = [f"/bench/{engine}-vibedb", "cluster", "dev", "--root", "/data/vibe",
                           "--replicas", "3", "--physical-nodes", str(cell["physical_nodes"]), "--node-log",
                           "--pg-listens" if cell.get("endpoint_mode") == "per-node" else "--pg-listen",
                           ",".join(labels), "--shard-binary", substitutions["{SHARD}"],
                           "--gateway-binary", substitutions["{GATEWAY}"], "--diagnostics-on-exit"]
                extra = args.candidate_arg
                mode = "cluster-dev"
            if cell["groups"] > 1 and mode == "cluster-dev":
                # Legacy parent supports one initial schema and live SQL DDL
                # enrollment into existing node logs. Candidate accepts all
                # schemas up front. Post-setup inventories must prove both.
                for table in cell["tables"] if engine == "candidate" else cell["tables"][:1]:
                    command.extend(["--table-schema", f"/bench/schema/{table}.sql"])
            command.extend(render_arg(value, substitutions) for value in extra)
            result["server_argv"] = command
            log = (destination / f"{engine}-server.log").open("wb")
            open_logs.append(log)
            result.setdefault("log_files", []).append(str(log_path_name(log)))
            process = subprocess.Popen(["docker", "exec", container, *command],
                                       stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
            markers = [args.ready_marker, "VibeDB development cluster ready:", "VibeDB development RF3 physical cluster ready:"]
            wait_for_marker(process, destination / f"{engine}-server.log", markers, args.ready_timeout)
            wait_for_tcp_ports(container, ports, args.ready_timeout)
            client_engine = "vibedb"

        result["node_endpoint_labels"] = {
            str(index): label for index, label in enumerate(labels)
        }
        result["status"] = "ready"
        client = ["/bench/rf3-sqlbench", "-engine", client_engine, "-url", urls[0],
                  "-urls", ",".join(urls),
                  "-rows", str(args.rows), "-operations", str(args.operations),
                  "-scans", str(args.scans), "-warmup", str(args.warmup),
                  "-repetitions", str(args.repetitions), "-clients", cell["clients"],
                  "-tables", ",".join(cell["tables"]), "-workloads", cell["workloads"],
                  "-group-distribution", cell["group_distribution"], "-skew-percent", str(args.skew_percent),
                  "-physical-nodes", str(9 if engine == "parent" else cell["physical_nodes"]), "-output", "/evidence/report.json"]
        if engine == "candidate" and cell["groups"] > 1:
            client.extend(["-require-existing-tables"])
        result["setup"] = {"client_binary": "/bench/rf3-sqlbench", "phase": "setup",
                           "tables": cell["tables"], "timed": False,
                           "parent_live_ddl_enrollment": engine == "parent" and cell["groups"] > 1}
        result["setup"]["started_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        setup_started = time.monotonic_ns()
        with (destination / "setup-client.log").open("wb") as setup_log:
            setup_result = run(["docker", "exec", container, *client, "-phase", "setup"],
                               stdout=setup_log, stderr=subprocess.STDOUT, check=False)
        result["setup"]["exit_code"] = setup_result.returncode
        result["setup"]["elapsed_ns"] = time.monotonic_ns() - setup_started
        write_json(destination / "setup.json", result["setup"])
        if setup_result.returncode:
            raise RunnerError("untimed shared-client setup failed; see setup-client.log")
        ready_inventory = process_inventory(container)
        save_inventory(destination, ready_inventory, "ready")
        result["published_topology_ready"] = copy_published_inventories(
            container, destination, ready_inventory, "ready")
        result["topology"]["identity_inventory_ready"] = published_identity_inventory(
            destination, "ready", result["published_topology_ready"])
        result["process_counts"] = process_counts(ready_inventory)
        result["topology"]["actual_serving_process_counts_ready"] = result["process_counts"]
        result["topology"]["published_inventory_files_ready"] = result["published_topology_ready"]["copied"]
        validate_serving_topology(engine, cell, result["process_counts"])
        validate_published_topology(engine, cell, result["topology"]["identity_inventory_ready"])
        if result["published_topology_ready"]["failed"]:
            raise RunnerError("published topology files could not all be retained")
        if engine == "crdb":
            retain_crdb_ranges(container, destination, cell, "ready")
        if engine == "candidate":
            targets = candidate_diagnostic_targets(destination, ready_inventory, cell["physical_nodes"])
            write_json(destination / "diagnostic-targets.json", targets)
            run(["docker", "cp", destination / "diagnostic-targets.json", container + ":/evidence/diagnostic-targets.json"])
            client.extend(["-diagnostic-targets", "/evidence/diagnostic-targets.json"])
            result["diagnostics"] = {"mode": "signal-acknowledged-snapshots", "targets": targets,
                                     "boundaries": "after warmup/before timer; after timer/before verification",
                                     "counter_deltas_include_background_work_between_snapshots": True}
            if getattr(args, "rf3_diagnostic", False):
                diagnostic_ready_file = "/evidence/rf3-diagnostic-ready"
                run(["docker", "exec", container, "rm", "-f", diagnostic_ready_file],
                    check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                diagnostic_log = (destination / "per-group-diagnostic.log").open("wb")
                diagnostic_process = subprocess.Popen(
                    ["docker", "exec", container, "/bench/rf3-diagnostic",
                     "-root", "/data/vibe",
                     "-output", "/evidence/per-group-snapshots.jsonl",
                     "-ready-file", diagnostic_ready_file,
                     "-interval", "500ms",
                     "-request-timeout", "350ms",
                     "-max-bytes", str(8 << 20)],
                    stdout=diagnostic_log, stderr=subprocess.STDOUT)
                result["diagnostics"]["per_group"] = {
                    "binary": "/bench/rf3-diagnostic",
                    "output": "per-group-snapshots.jsonl",
                    "interval_ms": 500,
                    "request_timeout_ms": 350,
                    "max_bytes": 8 << 20,
                    "sampling_cost_excluded_from_sql_measurement": True,
                    "preflight": "all group/member status and metrics cuts required before client launch",
                    "ready_file": diagnostic_ready_file,
                }
                result["diagnostics"]["per_group_preflight_seconds"] = wait_for_diagnostic_preflight(
                    diagnostic_process, container, diagnostic_ready_file)
        result["status"] = "measuring"
        write_json(destination / "run.json", result)
        client_log = (destination / "client.log").open("wb")
        result["client_control"] = {
            "binary": "/bench/rf3-sqlbench",
            "engine": client_engine,
            "endpoint_count": len(labels),
            "endpoint_routing": "round-robin-per-client",
            "urls_redacted": True,
        }
        try:
            measured = subprocess.run(["docker", "exec", container, *client, "-phase", "run"],
                                      stdout=client_log, stderr=subprocess.STDOUT, check=False)
        finally:
            client_log.close()
        if diagnostic_process is not None:
            diagnostic_process.terminate()
            try:
                diagnostic_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                diagnostic_process.kill()
                diagnostic_process.wait(timeout=5)
            result["diagnostics"]["per_group_exit_code"] = diagnostic_process.returncode
            diagnostic_process = None
            if diagnostic_log is not None:
                diagnostic_log.close()
                diagnostic_log = None
            copied = run(["docker", "cp", container + ":/evidence/per-group-snapshots.jsonl",
                          destination / "per-group-snapshots.jsonl"], check=False,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            result["diagnostics"]["per_group_copied"] = copied.returncode == 0
            if copied.returncode != 0:
                result["errors"].append("per-group diagnostic output was not retained")
        result.setdefault("log_files", []).append("client.log")
        result["client_exit_code"] = measured.returncode
        result["status"] = "completed" if measured.returncode == 0 else "failed"
        copied_report = run(["docker", "cp", container + ":/evidence/report.json", destination / "report.json"], check=False)
        if copied_report.returncode:
            raise RunnerError("client report was not retained")
        if engine == "candidate":
            run(["docker", "cp", container + ":/evidence/diagnostics", destination / "diagnostics"])
        result["validation"] = validate_client_report(destination / "report.json", args, cell, client_engine, engine)
        if not result["validation"]["complete"]:
            result["status"] = "failed"
            result["errors"].append("client report is incomplete or failed")
        inventory = process_inventory(container)
        save_inventory(destination, inventory, "after")
        result["published_topology_after"] = copy_published_inventories(
            container, destination, inventory, "after")
        result["topology"]["identity_inventory_after"] = published_identity_inventory(
            destination, "after", result["published_topology_after"])
        result["process_counts_after"] = process_counts(inventory)
        result["topology"]["actual_serving_process_counts_after"] = result["process_counts_after"]
        result["topology"]["published_inventory_files_after"] = result["published_topology_after"]["copied"]
        validate_serving_topology(engine, cell, result["process_counts_after"])
        validate_published_topology(engine, cell, result["topology"]["identity_inventory_after"])
        if engine == "crdb":
            retain_crdb_ranges(container, destination, cell, "after")
        result["stats_after"] = docker_stats(container)
        (destination / "docker-inspect-after.json").write_text(
            json.dumps(docker_json(["docker", "inspect", container]), indent=2) + "\n")
        for root in ("/data/vibe",) if engine != "crdb" else tuple(f"/data/crdb-{n}" for n in range(cell["physical_nodes"])):
            for kind, extra in (("allocated", []), ("apparent", ["--apparent-size"])):
                storage = run(["docker", "exec", container, "du", "--block-size=1", "-a", *extra, root], check=False,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                (destination / (Path(root).name + f"-storage-{kind}-bytes.txt")).write_bytes(storage.stdout)
                if storage.returncode:
                    raise RunnerError(f"{kind} storage inventory failed for {root}")
        if measured.returncode != 0:
            result["errors"].append("client exited nonzero")
    except Exception as exc:  # retain setup/readiness failures beside logs
        result["status"] = "failed"
        result["errors"].append(str(exc))
    finally:
        if diagnostic_process is not None:
            diagnostic_process.terminate()
            try:
                diagnostic_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                diagnostic_process.kill()
                diagnostic_process.wait(timeout=5)
            diagnostic_process = None
        if diagnostic_log is not None:
            diagnostic_log.close()
            diagnostic_log = None
        if result["status"] not in {"completed", "failed"}:
            result["status"] = "incomplete"
            result["errors"].append("runner interrupted before the fixture completed")
        try:
            result["forced_shutdown"] = bool(stop_processes(processes, container, destination)) if processes else False
            result["server_exit_codes"] = [process.poll() for process in processes]
            if container_created:
                # Capture partial topology and the last atomic client checkpoint
                # even when setup, readiness, verification or the runner failed.
                run(["docker", "cp", container + ":/evidence", destination / "raw"], check=False)
                partial_inventory = process_inventory(container)
                save_inventory(destination, partial_inventory, "stopped")
                result["published_topology_stopped"] = copy_published_inventories(container, destination, partial_inventory, "stopped")
                result["server_logs"] = copy_published_inventories(container, destination, partial_inventory, "server-logs", field="logs")
        except Exception as exc:
            result["status"] = "failed"
            result["errors"].append(f"retaining stopped fixture evidence: {exc}")
        finally:
            for log in open_logs:
                log.close()
            if container_created:
                try:
                    removed = run(["docker", "rm", "-f", container], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if removed.returncode:
                        raise RunnerError("Docker refused removal")
                except Exception as exc:
                    result["status"] = "failed"
                    result["errors"].append(f"fixture container cleanup failed; no further measurements are safe: {exc}")
            try:
                removed_volume = run(["docker", "volume", "rm", volume], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if removed_volume.returncode:
                    raise RunnerError("Docker refused volume removal")
            except Exception as exc:
                result["errors"].append(f"fixture volume cleanup failed: {exc}")
                result["status"] = "failed"
            write_json(destination / "run.json", result)
    return result


def retain_crdb_ranges(container, destination, cell, prefix):
    for table in cell["tables"]:
        with (destination / f"{prefix}-crdb-ranges-{table}.csv").open("wb") as output:
            run(["docker", "exec", container, "/bench/cockroach", "sql", "--certs-dir=/data/certs",
                 "--host=127.0.0.1:26257", "--format=csv", "-e", f"SHOW RANGES FROM TABLE {table} WITH DETAILS"],
                stdout=output, stderr=subprocess.STDOUT)


def validate_client_report(path, args, cell, engine, server_engine):
    spec = importlib.util.spec_from_file_location("rf3_sql_validator", getattr(args, "validator_path", Path(__file__).with_name("summarize-crdb-sql.py")))
    validator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(validator)
    report, samples = validator.load(path, engine)
    expected = {"Rows": args.rows, "Operations": args.operations, "ScanOperations": args.scans,
                "Warmup": args.warmup, "Repetitions": args.repetitions, "Clients": cell["clients"],
                "SeedBatch": 64, "VerifyEveryTrial": True,
                "Tables": cell["tables"], "Workloads": parse_workloads(cell["workloads"], "workloads"),
                "GroupDistribution": cell["group_distribution"], "SkewPercent": args.skew_percent,
                "PayloadBytes": DEFAULT_PAYLOAD_BYTES,
                "KeySelection": "splitmix64-independent-with-replacement-v1",
                "DiagnosticMode": "signal-acknowledged-snapshots" if server_engine == "candidate" else "none",
                "PhysicalNodes": 9 if server_engine == "parent" else cell["physical_nodes"],
                "EndpointCount": cell["physical_nodes"] if cell.get("endpoint_mode") == "per-node" else 1}
    if any(report["config"].get(key) != value for key, value in expected.items()):
        raise RunnerError("client report configuration differs from the planned workload")
    complete = report.get("status") == "complete" and not report.get("verification_error")
    return {"samples_checked": samples, "complete": complete}


def log_path_name(file_object):
    return Path(file_object.name).name


def build_binary(source, package, output, env):
    run(["go", "build", "-mod=readonly", "-trimpath", "-o", output, package], cwd=source, env=env)


def prepare_worktrees(repo, parent_ref, candidate_ref, root):
    parent = root / "parent"
    candidate = root / "candidate"
    if parent_ref == candidate_ref:
        raise RunnerError("parent and candidate refs must differ")
    run(["git", "worktree", "add", "--detach", parent, parent_ref], cwd=repo)
    try:
        run(["git", "worktree", "add", "--detach", candidate, candidate_ref], cwd=repo)
    except Exception:
        run(["git", "worktree", "remove", "--force", parent], cwd=repo, check=False)
        raise
    return parent, candidate


def cleanup_worktrees(repo, worktree_root, parent, candidate):
    for path in (candidate, parent):
        run(["git", "worktree", "remove", "--force", path], cwd=repo, check=False)
    shutil.rmtree(worktree_root, ignore_errors=True)


def build_all(args, destination, parent, candidate, client_source, arch):
    bins = destination / "bin"
    bins.mkdir(mode=0o700)
    go_version = text_output(["go", "version"])
    if not re.search(r"\bgo1\.27(?:\.\d+)?\s", go_version):
        raise RunnerError(f"Go 1.27 is required, got {go_version}")
    env = dict(os.environ, GOOS="linux", GOARCH=arch, CGO_ENABLED="0", GOEXPERIMENT="simd")
    for prefix, source in (("parent", parent), ("candidate", candidate)):
        for name, package in ((f"{prefix}-vibedb", "./cmd/vibedb"),
                              (f"{prefix}-vibedb-shard", "./cmd/vibedb-shard"),
                              (f"{prefix}-vibedb-gateway", "./cmd/vibedb-gateway")):
            build_binary(source, package, bins / name, env)
    module = client_source / "integration" / "pgclient"
    if not (module / "cmd" / "rf3-sqlbench" / "main.go").is_file():
        raise RunnerError("client-source must be a VibeDB repository root with integration/pgclient")
    build_binary(module, "./cmd/rf3-sqlbench", bins / "rf3-sqlbench", env)
    return bins, go_version


def parser():
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("output", type=Path)
    value.add_argument("--repo", type=Path, default=ROOT)
    value.add_argument("--parent-ref", default=PARENT_DEFAULT_REF)
    value.add_argument("--candidate-ref", required=True)
    value.add_argument("--client-source", type=Path, default=ROOT,
                       help="clean source tree used once for the shared client binary")
    value.add_argument("--matrix", choices=("base", "all"), default="base")
    value.add_argument("--physical-nodes", default="3", help="multigroup physical-node counts, e.g. 3,6")
    value.add_argument("--groups", type=int, default=4)
    value.add_argument("--tables", default="", help="explicit multigroup table names")
    value.add_argument("--table-prefix", default="rf3_sql_group")
    value.add_argument("--distributions", default="uniform,skewed")
    value.add_argument("--endpoint-modes", default="single,per-node",
                       help="matched single-entrypoint diagnostics and separate per-node frontend scaling cells")
    value.add_argument("--multigroup-workloads", default="mixed_read_update")
    value.add_argument("--clients", default="1,8")
    value.add_argument("--multigroup-clients", default="8")
    value.add_argument("--rows", type=int, default=8192)
    value.add_argument("--operations", type=int, default=20000)
    value.add_argument("--scans", type=int, default=2000)
    value.add_argument("--warmup", type=int, default=1000)
    value.add_argument("--repetitions", type=int, default=3)
    value.add_argument("--skew-percent", type=int, default=80)
    value.add_argument("--cpus", default="12")
    value.add_argument("--memory", default="24g")
    value.add_argument("--order", choices=("parent-first", "candidate-first", "both"), default="both")
    value.add_argument("--include-crdb", action=argparse.BooleanOptionalAction, default=True)
    value.add_argument("--candidate-arg", action="append", default=[],
                       help="one exact candidate cluster-dev argv token; supports {ROOT},{PG},{SQL_PORTS},{NODES},{GROUPS},{SCHEMA_DIR},{SHARD},{GATEWAY}")
    value.add_argument("--parent-arg", action="append", default=[],
                       help="one exact parent cluster-dev argv token; supports the same substitutions")
    value.add_argument("--vibedb-sql-ports", default="5432",
                       help="explicit loopback SQL frontend ports; per-node cells require one per physical node")
    value.add_argument("--ready-marker", default="VibeDB development cluster ready:")
    value.add_argument("--ready-timeout", type=int, default=120)
    return value


def main(argv=None):
    global COMMAND_LOG
    args = parser().parse_args(argv)
    destination = require_new_directory(args.output)
    COMMAND_LOG = destination / "control-commands.jsonl"
    if not (64 <= args.rows <= 1000000 and 1 <= args.operations <= 1000000 and 1 <= args.scans <= 100000 and
            0 <= args.warmup <= 100000 and 1 <= args.repetitions <= 20 and args.ready_timeout >= 1):
        raise RunnerError("rows/operations/scans/repetitions are invalid")
    if args.groups < 1 or args.groups > 63 or not 51 <= args.skew_percent <= 99:
        raise RunnerError("groups or skew-percent are invalid")
    repo = args.repo.resolve()
    client_source = args.client_source.resolve()
    if not repo.is_dir() or not client_source.is_dir():
        raise RunnerError("repo and client-source must be directories")
    manifest = {
        "schema": "vibedb.fused-rf3-sql/2",
        "invocation": redact([str(value) for value in ([__file__] + (argv if argv is not None else os.sys.argv[1:]))]),
        "crdb_image": CRDB,
        "runtime_image": RUNTIME,
        "limits": {"cpus": args.cpus, "memory": args.memory, "memory_swap": args.memory,
                   "swap_disabled": True, "shared_total_including_client": True},
        "client": {"source": str(client_source), "shared_for_parent_candidate_crdb": True},
        "matrix": {"name": args.matrix, "rows_per_table": args.rows,
                   "payload_bytes_per_row": DEFAULT_PAYLOAD_BYTES,
                   "total_rows_formula": "rows_per_table * table_count",
                   "total_payload_formula": "rows_per_table * table_count * payload_bytes_per_row",
                   "five_workload_default": DEFAULT_WORKLOADS,
                   "no_processes_as_replicas": True,
                   "single_host_physical_node_disclosure": True,
                   "baseline_parent_topology": "legacy node-log: 9 shard role processes + standalone gateway",
                   "candidate_topology": "fused serving process per requested physical node",
                   "crdb_topology": "one CRDB process per requested node in the same container"},
        "routing": {
            "mode": "ordinary_loopback_round_robin_per_client",
            "vibedb_sql_ports": args.vibedb_sql_ports,
            "crdb_sql_ports": "26257 + node index",
            "credentials_in_evidence": False,
        },
        "orders": [],
        "engine_sequences": {},
        "runs": [],
    }
    try:
        manifest["expected_resource_limits"] = resource_limits(args.cpus, args.memory)
        cells = cell_matrix(args)
        for cell in cells:
            clients = parse_positive_csv(cell["clients"], "clients")
            if any(value > 15 for value in clients) or args.operations < max(clients) or args.scans < max(clients):
                raise RunnerError("each configured client needs at least one operation and clients must be <=15")
        orders = (["parent-first"] if args.order == "parent-first" else
                  ["candidate-first"] if args.order == "candidate-first" else
                  ["parent-first", "candidate-first"])
        manifest["orders"] = orders
        engine_sequences = {order: engines_for_order(order, args.include_crdb)
                            for order in orders}
        manifest["engine_sequences"] = engine_sequences
        manifest["planned_runs"] = [{"cell": cell["id"], "order": order, "engine": engine}
                                    for order in orders for cell in cells
                                    for engine in engine_sequences[order]]
        manifest["control_sha256"] = {}
        controls = destination / "controls"
        controls.mkdir(mode=0o700)
        for name in ("run-fused-node-comparison.py", "summarize-crdb-sql.py"):
            source = Path(__file__).with_name(name)
            shutil.copy2(source, controls / name)
            manifest["control_sha256"][name] = hashlib.sha256((controls / name).read_bytes()).hexdigest()
        args.validator_path = controls / "summarize-crdb-sql.py"
        arch = docker_architecture()
        manifest["docker_architecture"] = arch
        manifest["docker_host"] = docker_json(["docker", "info", "--format", "{{json .}}"])
        source_snapshot = {"parent": git_info(repo), "client": git_info(client_source)}
        if source_snapshot["client"]["dirty"]:
            raise RunnerError("client-source must be clean so the shared client can be built from an immutable detached worktree")
        parent_revision = text_output(["git", "rev-parse", args.parent_ref + "^{commit}"], cwd=repo)
        candidate_revision = text_output(["git", "rev-parse", args.candidate_ref + "^{commit}"], cwd=repo)
        if parent_revision != PARENT_DEFAULT_REF or parent_revision == candidate_revision:
            raise RunnerError("comparison requires the pinned reviewed parent and a distinct candidate commit")
        with tempfile.TemporaryDirectory(prefix="vibedb-fused-worktrees-") as temp:
            worktree_root = Path(temp)
            parent, candidate = prepare_worktrees(repo, parent_revision, candidate_revision, worktree_root)
            client_tree = worktree_root / "client"
            try:
                run(["git", "worktree", "add", "--detach", client_tree, source_snapshot["client"]["revision"]], cwd=client_source)
                source_snapshot["parent"] = git_info(parent)
                source_snapshot["candidate"] = git_info(candidate)
                source_snapshot["client"] = git_info(client_tree)
                for name, info in source_snapshot.items():
                    if name in {"parent", "candidate", "client"}:
                        write_git_evidence(destination, name, info)
                bins, go_version = build_all(args, destination, parent, candidate, client_tree, arch)
                manifest["go_version"] = go_version
                manifest["images"] = {
                    "runtime": ensure_image(RUNTIME),
                }
                args.runtime_image_id = manifest["images"]["runtime"]["Id"]
                if args.include_crdb:
                    manifest["images"]["crdb"] = ensure_image(CRDB)
                    extract_crdb_binary(bins)
                manifest["source"] = {name: {key: value for key, value in info.items() if key != "patch"}
                                      for name, info in source_snapshot.items()}
                manifest["refs"] = {"parent": parent_revision, "candidate": candidate_revision,
                                    "requested_parent": args.parent_ref, "requested_candidate": args.candidate_ref}
                manifest["binary_sha256"] = binary_hashes(bins)
                write_json(destination / "manifest.json", manifest)
                for order in orders:
                    for cell in cells:
                        for engine in engine_sequences[order]:
                            run_dir = destination / cell["id"] / order / engine
                            schema = schema_files(run_dir, cell["tables"])
                            result = run_engine(args, cell, engine, order, bins, run_dir, schema, arch)
                            result["dataset"] = {
                                "rows_per_table": args.rows,
                                "table_count": len(cell["tables"]),
                                "total_rows": args.rows * len(cell["tables"]),
                                "payload_bytes_per_row": DEFAULT_PAYLOAD_BYTES,
                                "total_payload_bytes": args.rows * len(cell["tables"]) * DEFAULT_PAYLOAD_BYTES,
                                "group_distribution": cell["group_distribution"],
                            }
                            manifest["runs"].append(result)
                            write_json(run_dir / "run.json", result)
                            write_json(destination / "manifest.json", manifest)
                            if any("container cleanup failed" in error for error in result["errors"]):
                                raise RunnerError("stopped after fixture cleanup failure")
            finally:
                if client_tree.exists():
                    run(["git", "worktree", "remove", "--force", client_tree], cwd=client_source, check=False)
                cleanup_worktrees(repo, worktree_root, parent, candidate)
    except (Exception, KeyboardInterrupt) as exc:
        manifest["fatal_error"] = redact(str(exc) or "runner interrupted")
        manifest["status"] = "incomplete-or-failed"
        write_json(destination / "manifest.json", manifest)
        return 1
    failed = [run for run in manifest["runs"] if run.get("status") != "completed" or run.get("client_exit_code") != 0]
    manifest["status"] = "complete" if not failed else "incomplete-or-failed"
    manifest["promotion_gate"] = {
        "status": "not_evaluated",
        "reason": "the runner retains parent/candidate AB and BA evidence; Sol max review and all correctness gates are required before timing claims",
        "required_geomean_ratio": 1.25,
        "max_cell_throughput_regression": 0.05,
        "max_cell_p99_regression": 0.10,
        "both_orderings_required": True,
    }
    write_json(destination / "manifest.json", manifest)
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RunnerError as exc:
        print(f"run-fused-node-comparison: {exc}", file=os.sys.stderr)
        raise SystemExit(2)
