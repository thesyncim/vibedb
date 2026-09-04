#!/usr/bin/env python3
"""Reproduce a single-host RF3 SQL comparison, including failures.

Requires Docker, Python 3 and Go 1.27. Creates only uniquely named containers and
volumes; the output directory must be new. No host database is contacted.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
CRDB = "cockroachdb/cockroach:v26.3.1@sha256:204f131510c78393adb02345f289a8dbb32e1491e26cc92b6c7751f3b97be3c5"
RUNTIME = "golang:1.27-bookworm"


def run(args, *, check=True, **kwargs):
    return subprocess.run([str(x) for x in args], check=check, **kwargs)


def output(args):
    return subprocess.check_output([str(x) for x in args], text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--node-log", action="store_true", help="use fresh VibeDB shared-node log preparation and live group registration")
    parser.add_argument("--profile", action="store_true", help="instrument VibeDB with local CPU/trace profiles; diagnostic timings only")
    parser.add_argument("--rows", type=int, default=8192)
    parser.add_argument("--operations", type=int, default=20000)
    parser.add_argument("--scans", type=int, default=2000)
    parser.add_argument("--warmup", type=int, default=1000)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--clients", default="1,8")
    parser.add_argument("--order", choices=["vibedb-first", "crdb-first"], default="vibedb-first")
    args = parser.parse_args()
    dest = args.output.resolve()
    dest.mkdir(mode=0o700)  # refuses an existing evidence directory
    bins = dest / "bin"
    bins.mkdir()
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64", "amd64": "amd64"}[output(["docker", "info", "--format", "{{.Architecture}}"])]
    env = dict(os.environ, GOOS="linux", GOARCH=arch, CGO_ENABLED="0", GOEXPERIMENT="simd")
    run(["go", "build", "-o", str(bins) + "/", "./cmd/vibedb", "./cmd/vibedb-shard", "./cmd/vibedb-gateway"], cwd=ROOT, env=env)
    run(["go", "build", "-o", bins / "rf3-sqlbench", "./cmd/rf3-sqlbench"], cwd=ROOT / "integration/pgclient", env=env)
    run(["docker", "pull", CRDB])
    if run(["docker", "image", "inspect", RUNTIME], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode:
        run(["docker", "pull", RUNTIME])
    name = "vibedb-crdb-sql-" + uuid.uuid4().hex[:12]
    volume = name + "-data"
    extractor = name + "-binary"
    created = False
    failures = {}
    try:
        run(["docker", "create", "--name", extractor, CRDB])
        run(["docker", "cp", extractor + ":/cockroach/cockroach", bins / "cockroach"])
        run(["docker", "rm", extractor])
        manifest = {
            "source_revision": output(["git", "-C", ROOT, "rev-parse", "HEAD"]),
            "source_status": output(["git", "-C", ROOT, "status", "--porcelain"]),
            "go_version": output(["go", "version"]),
            "architecture": arch, "crdb_image": CRDB,
            "docker_info": json.loads(output(["docker", "info", "--format", "{{json .}}"])),
            "runtime_image": json.loads(output(["docker", "image", "inspect", RUNTIME])),
            "limits": {"shared_cpus": 12, "shared_memory_bytes": 24 << 30},
            "options": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
            "binary_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in bins.iterdir()},
            "method": "single Linux container; shared disk volume; engines run sequentially; SQL loopback trust/plaintext; inter-node TLS; no durability settings disabled",
        }
        (dest / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        (dest / "source.patch").write_bytes(subprocess.check_output(["git", "-C", ROOT, "diff", "HEAD"]))
        run(["docker", "volume", "create", volume])
        run(["docker", "run", "-d", "--name", name, "--cpus=12", "--memory=24g", "--mount", f"source={volume},target=/data", "--entrypoint", "sleep", RUNTIME, "infinity"])
        created = True
        run(["docker", "cp", bins, name + ":/bench"])

        def inside(*command, **kwargs):
            return run(["docker", "exec", name, *command], **kwargs)

        def shell(command, **kwargs):
            return inside("sh", "-c", command, **kwargs)

        def stop(processes):
            for process in processes:
                inside("pkill", "-TERM", "-x", process, check=False)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                rows = output(["docker", "exec", name, "ps", "-eo", "comm=,stat="]).splitlines()
                if not any(row.split()[0] in processes and not row.split()[1].startswith("Z") for row in rows):
                    return
                time.sleep(.5)
            # Measurements and full verification have finished. Draining all
            # CRDB voters at once can leave the last node awaiting its peers.
            # Stop only this runner's processes, record it, and ensure none can
            # consume resources in the next engine's measurement window.
            shell("printf '%s\\n' 'Forced post-measurement shutdown: " + ",".join(processes) + "' >> /evidence/shutdown-notes.txt")
            for process in processes:
                inside("pkill", "-KILL", "-x", process, check=False)
            for _ in range(20):
                rows = output(["docker", "exec", name, "ps", "-eo", "comm=,stat="]).splitlines()
                if not any(row.split()[0] in processes and not row.split()[1].startswith("Z") for row in rows):
                    return
                time.sleep(.5)
            raise RuntimeError("database process survived shutdown")

        shell("mkdir -p /evidence /data/certs")
        shell("uname -a > /evidence/platform.txt; stat -f -c %T /data >> /evidence/platform.txt; cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max >> /evidence/platform.txt")
        for cert in [["create-ca"], ["create-node", "localhost", "127.0.0.1"], ["create-client", "root"]]:
            inside("/bench/cockroach", "cert", *cert, "--certs-dir=/data/certs", "--ca-key=/data/ca.key")
        engines = ["vibedb", "cockroachdb"] if args.order == "vibedb-first" else ["cockroachdb", "vibedb"]
        for engine in engines:
            if engine == "vibedb":
                if args.profile:
                    shell("mkdir -p /evidence/profiles")
                profile_env = "VIBEDB_PROFILE_DIRECTORY=/evidence/profiles VIBEDB_PROFILE_DURATION=60s " if args.profile else ""
                node_log_flag = " --node-log" if args.node_log else ""
                shell(profile_env + "/bench/vibedb cluster dev --root /data/vibe --replicas 3" + node_log_flag + " --pg-listen 127.0.0.1:5432 --diagnostics-on-exit > /evidence/vibedb.log 2>&1 &")
                deadline = time.monotonic() + 90
                while True:
                    log = output(["docker", "exec", name, "cat", "/evidence/vibedb.log"])
                    if "VibeDB development cluster ready:" in log:
                        break
                    if time.monotonic() > deadline or "cluster dev: " in log:
                        raise RuntimeError("VibeDB startup failed: " + log)
                    time.sleep(.5)
                url = "postgresql://local@127.0.0.1:5432/vibedb?sslmode=disable"
                processes = ["vibedb", "vibedb-shard", "vibedb-gateway"]
            else:
                shell("for n in 0 1 2; do /bench/cockroach start --certs-dir=/data/certs --accept-sql-without-tls --store=/data/crdb-$n --listen-addr=127.0.0.1:$((26257+n)) --http-addr=127.0.0.1:$((8080+n)) --join=127.0.0.1:26257,127.0.0.1:26258,127.0.0.1:26259 --cache=512MiB --max-sql-memory=512MiB > /evidence/crdb-$n.log 2>&1 & done")
                inside("/bench/cockroach", "init", "--certs-dir=/data/certs", "--host=127.0.0.1:26257")
                for statement in ["SET CLUSTER SETTING server.host_based_authentication.configuration = 'host all all 127.0.0.1/32 trust'", "CREATE USER bench", "GRANT admin TO bench"]:
                    inside("/bench/cockroach", "sql", "--certs-dir=/data/certs", "--host=127.0.0.1:26257", "-e", statement)
                time.sleep(2)  # allow authentication setting to propagate before clients connect
                url = "postgresql://bench@127.0.0.1:26257/defaultdb?sslmode=disable"
                processes = ["cockroach"]
            shell(f"ps -eo pid,comm,args > /evidence/{engine}-processes.txt")
            with (dest / f"{engine}-client.log").open("w") as log:
                completed = inside("/bench/rf3-sqlbench", "-engine", engine, "-url", url,
                    "-rows", str(args.rows), "-operations", str(args.operations), "-scans", str(args.scans),
                    "-warmup", str(args.warmup), "-repetitions", str(args.repetitions), "-clients", args.clients,
                    "-output", f"/evidence/{engine}.json", stdout=log, stderr=subprocess.STDOUT, check=False)
                failures[engine] = completed.returncode
            shell(f"du -sk /data/* > /evidence/{engine}-storage.txt")
            # Untimed detail separates fixed journals/logs from data-dependent
            # growth; do not mistake the small fixture's total for amplification.
            storage_roots = "/data/vibe" if engine == "vibedb" else "/data/crdb-0 /data/crdb-1 /data/crdb-2"
            shell(f"du -ak {storage_roots} > /evidence/{engine}-storage-detail.txt")
            stop(processes)
            print(f"{engine}: exit={completed.returncode}; raw log: {dest / (engine + '-client.log')}", flush=True)
    finally:
        if created:
            run(["docker", "cp", name + ":/evidence", dest / "raw"], check=False)
            run(["docker", "rm", "-f", name], check=False)
        run(["docker", "rm", "-f", extractor], check=False, stderr=subprocess.DEVNULL)
        run(["docker", "volume", "rm", volume], check=False)
        (dest / "exit-codes.json").write_text(json.dumps(failures, indent=2) + "\n")
    return 1 if len(failures) != 2 or any(failures.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
