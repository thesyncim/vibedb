#!/usr/bin/env python3
"""Alternate isolated RF3 process runs; retain every result and latency sample."""

import argparse
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import time


def metrics(evidence):
    result = {
        "elapsed_ns": evidence["ElapsedNS"],
        "allocated_bytes": evidence["FinalAllocated"],
    }
    for kind in ("insert", "update", "read"):
        values = sorted(s["ElapsedNS"] for worker in evidence["Samples"]
                        for s in worker if s["Kind"] == kind)
        for quantile in (50, 95, 99):
            result[f"{kind}_p{quantile}_ns"] = values[math.ceil(len(values)*quantile/100)-1]
        result[f"{kind}_mean_ns"] = statistics.mean(values)
        result[f"{kind}_max_ns"] = values[-1]
    return result


def summarize(pairs):
    # Resample complete paired trials, never individual operations as though
    # they were independent experiments. Report uncertainty, not a timing gate.
    generator = random.Random(73917)
    result = {}
    for metric in pairs[0]["base"]:
        ratios = [p["candidate"][metric]/p["base"][metric] for p in pairs]
        logs = [math.log(r) for r in ratios]
        boot = sorted(math.exp(statistics.mean(generator.choices(logs, k=len(logs))))
                      for _ in range(10000))
        result[metric] = {
            "base_median": statistics.median(p["base"][metric] for p in pairs),
            "candidate_median": statistics.median(p["candidate"][metric] for p in pairs),
            "candidate_over_base_geomean": math.exp(statistics.mean(logs)),
            "paired_bootstrap_95_percent_interval": [boot[249], boot[9749]],
            "paired_ratios": ratios,
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-binary", required=True, type=Path)
    parser.add_argument("--candidate-binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--runs", type=int, default=9)
    args = parser.parse_args()
    if not 3 <= args.runs <= 30:
        parser.error("runs must be between 3 and 30")
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {"base": args.base_binary.resolve(), "candidate": args.candidate_binary.resolve()}
    environment = dict(os.environ, VIBEDB_NODE_SPACE_E2E="1", GOMAXPROCS="2")
    environment.pop("VIBEDB_NODE_SPACE_WRITES", None)
    environment.pop("VIBEDB_WAL_RETENTION_E2E", None)
    metadata = {
        "platform": platform.platform(), "runs_per_arm": args.runs,
        "gomaxprocs_per_process": 2, "checkpoint_interval_seconds": 2,
        "workers": 2, "writes_per_worker": 2048, "document_bytes": 65536,
        "order": "base/candidate on odd pairs; candidate/base on even pairs",
        "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest()
                          for name, path in binaries.items()},
        "started_unix_seconds": time.time(),
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2)+"\n")
    pairs = []
    for trial in range(1, args.runs+1):
        pair = {}
        for arm in (("base", "candidate") if trial % 2 else ("candidate", "base")):
            prefix = args.output / f"{trial:02d}-{arm}"
            environment["VIBEDB_NODE_SPACE_EVIDENCE"] = str(prefix.with_suffix(".json.gz").resolve())
            environment["VIBEDB_NODE_SPACE_EXPECT_RECLAIM"] = "1" if arm == "candidate" else "0"
            command = [str(binaries[arm]), "-test.run=^TestServeRF3NodeSpaceQualification$",
                       "-test.v", "-test.timeout=10m"]
            print(f"trial {trial}/{args.runs} {arm}", flush=True)
            with prefix.with_suffix(".log").open("x") as log:
                run = subprocess.run(command, env=environment, stdout=log,
                                     stderr=subprocess.STDOUT, timeout=630)
            if run.returncode:
                raise RuntimeError(f"{prefix}: exit {run.returncode}; retained failed trial")
            with gzip.open(prefix.with_suffix(".json.gz")) as source:
                evidence = json.load(source)
            if not evidence["Passed"] or any(len(w) != 3072 for w in evidence["Samples"]):
                raise RuntimeError(f"{prefix}: incomplete/unqualified samples")
            pair[arm] = metrics(evidence)
            print(json.dumps(pair[arm], sort_keys=True), flush=True)
        pairs.append(pair)
        (args.output / "pairs.json").write_text(json.dumps(pairs, indent=2)+"\n")
    (args.output / "comparison.json").write_text(json.dumps(summarize(pairs), indent=2)+"\n")


if __name__ == "__main__":
    main()
