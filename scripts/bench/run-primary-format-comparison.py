#!/usr/bin/env python3
"""Retain alternating paired primary-format benchmark runs on one idle runner."""
import argparse
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


def read_metrics(output):
    metrics = {}
    for line in output.splitlines():
        fields = line.split()
        if not fields or not fields[0].startswith("Benchmark") or len(fields) < 4:
            continue
        try:
            int(fields[1])
            for offset in range(2, len(fields)-1, 2):
                value = float(fields[offset])
                unit = fields[offset+1]
                metrics[fields[0]+" "+unit] = value
        except ValueError:
            raise RuntimeError("malformed benchmark row: "+line)
    if not metrics:
        raise RuntimeError("benchmark produced no metrics")
    return metrics


def summarize(pairs):
    generator = random.Random(917365)
    keys = pairs[0]["base"].keys()
    if any(p[arm].keys() != keys for p in pairs for arm in ("base", "candidate")):
        raise RuntimeError("benchmark metric sets differ")
    results = {}
    for key in sorted(keys):
        base = [p["base"][key] for p in pairs]
        candidate = [p["candidate"][key] for p in pairs]
        row = {"base_median": statistics.median(base),
               "candidate_median": statistics.median(candidate)}
        if all(x > 0 for x in base+candidate):
            logs = [math.log(c/b) for c,b in zip(candidate,base)]
            boots = sorted(math.exp(statistics.mean(generator.choices(logs,k=len(logs))))
                           for _ in range(10000))
            row.update(candidate_over_base_geomean=math.exp(statistics.mean(logs)),
                       paired_bootstrap_95_percent_interval=[boots[249],boots[9749]],
                       paired_ratios=[c/b for c,b in zip(candidate,base)])
        results[key] = row
    return results


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binaries",required=True,type=Path)
    p.add_argument("--output",required=True,type=Path)
    p.add_argument("--suite",choices=("primary","query","all"),default="primary")
    p.add_argument("--runs",type=int,default=6)
    args=p.parse_args()
    if not 3 <= args.runs <= 30: p.error("runs must be 3..30")
    args.output.mkdir(parents=True,exist_ok=False)
    selected_suites=("primary","query") if args.suite == "all" else (args.suite,)
    suite_patterns={
        "storeio":r"^BenchmarkCompactRankFormat$",
        "durable":r"^(BenchmarkUnifiedGetRaw|BenchmarkUnifiedPrimaryReplace|BenchmarkUnifiedScanAllBytesLowCardinality|BenchmarkUnifiedScanAllBytesHighCardinality|BenchmarkFileStoreBatchWrite)$",
        "query":r"^BenchmarkRankAffineQueryFormat$",
    }
    suite_binaries={
        "primary":("storeio","durable"),
        "query":("query",),
    }
    selected_binary_suites=tuple(suite for group in selected_suites
                                 for suite in suite_binaries[group])
    binaries={(arm,suite):(args.binaries/f"{arm}-{suite}.test").resolve()
              for arm in ("base","candidate")
              for suite in selected_binary_suites}
    metadata={"started_unix_seconds":time.time(),"platform":platform.platform(),
              "suite":args.suite,"selected_suites":list(selected_suites),
              "selected_binary_suites":list(selected_binary_suites),
              "benchmark_patterns":{suite:suite_patterns[suite]
                                     for suite in selected_binary_suites},
              "runs_per_arm":args.runs,"benchtime":"200ms","gomaxprocs":1,
              "order":"base/candidate odd pairs; candidate/base even pairs",
              "binary_sha256":{f"{a}-{s}":hashlib.sha256(path.read_bytes()).hexdigest()
                               for (a,s),path in binaries.items()}}
    (args.output/"environment.json").write_text(json.dumps(metadata,indent=2)+"\n")
    env=dict(os.environ,GOMAXPROCS="1")
    pairs=[]
    for trial in range(1,args.runs+1):
        pair={}
        for arm in (("base","candidate") if trial%2 else ("candidate","base")):
            metrics={}
            for suite in selected_binary_suites:
                pattern=suite_patterns[suite]
                command=[str(binaries[arm,suite]),"-test.run=^$","-test.bench="+pattern,
                         "-test.benchtime=200ms","-test.count=1","-test.cpu=1","-test.timeout=8m"]
                logfile=args.output/f"{trial:02d}-{arm}-{suite}.txt"
                with logfile.open("w") as stream:
                    run_env=dict(env)
                    run_env.pop("VIBEDB_EXPECT_RANK_AFFINE_QUERY",None)
                    if suite == "query" and arm == "candidate":
                        run_env["VIBEDB_EXPECT_RANK_AFFINE_QUERY"]="1"
                    run=subprocess.run(command,env=run_env,stdout=stream,stderr=subprocess.STDOUT)
                if run.returncode: raise RuntimeError(f"{logfile.name} failed: {run.returncode}")
                metrics.update({suite+"/"+k:v for k,v in read_metrics(logfile.read_text()).items()})
            pair[arm]=metrics
            print(f"completed pair {trial}/{args.runs} {arm}",flush=True)
        pairs.append(pair)
        (args.output/"pairs.json").write_text(json.dumps(pairs,indent=2)+"\n")
    (args.output/"comparison.json").write_text(json.dumps(summarize(pairs),indent=2)+"\n")

if __name__=="__main__": main()
