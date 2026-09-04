#!/usr/bin/env python3
"""Summarize pg.* regions from Go 1.27 `go tool trace -d=parsed` output.

Only matched regions contribute. Report unmatched edges because flight-recorder
retention can truncate a region. Nested regions must not be added to parents.
Times are diagnostic wall time, including waiting, not CPU time.
"""
import argparse
from collections import defaultdict
import json
import math
import re
from statistics import mean, median

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("parsed_trace")
args = parser.parse_args()
pattern = re.compile(r' G=(\d+) Region(Begin|End) Time=(\d+) Task=\d+ Type="(pg\.[^"]+)"')
pending = defaultdict(list)
durations = defaultdict(list)
unmatched_end = defaultdict(int)
with open(args.parsed_trace) as stream:
    for line in stream:
        match = pattern.search(line)
        if not match:
            continue
        goroutine, kind, timestamp, name = match.groups()
        timestamp = int(timestamp)
        key = goroutine, name
        if kind == "Begin":
            pending[key].append(timestamp)
        elif pending[key]:
            duration = timestamp - pending[key].pop()
            if duration < 0:
                raise ValueError("negative region duration")
            durations[name].append(duration / 1e6)
        else:
            unmatched_end[name] += 1
result = {}
for name in sorted(set(durations) | set(unmatched_end) | {name for _, name in pending}):
    values = sorted(durations[name])
    result[name] = {
        "count": len(values), "total_ms": sum(values),
        "mean_ms": mean(values) if values else None,
        "p50_ms": median(values) if values else None,
        "p95_ms": values[math.ceil(.95 * len(values))-1] if values else None,
        "unmatched_begin": sum(len(stack) for (_, region), stack in pending.items() if region == name),
        "unmatched_end": unmatched_end[name],
    }
print(json.dumps(result, indent=2))
