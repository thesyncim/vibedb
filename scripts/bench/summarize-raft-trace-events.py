#!/usr/bin/env python3
"""Summarize ``raft.append`` events from Go 1.27 parsed trace output.

Only unmatched ``submit`` records are retained while the trace is read.
Completed delays are grouped as entry-bearing, hint candidates, sync-empty,
snapshot-empty, or unknown when newer fields are absent.  ``empty`` is the
aggregate of the four zero-entry groups.
"""

import argparse
import ast
import json
import math
import re
import sys
from pathlib import Path
from statistics import mean, median
from typing import Dict, Iterable, List, Optional, Tuple


_TIME = re.compile(r"\bLog\s+Time=(\d+)\b")
_CATEGORY = re.compile(r"\bCategory=(\"(?:\\.|[^\"\\])*\")")
_MESSAGE = re.compile(r"\bMessage=(\"(?:\\.|[^\"\\])*\")")
_FIELD = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)")
_BUCKETS = ("entry-bearing", "hint_candidate", "empty_sync", "empty_snapshot", "unknown")
_EMPTY_BUCKETS = frozenset(_BUCKETS[1:])


def _unquote(value: str) -> str:
    try:
        decoded = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value[1:-1]
    return decoded if isinstance(decoded, str) else str(decoded)


def _event(line: str) -> Optional[Tuple[int, Dict[str, str]]]:
    timestamp = _TIME.search(line)
    category = _CATEGORY.search(line)
    message = _MESSAGE.search(line)
    if timestamp is None or category is None or message is None:
        return None
    if _unquote(category.group(1)) != "raft.append":
        return None
    fields = {match.group(1): match.group(2) for match in _FIELD.finditer(_unquote(message.group(1)))}
    return int(timestamp.group(1)), fields


def _number(fields: Dict[str, str], name: str, base: int = 10) -> Optional[int]:
    value = fields.get(name)
    if value is None:
        return None
    try:
        return int(value, base)
    except ValueError:
        return None


def _sync_name(value: Optional[str]) -> str:
    if value is None:
        return "unknown"
    lowered = value.lower()
    if lowered in ("true", "1"):
        return "true"
    if lowered in ("false", "0"):
        return "false"
    return "unknown"


def _bool_name(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    lowered = value.lower()
    if lowered in ("true", "1"):
        return True
    if lowered in ("false", "0"):
        return False
    return None


def _classify(fields: Dict[str, str]) -> Tuple[str, str]:
    entries = _number(fields, "entries")
    sync = _sync_name(fields.get("sync"))
    snapshot = _bool_name(fields.get("snapshot"))
    if entries is None or entries < 0:
        return "unknown", sync
    if entries > 0:
        return "entry-bearing", sync
    if sync == "unknown" or snapshot is None:
        return "unknown", sync
    if snapshot:
        return "empty_snapshot", sync
    if sync == "true":
        return "empty_sync", sync
    return "hint_candidate", sync


def _distribution(delays: List[float]) -> Dict[str, object]:
    ordered = sorted(delays)
    return {
        "count": len(ordered),
        "total_ms": sum(ordered),
        "mean_ms": mean(ordered) if ordered else None,
        "p50_ms": median(ordered) if ordered else None,
        "p95_ms": ordered[math.ceil(0.95 * len(ordered)) - 1] if ordered else None,
    }


def summarize(lines: Iterable[str]) -> Dict[str, object]:
    bucket_names = _BUCKETS + ("empty",)
    buckets = {
        name: {
            "delays": [],
            "unmatched_submit": 0,
            "unmatched_complete": 0,
            "duplicates": 0,
            "submit_sync": {"true": 0, "false": 0, "unknown": 0},
        }
        for name in bucket_names
    }
    # (group, ReadyID) -> (submit timestamp, bucket).
    pending: Dict[Tuple[int, int], Tuple[int, str]] = {}

    for line in lines:
        parsed = _event(line)
        if parsed is None:
            continue
        timestamp, fields = parsed
        group = _number(fields, "group", 16)
        ready = _number(fields, "ready")
        if group is None or ready is None:
            continue
        bucket, sync = _classify(fields)
        stage = fields.get("event")
        key = (group, ready)

        if stage == "submit":
            buckets[bucket]["submit_sync"][sync] += 1
            if bucket in _EMPTY_BUCKETS:
                buckets["empty"]["submit_sync"][sync] += 1
            if key in pending:
                # Keep the first boundary across a retry of this Ready.
                buckets[pending[key][1]]["duplicates"] += 1
                if pending[key][1] in _EMPTY_BUCKETS:
                    buckets["empty"]["duplicates"] += 1
            else:
                pending[key] = (timestamp, bucket)
        elif stage == "complete":
            # ``complete`` is emitted only after a successful persistence
            # result.  An optional success=false remains unmatched.
            if fields.get("success", "true").lower() in ("false", "0"):
                continue
            submitted = pending.pop(key, None)
            if submitted is None:
                buckets[bucket]["unmatched_complete"] += 1
                if bucket in _EMPTY_BUCKETS:
                    buckets["empty"]["unmatched_complete"] += 1
                continue
            started, submitted_bucket = submitted
            if timestamp < started:
                raise ValueError("negative raft.append interval")
            delay = (timestamp - started) / 1e6
            buckets[submitted_bucket]["delays"].append(delay)
            if submitted_bucket in _EMPTY_BUCKETS:
                buckets["empty"]["delays"].append(delay)

    for _, bucket in pending.values():
        buckets[bucket]["unmatched_submit"] += 1
        if bucket in _EMPTY_BUCKETS:
            buckets["empty"]["unmatched_submit"] += 1

    result = {"append": {}}
    for name in bucket_names:
        bucket = buckets[name]
        stats = _distribution(bucket["delays"])
        stats.update({
            "unmatched_submit": bucket["unmatched_submit"],
            "unmatched_complete": bucket["unmatched_complete"],
            "unmatched": bucket["unmatched_submit"] + bucket["unmatched_complete"],
            "duplicates": bucket["duplicates"],
            "submit_sync": bucket["submit_sync"],
        })
        result["append"][name] = stats
    result["scope_caveats"] = [
        "Intervals are same-process diagnostic wall time from Ready submit to the owner consuming a successful persistence result; persistence queueing and wake/scheduling latency are included.",
        "Only group and ReadyID are paired. A trace may begin or end mid-flight, and those boundary records are reported as unmatched.",
        "A hint_candidate is entries=0, sync=false, snapshot=false. Older records missing sync or snapshot remain unknown, so empty delays are not all attributed to hints.",
        "This parser does not subtract timestamps across processes or nodes; p50 and p95 are exact over completed delays.",
    ]
    return result


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("parsed_trace", type=Path, help="parsed trace path, or - for stdin")
    args = parser.parse_args(argv)
    if str(args.parsed_trace) == "-":
        result = summarize(sys.stdin)
    else:
        with args.parsed_trace.open(encoding="utf-8", errors="replace") as stream:
            result = summarize(stream)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
