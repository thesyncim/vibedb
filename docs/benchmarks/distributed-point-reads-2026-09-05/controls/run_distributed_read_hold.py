#!/usr/bin/env python3
"""Gate the first matched-read fixture run behind an explicit host marker."""

from datetime import datetime, timezone
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import time


RUNNER = Path("/private/tmp/vibedb-distributed-read-performance/scripts/bench/run-distributed-read-comparison.py")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_runner():
    spec = importlib.util.spec_from_file_location("held_distributed_read_runner", RUNNER)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load unchanged runner {RUNNER}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_wrapper(argv):
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--start-marker", required=True, type=Path)
    parser.add_argument("--ready-marker", required=True, type=Path)
    return parser.parse_known_args(argv)


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv=None):
    wrapper_args, forwarded = parse_wrapper(sys.argv[1:] if argv is None else argv)
    start_marker = wrapper_args.start_marker.resolve()
    ready_marker = wrapper_args.ready_marker.resolve()
    if start_marker == ready_marker:
        raise RuntimeError("--start-marker and --ready-marker must differ")
    for marker in (start_marker, ready_marker):
        if marker.exists():
            raise RuntimeError(f"marker must be fresh and absent: {marker}")
        if not marker.parent.is_dir():
            raise RuntimeError(f"marker parent does not exist: {marker.parent}")

    runner = load_runner()
    original = runner.parser().parse_args(forwarded)
    output = original.output.resolve()
    control = output.parent / (output.name + ".hold-control")
    if control.exists():
        raise RuntimeError(f"hold control directory already exists: {control}")
    control.mkdir(mode=0o700)
    wrapper_copy = control / Path(__file__).name
    shutil.copy2(__file__, wrapper_copy)
    record = {
        "schema": "vibedb.distributed-read-hold/1",
        "wrapper": {"path": str(wrapper_copy), "sha256": digest(wrapper_copy)},
        "runner": {"path": str(RUNNER), "sha256": digest(RUNNER)},
        "output": str(output),
        "start_marker": str(start_marker),
        "ready_marker": str(ready_marker),
        "argv": list(sys.argv if argv is None else [str(Path(__file__))] + list(argv)),
        "forwarded_argv": forwarded,
        "status": "preparing",
    }
    record_path = control / "launcher.json"
    write_json(record_path, record)

    gate = {"first": True}
    original_load_fixture = runner.load_fixture

    def gated_fixture(*args, **kwargs):
        fixture = original_load_fixture(*args, **kwargs)
        original_run_engine = fixture.run_engine

        def gated_run_engine(*engine_args, **engine_kwargs):
            if gate["first"]:
                gate["first"] = False
                record["status"] = "ready"
                record["ready_utc"] = utc_now()
                write_json(record_path, record)
                ready_marker.write_text(
                    json.dumps({"ready_utc": record["ready_utc"], "control": str(control)}) + "\n",
                    encoding="utf-8",
                )
                while not start_marker.exists():
                    time.sleep(0.25)
                record["status"] = "started"
                record["start_observed_utc"] = utc_now()
                write_json(record_path, record)
            return original_run_engine(*engine_args, **engine_kwargs)

        fixture.run_engine = gated_run_engine
        return fixture

    runner.load_fixture = gated_fixture
    return runner.main(forwarded)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"run_distributed_read_hold: {exc}", file=sys.stderr)
        raise SystemExit(2)
