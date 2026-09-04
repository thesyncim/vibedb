import importlib.util
import hashlib
import json
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("summarize-crdb-sql.py")
SPEC = importlib.util.spec_from_file_location("summarize_crdb_sql", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def write_report(directory, report):
    path = Path(directory) / (report["config"]["Engine"] + ".json")
    path.write_text(json.dumps(report))
    return path


def report(engine="vibedb", *, new=False):
    config = {
        "Engine": engine,
        "Rows": 64,
        "PayloadBytes": 256,
        "Operations": 2,
        "ScanOperations": 1,
        "Warmup": 0,
        "Repetitions": 1,
        "Clients": "1",
        "Protocol": "test",
    }
    result = {
        "engine": engine,
        "workload": "point_hit",
        "clients": 1,
        "repetition": 1,
        "operations": 2,
        "errors": 0,
        "elapsed_ns": 100,
        "successful_ops_per_second": 20000000.0,
        "p50_ns": 10,
        "p95_ns": 20,
        "p99_ns": 20,
        "verified": True,
        "samples": [
            {"client": 0, "ordinal": 0, "ns": 10},
            {"client": 0, "ordinal": 1, "ns": 20},
        ],
    }
    result["measurement_started_utc"] = "2026-09-04T00:00:00.123456789Z"
    result["samples"][0]["start_offset_ns"] = 1
    result["samples"][1]["start_offset_ns"] = 20
    if new:
        config.update({
            "SeedBatch": 64,
            "VerifyEveryTrial": True,
            "Tables": ["rf3_sql_bench", "orders_01"],
            "Workloads": ["point_hit"],
            "GroupDistribution": "uniform",
            "SkewPercent": 80,
            "PhysicalNodes": 3,
            "EndpointCount": 2,
            "EndpointRouting": "round-robin-per-client",
            "KeySelection": "splitmix64-independent-with-replacement-v1",
        })
        result["samples"][0].update({"group": 0, "table": "rf3_sql_bench", "endpoint": 0})
        result["samples"][1].update({"group": 1, "table": "orders_01", "endpoint": 0})
    else:
        result.pop("measurement_started_utc")
        for sample in result["samples"]:
            sample.pop("start_offset_ns")
    return {"config": config, "results": [result], "verification_error": "incomplete fixture"}


def diagnostic_report(directory):
    value = report(new=True)
    value.update(schema_version=2, status="complete", started_utc="2026-09-04T00:00:00Z")
    value.pop("verification_error")
    value["config"]["DiagnosticMode"] = "signal-acknowledged-snapshots"
    trial = value["results"][0]
    for sample in trial["samples"]:
        sample["operation"] = "point_hit"
    bracket = {"before_completed_offset_ns": -5, "after_started_offset_ns": trial["elapsed_ns"], "before": [], "after": [], "deltas": []}
    trial["diagnostics"] = bracket
    root = Path(directory) / "diagnostics"
    root.mkdir()
    for index in range(3):
        node = f"{index+1:032x}"
        for phase, serial, count in (("before", 1, 1 << 60), ("after", 2, (1 << 60)+3)):
            snapshot = {"node_id": node, "pid": 100+index, "serial": serial, "utc": "2026-09-04T00:00:00Z", "event": "snapshot",
                        "ready_wave_group_histogram": [0, count]}
            snapshot.update({key: count for key in MODULE.DIAGNOSTIC_COUNTERS})
            raw = (json.dumps(snapshot, indent=1) + "\n").encode()
            name = f"node{index}-{phase}.json"
            (root / name).write_bytes(raw)
            bracket[phase].append({"node_id": node, "pid": 100+index, "serial": serial, "snapshot": snapshot,
                                   "sha256": hashlib.sha256(raw).hexdigest(), "file": "diagnostics/"+name})
        bracket["deltas"].append({"node_id": node, "counters": {key: 3 for key in MODULE.DIAGNOSTIC_COUNTERS},
                                  "ready_wave_group_histogram": [0, 3]})
    return value


def uniform_report(workload):
    value = report(new=True)
    value.update(schema_version=2, status="complete", started_utc="2026-09-04T00:00:00Z")
    value.pop("verification_error")
    value["config"]["Workloads"] = [workload]
    trial = value["results"][0]
    trial["workload"] = workload
    for ordinal, sample in enumerate(trial["samples"]):
        sample["operation"] = MODULE.operation_for(workload, ordinal)
    return value


class SummarizeCRDBSQLTest(unittest.TestCase):
    def test_diagnostic_brackets_verify_exact_archives_and_integer_deltas(self):
        with tempfile.TemporaryDirectory() as directory:
            value = diagnostic_report(directory)
            _, checked = MODULE.load(write_report(directory, value), "vibedb")
            self.assertEqual(checked, 2)
            value["results"][0]["diagnostics"]["deltas"][0]["counters"]["ready_waves"] = 4
            with self.assertRaisesRegex(ValueError, "counter delta"):
                MODULE.load(write_report(directory, value), "vibedb")

    def test_diagnostic_archive_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            value = diagnostic_report(directory)
            (Path(directory) / "diagnostics/node0-before.json").write_text("{}")
            with self.assertRaisesRegex(ValueError, "bytes/hash"):
                MODULE.load(write_report(directory, value), "vibedb")

    def test_diagnostics_cannot_overlap_timer_or_omit_nodes(self):
        for field, replacement in (("before_completed_offset_ns", 1), ("after_started_offset_ns", 99), ("after", [])):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                value = diagnostic_report(directory)
                value["results"][0]["diagnostics"][field] = replacement
                with self.assertRaises(ValueError):
                    MODULE.load(write_report(directory, value), "vibedb")

    def test_old_report_without_timing_fields_stays_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            path = write_report(directory, report(new=False))
            loaded, samples = MODULE.load(path, "vibedb")
            self.assertEqual(samples, 2)
            self.assertNotIn("measurement_started_utc", loaded["results"][0])

    def test_new_report_checks_anchor_offsets_and_group_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = write_report(directory, report(new=True))
            _, samples = MODULE.load(path, "vibedb")
            self.assertEqual(samples, 2)

    def test_new_seed_and_verification_controls_are_validated(self):
        for field, invalid in (("SeedBatch", 0), ("SeedBatch", True),
                               ("VerifyEveryTrial", 1), ("VerifyEveryTrial", "true")):
            with self.subTest(field=field, invalid=invalid), tempfile.TemporaryDirectory() as directory:
                value = report(new=True)
                value.update(schema_version=2, status="complete", started_utc="2026-09-04T00:00:00Z")
                value.pop("verification_error")
                value["config"][field] = invalid
                with self.assertRaises(ValueError):
                    MODULE.load(write_report(directory, value), "vibedb")

    def test_uniform_workloads_validate_actual_operation_labels(self):
        for workload in ("update_uniform", "mixed_uniform"):
            with self.subTest(workload=workload), tempfile.TemporaryDirectory() as directory:
                value = uniform_report(workload)
                _, samples = MODULE.load(write_report(directory, value), "vibedb")
                self.assertEqual(samples, 2)

    def test_uniform_workloads_reject_incorrect_operation_mix(self):
        for workload in ("update_uniform", "mixed_uniform"):
            with self.subTest(workload=workload), tempfile.TemporaryDirectory() as directory:
                value = uniform_report(workload)
                trial = value["results"][0]
                for ordinal, sample in enumerate(trial["samples"]):
                    expected = MODULE.operation_for(workload, ordinal)
                    sample["operation"] = "point_hit" if expected == "update_existing" else "update_existing"
                with self.assertRaisesRegex(ValueError, "operation identity"):
                    MODULE.load(write_report(directory, value), "vibedb")

    def test_uniform_workloads_cannot_use_legacy_schema_to_omit_operation_labels(self):
        for workload in ("update_uniform", "mixed_uniform"):
            for schema in (None, 1):
                with self.subTest(workload=workload, schema=schema), tempfile.TemporaryDirectory() as directory:
                    value = uniform_report(workload)
                    if schema is None:
                        value.pop("schema_version")
                    else:
                        value["schema_version"] = schema
                    for sample in value["results"][0]["samples"]:
                        sample.pop("operation")
                    with self.assertRaisesRegex(ValueError, "uniform workloads require report schema 2"):
                        MODULE.load(write_report(directory, value), "vibedb")

    def test_uniform_workloads_reject_wrong_sample_count_assignment_and_repetition(self):
        for workload in ("update_uniform", "mixed_uniform"):
            for change in ("sample_count", "client", "repetition", "missing_operation"):
                with self.subTest(workload=workload, change=change), tempfile.TemporaryDirectory() as directory:
                    value = uniform_report(workload)
                    trial = value["results"][0]
                    if change == "sample_count":
                        trial["samples"].pop()
                    elif change == "client":
                        trial["samples"][1]["client"] = 1
                    elif change == "repetition":
                        trial["repetition"] = 2
                    else:
                        trial["samples"][0].pop("operation")
                    with self.assertRaises(ValueError):
                        MODULE.load(write_report(directory, value), "vibedb")

    def test_uniform_trial_must_exercise_every_configured_client(self):
        for workload in ("update_uniform", "mixed_uniform"):
            with self.subTest(workload=workload), tempfile.TemporaryDirectory() as directory:
                value = uniform_report(workload)
                value["config"]["Clients"] = "8"
                trial = value["results"][0]
                trial["clients"] = 8
                trial["samples"][1].update(client=1, endpoint=1)
                with self.assertRaisesRegex(ValueError, "operation count/engine mismatch"):
                    MODULE.load(write_report(directory, value), "vibedb")

    def assert_invalid_new_report(self, mutate):
        with tempfile.TemporaryDirectory() as directory:
            value = report(new=True)
            mutate(value["results"][0])
            path = write_report(directory, value)
            with self.assertRaises(ValueError):
                MODULE.load(path, "vibedb")

    def test_negative_offset_is_rejected(self):
        self.assert_invalid_new_report(lambda trial: trial["samples"][0].update(start_offset_ns=-1))

    def test_offset_after_elapsed_is_rejected(self):
        self.assert_invalid_new_report(lambda trial: trial["samples"][1].update(start_offset_ns=101))

    def test_sample_end_after_elapsed_is_rejected(self):
        self.assert_invalid_new_report(lambda trial: trial["samples"][1].update(start_offset_ns=90))

    def test_closed_loop_overlap_is_rejected(self):
        self.assert_invalid_new_report(lambda trial: trial["samples"][1].update(start_offset_ns=5))

    def test_non_rfc3339_anchor_is_rejected(self):
        self.assert_invalid_new_report(lambda trial: trial.update(measurement_started_utc="2026-09-04 00:00:00Z"))

    def test_new_group_metadata_cannot_be_removed_to_bypass_validation(self):
        def mutate(trial):
            for sample in trial["samples"]:
                sample.pop("group")
                sample.pop("table")
        self.assert_invalid_new_report(mutate)

    def test_schema_two_retains_pretrial_failure(self):
        value = report(new=True)
        value.update(schema_version=2, status="failed", started_utc="2026-09-04T00:00:00Z", results=[])
        with tempfile.TemporaryDirectory() as directory:
            _, checked = MODULE.load(write_report(directory, value), "vibedb")
        self.assertEqual(checked, 0)

    def test_schema_two_requires_operation_and_timing_metadata(self):
        value = report(new=True)
        value.update(schema_version=2, status="incomplete", started_utc="2026-09-04T00:00:00Z")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "operation identity"):
                MODULE.load(write_report(directory, value), "vibedb")
            for sample in value["results"][0]["samples"]:
                sample["operation"] = "point_hit"
            _, checked = MODULE.load(write_report(directory, value), "vibedb")
            self.assertEqual(checked, 2)

    def test_duplicate_clients_are_rejected(self):
        value = report(new=True)
        value["config"]["Clients"] = "1,1"
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "invalid clients"):
                MODULE.load(write_report(directory, value), "vibedb")

    def test_group_identity_is_rejected(self):
        self.assert_invalid_new_report(lambda trial: trial["samples"][1].update(group=0))

    def test_endpoint_identity_is_rejected(self):
        self.assert_invalid_new_report(lambda trial: trial["samples"][1].update(endpoint=1))

    def test_percentile_is_rejected(self):
        self.assert_invalid_new_report(lambda trial: trial.update(p99_ns=10))

    def test_invalid_table_identifier_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            value = report(new=True)
            value["config"]["Tables"] = ["unsafe-table", "orders_01"]
            path = write_report(directory, value)
            with self.assertRaises(ValueError):
                MODULE.load(path, "vibedb")


if __name__ == "__main__":
    unittest.main()
