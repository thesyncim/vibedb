import importlib.util
import json
import subprocess
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("run-read-authority-fault-qualification.py")
SPEC = importlib.util.spec_from_file_location(
    "run_read_authority_fault_qualification", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class FaultQualificationProvenanceTest(unittest.TestCase):
    def test_source_snapshot_preserves_porcelain_status_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory) / "repo"
            destination = Path(directory) / "evidence"
            repo.mkdir()
            destination.mkdir()
            run = lambda *args: subprocess.run(
                ["git", "-C", str(repo), *args], check=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            run("init", "-q")
            run("config", "user.email", "qualification-test@example.invalid")
            run("config", "user.name", "qualification-test")
            (repo / "tracked.txt").write_text("before\n")
            run("add", "tracked.txt")
            run("commit", "-qm", "initial")
            (repo / "tracked.txt").write_text("after\n")
            (repo / "untracked.txt").write_text("new\n")

            fixture = MODULE.load_fixture()
            fixture.COMMAND_LOG = None
            snapshot = MODULE.source_snapshot(fixture, repo, destination, "before")
            records = {record["path"]: record for record in snapshot["files"]}

            self.assertEqual(records["tracked.txt"]["status"], " M")
            self.assertEqual(records["untracked.txt"]["status"], "??")
            self.assertEqual(snapshot["file_sha256"], {
                "tracked.txt": records["tracked.txt"]["sha256"],
                "untracked.txt": records["untracked.txt"]["sha256"],
            })
            self.assertEqual(
                (destination / "source-before-files" / "tracked.txt").read_text(),
                "after\n")
            self.assertEqual(
                (destination / "source-before-files" / "untracked.txt").read_text(),
                "new\n")

    def test_per_group_diagnostic_summary_requires_complete_rf3_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshots.jsonl"
            members = [{"member_id": member, "node_id": f"{member:032x}",
                        "status": {"term": 1}, "metrics": {"applied_entries": 1}}
                       for member in range(1, 4)]
            groups = [{"group_id": f"{group:032x}", "members": members}
                      for group in range(7)]
            path.write_text(json.dumps({"schema": "vibedb.rf3-diagnostic/1",
                                        "sequence": 1, "elapsed_ns": 123,
                                        "groups": groups, "expected_cuts": 21,
                                        "valid_cuts": 21, "preflight_ready": True,
                                        "sampling_errors": 0}) + "\n")
            summary = MODULE.per_group_diagnostic_summary(path)
            self.assertEqual(summary["records"], 1)
            self.assertEqual(summary["max_cycle_elapsed_ns"], 123)
            self.assertEqual(summary["valid_cuts"], 21)
            self.assertEqual(summary["preflight_ready_records"], 1)
            self.assertTrue(summary["complete_shape"])

            path.write_text(json.dumps({"schema": "vibedb.rf3-diagnostic/1",
                                        "sequence": 2, "groups": groups[:-1]}) + "\n")
            summary = MODULE.per_group_diagnostic_summary(path)
            self.assertEqual(summary["records"], 0)
            self.assertFalse(summary["complete_shape"])
            self.assertTrue(summary["parse_errors"])

    def test_diagnostic_output_path_falls_back_to_partial_raw_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "candidate"
            path = run_dir / "raw" / "per-group-snapshots.jsonl"
            path.parent.mkdir(parents=True)
            path.write_text("partial\n")

            self.assertEqual(MODULE.diagnostic_output_path(run_dir), path)

    def test_group_timeline_keeps_member_terms_and_node_authority_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            path = directory / "snapshots.jsonl"
            output = directory / "timeline.json"
            members = []
            node_metrics = []
            for member in range(1, 4):
                node_id = f"{member:032x}"
                members.append({
                    "member_id": member,
                    "node_id": node_id,
                    "status": {
                        "term": member,
                        "leader_id": 2,
                        "commit": 10 + member,
                        "applied": 9 + member,
                        "checkpoint_applied": 8 + member,
                        "raft_state": 0,
                        "raft_state_name": "StateFollower",
                    },
                    "progress": {"match": member},
                    "metrics": {
                        "applied_entries": member,
                        "ready_persisted": member + 1,
                        "commit_advancements": member + 2,
                        "committed_entries": member + 3,
                    },
                })
                node_metrics.append({
                    "node_id": node_id,
                    "scope": "node_process",
                    "source": "rf3-diagnostics-file" if member == 1 else "servicemetrics",
                    "utc": "2026-09-05T18:00:00Z",
                    "serial": member,
                    "pid": 40 + member,
                    "authority_available": member == 1,
                    "authority_error": None if member == 1 else "diagnostic file unavailable",
                    "metrics": ({
                        "authority_read_hits": 100 + member,
                        "authority_round_attempts": 10 + member,
                    } if member == 1 else {
                        "applied_entries": member,
                    }),
                    "error": None if member == 1 else "diagnostic file unavailable",
                })
            groups = [{"group_id": f"{group:032x}", "distribution": "system",
                       "shard": "all", "members": members} for group in range(6)]
            groups.append({"group_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                           "distribution": "table-rf3_sql_group-b1c8362a213f",
                           "shard": "all", "members": members})
            path.write_text(json.dumps({
                "schema": "vibedb.rf3-diagnostic/1",
                "sequence": 4,
                "utc": "2026-09-05T18:00:00Z",
                "elapsed_ns": 12,
                "groups": groups,
                "node_metrics": node_metrics,
                "expected_cuts": 21,
                "valid_cuts": 21,
                "preflight_ready": True,
                "sampling_errors": 0,
            }) + "\n")

            summary = MODULE.write_group_timeline(path, output, "rf3_sql_group")
            self.assertTrue(summary["complete"])
            payload = json.loads(output.read_text())
            self.assertEqual(payload["terms_scope"].count("cross-group"), 1)
            self.assertEqual([member["term"] for member in payload["records"][0]["members"]], [1, 2, 3])
            self.assertNotIn("term", payload["records"][0])
            self.assertEqual(
                payload["records"][0]["members"][0]["authority_metrics"]["scope"],
                "node_process")
            self.assertEqual(
                payload["records"][0]["members"][0]["authority_metrics"]["metrics"]["authority_read_hits"],
                101)
            node_one = payload["records"][0]["node_process_metrics"]["00000000000000000000000000000001"]
            self.assertEqual(node_one["source"], "rf3-diagnostics-file")
            self.assertEqual(node_one["utc"], "2026-09-05T18:00:00Z")
            self.assertEqual(node_one["serial"], 1)
            self.assertEqual(node_one["pid"], 41)
            self.assertTrue(node_one["authority_available"])
            node_two = payload["records"][0]["node_process_metrics"]["00000000000000000000000000000002"]
            self.assertEqual(node_two["source"], "servicemetrics")
            self.assertFalse(node_two["authority_available"])
            self.assertEqual(node_two["error"], "diagnostic file unavailable")
            self.assertNotIn("authority_read_hits", node_two["metrics"])

    def test_primary_result_failure_keeps_client_error_when_restart_is_absent(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "candidate"
            run_dir.mkdir()
            error = "gateway: no authenticated replica reported itself as leader (ordinal=4)"
            (run_dir / "client.log").write_text("warmup: ERROR: " + error + "\n")
            failure = MODULE.primary_result_failure({
                "status": "failed",
                "client_exit_code": 1,
                "errors": ["client report is incomplete or failed", "client exited nonzero"],
            }, run_dir)

            self.assertEqual(failure["client_error_tail"], "warmup: ERROR: " + error)
            self.assertEqual(failure["client_log"], "candidate/client.log")

            self.assertIsNone(MODULE.primary_result_failure({
                "status": "failed",
                "client_exit_code": 0,
                "validation": {"complete": True},
                "errors": ["post-CONT diagnostic latch was not retained"],
            }, run_dir))
            self.assertIsNotNone(MODULE.primary_result_failure({
                "status": "failed",
                "client_exit_code": 0,
                "validation": {"complete": False},
                "errors": ["client report is incomplete or failed"],
            }, run_dir))

            self.assertFalse(MODULE.fault_qualification_complete(
                False, {"status": "verified-signals"}, None))
            self.assertFalse(MODULE.fault_qualification_complete(
                False, {"status": "verified-signals"}, {"status": "verified"}, False))
            self.assertTrue(MODULE.fault_qualification_complete(
                True, None, None))

    def test_post_cont_latch_requires_all_post_cont_authority_snapshots(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "post-cont-cut.json"
            counters = {
                name: 0 for name in MODULE.REQUIRED_AUTHORITY_COUNTERS
            }
            nodes = []
            for member in range(1, 4):
                nodes.append({
                    "node_id": f"{member:032x}",
                    "source": "rf3-diagnostics-file",
                    "utc": "2026-09-05T18:00:00Z",
                    "serial": member,
                    "pid": 40 + member,
                    "authority_available": True,
                    "metrics": dict(counters),
                })
            cycle = {
                "schema": "vibedb.rf3-diagnostic/1",
                "sequence": 7,
                "utc": "2026-09-05T18:00:02Z",
                "expected_cuts": 21,
                "valid_cuts": 21,
                "preflight_ready": True,
                "node_metrics": nodes,
                "latch": {"sequence": 7, "complete": True},
            }
            path.write_text(json.dumps({
                "schema": "vibedb.rf3-diagnostic-latch/1",
                "event": "post-cont",
                "requested_utc": "2026-09-05T18:00:00Z",
                "armed_utc": "2026-09-05T18:00:01Z",
                "captured_utc": "2026-09-05T18:00:03Z",
                "sequence": 7,
                "cycle": cycle,
            }))
            summary = MODULE.validate_post_cont_latch(path)
            self.assertTrue(summary["complete"])
            self.assertEqual(summary["authority_snapshot_count"], 3)

            nodes[1]["authority_available"] = False
            path.write_text(json.dumps({
                "schema": "vibedb.rf3-diagnostic-latch/1",
                "event": "post-cont",
                "requested_utc": "2026-09-05T18:00:00Z",
                "armed_utc": "2026-09-05T18:00:01Z",
                "captured_utc": "2026-09-05T18:00:03Z",
                "sequence": 7,
                "cycle": cycle,
            }))
            with self.assertRaisesRegex(ValueError, "lacks authority availability"):
                MODULE.validate_post_cont_latch(path)


if __name__ == "__main__":
    unittest.main()
