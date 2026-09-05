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
            members = [{"member_id": member, "node_id": f"{member:032x}"}
                       for member in range(1, 4)]
            groups = [{"group_id": f"{group:032x}", "members": members}
                      for group in range(7)]
            path.write_text(json.dumps({"schema": "vibedb.rf3-diagnostic/1",
                                        "sequence": 1, "elapsed_ns": 123,
                                        "groups": groups, "sampling_errors": 0}) + "\n")
            summary = MODULE.per_group_diagnostic_summary(path)
            self.assertEqual(summary["records"], 1)
            self.assertEqual(summary["max_cycle_elapsed_ns"], 123)
            self.assertTrue(summary["complete_shape"])

            path.write_text(json.dumps({"schema": "vibedb.rf3-diagnostic/1",
                                        "sequence": 2, "groups": groups[:-1]}) + "\n")
            summary = MODULE.per_group_diagnostic_summary(path)
            self.assertEqual(summary["records"], 0)
            self.assertFalse(summary["complete_shape"])
            self.assertTrue(summary["parse_errors"])


if __name__ == "__main__":
    unittest.main()
