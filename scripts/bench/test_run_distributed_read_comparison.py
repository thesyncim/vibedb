from pathlib import Path
import importlib.util
import json
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("run-distributed-read-comparison.py")
SPEC = importlib.util.spec_from_file_location("run_distributed_read_comparison", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
FIXTURE = MODULE.load_fixture()


class DistributedReadRunnerTest(unittest.TestCase):
    def selected(self, *extra):
        return MODULE.parser().parse_args([
            "/private/tmp/distributed-read-evidence",
            "--baseline-ref", "base-ref",
            "--candidate-ref", "candidate-ref",
            *extra,
        ])

    def test_parser_defaults_match_read_matrix(self):
        selected = self.selected()
        self.assertEqual(selected.workloads, MODULE.DEFAULT_WORKLOADS)
        self.assertEqual(selected.clients, "1,8")
        self.assertEqual(selected.groups, 4)
        self.assertEqual(selected.physical_nodes, 3)
        self.assertEqual(selected.rows, 8192)
        self.assertEqual(selected.scans, 2000)
        self.assertEqual(selected.operations, 20000)
        self.assertEqual(selected.warmup, 1000)
        self.assertEqual(selected.repetitions, 3)
        self.assertEqual(selected.after_arg, [])

    def test_options_allow_group_focus_and_write_control(self):
        selected = self.selected(
            "--workloads", "group_16",
            "--groups", "16",
            "--clients", "8",
            "--rows", "8192",
            "--scans", "1000",
        )
        got = MODULE.validate_options(selected, FIXTURE)
        self.assertEqual(got["workloads"], ["group_16"])
        self.assertEqual(got["clients"], [8])
        self.assertEqual(selected.groups, 16)

        update = self.selected("--workloads", "point_hit,update_existing")
        self.assertEqual(MODULE.validate_options(update, FIXTURE)["workloads"],
                         ["point_hit", "update_existing"])

    def test_options_reject_unbounded_workloads_clients_and_counts(self):
        for extra, message in (
                (("--workloads", "update_uniform"), "workloads"),
                (("--clients", "4"), "clients"),
                (("--rows", "63"), "rows"),
                (("--scans", "0"), "scans"),
                (("--operations", "7", "--clients", "1,8"), "operations"),
        ):
            with self.subTest(message=message):
                selected = self.selected(*extra)
                with self.assertRaisesRegex(FIXTURE.RunnerError, message):
                    MODULE.validate_options(selected, FIXTURE)

    def test_options_allow_complete_read_write_matrix_and_mixed_control(self):
        selected = self.selected(
            "--workloads",
            "point_hit,point_miss,range_32,range_64,range_256,group_16,update_existing,mixed_read_update",
            "--groups", "16", "--clients", "1,8")
        got = MODULE.validate_options(selected, FIXTURE)
        self.assertEqual(got["workloads"], [
            "point_hit", "point_miss", "range_32", "range_64", "range_256",
            "group_16", "update_existing", "mixed_read_update",
        ])
        mixed = self.selected("--workloads", "mixed")
        self.assertEqual(MODULE.validate_options(mixed, FIXTURE)["workloads"],
                         ["mixed_read_update"])

    def test_cells_are_same_fused_candidate_shape_for_every_arm(self):
        selected = self.selected("--workloads", "point_hit,group_16")
        options = MODULE.validate_options(selected, FIXTURE)
        cells = MODULE.read_cells(selected, FIXTURE, options["workloads"], options["clients"])
        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell["kind"], "distributed-read")
        self.assertEqual(cell["physical_nodes"], 3)
        self.assertEqual(cell["groups"], 4)
        self.assertEqual(cell["tables"], [
            "rf3_sql_group", "rf3_sql_group_01", "rf3_sql_group_02", "rf3_sql_group_03",
        ])
        self.assertEqual(cell["clients"], "1,8")
        self.assertEqual(cell["workloads"], "point_hit,group_16")
        self.assertEqual(MODULE.ORDER_ENGINES["before-first"], ("before", "after", "crdb"))
        self.assertEqual(MODULE.ORDER_ENGINES["after-first"], ("crdb", "after", "before"))

    def test_arm_directories_copy_both_revisions_as_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = root / "bin"
            binaries.mkdir()
            for name, content in (
                    ("parent-vibedb", "before-server"),
                    ("parent-vibedb-shard", "before-shard"),
                    ("parent-vibedb-gateway", "before-gateway"),
                    ("candidate-vibedb", "after-server"),
                    ("candidate-vibedb-shard", "after-shard"),
                    ("candidate-vibedb-gateway", "after-gateway"),
                    ("rf3-sqlbench", "shared-client"),
                    ("cockroach", "oracle")):
                (binaries / name).write_text(content)
            arms = MODULE.arm_binary_directories(root / "evidence", binaries, FIXTURE)
            self.assertEqual(
                {path.name for path in arms["before"].iterdir()},
                {"candidate-vibedb", "candidate-vibedb-shard", "candidate-vibedb-gateway", "rf3-sqlbench"},
            )
            self.assertEqual(
                {path.name for path in arms["after"].iterdir()},
                {"candidate-vibedb", "candidate-vibedb-shard", "candidate-vibedb-gateway", "rf3-sqlbench"},
            )
            self.assertEqual((arms["before"] / "candidate-vibedb-shard").read_text(), "before-shard")
            self.assertEqual((arms["after"] / "candidate-vibedb-shard").read_text(), "after-shard")
            self.assertEqual((arms["before"] / "rf3-sqlbench").read_text(), "shared-client")
            self.assertIs(arms["crdb"], binaries)

    def test_resolve_refs_requires_distinct_ancestor_commits(self):
        selected = self.selected()
        calls = []

        def text_output(argv, **kwargs):
            calls.append(argv)
            return "base-sha" if argv[2].startswith("base") else "candidate-sha"

        with patch.object(FIXTURE, "text_output", side_effect=text_output), \
                patch.object(FIXTURE, "run") as run:
            refs = MODULE.resolve_refs(
                selected, FIXTURE, Path("/repo"), {"revision": "client-sha"})
        self.assertEqual(refs["before"], "base-sha")
        self.assertEqual(refs["after"], "candidate-sha")
        self.assertTrue(refs["baseline_is_ancestor"])
        run.assert_called_once_with(
            ["git", "merge-base", "--is-ancestor", "base-sha", "candidate-sha"],
            cwd=Path("/repo"))
        self.assertEqual(len(calls), 2)

        with patch.object(FIXTURE, "text_output", return_value="same-sha"):
            with self.assertRaisesRegex(FIXTURE.RunnerError, "distinct"):
                MODULE.resolve_refs(
                    selected, FIXTURE, Path("/repo"), {"revision": "client-sha"})

        with patch.object(FIXTURE, "text_output", return_value="same-sha"), \
                patch.object(FIXTURE, "run") as run:
            refs = MODULE.resolve_refs(
                selected, FIXTURE, Path("/repo"), {"revision": "client-sha"},
                ["--read-authority"])
        self.assertEqual(refs["before"], "same-sha")
        self.assertEqual(refs["after"], "same-sha")
        self.assertTrue(refs["same_revision"])
        self.assertEqual(refs["same_revision_reason"], "after-arg feature toggle")
        run.assert_called_once_with(
            ["git", "merge-base", "--is-ancestor", "same-sha", "same-sha"],
            cwd=Path("/repo"))

    def test_duplicate_detached_worktrees_are_used_for_same_revision_toggle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = []

            class FakeFixture:
                def run(self, argv, **kwargs):
                    calls.append((list(argv), kwargs))
                    if argv[:4] == ["git", "worktree", "add", "--detach"]:
                        Path(argv[4]).mkdir()

            before, after = MODULE.prepare_worktrees(
                Path("/repo"), "same-sha", "same-sha", root, FakeFixture())

        self.assertEqual(before, root / "parent")
        self.assertEqual(after, root / "candidate")
        self.assertEqual([call[0] for call in calls], [
            ["git", "worktree", "add", "--detach", root / "parent", "same-sha"],
            ["git", "worktree", "add", "--detach", root / "candidate", "same-sha"],
        ])

    def test_same_revision_requires_equal_server_binary_hashes(self):
        class FakeFixture:
            class RunnerError(RuntimeError):
                pass

        hashes = {
            "parent-vibedb": "a", "candidate-vibedb": "a",
            "parent-vibedb-shard": "b", "candidate-vibedb-shard": "b",
            "parent-vibedb-gateway": "c", "candidate-vibedb-gateway": "c",
        }
        identity = MODULE.verify_same_revision_binaries(hashes, FakeFixture)
        self.assertTrue(identity["verified"])
        self.assertEqual(identity["pairs"]["vibedb-shard"]["sha256"], "b")

        hashes["candidate-vibedb-gateway"] = "different"
        with self.assertRaisesRegex(FakeFixture.RunnerError, "gateway binaries"):
            MODULE.verify_same_revision_binaries(hashes, FakeFixture)

    def test_make_fixture_args_keeps_shared_client_and_oracle_contract(self):
        selected = self.selected("--groups", "16", "--physical-nodes", "6",
                                 "--workloads", "group_16", "--scans", "1000")
        options = MODULE.validate_options(selected, FIXTURE)
        args = MODULE.make_fixture_args(selected, FIXTURE, options["workloads"])
        self.assertEqual(args.candidate_ref, "candidate-ref")
        self.assertEqual(args.groups, 16)
        self.assertEqual(args.physical_nodes, "6")
        self.assertEqual(args.multigroup_workloads, "group_16")
        self.assertEqual(args.clients, "1,8")
        self.assertTrue(args.include_crdb)
        self.assertEqual(args.order, "both")

    def test_late_failure_preserves_rich_manifest(self):
        # The real main() is exercised through mocked fixture boundaries.  A
        # failed final arm must leave prior runs, hashes, controls, and the
        # active arm in manifest.json for diagnosis.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "evidence"
            selected = MODULE.parser().parse_args([
                str(output), "--baseline-ref", "base-ref", "--candidate-ref", "candidate-ref",
                "--after-arg=--read-authority",
                "--rows", "64", "--operations", "8", "--scans", "8", "--warmup", "0",
                "--repetitions", "1",
            ])
            events = []

            def fake_git_info(path):
                return {"revision": "client-sha", "status": "", "dirty": False,
                        "patch_sha256": "patch-sha", "patch": b""}

            def fake_text_output(argv, **kwargs):
                if len(argv) >= 2 and argv[0:2] == ["git", "rev-parse"]:
                    return "base-sha" if "base-ref" in argv[2] else "candidate-sha"
                if argv[:3] == ["go", "version", "-m"]:
                    return ("\tbuild\tGOEXPERIMENT=simd\n"
                            "\tbuild\tGOOS=linux\n"
                            "\tbuild\tGOARCH=amd64\n")
                return "file format"

            def fake_run(argv, **kwargs):
                if argv[:3] == ["git", "diff", "--binary"] and kwargs.get("stdout"):
                    kwargs["stdout"].write(b"binary patch")
                return SimpleNamespace(returncode=0, stdout=b"")

            def fake_prepare(repo, before, after, work):
                parent, candidate = work / "parent", work / "candidate"
                parent.mkdir()
                candidate.mkdir()
                return parent, candidate

            def fake_build(args, destination, before, after, client, arch):
                events.append("build")
                binaries = destination / "bin"
                binaries.mkdir()
                for name in (
                        "parent-vibedb", "parent-vibedb-shard", "parent-vibedb-gateway",
                        "candidate-vibedb", "candidate-vibedb-shard", "candidate-vibedb-gateway",
                        "rf3-sqlbench", "cockroach"):
                    (binaries / name).write_text(name)
                return binaries, "go version go1.27.0 linux/amd64"

            def fake_run_engine(args, cell, engine, order, binaries, run_dir, schema, arch):
                events.append((order, engine, Path(binaries).name,
                               list(args.candidate_arg)))
                # The fixture may mutate its private Namespace; no later arm or
                # order may inherit that list.
                args.candidate_arg.append("--fixture-mutated")
                if len(events) - 1 == 5:
                    return {"status": "failed", "client_exit_code": 1, "errors": ["late failure"]}
                return {"status": "completed", "client_exit_code": 0, "errors": []}

            with patch.object(MODULE, "load_fixture", return_value=FIXTURE), \
                    patch.object(FIXTURE, "git_info", side_effect=fake_git_info), \
                    patch.object(FIXTURE, "text_output", side_effect=fake_text_output), \
                    patch.object(FIXTURE, "run", side_effect=fake_run), \
                    patch.object(FIXTURE, "docker_architecture", return_value="amd64"), \
                    patch.object(FIXTURE, "docker_json", return_value={}), \
                    patch.object(FIXTURE, "prepare_worktrees", side_effect=fake_prepare), \
                    patch.object(FIXTURE, "cleanup_worktrees"), \
                    patch.object(FIXTURE, "build_all", side_effect=fake_build), \
                    patch.object(FIXTURE, "ensure_image", return_value={"Id": "image-id"}), \
                    patch.object(FIXTURE, "extract_crdb_binary"), \
                    patch.object(FIXTURE, "run_engine", side_effect=fake_run_engine):
                code = MODULE.main([
                    str(output), "--baseline-ref", "base-ref", "--candidate-ref", "candidate-ref",
                    "--after-arg=--read-authority",
                    "--rows", "64", "--operations", "8", "--scans", "8", "--warmup", "0",
                    "--repetitions", "1",
                ])

            self.assertEqual(code, 1)
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "incomplete-or-failed")
            self.assertEqual(len(manifest["runs"]), 5)
            self.assertIn("build_binary_sha256", manifest)
            self.assertIn("control_sha256", manifest)
            self.assertIn("active_run", manifest)
            self.assertFalse(manifest["profiling"])
            self.assertEqual(manifest["arm_args"], {
                "before": [], "after": ["--read-authority"], "crdb": [],
            })
            self.assertEqual([run["candidate_arg"] for run in manifest["runs"]], [
                [], ["--read-authority"], [], [], ["--read-authority"],
            ])
            self.assertEqual(events[0], "build")
            self.assertEqual([event[1] for event in events[1:]],
                             ["candidate", "candidate", "crdb", "crdb", "candidate"])
            self.assertEqual([event[2] for event in events[1:]],
                             ["before-bin", "after-bin", "bin", "bin", "after-bin"])
            self.assertEqual([event[3] for event in events[1:]], [
                [], ["--read-authority"], [], [], ["--read-authority"],
            ])


if __name__ == "__main__":
    unittest.main()
