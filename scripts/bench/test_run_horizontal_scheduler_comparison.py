import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("run-horizontal-scheduler-comparison.py")
SPEC = importlib.util.spec_from_file_location(
    "run_horizontal_scheduler_comparison", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class HorizontalSchedulerRunnerTest(unittest.TestCase):
    def test_after_args_are_injected_only_into_after_fixture_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "evidence"
            calls = []
            parser_args = []

            class FakeParser:
                def parse_args(self, values):
                    parser_args.append(values)
                    return SimpleNamespace(candidate_arg=[], cpus="12", memory="24g")

            class FakeRunnerError(RuntimeError):
                pass

            cell = {
                "id": "multigroup-3n-uniform",
                "kind": "multigroup",
                "physical_nodes": 3,
                "tables": ["rf3_sql_group"],
                "workloads": "mixed_uniform",
                "group_distribution": "uniform",
                "clients": "8",
                "groups": 1,
                "endpoint_mode": "single",
            }

            fixture = SimpleNamespace()
            fixture.RunnerError = FakeRunnerError
            fixture.CRDB = "crdb-image"
            fixture.RUNTIME = "runtime-image"
            fixture.COMMAND_LOG = None
            fixture.parser = lambda: FakeParser()
            fixture.parse_positive_csv = lambda raw, name: [int(value) for value in raw.split(",")]
            fixture.cell_matrix = lambda args: [cell]
            fixture.resource_limits = lambda cpus, memory: {
                "cpus": cpus, "memory": memory,
            }
            fixture.require_new_directory = lambda path: path.mkdir() or path

            def write_json(path, value):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(value, indent=2) + "\n")

            fixture.write_json = write_json

            def text_output(argv, **kwargs):
                if argv[:2] == ["git", "rev-parse"]:
                    return "before-sha" if "baseline-ref" in argv[2] else "after-sha"
                if argv[:3] == ["go", "version", "-m"]:
                    return "\tbuild\tGOEXPERIMENT=simd\n\tbuild\tGOOS=linux\n\tbuild\tGOARCH=amd64\n"
                return "file output"

            fixture.text_output = text_output

            def run(argv, **kwargs):
                if argv[:3] == ["git", "diff", "--binary"]:
                    kwargs["stdout"].write(b"binary patch")
                return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

            fixture.run = run

            def git_info(path):
                return {
                    "revision": "client-sha",
                    "status": "",
                    "dirty": False,
                    "patch_sha256": "patch-sha",
                    "patch": b"",
                }

            fixture.git_info = git_info

            def write_git_evidence(destination, name, info):
                (destination / f"source-{name}.patch").write_bytes(info.pop("patch"))
                (destination / f"source-{name}.json").write_text(json.dumps(info) + "\n")

            fixture.write_git_evidence = write_git_evidence
            fixture.docker_architecture = lambda: "amd64"
            fixture.docker_json = lambda argv: {}

            def prepare_worktrees(repo, before, after, work):
                parent, candidate = work / "parent", work / "candidate"
                parent.mkdir()
                candidate.mkdir()
                return parent, candidate

            fixture.prepare_worktrees = prepare_worktrees
            fixture.cleanup_worktrees = lambda *args, **kwargs: None

            def build_all(args, destination, before, after, client, arch):
                binaries = destination / "bin"
                binaries.mkdir()
                for name in (
                        "parent-vibedb", "parent-vibedb-shard", "parent-vibedb-gateway",
                        "candidate-vibedb", "candidate-vibedb-shard", "candidate-vibedb-gateway",
                        "rf3-sqlbench", "cockroach"):
                    (binaries / name).write_text(name)
                return binaries, "go version go1.27.0 linux/amd64"

            fixture.build_all = build_all
            fixture.ensure_image = lambda image: {"Id": image + "-id"}
            fixture.extract_crdb_binary = lambda binaries: None
            fixture.binary_hashes = lambda path: {
                item.name: "hash" for item in sorted(path.iterdir()) if item.is_file()
            }

            def schema_files(destination, tables):
                destination.mkdir(parents=True, exist_ok=True)
                schema = destination / "schema"
                schema.mkdir()
                for table in tables:
                    (schema / f"{table}.sql").write_text("-- schema\n")
                return schema

            fixture.schema_files = schema_files

            def run_engine(args, cell, engine, order, binaries, destination, schema, arch):
                calls.append((order, engine, list(args.candidate_arg), args))
                # A fixture mutation must not leak into a later arm or order.
                args.candidate_arg.append("--fixture-mutated")
                return {"status": "completed", "client_exit_code": 0, "errors": []}

            fixture.run_engine = run_engine

            invocation = [
                str(output),
                "--baseline-ref", "baseline-ref",
                "--candidate-ref", "candidate-ref",
                "--after-arg=--fixture-token-a",
                "--after-arg=--fixture-token-b",
            ]
            with patch.object(MODULE, "load_fixture", return_value=fixture), \
                    patch.object(sys, "argv", [str(MODULE_PATH), *invocation]):
                self.assertEqual(MODULE.main(), 0)

            manifest = json.loads((output / "manifest.json").read_text())
            expected_after = ["--fixture-token-a", "--fixture-token-b"]
            self.assertEqual(manifest["arm_args"], {
                "before": [], "after": expected_after, "crdb": [],
            })
            self.assertEqual(
                [(order, engine, candidate_arg) for order, engine, candidate_arg, _ in calls],
                [
                    ("before-first", "candidate", []),
                    ("before-first", "candidate", expected_after),
                    ("before-first", "crdb", []),
                    ("after-first", "crdb", []),
                    ("after-first", "candidate", expected_after),
                    ("after-first", "candidate", []),
                ],
            )
            self.assertEqual([run["candidate_arg"] for run in manifest["runs"]], [
                [], expected_after, [], [], expected_after, [],
            ])
            self.assertEqual(parser_args[0][parser_args[0].index("--candidate-ref") + 1], "candidate-ref")
            self.assertEqual(calls[0][3].candidate_arg, ["--fixture-mutated"])
            self.assertEqual(calls[1][3].candidate_arg, expected_after + ["--fixture-mutated"])
            self.assertEqual(calls[4][3].candidate_arg, expected_after + ["--fixture-mutated"])
            self.assertEqual(calls[5][3].candidate_arg, ["--fixture-mutated"])

    def test_default_arm_args_are_empty(self):
        self.assertEqual(MODULE.candidate_args_for_arm("before", []), [])
        self.assertEqual(MODULE.candidate_args_for_arm("after", []), [])
        self.assertEqual(MODULE.candidate_args_for_arm("crdb", []), [])


if __name__ == "__main__":
    unittest.main()
