"""Exercise the CI selector without compiling or running the database suites."""
import collections
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("test-shard.sh").resolve()
PREFIX = "github.com/thesyncim/vibedb"
PACKAGES = [PREFIX + suffix for suffix in (
    "", "/store/durable", "/query", "/store", "/sql/driver", "/pgwire",
    "/cmd/vibedb-gateway", "/cmd/vibedb-shard", "/internal/raftservice",
    "/new-package", "/store/durable/new-package",
)]


class TestShard(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        directory = Path(self.temp.name)
        fake_go = directory / "go"
        fake_go.write_text('''#!/usr/bin/env python3
import json, os, sys
if sys.argv[1:] == ["list", "./..."]:
    print(os.environ["TEST_PACKAGES"])
    sys.exit(int(os.environ.get("LIST_STATUS", "0")))
if sys.argv[1] == "test":
    print(json.dumps(sys.argv[1:]))
    sys.exit(int(os.environ.get("TEST_STATUS", "0")))
if sys.argv[1] == "vet":
    print(json.dumps(sys.argv[1:]))
    sys.exit(int(os.environ.get("VET_STATUS", "0")))
sys.exit(99)
''')
        fake_go.chmod(0o755)
        self.env = dict(os.environ, PATH=str(directory) + os.pathsep + os.environ["PATH"],
                        TEST_PACKAGES="\n".join(PACKAGES))

    def run_shard(self, *args):
        return subprocess.run(["bash", str(SCRIPT), *args], env=self.env,
                              text=True, capture_output=True)

    def test_disjoint_and_complete_including_new_packages(self):
        selected = []
        for shard in ("durable", "sql", "process", "core"):
            result = self.run_shard(shard, "--list")
            self.assertEqual(result.returncode, 0, result.stderr)
            selected.extend(result.stdout.splitlines())
        self.assertEqual(collections.Counter(selected), collections.Counter(PACKAGES))

    def test_pressure_filters_are_complementary(self):
        regular = json.loads(self.run_shard("durable").stdout.splitlines()[-1])
        churn = json.loads(self.run_shard("durable-churn").stdout.splitlines()[-1])
        large = json.loads(self.run_shard("durable-large-cache").stdout.splitlines()[-1])
        self.assertEqual(regular[-1], PREFIX + "/store/durable")
        self.assertEqual(churn[-1], regular[-1])
        self.assertEqual(large[-1], regular[-1])
        skip = regular[regular.index("-skip") + 1]
        runs = [churn[churn.index("-run") + 1], large[large.index("-run") + 1]]
        self.assertEqual(
            runs,
            ["^TestFilePrimaryChurnQualification$",
             "^TestFilePrimaryLargerThanCacheQualification$"],
        )
        for test in ("TestFilePrimaryChurnQualification",
                     "TestFilePrimaryLargerThanCacheQualification",
                     "TestNewDurableBehavior", "ExampleStore", "FuzzRecovery",
                     "TestFilePrimaryChurnQualificationExtra"):
            selected = [re.search(skip, test) is None]
            selected.extend(re.search(run, test) is not None for run in runs)
            self.assertEqual(sum(selected), 1, (test, selected))

    def test_invalid_arguments_fail(self):
        for args in ((), ("typo",), ("core", "typo"), ("core", "--list", "extra")):
            self.assertNotEqual(self.run_shard(*args).returncode, 0)

    def test_partial_failed_discovery_cannot_run_tests(self):
        self.env["LIST_STATUS"] = "7"
        result = self.run_shard("core")
        self.assertEqual(result.returncode, 7)
        self.assertEqual(result.stdout, "")

    def test_empty_shard_fails(self):
        self.env["TEST_PACKAGES"] = PREFIX + "/query"
        self.assertNotEqual(self.run_shard("durable").returncode, 0)

    def test_serial_execution_and_failure_propagation(self):
        self.env["TEST_STATUS"] = "9"
        result = self.run_shard("process")
        self.assertEqual(result.returncode, 9)
        args = json.loads(result.stdout.splitlines()[-1])
        self.assertEqual(args, ["test", "-json", "-p=2", "-timeout=25m",
                                PREFIX + "/cmd/vibedb-gateway", PREFIX + "/cmd/vibedb-shard"])

    def test_core_runs_full_vet_and_tests_concurrently(self):
        result = self.run_shard("core")
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = [json.loads(line) for line in result.stdout.splitlines()
                    if line.startswith("[")]
        self.assertIn(["vet", "./..."], commands)
        core_test = next(command for command in commands if command[0] == "test")
        self.assertEqual(core_test[1:4], ["-json", "-p=4", "-timeout=25m"])

        self.env["VET_STATUS"] = "8"
        self.assertEqual(self.run_shard("core").returncode, 8)


if __name__ == "__main__":
    unittest.main()
