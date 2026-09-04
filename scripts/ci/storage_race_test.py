"""Validate that the storage race lanes retain a disjoint selector."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("storage-race.sh").resolve()


class StorageRace(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        fake_go = Path(self.temp.name) / "go"
        fake_go.write_text(
            "#!/usr/bin/env python3\n"
            "import json, pathlib, sys\n"
            "args = sys.argv[1:]\n"
            "if '-c' in args:\n"
            "    output = pathlib.Path(args[args.index('-o') + 1])\n"
            "    output.write_text('#!/usr/bin/env python3\\nimport json,sys\\nprint(json.dumps(sys.argv[1:]))\\n')\n"
            "    output.chmod(0o755)\n"
            "else:\n"
            "    print(json.dumps(args))\n"
        )
        fake_go.chmod(0o755)
        self.env = dict(os.environ, PATH=self.temp.name + os.pathsep + os.environ["PATH"])

    def run_lane(self, lane):
        result = subprocess.run(["bash", str(SCRIPT), lane], env=self.env,
                                text=True, capture_output=True)
        return result, json.loads(result.stdout) if result.returncode == 0 else None

    def test_lanes_keep_original_selection_exactly_once(self):
        commands = {}
        for lane in ("storeio", "durable-heavy", "durable-rest"):
            result, commands[lane] = self.run_lane(lane)
            self.assertEqual(result.returncode, 0, result.stderr)

        self.assertEqual(commands["storeio"][-1], "./internal/storeio/")
        for lane in ("durable-heavy", "durable-rest"):
            self.assertEqual(commands[lane][-1], "./store/durable/")

        heavy = "TestFilePrimaryAdvancedRepackAmplification"
        selected = commands["durable-rest"][commands["durable-rest"].index("-run") + 1]
        skipped = commands["durable-rest"][commands["durable-rest"].index("-skip") + 1]
        heavy_run = commands["durable-heavy"][commands["durable-heavy"].index("-run") + 1]
        for name in (heavy, "TestFilePrimaryRepackRoundTrip",
                     "TestCommitterPortableLifecycle", "TestUnrelated",
                     "TestFilePrimaryTenMillionQualification"):
            original = re.search(selected, name) is not None and "Qualification" not in name
            lanes = [re.search(heavy_run, name) is not None,
                     re.search(selected, name) is not None and re.search(skipped, name) is None]
            self.assertEqual(sum(lanes), int(original), (name, lanes))

    def test_unknown_lane_fails(self):
        result, _ = self.run_lane("typo")
        self.assertEqual(result.returncode, 2)

    def test_all_compiles_once_and_runs_all_three_partitions(self):
        result = subprocess.run(["bash", str(SCRIPT), "all"], env=self.env,
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(len(commands), 3)
        self.assertEqual(sum("./internal/storeio/" in command for command in commands), 1)
        self.assertEqual(sum(any(arg == "-test.run=^TestFilePrimaryAdvancedRepackAmplification$"
                                 for arg in command) for command in commands), 1)
        self.assertEqual(sum(any(arg.startswith("-test.skip=Qualification|")
                                 for arg in command) for command in commands), 1)


if __name__ == "__main__":
    unittest.main()
