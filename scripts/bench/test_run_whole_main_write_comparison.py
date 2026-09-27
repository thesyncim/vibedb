import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name("run-whole-main-write-comparison.py")
SPEC = importlib.util.spec_from_file_location("run_whole_main_write_comparison", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


WORKLOADS = {
    "sequential-insert": "BenchmarkReplicatedApplyBatch64SequentialInsert",
    "compact-shared-payload": "BenchmarkReplicatedApplyBatch64CompactSharedPayload",
}
REQUIRED_METRICS = {
    "rows": 131072,
    "ns/op": 12_345_678_900,
    "B/op": 502_523_904,
    "allocs/op": 1_996_120,
    "final-allocated-file-B": 63_111_168,
    "final-apparent-file-B": 81_657_856,
    "final-fold-ns/row": 1234.5,
}
FAKE_TIME_STDERR = (
    "Maximum resident set size (kbytes): 1234\n"
    "Elapsed (wall clock) time (h:mm:ss or m:ss): 0:00.01\n"
)


def benchmark_line(name, metrics=None, iterations=1, suffix=8):
    values = dict(REQUIRED_METRICS)
    if metrics:
        values.update(metrics)
    benchmark = name if name.startswith("Benchmark") else "Benchmark" + name
    fields = [f"{benchmark}-{suffix}", str(iterations)]
    fields.extend(f"{value} {unit}" for unit, value in values.items() if value is not None)
    return "\t".join(fields)


def successful_process(argv, stdout, stderr=FAKE_TIME_STDERR):
    return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr=stderr)


class WholeMainWriteComparisonTest(unittest.TestCase):
    def test_parse_one_complete_numeric_benchmark_row(self):
        expected = WORKLOADS["sequential-insert"]
        parsed = MODULE.parse_benchmark_output(
            "unrelated benchmark output\n" + benchmark_line(expected), expected)
        self.assertEqual(parsed["benchmark"], expected + "-8")
        self.assertEqual(parsed["iterations"], 1)
        for metric, value in REQUIRED_METRICS.items():
            self.assertEqual(parsed["metrics"][metric], float(value))

    def test_parser_rejects_missing_or_duplicate_rows(self):
        expected = WORKLOADS["sequential-insert"]
        cases = {
            "missing": "BenchmarkOther-8 1 12 ns/op",
            "duplicate": "\n".join((benchmark_line(expected), benchmark_line(expected, suffix=16))),
        }
        for name, output in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(MODULE.RunnerError):
                    MODULE.parse_benchmark_output(output, expected)

    def test_parser_rejects_wrong_iteration_row_count_and_missing_space_metrics(self):
        expected = WORKLOADS["compact-shared-payload"]
        cases = {
            "wrong iteration count": benchmark_line(expected, iterations=2),
            "wrong corpus rows": benchmark_line(expected, metrics={"rows": 131071}),
            "missing rows": benchmark_line(expected, metrics={"rows": None}),
            "missing final allocated space": benchmark_line(
                expected, metrics={"final-allocated-file-B": None}),
            "missing final fold metric": benchmark_line(expected, metrics={"final-fold-ns/row": None}),
        }
        for name, output in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(MODULE.RunnerError):
                    MODULE.parse_benchmark_output(output, expected)

    def test_parser_rejects_nonfinite_values_in_required_and_extra_metrics(self):
        expected = WORKLOADS["sequential-insert"]
        cases = {
            "nan runtime": {"ns/op": "NaN"},
            "infinite allocation": {"final-allocated-file-B": "+Inf"},
            "negative infinite fold": {"final-fold-ns/row": "-Inf"},
            "nonfinite extra metric": {"committed-batches": "NaN"},
            "zero runtime": {"ns/op": 0},
        }
        for name, metrics in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(MODULE.RunnerError):
                    MODULE.parse_benchmark_output(benchmark_line(expected, metrics), expected)

    def test_sample_plan_has_exact_twelve_runs_and_three_alternating_pairs_per_workload(self):
        samples = MODULE.planned_samples()
        self.assertEqual(len(samples), 12)
        expected_arms = {
            1: ["baseline", "candidate"],
            2: ["candidate", "baseline"],
            3: ["baseline", "candidate"],
        }
        for workload, benchmark in WORKLOADS.items():
            selected = [sample for sample in samples if sample["workload"] == workload]
            self.assertEqual(len(selected), 6)
            self.assertEqual({sample["benchmark"] for sample in selected}, {benchmark})
            for pair in range(1, 4):
                pair_samples = [sample for sample in selected if sample["pair"] == pair]
                self.assertEqual([sample["arm"] for sample in pair_samples], expected_arms[pair])

    def make_inputs(self, root):
        binaries = {}
        for arm in ("baseline", "candidate"):
            path = root / f"{arm}.test"
            path.write_bytes((arm + " test binary").encode())
            binaries[arm] = path
        return binaries

    def make_process_runner(self, samples, calls, fail_at=None, interrupt_at=None,
                            runtime_values=None, stderr=FAKE_TIME_STDERR):
        def run(argv, **kwargs):
            index = len(calls)
            calls.append((list(argv), dict(kwargs)))
            if index == interrupt_at:
                raise KeyboardInterrupt()
            sample = samples[index]
            metrics = {}
            if runtime_values is not None:
                metrics["ns/op"] = runtime_values[sample["workload"]][sample["arm"]][sample["pair"] - 1]
            output = benchmark_line(sample["benchmark"], metrics=metrics)
            return subprocess.CompletedProcess(argv, 1 if index == fail_at else 0,
                                               stdout=output, stderr=stderr)
        return run

    def test_run_uses_exact_alternating_order_binary_and_single_benchmark_flags(self):
        samples = MODULE.planned_samples()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = self.make_inputs(root)
            output = root / "evidence"
            tempdir = root / "benchmark-tmp"
            tempdir.mkdir()
            calls = []
            with mock.patch.dict(MODULE.os.environ, {"TMPDIR": str(tempdir)}):
                MODULE.run_comparison(
                    binaries, output, gomaxprocs=5,
                    process_runner=self.make_process_runner(samples, calls),
                    time_binary="fake-time")

            self.assertEqual(len(calls), 12)
            manifest = json.loads((output / "runs.json").read_text())
            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(len(manifest["runs"]), 12)
            self.assertTrue((output / "comparison.json").is_file())
            comparison = json.loads((output / "comparison.json").read_text())
            self.assertEqual(comparison["workload_scope"]["rows_per_operation"], 131072)
            self.assertTrue(comparison["resource_scope"]["disk_peak_is_sampled_not_continuously_observed"])
            self.assertEqual(comparison["tmpdir"], str(tempdir))
            self.assertIn("baseline_over_candidate_ratio_of_medians",
                          comparison["workloads"]["sequential-insert"]["metrics"]["ns/op"])
            self.assertEqual(len(comparison["workloads"]["sequential-insert"]["metrics"]),
                             len(REQUIRED_METRICS))
            self.assertEqual(manifest["tmpdir"], str(tempdir))
            for sample, run, (argv, kwargs) in zip(samples, manifest["runs"], calls):
                with self.subTest(ordinal=run["ordinal"]):
                    self.assertEqual(run["workload"], sample["workload"])
                    self.assertEqual(run["pair"], sample["pair"])
                    self.assertEqual(run["arm"], sample["arm"])
                    self.assertEqual(run["status"], "complete")
                    self.assertEqual(run["command"], argv)
                    self.assertIn(str(binaries[sample["arm"]]), argv)
                    self.assertIn("-test.run=^$", argv)
                    self.assertIn("-test.count=1", argv)
                    self.assertIn("-test.benchtime=1x", argv)
                    self.assertIn("-test.benchmem", argv)
                    bench_flags = [arg for arg in argv if arg.startswith("-test.bench=")]
                    self.assertEqual(bench_flags, [f"-test.bench=^{sample['benchmark']}$"])
                    self.assertEqual(kwargs["env"]["GOMAXPROCS"], "5")
                    self.assertEqual(kwargs["env"]["LC_ALL"], "C")
                    self.assertEqual(kwargs["env"]["TMPDIR"], str(tempdir))
                    self.assertIn("fake-time", argv)
                    self.assertTrue((output / run["stdout_file"]).is_file())
                    self.assertTrue((output / run["stderr_file"]).is_file())
                    self.assertEqual(run["time_metrics"]["maximum_resident_set_kib"], 1234)
                    self.assertIn("started_utc", run)
                    self.assertIn("completed_utc", run)
                    self.assertGreaterEqual(run["wall_seconds"], 0)

    def test_comparison_uses_ratio_of_medians_and_preserves_each_pair(self):
        samples = MODULE.planned_samples()
        values = {
            workload: {"baseline": [30.0, 20.0, 10.0], "candidate": [15.0, 10.0, 5.0]}
            for workload in WORKLOADS
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = self.make_inputs(root)
            output = root / "evidence"
            calls = []
            MODULE.run_comparison(
                binaries, output, gomaxprocs=2,
                process_runner=self.make_process_runner(
                    samples, calls, runtime_values=values),
                time_binary="fake-time")
            comparison = json.loads((output / "comparison.json").read_text())
            result = comparison["workloads"]["sequential-insert"]["metrics"]["ns/op"]
            self.assertEqual(result["baseline_median"], 20.0)
            self.assertEqual(result["candidate_median"], 10.0)
            self.assertEqual(result["baseline_over_candidate_ratio_of_medians"], 2.0)
            self.assertEqual(result["paired_baseline_over_candidate_ratios"], [2.0, 2.0, 2.0])

    def test_nonfinite_summary_ratio_fails_without_publishing_comparison(self):
        samples = MODULE.planned_samples()
        values = {
            workload: {"baseline": [1e308] * 3, "candidate": [1e-308] * 3}
            for workload in WORKLOADS
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = self.make_inputs(root)
            output = root / "evidence"
            calls = []
            with self.assertRaisesRegex(MODULE.RunnerError, "non-finite"):
                MODULE.run_comparison(
                    binaries, output, gomaxprocs=2,
                    process_runner=self.make_process_runner(
                        samples, calls, runtime_values=values),
                    time_binary="fake-time")
            manifest = json.loads((output / "runs.json").read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertFalse((output / "comparison.json").exists())

    def test_comparison_rename_failure_marks_run_failed_and_removes_success_file(self):
        samples = MODULE.planned_samples()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = self.make_inputs(root)
            output = root / "evidence"
            calls = []
            real_replace = MODULE.os.replace

            def fail_comparison_rename(source, destination):
                if Path(destination).name == "comparison.json":
                    raise OSError("simulated final comparison rename failure")
                return real_replace(source, destination)

            with mock.patch.object(MODULE.os, "replace", side_effect=fail_comparison_rename):
                with self.assertRaisesRegex(MODULE.RunnerError, "finalize comparison"):
                    MODULE.run_comparison(
                        binaries, output, gomaxprocs=2,
                        process_runner=self.make_process_runner(samples, calls),
                        time_binary="fake-time")
            manifest = json.loads((output / "runs.json").read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertFalse((output / "comparison.json").exists())
            self.assertFalse((output / "comparison.json.tmp").exists())

    def test_nonzero_process_with_plausible_output_never_produces_success(self):
        samples = MODULE.planned_samples()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = self.make_inputs(root)
            output = root / "evidence"
            calls = []
            with self.assertRaises(MODULE.RunnerError):
                MODULE.run_comparison(
                    binaries, output, gomaxprocs=3,
                    process_runner=self.make_process_runner(samples, calls, fail_at=1),
                    time_binary="fake-time")
            manifest = json.loads((output / "runs.json").read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(manifest["runs"][1]["status"], "failed")
            self.assertEqual(manifest["runs"][1]["exit_code"], 1)
            self.assertFalse((output / "comparison.json").exists())

    def test_success_without_gnu_time_rss_is_rejected(self):
        samples = MODULE.planned_samples()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = self.make_inputs(root)
            output = root / "evidence"
            calls = []
            with self.assertRaises(MODULE.RunnerError):
                MODULE.run_comparison(
                    binaries, output, gomaxprocs=2,
                    process_runner=self.make_process_runner(samples, calls, stderr=""),
                    time_binary="fake-time")
            manifest = json.loads((output / "runs.json").read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertIn("usable maximum RSS", manifest["runs"][0]["error"])
            self.assertFalse((output / "comparison.json").exists())

    def test_interruption_preserves_completed_samples_without_success_artifact(self):
        samples = MODULE.planned_samples()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = self.make_inputs(root)
            output = root / "evidence"
            calls = []
            with self.assertRaises(KeyboardInterrupt):
                MODULE.run_comparison(
                    binaries, output, gomaxprocs=2,
                    process_runner=self.make_process_runner(samples, calls, interrupt_at=4),
                    time_binary="fake-time")
            manifest = json.loads((output / "runs.json").read_text())
            self.assertEqual(manifest["status"], "interrupted")
            self.assertGreaterEqual(len(manifest["runs"]), 4)
            self.assertTrue(all(run["status"] == "complete" for run in manifest["runs"][:4]))
            interrupted = manifest["runs"][-1]
            self.assertEqual(interrupted["status"], "interrupted")
            self.assertEqual(interrupted["stdout_capture"], "unavailable")
            self.assertEqual(interrupted["stderr_capture"], "unavailable")
            self.assertIsNone(interrupted["stdout_file"])
            self.assertIsNone(interrupted["stderr_file"])
            self.assertFalse((output / "comparison.json").exists())
            for run in manifest["runs"][:4]:
                self.assertTrue((output / run["stdout_file"]).is_file())
                self.assertTrue((output / run["stderr_file"]).is_file())

    def test_invalid_success_exit_output_and_stale_success_directory_are_rejected(self):
        samples = MODULE.planned_samples()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = self.make_inputs(root)
            output = root / "evidence"
            calls = []

            def bad_output_runner(argv, **kwargs):
                calls.append((list(argv), dict(kwargs)))
                return successful_process(
                    argv,
                    benchmark_line("BenchmarkUnexpected", metrics={}),
                    stderr=FAKE_TIME_STDERR,
                )

            with self.assertRaises(MODULE.RunnerError):
                MODULE.run_comparison(
                    binaries, output, gomaxprocs=2,
                    process_runner=bad_output_runner, time_binary="fake-time")
            self.assertFalse((output / "comparison.json").exists())

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = self.make_inputs(root)
            output = root / "old-evidence"
            output.mkdir()
            prior_success = output / "comparison.json"
            prior_success.write_text('{"status":"complete","sentinel":"old"}\n')
            calls = []
            with self.assertRaises(MODULE.RunnerError):
                MODULE.run_comparison(
                    binaries, output, gomaxprocs=2,
                    process_runner=self.make_process_runner(samples, calls),
                    time_binary="fake-time")
            self.assertEqual(calls, [])
            self.assertEqual(json.loads(prior_success.read_text())["sentinel"], "old")


if __name__ == "__main__":
    unittest.main()
