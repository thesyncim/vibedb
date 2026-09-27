import importlib.util
import hashlib
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

    def make_inputs(self, root, tmpdir=None):
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


class WholeMainWriteProfileTest(unittest.TestCase):
    def make_inputs(self, root, tmpdir=None):
        binaries = {}
        sources = {}
        for arm in ("baseline", "candidate"):
            binary = root / f"{arm}.test"
            binary.write_bytes((arm + " diagnostic binary").encode())
            source = root / f"{arm}-source"
            source.mkdir()
            binaries[arm] = binary
            sources[arm] = source
        timing = root / "comparison"
        timing.mkdir()
        (timing / "runs.json").write_text(json.dumps({
            "status": "complete",
            "runs": [{"ordinal": index} for index in range(12)],
            "goexperiment": "simd",
            "gomaxprocs": 2,
            "tmpdir": str(tmpdir if tmpdir is not None else MODULE.os.environ.get("TMPDIR", "")),
            "binary_sha256": {
                arm: hashlib.sha256(path.read_bytes()).hexdigest()
                for arm, path in binaries.items()
            },
        }))
        (timing / "comparison.json").write_text(json.dumps({"status": "complete"}))
        build = root / "build-metadata.json"
        build.write_text(json.dumps({
            "workflow_revision": "d" * 40,
            "baseline_revision": "a" * 40,
            "candidate_revision": "b" * 40,
            "harness_revision": "c" * 40,
            "harness_sha256": "e" * 64,
            "go_version": "go version go1.27.1 linux/amd64",
            "go_settings": {"GOOS": "linux", "GOARCH": "amd64"},
            "goexperiment": "simd",
            "gomaxprocs": "2",
            "tmpdir": str(tmpdir if tmpdir is not None else MODULE.os.environ.get("TMPDIR", "")),
        }))
        return binaries, sources, timing, build

    def make_runner(self, calls, fail_export_at=None, omit_profile_at=None,
                    malformed_profile_at=None, empty_export_at=None,
                    silent_export_at=None, silent_export_status=1):
        profile_calls = 0
        export_calls = 0

        def run(argv, **kwargs):
            nonlocal profile_calls, export_calls
            calls.append((list(argv), dict(kwargs)))
            if argv[0] == "fake-time":
                index = profile_calls
                profile_calls += 1
                if index != omit_profile_at:
                    profile_arg = next(arg for arg in argv if arg.startswith("-test.cpuprofile="))
                    Path(profile_arg.split("=", 1)[1]).write_bytes(b"test profile bytes")
                benchmark = next(arg.split("=", 1)[1].strip("^$")
                                 for arg in argv if arg.startswith("-test.bench="))
                if index == malformed_profile_at:
                    return successful_process(argv, "BenchmarkUnexpected-2 1 1 ns/op\n",
                                              FAKE_TIME_STDERR)
                return successful_process(
                    argv, benchmark_line(benchmark), FAKE_TIME_STDERR +
                    "User time (seconds): 0.01\nSystem time (seconds): 0.00\n"
                    "Percent of CPU this job got: 100%\n")
            self.assert_pprof = True
            index = export_calls
            export_calls += 1
            if index == empty_export_at:
                return subprocess.CompletedProcess(
                    argv, 1, stdout="", stderr="no matches found for regexp\n")
            if index == silent_export_at:
                return subprocess.CompletedProcess(
                    argv, silent_export_status, stdout="", stderr="")
            return subprocess.CompletedProcess(
                argv,
                1 if index == fail_export_at else 0,
                stdout="Showing nodes accounting for 90%, 2.5s total\n",
                stderr="",
            )

        return run

    def test_optional_candidate_revision_defaults_and_resolves_to_full_sha(self):
        workflow_sha = "A" * 40
        requested_sha = "b" * 40
        calls = []

        def resolve(argv, **kwargs):
            calls.append((argv, kwargs))
            selected = argv[-1].split("^{", 1)[0]
            value = workflow_sha if selected == "refs/workflow" else requested_sha
            return subprocess.CompletedProcess(argv, 0, stdout=value + "\n", stderr="")

        self.assertEqual(MODULE.resolve_commit_revision(
            "", "refs/workflow", Path("/repo"), process_runner=resolve), workflow_sha.lower())
        self.assertEqual(MODULE.resolve_commit_revision(
            "refs/candidate", "refs/workflow", Path("/repo"), process_runner=resolve),
            requested_sha)
        self.assertEqual(len(calls), 2)
        self.assertIn("refs/workflow^{commit}", calls[0][0])
        self.assertIn("refs/candidate^{commit}", calls[1][0])
        with self.assertRaisesRegex(MODULE.RunnerError, "revision is required"):
            MODULE.resolve_commit_revision("", "", Path("/repo"), process_runner=resolve)

        def invalid_revision(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 128, stdout="", stderr="bad revision")

        with self.assertRaisesRegex(MODULE.RunnerError, "does not resolve"):
            MODULE.resolve_commit_revision(
                "missing", "", Path("/repo"), process_runner=invalid_revision)

    def test_profile_plan_has_four_separate_one_shot_diagnostics(self):
        samples = MODULE.planned_profile_samples()
        self.assertEqual(len(samples), 4)
        self.assertEqual(
            [(sample["workload"], sample["arm"]) for sample in samples],
            [("sequential-insert", "baseline"), ("sequential-insert", "candidate"),
             ("compact-shared-payload", "baseline"),
             ("compact-shared-payload", "candidate")],
        )
        self.assertEqual(MODULE.PROFILE_SCOPE["iterations_per_process"], 1)
        self.assertTrue(MODULE.PROFILE_SCOPE["diagnostic_only"])

    def test_profiles_reject_binary_or_environment_drift_before_starting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tmpdir = root / "temp-db"
            tmpdir.mkdir()
            binaries, sources, timing, build = self.make_inputs(root, tmpdir=tmpdir)
            calls = []
            binaries["candidate"].write_bytes(b"changed after timing")
            with mock.patch.dict(MODULE.os.environ, {
                "GOEXPERIMENT": "simd", "TMPDIR": str(tmpdir),
            }):
                with self.assertRaisesRegex(MODULE.RunnerError, "do not match"):
                    MODULE.run_profiles(
                        binaries, root / "profile-hash-mismatch", 2, timing, build, sources,
                        process_runner=self.make_runner(calls), time_binary="fake-time",
                        go_command="fake-go")
            self.assertEqual(calls, [])

        for name, target, setting, value, expected_error in (
            ("build GOEXPERIMENT", "build", "goexperiment", "nosimd", "GOEXPERIMENT differs from the timed build"),
            ("build GOMAXPROCS", "build", "gomaxprocs", "3", "GOMAXPROCS differs from the timed build"),
            ("build TMPDIR", "build", "tmpdir", "/other/tmp", "TMPDIR differs from the timed build"),
            ("timing GOEXPERIMENT", "timing", "goexperiment", "nosimd", "GOEXPERIMENT differs from the completed timing run"),
            ("timing GOMAXPROCS", "timing", "gomaxprocs", 3, "GOMAXPROCS differs from the completed timing run"),
            ("timing TMPDIR", "timing", "tmpdir", "/other/tmp", "TMPDIR differs from the completed timing run"),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                tmpdir = root / "timed-temp"
                tmpdir.mkdir()
                binaries, sources, timing, build = self.make_inputs(root, tmpdir=tmpdir)
                metadata_path = build if target == "build" else timing / "runs.json"
                metadata = json.loads(metadata_path.read_text())
                metadata[setting] = value
                metadata_path.write_text(json.dumps(metadata))
                calls = []
                with mock.patch.dict(MODULE.os.environ, {
                    "GOEXPERIMENT": "simd", "TMPDIR": str(tmpdir),
                }):
                    with self.assertRaisesRegex(MODULE.RunnerError, expected_error):
                        MODULE.run_profiles(
                            binaries, root / "profile-environment-mismatch", 2,
                            timing, build, sources,
                            process_runner=self.make_runner(calls), time_binary="fake-time",
                            go_command="fake-go")
                self.assertEqual(calls, [])

    def test_profiles_require_successful_timing_and_preserve_it_on_profile_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries, sources, timing, build = self.make_inputs(root)
            runs_before = hashlib.sha256((timing / "runs.json").read_bytes()).hexdigest()
            comparison_before = hashlib.sha256((timing / "comparison.json").read_bytes()).hexdigest()
            (timing / "runs.json").write_text(json.dumps({"status": "running", "runs": []}))
            calls = []
            with self.assertRaisesRegex(MODULE.RunnerError, "twelve completed"):
                MODULE.run_profiles(
                    binaries, root / "profiles", 2, timing, build, sources,
                    process_runner=self.make_runner(calls), time_binary="fake-time",
                    go_command="fake-go")
            self.assertEqual(calls, [])
            tmpdir = root / "outside-temp"
            tmpdir.mkdir()
            (timing / "runs.json").write_text(json.dumps({
                "status": "complete", "runs": [{"ordinal": index} for index in range(12)],
                "goexperiment": "simd", "gomaxprocs": 2, "tmpdir": str(tmpdir),
                "binary_sha256": {
                    arm: hashlib.sha256(path.read_bytes()).hexdigest()
                    for arm, path in binaries.items()
                },
            }))
            runs_before = hashlib.sha256((timing / "runs.json").read_bytes()).hexdigest()
            comparison_before = hashlib.sha256((timing / "comparison.json").read_bytes()).hexdigest()
            build_data = json.loads(build.read_text())
            build_data["tmpdir"] = str(tmpdir)
            build.write_text(json.dumps(build_data))

            with mock.patch.dict(MODULE.os.environ, {
                "GOEXPERIMENT": "simd", "TMPDIR": str(tmpdir),
            }):
                with self.assertRaisesRegex(MODULE.RunnerError, "pprof exited with status 1"):
                    MODULE.run_profiles(
                        binaries, root / "profiles", 2, timing, build, sources,
                        process_runner=self.make_runner(calls, fail_export_at=2),
                        time_binary="fake-time", go_command="fake-go")
            manifest = json.loads((root / "profiles" / "profiles.json").read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(manifest["runs"][0]["status"], "failed")
            self.assertEqual(manifest["runs"][0]["exports"][-1]["status"], "failed")
            self.assertEqual(len(manifest["runs"]), 1)
            self.assertTrue((root / "profiles" / manifest["runs"][0]["profile_file"]).is_file())
            self.assertTrue(all((root / "profiles" / item).is_file() for item in (
                manifest["runs"][0]["stdout_file"],
            )))
            self.assertEqual(manifest["runs"][0]["stderr_capture"], "captured")
            self.assertEqual(manifest["runs"][0]["exports"][-1]["stderr_capture"], "captured")
            self.assertEqual(runs_before, hashlib.sha256((timing / "runs.json").read_bytes()).hexdigest())
            self.assertEqual(comparison_before, hashlib.sha256((timing / "comparison.json").read_bytes()).hexdigest())

    def test_profiles_run_serially_capture_raw_logs_and_export_flat_cumulative_and_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tmpdir = root / "temp-db"
            tmpdir.mkdir()
            binaries, sources, timing, build = self.make_inputs(root, tmpdir=tmpdir)
            calls = []
            with mock.patch.dict(MODULE.os.environ, {
                "GOEXPERIMENT": "simd", "TMPDIR": str(tmpdir),
            }):
                manifest = MODULE.run_profiles(
                    binaries, root / "profiles", 2, timing, build, sources,
                    process_runner=self.make_runner(calls), time_binary="fake-time",
                    go_command="fake-go")
            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(len(manifest["runs"]), 4)
            self.assertEqual(manifest["tmpdir"], str(tmpdir))
            self.assertEqual(manifest["profile_scope"]["processes"], 4)
            self.assertEqual(len(calls), 4 * (1 + 2 + len(MODULE.PROFILE_GROUPS)))
            for run in manifest["runs"]:
                with self.subTest(ordinal=run["ordinal"]):
                    self.assertEqual(run["status"], "complete")
                    self.assertEqual(run["iterations"], 1)
                    self.assertEqual(run["metrics"]["rows"], 131072)
                    self.assertEqual(run["time_metrics"]["maximum_resident_set_kib"], 1234)
                    self.assertEqual(run["time_metrics"]["user_seconds"], 0.01)
                    self.assertEqual(run["time_metrics"]["cpu_percent"], 100.0)
                    self.assertTrue((root / "profiles" / run["profile_file"]).is_file())
                    self.assertIn("-test.cpuprofile=", " ".join(run["command"]))
                    self.assertEqual(len(run["exports"]), 2 + len(MODULE.PROFILE_GROUPS))
                    labels = {export["name"] for export in run["exports"]}
                    self.assertIn("flat-top", labels)
                    self.assertIn("cumulative-top", labels)
                    for label in MODULE.PROFILE_GROUPS:
                        self.assertIn("source-" + label, labels)
                    for export in run["exports"]:
                        self.assertEqual(export["status"], "complete")
                        self.assertTrue((root / "profiles" / export["stdout_file"]).is_file())
            for argv, kwargs in calls:
                self.assertEqual(kwargs["env"]["GOMAXPROCS"], "2")
                self.assertEqual(kwargs["env"]["GOEXPERIMENT"], "simd")
                self.assertEqual(kwargs["env"]["LC_ALL"], "C")
                self.assertEqual(kwargs["env"]["TMPDIR"], str(tmpdir))
                self.assertIn("cwd", kwargs)

    def test_empty_source_category_is_distinguished_from_failed_profile_processing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tmpdir = root / "temp-db"
            tmpdir.mkdir()
            binaries, sources, timing, build = self.make_inputs(root, tmpdir=tmpdir)
            calls = []
            with mock.patch.dict(MODULE.os.environ, {
                "GOEXPERIMENT": "simd", "TMPDIR": str(tmpdir),
            }):
                manifest = MODULE.run_profiles(
                    binaries, root / "profiles", 2, timing, build, sources,
                    process_runner=self.make_runner(calls, empty_export_at=2),
                    time_binary="fake-time", go_command="fake-go")
            self.assertEqual(manifest["status"], "complete")
            empty = manifest["runs"][0]["exports"][2]
            self.assertEqual(empty["status"], "empty")
            self.assertIn("no matching sampled source", empty["empty_reason"])

    def test_silent_nonzero_pprof_source_exports_fail_and_preserve_raw_captures(self):
        for name, returncode in (("silent exit 1", 1), ("killed", -9)):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                tmpdir = root / "temp-db"
                tmpdir.mkdir()
                binaries, sources, timing, build = self.make_inputs(root, tmpdir=tmpdir)
                calls = []
                with mock.patch.dict(MODULE.os.environ, {
                    "GOEXPERIMENT": "simd", "TMPDIR": str(tmpdir),
                }):
                    with self.assertRaisesRegex(MODULE.RunnerError, "pprof exited with status"):
                        MODULE.run_profiles(
                            binaries, root / "profiles", 2, timing, build, sources,
                            process_runner=self.make_runner(
                                calls, silent_export_at=2, silent_export_status=returncode),
                            time_binary="fake-time", go_command="fake-go")
                manifest = json.loads((root / "profiles" / "profiles.json").read_text())
                run = manifest["runs"][0]
                export = run["exports"][2]
                self.assertEqual(manifest["status"], "failed")
                self.assertEqual(export["status"], "failed")
                self.assertEqual(export["exit_code"], returncode)
                self.assertEqual(export["stdout_capture"], "captured")
                self.assertEqual(export["stderr_capture"], "captured")
                self.assertEqual((root / "profiles" / export["stdout_file"]).read_text(), "")
                self.assertEqual((root / "profiles" / export["stderr_file"]).read_text(), "")
                self.assertEqual(len(calls), 1 + 3)

    def test_malformed_profile_benchmark_keeps_captured_output_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries, sources, timing, build = self.make_inputs(root)
            calls = []
            with self.assertRaisesRegex(MODULE.RunnerError, "unexpected benchmark row"):
                MODULE.run_profiles(
                    binaries, root / "profiles", 2, timing, build, sources,
                    process_runner=self.make_runner(calls, malformed_profile_at=0),
                    time_binary="fake-time", go_command="fake-go")
            run = json.loads((root / "profiles" / "profiles.json").read_text())["runs"][0]
            self.assertEqual(run["status"], "failed")
            self.assertEqual(run["stdout_capture"], "captured")
            self.assertEqual(run["stderr_capture"], "captured")
            self.assertTrue((root / "profiles" / run["stdout_file"]).is_file())
            self.assertTrue((root / "profiles" / run["stderr_file"]).is_file())

    def test_missing_profile_file_is_a_preserved_diagnostic_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries, sources, timing, build = self.make_inputs(root)
            calls = []
            with self.assertRaisesRegex(MODULE.RunnerError, "nonempty CPU profile"):
                MODULE.run_profiles(
                    binaries, root / "profiles", 2, timing, build, sources,
                    process_runner=self.make_runner(calls, omit_profile_at=0),
                    time_binary="fake-time", go_command="fake-go")
            manifest = json.loads((root / "profiles" / "profiles.json").read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(manifest["runs"][0]["profile_capture"], "unavailable")
            self.assertTrue((root / "profiles" / manifest["runs"][0]["stdout_file"]).is_file())


if __name__ == "__main__":
    unittest.main()
