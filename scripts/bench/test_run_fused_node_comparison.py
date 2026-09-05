from contextlib import ExitStack
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("run-fused-node-comparison.py")
SPEC = importlib.util.spec_from_file_location("run_fused_node_comparison", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Arguments:
    matrix = "all"
    clients = "1,8"
    physical_nodes = "3,6"
    groups = 4
    tables = ""
    table_prefix = "rf3_sql_group"
    distributions = "uniform,skewed"
    endpoint_modes = "single,per-node"
    multigroup_workloads = "mixed_read_update"
    multigroup_clients = "8"


class FusedNodeRunnerTest(unittest.TestCase):
    def test_latch_copy_error_cannot_leave_completed_status(self):
        result = {"status": "completed", "errors": [
            "post-CONT diagnostic latch output was not retained"]}
        self.assertIs(MODULE.mark_result_failed_if_errors(result), result)
        self.assertEqual(result["status"], "failed")

    def test_diagnostic_bindings_use_manifest_local_member_and_exact_ready_pid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable_rows, process_rows = [], []
            members = [{"member_id": index, "node_id": f"{index:032x}"} for index in range(1, 4)]
            for index in range(1, 4):
                remote = f"/data/node{index}/serve.vibejson"
                path = root / "published/ready" / remote.lstrip("/")
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps({"node_log": {"path": f"/data/node{index}/node-log"}, "groups": [
                    {"route": {"member_id": index}, "members": members}]}))
                executable_rows.append(f"{100+index}\t/bench/candidate-vibedb-shard")
                process_rows.append(f"{100+index} 1 candidate-vibed /bench/candidate-vibedb-shard serve-node -manifest {remote}")
            inventory = {"executables": {"text": "\n".join(executable_rows)}, "processes": {"text": "\n".join(process_rows)}}
            targets = MODULE.candidate_diagnostic_targets(root, inventory, 3)
            self.assertEqual([target["pid"] for target in targets], [101, 102, 103])
            self.assertEqual(targets[0]["node_id"], f"{1:032x}")
            self.assertEqual(targets[0]["path"], "/data/node1/rf3-diagnostics.json")
            inventory["processes"]["text"] = inventory["processes"]["text"].replace("serve-node", "serve-rf3", 1)
            with self.assertRaisesRegex(MODULE.RunnerError, "ready serve-node"):
                MODULE.candidate_diagnostic_targets(root, inventory, 3)

    def test_aggregate_resource_ceiling_includes_disabled_swap(self):
        expected = {"NanoCpus": 12000000000, "Memory": 24 << 30, "MemorySwap": 24 << 30}
        self.assertEqual(MODULE.resource_limits("12", "24g"), expected)
        MODULE.validate_resource_limits({"HostConfig": expected}, "12", "24g")
        with self.assertRaisesRegex(MODULE.RunnerError, "aggregate limits"):
            MODULE.validate_resource_limits({"HostConfig": dict(expected, MemorySwap=-1)}, "12", "24g")
        for quota in ("0", "NaN", "Infinity", "-1"):
            with self.assertRaises(MODULE.RunnerError):
                MODULE.resource_limits(quota, "24g")

    def run_mocked_control_sequence(self, include_crdb):
        events = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            destination = root / "evidence"
            def prepare(repo, parent_ref, candidate_ref, worktrees):
                parent, candidate = worktrees / "parent", worktrees / "candidate"
                parent.mkdir()
                candidate.mkdir()
                return parent, candidate
            def build(args, output, parent, candidate, client, arch):
                events.append("build")
                binary = output / "bin"
                binary.mkdir()
                (binary / "rf3-sqlbench").write_text("shared binary")
                return binary, "go version go1.27.0 linux/amd64"
            def engine(args, cell, engine, order, bins, output, schema, arch):
                events.append((order, engine))
                self.assertTrue(schema.is_dir())
                return {"status": "completed", "client_exit_code": 0, "errors": []}
            source = {"revision": "client", "status": "", "dirty": False, "patch": b"", "patch_sha256": "unused"}
            with ExitStack() as stack:
                stack.enter_context(patch.object(MODULE, "docker_architecture", return_value="amd64"))
                stack.enter_context(patch.object(MODULE, "docker_json", return_value={}))
                stack.enter_context(patch.object(MODULE, "git_info", side_effect=lambda _: dict(source)))
                stack.enter_context(patch.object(MODULE, "text_output",
                                                 side_effect=[MODULE.PARENT_DEFAULT_REF, "candidate"]))
                stack.enter_context(patch.object(MODULE, "prepare_worktrees", side_effect=prepare))
                stack.enter_context(patch.object(MODULE, "build_all", side_effect=build))
                stack.enter_context(patch.object(MODULE, "ensure_image", return_value={"Id": "sha256:runtime"}))
                stack.enter_context(patch.object(MODULE, "run"))
                stack.enter_context(patch.object(MODULE, "cleanup_worktrees"))
                stack.enter_context(patch.object(MODULE, "run_engine", side_effect=engine))
                if include_crdb:
                    stack.enter_context(patch.object(MODULE, "extract_crdb_binary", return_value=None))
                invocation = [str(destination), "--repo", str(root), "--client-source", str(root),
                              "--candidate-ref", "candidate"]
                if not include_crdb:
                    invocation.append("--no-include-crdb")
                code = MODULE.main(invocation)
            manifest = json.loads((destination / "manifest.json").read_text())
            return code, events, manifest

    def test_full_control_sequence_builds_once_before_both_orderings(self):
        code, events, manifest = self.run_mocked_control_sequence(False)
        self.assertEqual(code, 0)
        self.assertEqual(events, ["build", ("parent-first", "parent"), ("parent-first", "candidate"),
                                  ("candidate-first", "candidate"), ("candidate-first", "parent")])
        self.assertEqual(manifest["engine_sequences"], {
            "parent-first": ["parent", "candidate"],
            "candidate-first": ["candidate", "parent"],
        })
        self.assertEqual(
            [(run["order"], run["engine"]) for run in manifest["planned_runs"]],
            [("parent-first", "parent"), ("parent-first", "candidate"),
             ("candidate-first", "candidate"), ("candidate-first", "parent")],
        )
        self.assertEqual(len(manifest["planned_runs"]), len(manifest["runs"]))
        self.assertEqual(manifest["promotion_gate"]["required_geomean_ratio"], 1.25)
        self.assertEqual(manifest["promotion_gate"]["max_cell_throughput_regression"], .05)
        self.assertEqual(manifest["promotion_gate"]["max_cell_p99_regression"], .10)
        MODULE.COMMAND_LOG = None

    def test_included_crdb_reverses_the_complete_engine_sequence(self):
        code, events, manifest = self.run_mocked_control_sequence(True)
        self.assertEqual(code, 0)
        self.assertEqual(events, ["build", ("parent-first", "parent"), ("parent-first", "candidate"),
                                  ("parent-first", "crdb"), ("candidate-first", "crdb"),
                                  ("candidate-first", "candidate"), ("candidate-first", "parent")])
        self.assertEqual(manifest["engine_sequences"], {
            "parent-first": ["parent", "candidate", "crdb"],
            "candidate-first": ["crdb", "candidate", "parent"],
        })
        self.assertEqual(
            [(run["order"], run["engine"]) for run in manifest["planned_runs"]],
            [("parent-first", "parent"), ("parent-first", "candidate"), ("parent-first", "crdb"),
             ("candidate-first", "crdb"), ("candidate-first", "candidate"), ("candidate-first", "parent")],
        )
        self.assertEqual(len(manifest["planned_runs"]), len(manifest["runs"]))
        MODULE.COMMAND_LOG = None

    def test_all_matrix_contains_baseline_and_both_node_distributions(self):
        cells = MODULE.cell_matrix(Arguments())
        self.assertEqual(cells[0]["workloads"], MODULE.DEFAULT_WORKLOADS)
        self.assertEqual(
            [cell["id"] for cell in cells],
            [
                "baseline-c1-c8",
                "multigroup-3n-uniform",
                "multigroup-3n-uniform-frontends",
                "multigroup-3n-skewed",
                "multigroup-3n-skewed-frontends",
                "multigroup-6n-uniform",
                "multigroup-6n-uniform-frontends",
                "multigroup-6n-skewed",
                "multigroup-6n-skewed-frontends",
            ],
        )
        for cell in cells[1:]:
            self.assertEqual(cell["groups"], 4)
            self.assertEqual(cell["workloads"], "mixed_read_update")

    def test_table_names_are_safe_and_unique(self):
        self.assertEqual(MODULE.table_names(3, "rf3_sql_group"),
                         ["rf3_sql_group", "rf3_sql_group_01", "rf3_sql_group_02"])
        with self.assertRaises(MODULE.RunnerError):
            MODULE.parse_tables("orders,orders")
        with self.assertRaises(MODULE.RunnerError):
            MODULE.table_names(2, "RF3")

    def test_workload_alias_is_canonicalized(self):
        self.assertEqual(MODULE.parse_workloads("mixed", "workloads"),
                         ["mixed_read_update"])
        self.assertEqual(MODULE.parse_workloads("update_uniform,mixed_uniform", "workloads"),
                         ["update_uniform", "mixed_uniform"])
        with self.assertRaises(MODULE.RunnerError):
            MODULE.parse_workloads("point_hit,point_hit", "workloads")

    def test_uniform_workloads_are_explicit_and_leave_default_matrix_unchanged(self):
        self.assertIn("update_uniform", MODULE.ALLOWED_WORKLOADS)
        self.assertIn("mixed_uniform", MODULE.ALLOWED_WORKLOADS)
        self.assertEqual(MODULE.cell_matrix(Arguments())[0]["workloads"], MODULE.DEFAULT_WORKLOADS)

    def test_optional_range_sizes_are_supported_without_changing_defaults(self):
        self.assertEqual(MODULE.parse_workloads("range_32,range_256", "workloads"),
                         ["range_32", "range_256"])
        self.assertEqual(MODULE.DEFAULT_WORKLOADS,
                         "point_hit,point_miss,range_64,group_16,update_existing")

    def test_client_report_requires_seed_and_per_trial_verification_controls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "report.json"
            report_path.write_text("{}")
            config = {
                "Rows": 64, "Operations": 20, "ScanOperations": 10,
                "Warmup": 2, "Repetitions": 1, "Clients": "1",
                "SeedBatch": 64, "VerifyEveryTrial": True,
                "Tables": ["rf3_sql_bench"], "Workloads": ["point_hit"],
                "GroupDistribution": "uniform", "SkewPercent": 80,
                "PayloadBytes": MODULE.DEFAULT_PAYLOAD_BYTES,
                "KeySelection": "splitmix64-independent-with-replacement-v1",
                "DiagnosticMode": "none", "PhysicalNodes": 9, "EndpointCount": 1,
            }

            class Args:
                rows, operations, scans, warmup, repetitions = 64, 20, 10, 2, 1
                skew_percent = 80

            cell = {"clients": "1", "tables": ["rf3_sql_bench"], "workloads": "point_hit",
                    "group_distribution": "uniform", "physical_nodes": 3, "endpoint_mode": "single"}
            for index, (field, invalid) in enumerate(((None, None), ("SeedBatch", 32), ("VerifyEveryTrial", False))):
                # A fresh path prevents SourceFileLoader from reusing a same-second
                # .pyc when each fake validator variant is written in place.
                validator = root / f"validator-{index}.py"
                Args.validator_path = validator
                candidate = dict(config)
                if field is not None:
                    candidate[field] = invalid
                validator.write_text("def load(path, engine):\n    return " + repr({"config": candidate, "status": "complete"}) + ", 20\n")
                if field is None:
                    got = MODULE.validate_client_report(report_path, Args, cell, "vibedb", "parent")
                    self.assertEqual(got, {"samples_checked": 20, "complete": True})
                else:
                    with self.assertRaisesRegex(MODULE.RunnerError, "planned workload"):
                        MODULE.validate_client_report(report_path, Args, cell, "vibedb", "parent")

    def test_output_directory_refuses_reuse(self):
        with tempfile.TemporaryDirectory() as parent:
            path = Path(parent) / "evidence"
            path.mkdir()
            with self.assertRaises(MODULE.RunnerError):
                MODULE.require_new_directory(path)

    def test_process_counts_use_exact_executable_not_truncated_comm(self):
        inventory = {"executables": {"exit_code": 0, "text": "1\t/bin/sleep\n10\t/bench/vibedb\n11\t/bench/candidate-vibedb-shard\n12\t/bench/parent-vibedb-gateway\n13\t/bench/cockroach\n14\t/tmp/vibedb-gateway-wrapper\n"},
                     "processes": {"text": "11 1 candidate-vibed /bench/candidate-vibedb-shard\n12 1 parent-vibedb-ga /bench/parent-vibedb-gateway\n"}}
        self.assertEqual(MODULE.process_counts(inventory), {
            "vibedb": 1,
            "vibedb_shard": 1,
            "vibedb_gateway": 1,
            "cockroach": 1,
        })
        with self.assertRaises(MODULE.RunnerError):
            MODULE.process_counts({"processes": inventory["processes"]})

    def test_topology_expectation_keeps_legacy_parent_distinct(self):
        baseline = {"kind": "baseline", "physical_nodes": 3}
        self.assertEqual(MODULE.serving_topology_expectation("parent", baseline),
                         {"vibedb_shard": 9, "vibedb_gateway": 1})
        self.assertEqual(MODULE.serving_topology_expectation("candidate", baseline),
                         {"vibedb_shard": 3, "vibedb_gateway": 0})
        with self.assertRaisesRegex(MODULE.RunnerError, "topology mismatch"):
            MODULE.validate_serving_topology("parent", baseline,
                                             {"vibedb_shard": 3, "vibedb_gateway": 1})

    def test_multigroup_requires_explicit_vibedb_frontends(self):
        args = type("Args", (), {"vibedb_sql_ports": "5432"})()
        cell = {"kind": "multigroup", "physical_nodes": 3, "id": "multigroup-3n-uniform-frontends", "endpoint_mode": "per-node"}
        with self.assertRaisesRegex(MODULE.RunnerError, "missing dependency"):
            MODULE.sql_ports_for(args, cell, "parent")
        args.vibedb_sql_ports = "5432,5433,5434"
        self.assertEqual(MODULE.sql_ports_for(args, cell, "parent"), [5432, 5433, 5434])
        args.vibedb_sql_ports = "5432,5433,5434,5435,5436,5437"
        self.assertEqual(MODULE.sql_ports_for(args, cell, "parent"), [5432, 5433, 5434])
        baseline = {"kind": "baseline", "physical_nodes": 3, "id": "baseline-c1-c8"}
        self.assertEqual(MODULE.sql_ports_for(args, baseline, "parent"), [5432])
        self.assertEqual(MODULE.sql_ports_for(args, baseline, "crdb"), [26257])

    def test_parent_multigroup_single_entrypoint_remains_supported(self):
        cell = {"physical_nodes": 3, "kind": "multigroup", "endpoint_mode": "single"}
        self.assertIsNone(MODULE.unsupported_reason("parent", cell))
        self.assertEqual(MODULE.serving_topology_expectation("parent", cell),
                         {"vibedb_shard": 9, "vibedb_gateway": 1})
        cell["endpoint_mode"] = "per-node"
        self.assertIn("one standalone", MODULE.unsupported_reason("parent", cell))
        cell["physical_nodes"] = 6
        self.assertIn("six-physical", MODULE.unsupported_reason("parent", cell))

    def test_precreated_schema_directory_and_unsupported_record_are_retained(self):
        cell = MODULE.cell_matrix(Arguments())[-1]
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory) / "engine"
            schema = MODULE.schema_files(dest, cell["tables"])
            args = type("Args", (), {"cpus": "12", "memory": "24g"})()
            with patch.object(MODULE, "run") as command:
                result = MODULE.run_engine(args, cell, "parent", "parent-first", Path(directory), dest, schema, "amd64")
            command.assert_not_called()
            self.assertEqual(result["status"], "unsupported")
            self.assertEqual(json.loads((dest / "run.json").read_text())["status"], "unsupported")

    def test_shared_client_is_built_from_nested_module(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = root / "source"
            main = client / "integration/pgclient/cmd/rf3-sqlbench/main.go"
            main.parent.mkdir(parents=True)
            main.write_text("package main\n")
            with patch.object(MODULE, "text_output", return_value="go version go1.27.0 linux/amd64"), patch.object(MODULE, "build_binary") as build:
                MODULE.build_all(None, root, root / "parent", root / "candidate", client, "amd64")
            self.assertEqual(build.call_count, 7)
            self.assertEqual(build.call_args.args[0], client / "integration/pgclient")

    def test_url_credentials_are_redacted_from_control_evidence(self):
        value = MODULE.redact({"argv": ["postgresql://alice:swordfish@127.0.0.1/db?password=another-secret"]})
        text = json.dumps(value)
        self.assertNotIn("alice", text)
        self.assertNotIn("swordfish", text)
        self.assertNotIn("another-secret", text)

    def test_identity_inventory_excludes_gateway_policy_nodes_and_requires_rf3(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "published/ready/data/manifest.vibejson"
            path.parent.mkdir(parents=True)
            value = {"gateway_node": "g1", "node_id": "irrelevant-policy-node", "groups": [
                {"group_id": "data1", "table": "orders", "members": [
                    {"member_id": index, "node_id": f"n{index}"} for index in range(1, 4)]}],
                "nodes": [{"node_log": {"path": f"/data/n{i}/node-log"}} for i in range(1, 4)]}
            path.write_text(json.dumps(value))
            got = MODULE.published_identity_inventory(root, "ready", {"copied": ["/data/manifest.vibejson"]})
            self.assertEqual(got["storage_node_ids"], ["n1", "n2", "n3"])
            self.assertEqual(got["gateway_node_ids"], ["g1"])
            cell = {"tables": ["orders"], "physical_nodes": 3}
            MODULE.validate_published_topology("candidate", cell, got)
            got["group_rosters"]["data1"] = [["n1", "n1", "n3"]]
            with self.assertRaisesRegex(MODULE.RunnerError, "three-node RF3"):
                MODULE.validate_published_topology("candidate", cell, got)


if __name__ == "__main__":
    unittest.main()
