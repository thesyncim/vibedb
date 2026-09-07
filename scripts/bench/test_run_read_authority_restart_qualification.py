import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name("run-read-authority-restart-qualification.py")
SPEC = importlib.util.spec_from_file_location("run_read_authority_restart_qualification", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


GROUPS = ["0000000000000000000000000000000001", "0000000000000000000000000000000002"]


def request(group_id=GROUPS[0], nonce=7):
    return {
        "group": {
            "cluster_id": "a" * 32,
            "cluster_incarnation": "b" * 32,
            "topology_recovery_epoch": 1,
            "shard_incarnation": "c" * 32,
            "group_id": group_id,
        },
        "term": 4,
        "holder": 1,
        "holder_incarnation": 12,
        "config": {"applied_version": 3, "digest": "d" * 64,
                   "joint": False, "pending": False},
        "policy_version": 1,
        "policy_digest": "e" * 64,
        "nonce": nonce,
        "start_at_ns": 100,
    }


def group(group_id, member_id, node_incarnation=12, *, holder=False, promise=None,
          available_observation=True, sample=150, until=1000, tick_blocked=1):
    observed_request = promise or request(group_id, nonce=1)
    value = {
        "runtime_identity": {
            "group": {"cluster_id": "a" * 32, "cluster_incarnation": "b" * 32,
                       "topology_recovery_epoch": 1, "shard_incarnation": "c" * 32,
                       "group_id": group_id},
            "member_id": member_id, "store_id": "f" * 32,
            "node_incarnation": node_incarnation,
        },
        "policy_version": observed_request["policy_version"],
        "policy_digest": observed_request["policy_digest"],
        "clock": {"sample_ns": sample, "initialized": True, "faulted": False},
        "promise": {"has_record": promise is not None, "request": promise or {},
                     "promise_until_ns": 2000 if promise is not None else 0,
                     "quarantine_at_ns": 0, "quarantine_until_ns": until,
                     "quarantine_configured": True, "quarantine_known": True,
                     "quarantine_active": promise is None, "quarantine_error": False,
                     "active_known": promise is not None,
                     "active": promise is not None},
        "holder": {"available": holder, "request": promise or {},
                    "expires_at_ns": 2000 if holder else 0,
                    "accepted_voter_ids": [1, 2, 3] if holder else []},
        "gate": {"inputs": [{"input": "tick", "blocked_by_quarantine": tick_blocked,
                               "quarantine_blocked_time": {"available": True,
                                                            "first_ns": 1,
                                                            "last_ns": 10}}]},
        "observation_available": available_observation,
        "observation": {"group": observed_request["group"],
                         "term": observed_request["term"],
                         "leader": observed_request["holder"],
                         "leader_incarnation": observed_request["holder_incarnation"],
                         "config": observed_request["config"],
                         "stable": True, "current_term_committed": True},
    }
    return value


def snapshot(node, values):
    return {"event": "snapshot", "pid": 100 + node, "node_id": f"{node:032x}",
            "serial": node, "read_authority_evidence_available": True,
            "read_authority_evidence": values}


class _Completed:
    def __init__(self, returncode):
        self.returncode = returncode
        self.stdout = b""


class _StartupCopyFixture:
    def __init__(self, records):
        self.records = iter(records)

    def run(self, argv, **kwargs):
        record = next(self.records)
        if record is None:
            return _Completed(1)
        Path(argv[-1]).write_text(json.dumps(record), encoding="utf-8")
        return _Completed(0)


class RestartQualificationContractTest(unittest.TestCase):
    def test_exact_group_coverage_rejects_missing_and_duplicate_groups(self):
        values = [group(group_id, 1, promise=request(group_id)) for group_id in GROUPS]
        self.assertEqual(set(MODULE.exact_group_map(values, GROUPS, "cut")), set(GROUPS))
        with self.assertRaisesRegex(MODULE.RunnerError, "exactly"):
            MODULE.exact_group_map(values[:1], GROUPS, "cut")
        with self.assertRaisesRegex(MODULE.RunnerError, "duplicate"):
            MODULE.exact_group_map([values[0], values[0]], GROUPS, "cut")

    def test_grant_requires_exact_holder_request_and_accepted_nonholder(self):
        holder_request = request()
        holder = {GROUPS[0]: group(GROUPS[0], 1, holder=True, promise=holder_request),
                  GROUPS[1]: group(GROUPS[1], 1, holder=True, promise=request(GROUPS[1]))}
        nonholder = {GROUPS[0]: group(GROUPS[0], 2, promise=holder_request),
                     GROUPS[1]: group(GROUPS[1], 2, promise=request(GROUPS[1]))}
        found = MODULE.grant_candidates({"holder": snapshot(1, list(holder.values())),
                                         "nonholder": snapshot(2, list(nonholder.values()))}, GROUPS)
        self.assertEqual(len(found), 2)
        self.assertEqual(found[0]["nonholder_member_id"], 2)
        changed = dict(holder_request)
        changed["nonce"] += 1
        nonholder[GROUPS[0]] = group(GROUPS[0], 2, promise=changed)
        self.assertEqual(
            [x for x in MODULE.grant_candidates(
                {"holder": snapshot(1, list(holder.values())),
                 "nonholder": snapshot(2, list(nonholder.values()))}, GROUPS)
             if x["group_id"] == GROUPS[0]], [],
        )

    def test_quarantine_requires_same_boot_tick_before_deadline_and_no_grant(self):
        values = [group(group_id, 2, promise=None) for group_id in GROUPS]
        cut = snapshot(2, values)
        self.assertEqual(set(MODULE.validate_boot_quarantine(cut, GROUPS, "early")), set(GROUPS))
        values[0]["clock"]["sample_ns"] = values[0]["promise"]["quarantine_until_ns"]
        with self.assertRaisesRegex(MODULE.RunnerError, "after its quarantine deadline"):
            MODULE.validate_boot_quarantine(snapshot(2, values), GROUPS, "early")

    def test_acknowledged_update_requires_a_later_active_trial(self):
        completed = {
            "workload": "update_existing", "clients": 1, "repetition": 1,
            "operations": 8, "errors": 0, "verified": True,
        }
        report = {
            "results": [completed],
            "active_trial": {"workload": "update_existing", "clients": 1,
                             "repetition": 1, "phase": "verifying"},
        }
        self.assertIsNone(MODULE.acknowledged_update_progress(report))
        report["active_trial"]["clients"] = 8
        progress = MODULE.acknowledged_update_progress(report)
        self.assertEqual(progress["acknowledged_update_trials"], 1)
        self.assertEqual(progress["acknowledged_update_operations"], 8)
        self.assertEqual(progress["active_trial"]["clients"], 8)

    def test_original_holder_overlap_requires_the_same_live_request(self):
        original = request()
        values = [
            group(GROUPS[0], 1, holder=True, promise=original, sample=200, until=1000),
            group(GROUPS[1], 1, holder=True, promise=request(GROUPS[1]), sample=200, until=1000),
        ]
        proof = MODULE.validate_original_holder_live(
            snapshot(1, values), GROUPS, GROUPS[0], original, 1, "holder overlap")
        self.assertEqual(proof["request"], original)
        values[0]["holder"]["request"] = request(nonce=original["nonce"] + 1)
        with self.assertRaisesRegex(MODULE.RunnerError, "request changed"):
            MODULE.validate_original_holder_live(
                snapshot(1, values), GROUPS, GROUPS[0], original, 1, "holder overlap")

    def test_post_quarantine_proof_uses_same_voter_grant_time(self):
        values = [
            group(GROUPS[0], 2, promise=request(), sample=1600, until=1000),
            group(GROUPS[1], 2, promise=request(GROUPS[1]), sample=1600, until=1000),
        ]
        proof = MODULE.validate_post_quarantine(
            snapshot(2, values), GROUPS, GROUPS[0], 2, 1000, "post")
        self.assertEqual(proof["granted_at_ns"], 1000)
        values[0]["promise"]["promise_until_ns"] = 1900
        with self.assertRaisesRegex(MODULE.RunnerError, "before quarantine expiry"):
            MODULE.validate_post_quarantine(
                snapshot(2, values), GROUPS, GROUPS[0], 2, 1000, "post")

    def test_policy_marker_comparison_is_exact(self):
        first = {"node": [{"policy_version": 1, "policy_digest": "a",
                            "voters": [1, 2, 3], "enabled": True}]}
        second = json.loads(json.dumps(first))
        self.assertTrue(MODULE.same_policy(first, second))
        second["node"][0]["policy_digest"] = "b"
        self.assertFalse(MODULE.same_policy(first, second))

    def test_selected_restart_requires_new_pid_and_incarnation(self):
        old_target = {"pid": 101, "manifest_path": "/data/vibe/node-2/serve-rf3.vibejson",
                      "serve_argv": ["/bench/candidate-vibedb-shard", "serve-node", "-manifest",
                                     "/data/vibe/node-2/serve-rf3.vibejson", "-reload-prepared-groups"],
                      "executable": "/bench/candidate-vibedb-shard"}
        new_target = {"pid": 201, "manifest_path": old_target["manifest_path"],
                      "serve_argv": list(old_target["serve_argv"]),
                      "executable": old_target["executable"]}
        old = snapshot(1, [group(group_id, 2, node_incarnation=7,
                                 promise=request(group_id)) for group_id in GROUPS])
        new = snapshot(1, [group(group_id, 2, node_incarnation=8,
                                 promise=request(group_id)) for group_id in GROUPS])
        value = MODULE.validate_selected_restart(old_target, new_target, old, new, GROUPS)
        self.assertEqual(value["new_pid"], 201)
        self.assertEqual(len(value["groups"]), len(GROUPS))
        new_target["serve_argv"][-1] = "-reload-prepared-groups-old"
        with self.assertRaisesRegex(MODULE.RunnerError, "prepared serve-node argv"):
            MODULE.validate_selected_restart(old_target, new_target, old, new, GROUPS)

    def test_startup_wait_rejects_stale_supervisor_pid(self):
        stale = {"event": "read_authority_startup", "pid": 101, "serial": 1}
        fresh = {"event": "read_authority_startup", "pid": 202, "serial": 1}
        target = {"node_number": 2, "path": "/data/vibe/node-2/rf3-diagnostics.json"}
        fixture = _StartupCopyFixture([stale, fresh])
        with tempfile.TemporaryDirectory() as root, mock.patch.object(
                MODULE, "validate_startup_value", side_effect=lambda value, *args: value) as validate:
            observed, value = MODULE.wait_for_startup(
                fixture, "container", target, Path(root), "initial", [], 1,
                disallowed_pids={101})
        self.assertEqual(observed["pid"], 202)
        self.assertEqual(value["pid"], 202)
        self.assertEqual(validate.call_count, 1)


if __name__ == "__main__":
    unittest.main()
