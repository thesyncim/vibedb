#!/usr/bin/env python3
"""Qualify one RF3 read-authority voter restart on a retained lab fixture.

The fixture has one deliberately selected fault row.  ``cluster dev`` is used
only to create and seed the durable root; it is stopped through its normal
SIGTERM path before the runner starts the exact three ``serve-node`` commands
from the prepared manifests itself.  The runner then checks a fresh boot's
quarantine using same-boot monotonic timestamps, kills one currently accepted
voter, restarts that voter on the same root and policy, and verifies a new
quorum grant plus the independently maintained SQL recovery oracle.

This is a bounded qualification for one restart fault.  It is deliberately not
an implementation of a general clock, partition, or membership qualification.
The lab build tag and the explicit command-line opt-in are required because the
standard authority gate remains unchanged.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import time
import uuid


ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = ROOT / "scripts/bench/run-fused-node-comparison.py"
FAULT_PATH = ROOT / "scripts/bench/run-read-authority-fault-qualification.py"
RUNTIME = (
    "golang:1.27-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b"
)
LAB_BUILD_TAG = "vibedb_rf3_read_authority_lab"
EXPECTED_NODES = 3
EXPECTED_VOTERS = (1, 2, 3)
DEFAULT_QUARANTINE_WAIT = 15.0
DEFAULT_TABLES = "rf3_authority_restart"
DEFAULT_WORKLOADS = "update_existing,point_hit,point_miss"


class RunnerError(RuntimeError):
    """A fixture-control or evidence-contract failure."""


class MissedWindowError(RunnerError):
    """The requested restart overlap or post-restart grant window was missed."""


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RunnerError("could not load helper " + str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_fixture():
    return load_module("vibedb_restart_fixture", FIXTURE_PATH)


def load_fault_helpers():
    return load_module("vibedb_restart_fault_helpers", FAULT_PATH)


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json(path, value):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        temporary.write_text(json.dumps(value, indent=2) + "\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def parse_json(path):
    try:
        value = json.loads(path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RunnerError("could not parse " + str(path) + ": " + str(exc)) from exc
    if not isinstance(value, dict):
        raise RunnerError(str(path) + " is not a JSON object")
    return value


def _int(value, name, minimum=None):
    if isinstance(value, bool) or not isinstance(value, int):
        raise RunnerError(name + " is not an integer")
    if minimum is not None and value < minimum:
        raise RunnerError(name + " is below " + str(minimum))
    return value


def _string(value, name):
    if not isinstance(value, str) or not value:
        raise RunnerError(name + " is missing")
    return value


def _group_id(group, context):
    runtime = group.get("runtime_identity")
    if not isinstance(runtime, dict):
        raise RunnerError(context + " has no runtime_identity")
    identity = runtime.get("group")
    if not isinstance(identity, dict):
        raise RunnerError(context + " has no runtime group identity")
    return _string(identity.get("group_id"), context + " group_id")


def exact_group_map(groups, expected_groups, context):
    """Return a map only when diagnostic coverage is exact and duplicate-free."""
    if not isinstance(groups, list) or len(groups) != len(expected_groups):
        raise RunnerError(
            context + " coverage has " + str(len(groups) if isinstance(groups, list) else "non-list") +
            "; expected exactly " + str(len(expected_groups)) + " distinct groups")
    expected = set(expected_groups)
    result = {}
    for index, group in enumerate(groups):
        if not isinstance(group, dict):
            raise RunnerError(context + " group " + str(index) + " is not an object")
        group_id = _group_id(group, context + " group " + str(index))
        if group_id not in expected:
            raise RunnerError(context + " contains unexpected group " + group_id)
        if group_id in result:
            raise RunnerError(context + " contains duplicate group " + group_id)
        result[group_id] = group
    if set(result) != expected:
        raise RunnerError(context + " does not cover the expected group set")
    return result


def snapshot_groups(snapshot, expected_groups):
    if snapshot.get("read_authority_evidence_available") is not True:
        raise RunnerError("snapshot read-authority evidence is not marked available")
    return exact_group_map(
        snapshot.get("read_authority_evidence"), expected_groups,
        "snapshot read-authority evidence")


def startup_groups(startup, expected_groups):
    # The evidence producer keeps the startup event distinct from later cuts.
    # Accept its compact ``read_authority_evidence`` spelling as the schema is
    # finalized, while still requiring the startup event and exact group set.
    if startup.get("read_authority_evidence_available") is not True:
        raise RunnerError("startup read-authority evidence is not marked available")
    evidence = startup.get("read_authority_startup_evidence")
    if evidence is None:
        evidence = startup.get("read_authority_evidence")
    return exact_group_map(
        evidence, expected_groups,
        "startup read-authority evidence")


def startup_quarantine_map(startup, expected_groups):
    result = {}
    for group_id, group in startup_groups(startup, expected_groups).items():
        _, at_ns, until_ns = quarantine_fields(group, "startup " + group_id)
        result[group_id] = {
            "quarantine_at_ns": at_ns,
            "quarantine_until_ns": until_ns,
        }
    return result


def _required_request(request, context):
    if not isinstance(request, dict):
        raise RunnerError(context + " is not an object")
    group = request.get("group")
    if not isinstance(group, dict):
        raise RunnerError(context + " has no group identity")
    for key in ("cluster_id", "cluster_incarnation", "shard_incarnation", "group_id"):
        _string(group.get(key), context + ".group." + key)
    _int(group.get("topology_recovery_epoch"), context + ".group.topology_recovery_epoch", 1)
    for key in ("term", "holder", "holder_incarnation", "nonce"):
        _int(request.get(key), context + "." + key, 1)
    config = request.get("config")
    if not isinstance(config, dict):
        raise RunnerError(context + " has no config identity")
    _int(config.get("applied_version"), context + ".config.applied_version", 1)
    _string(config.get("digest"), context + ".config.digest")
    if not isinstance(config.get("joint"), bool) or not isinstance(config.get("pending"), bool):
        raise RunnerError(context + " config joint/pending are not booleans")
    if config.get("joint") is True or config.get("pending") is True:
        raise RunnerError(context + " config is not stable")
    _int(request.get("policy_version"), context + ".policy_version", 1)
    _string(request.get("policy_digest"), context + ".policy_digest")
    _int(request.get("start_at_ns"), context + ".start_at_ns", 0)
    return request


def request_key(request, context="authority request"):
    """Canonical identity for a grant; all safety-relevant fields are required."""
    request = _required_request(request, context)
    return json.dumps(request, sort_keys=True, separators=(",", ":"))


def validate_request_binding(request, group, context):
    """Require a request to name the runtime group that supplied its cut."""
    request = _required_request(request, context + ".request")
    runtime = group.get("runtime_identity")
    if not isinstance(runtime, dict) or not isinstance(runtime.get("group"), dict):
        raise RunnerError(context + " has no runtime group identity")
    runtime_group = runtime["group"]
    request_group = request["group"]
    for key in ("cluster_id", "cluster_incarnation", "topology_recovery_epoch",
                "shard_incarnation", "group_id"):
        if request_group.get(key) != runtime_group.get(key):
            raise RunnerError(context + " request/runtime group identity differs in " + key)
    return request


def validate_request_observation(request, group, context):
    """Bind request term/config/holder identity to the observed current cut."""
    request = validate_request_binding(request, group, context)
    observation = group.get("observation")
    if group.get("observation_available") is not True or not isinstance(observation, dict):
        raise RunnerError(context + " has no current authority observation")
    observed_group = observation.get("group")
    if not isinstance(observed_group, dict):
        raise RunnerError(context + " observation has no group identity")
    for key in ("cluster_id", "cluster_incarnation", "topology_recovery_epoch",
                "shard_incarnation", "group_id"):
        if observed_group.get(key) != request["group"].get(key):
            raise RunnerError(context + " observation/request group differs in " + key)
    if _int(observation.get("term"), context + ".observation.term", 1) != request["term"]:
        raise RunnerError(context + " observation term differs from request")
    if _int(observation.get("leader"), context + ".observation.leader", 1) != request["holder"]:
        raise RunnerError(context + " observation leader differs from request holder")
    if _int(observation.get("leader_incarnation"), context + ".observation.leader_incarnation", 1) != request["holder_incarnation"]:
        raise RunnerError(context + " observation leader incarnation differs from request")
    observed_config = observation.get("config")
    if not isinstance(observed_config, dict):
        raise RunnerError(context + " observation has no configuration")
    for key in ("applied_version", "digest", "joint", "pending"):
        if observed_config.get(key) != request["config"].get(key):
            raise RunnerError(context + " observation config differs in " + key)
    if group.get("policy_version") != request["policy_version"] or \
            group.get("policy_digest") != request["policy_digest"]:
        raise RunnerError(context + " request policy differs from group policy")
    if observation.get("stable") is not True or observation.get("current_term_committed") is not True:
        raise RunnerError(context + " observation is not stable and current-term committed")
    return request


def runtime_member(group, context):
    runtime = group.get("runtime_identity")
    if not isinstance(runtime, dict):
        raise RunnerError(context + " has no runtime identity")
    return _int(runtime.get("member_id"), context + ".member_id", 1)


def runtime_node_incarnation(group, context):
    runtime = group.get("runtime_identity")
    if not isinstance(runtime, dict):
        raise RunnerError(context + " has no runtime identity")
    return _int(runtime.get("node_incarnation"), context + ".node_incarnation", 1)


def runtime_store_id(group, context):
    runtime = group.get("runtime_identity")
    if not isinstance(runtime, dict):
        raise RunnerError(context + " has no runtime identity")
    return _string(runtime.get("store_id"), context + ".store_id")


def accepted_voters(holder, context):
    values = holder.get("accepted_voter_ids")
    if not isinstance(values, list) or not values:
        raise RunnerError(context + " has no accepted voter list")
    result = []
    for value in values:
        value = _int(value, context + ".accepted_voter_id", 1)
        if value in result:
            raise RunnerError(context + " has duplicate accepted voter " + str(value))
        result.append(value)
    return set(result)


def grant_candidates(snapshots, expected_groups):
    """Find exact holder/nonholder request pairs across the current RF3 cuts."""
    by_node = {}
    for node_id, snapshot in snapshots.items():
        groups = snapshot_groups(snapshot, expected_groups)
        by_node[node_id] = groups
    candidates = []
    for group_id in expected_groups:
        holders = []
        for node_id, groups in by_node.items():
            group = groups[group_id]
            holder = group.get("holder")
            if not isinstance(holder, dict) or holder.get("available") is not True:
                continue
            request = holder.get("request")
            request = validate_request_observation(
                request, group, "holder " + node_id + " " + group_id)
            holder_key = request_key(request, "holder " + node_id + " " + group_id)
            holder_member = runtime_member(group, "holder " + node_id + " " + group_id)
            holder_incarnation = runtime_node_incarnation(
                group, "holder " + node_id + " " + group_id)
            if request["holder"] != holder_member:
                raise RunnerError("holder request does not name its runtime member for group " + group_id)
            if request["holder_incarnation"] != holder_incarnation:
                raise RunnerError("holder request does not name its runtime incarnation for group " + group_id)
            accepted = accepted_voters(holder, "holder " + node_id + " " + group_id)
            if not accepted.issubset(set(EXPECTED_VOTERS)) or len(accepted) < 2:
                raise RunnerError("holder accepted-voter set is not a complete RF3 subset for group " + group_id)
            if holder_member not in accepted:
                raise RunnerError("holder does not accept itself for group " + group_id)
            holders.append((node_id, group, holder, holder_key, holder_member, accepted))
        for holder_node, holder_group, holder, holder_key, holder_member, accepted in holders:
            for node_id, groups in by_node.items():
                if node_id == holder_node:
                    continue
                group = groups[group_id]
                member = runtime_member(group, "nonholder " + node_id + " " + group_id)
                if member == holder_member or member not in accepted:
                    continue
                promise = group.get("promise")
                if not isinstance(promise, dict):
                    continue
                try:
                    promise_request = validate_request_observation(
                        promise.get("request"), group,
                        "promise " + node_id + " " + group_id)
                    promise_key = request_key(
                        promise_request, "promise " + node_id + " " + group_id)
                except RunnerError:
                    continue
                if promise_key != holder_key:
                    continue
                if promise.get("has_record") is not True or \
                        promise.get("active_known") is not True or \
                        promise.get("active") is not True:
                    continue
                candidates.append({
                    "group_id": group_id,
                    "holder_node_id": holder_node,
                    "holder_member_id": holder_member,
                    "holder_request": holder.get("request"),
                    "accepted_voter_ids": sorted(accepted),
                    "nonholder_node_id": node_id,
                    "nonholder_member_id": member,
                    "nonholder_request": promise.get("request"),
                })
    return candidates


def validate_original_holder_live(snapshot, expected_groups, selected_group,
                                  original_request, original_holder_member,
                                  context):
    """Prove the pre-fault holder survived long enough to overlap boot."""
    group = selected_group_record(snapshot, selected_group, expected_groups)
    holder = group.get("holder")
    if not isinstance(holder, dict) or holder.get("available") is not True:
        raise RunnerError(context + " no longer exposes the original holder")
    request = validate_request_binding(holder.get("request"), group, context + ".holder")
    if request_key(request, context + ".holder.request") != request_key(
            original_request, context + ".original.request"):
        raise RunnerError(context + " holder request changed before restart overlap")
    if runtime_member(group, context + ".holder") != original_holder_member:
        raise RunnerError(context + " holder member changed before restart overlap")
    observation = group.get("observation")
    if not isinstance(observation, dict) or observation.get("stable") is not True or \
            observation.get("current_term_committed") is not True:
        raise RunnerError(context + " original holder has no stable committed observation")
    sample = clock_sample(group, context + ".holder")
    expires = _int(holder.get("expires_at_ns"), context + ".holder.expires_at_ns", 1)
    if sample >= expires:
        raise RunnerError(context + " original holder was sampled after its local expiry")
    return {
        "group_id": selected_group, "holder_member_id": original_holder_member,
        "request": request, "sample_ns": sample, "expires_at_ns": expires,
    }


def selected_group_record(snapshot, group_id, expected_groups):
    groups = snapshot_groups(snapshot, expected_groups)
    if group_id not in groups:
        raise RunnerError("snapshot omitted selected group " + group_id)
    return groups[group_id]


def quarantine_fields(group, context):
    promise = group.get("promise")
    if not isinstance(promise, dict):
        raise RunnerError(context + " has no promise state")
    # A freshly configured checked clock legitimately reports an elapsed
    # sample of zero. Keep the lower bound on the deadline while allowing
    # quarantine_at_ns == 0 from the startup cut.
    at_ns = _int(promise.get("quarantine_at_ns"), context + ".quarantine_at_ns", 0)
    until_ns = _int(promise.get("quarantine_until_ns"), context + ".quarantine_until_ns", 1)
    if until_ns <= at_ns:
        raise RunnerError(context + " has a non-positive quarantine interval")
    return promise, at_ns, until_ns


def clock_sample(group, context):
    clock = group.get("clock")
    if not isinstance(clock, dict):
        raise RunnerError(context + " has no clock")
    sample = _int(clock.get("sample_ns"), context + ".clock.sample_ns", 0)
    if clock.get("faulted") is True or clock.get("initialized") is not True:
        raise RunnerError(context + " clock is not initialized and healthy")
    return sample


def tick_quarantine_block(group, context):
    gate = group.get("gate")
    if not isinstance(gate, dict):
        raise RunnerError(context + " has no gate evidence")
    inputs = gate.get("inputs")
    if not isinstance(inputs, list):
        raise RunnerError(context + " gate inputs are missing")
    ticks = [entry for entry in inputs if isinstance(entry, dict) and entry.get("input") == "tick"]
    if len(ticks) != 1:
        raise RunnerError(context + " does not have exactly one tick gate input")
    tick = ticks[0]
    blocked = _int(tick.get("blocked_by_quarantine"), context + ".tick.blocked_by_quarantine", 0)
    timing = tick.get("quarantine_blocked_time")
    if not isinstance(timing, dict) or timing.get("available") is not True:
        raise RunnerError(context + " tick has no quarantine blocked timestamp")
    first = _int(timing.get("first_ns"), context + ".tick.first_ns", 0)
    last = _int(timing.get("last_ns"), context + ".tick.last_ns", 0)
    if last < first:
        raise RunnerError(context + " tick quarantine timestamps regress")
    return {"blocked": blocked, "first_ns": first, "last_ns": last}


def validate_boot_quarantine(snapshot, expected_groups, context,
                             expected_quarantine=None, policy_timing=None):
    """Require every covered group to show a same-boot blocked election tick."""
    groups = snapshot_groups(snapshot, expected_groups)
    records = {}
    for group_id, group in groups.items():
        promise, at_ns, until_ns = quarantine_fields(group, context + " " + group_id)
        sample = clock_sample(group, context + " " + group_id)
        if promise.get("quarantine_configured") is not True:
            raise RunnerError(context + " " + group_id + " quarantine is not configured")
        if promise.get("quarantine_known") is not True or promise.get("quarantine_active") is not True:
            raise RunnerError(context + " " + group_id + " quarantine is not active")
        if promise.get("quarantine_error") is True:
            raise RunnerError(context + " " + group_id + " quarantine has a clock/policy error")
        # A fresh boot must not already carry a newly granted promise. The
        # aggregate round counters below are holder-side; this per-voter check
        # closes the follower-side grant path as well.
        if promise.get("has_record") is not False or promise.get("active") is not False:
            raise RunnerError(context + " " + group_id + " has a voter promise before quarantine expiry")
        if expected_quarantine is not None:
            expected = expected_quarantine.get(group_id)
            if expected is None:
                raise RunnerError(context + " has no startup deadline for " + group_id)
            if (at_ns, until_ns) != (expected["quarantine_at_ns"], expected["quarantine_until_ns"]):
                raise RunnerError(context + " " + group_id + " changed its boot quarantine timestamps")
        if policy_timing is not None and until_ns - at_ns != policy_timing["quarantine_ns"]:
            raise RunnerError(context + " " + group_id + " quarantine duration differs from manifest policy")
        tick = tick_quarantine_block(group, context + " " + group_id)
        if sample >= until_ns:
            raise RunnerError(context + " " + group_id + " was sampled after its quarantine deadline")
        if tick["blocked"] < 1:
            raise RunnerError(context + " " + group_id + " has no blocked quarantine tick")
        if tick["first_ns"] < at_ns or tick["last_ns"] > sample:
            raise RunnerError(context + " " + group_id + " tick timing is outside the same-boot quarantine")
        holder = group.get("holder")
        if not isinstance(holder, dict) or holder.get("available") is True:
            raise RunnerError(context + " " + group_id + " exposes a holder before quarantine expiry")
        records[group_id] = {
            "sample_ns": sample,
            "quarantine_at_ns": at_ns,
            "quarantine_until_ns": until_ns,
            "tick": tick,
            "promise_active": promise.get("active"),
        }
    # Process-wide counters remain a useful corroboration when present, but the
    # contract does not depend on a global flag or global gate timer: every
    # group's blocked tick and unavailable holder are checked above.
    for field in ("read_authority_grants_accepted", "read_authority_rounds_started"):
        if field in snapshot:
            _int(snapshot.get(field), context + "." + field, 0)
            if snapshot[field] != 0:
                raise RunnerError(context + " has " + field + " before quarantine expiry")
    return records


def validate_post_quarantine(snapshot, expected_groups, selected_group, selected_member,
                             max_grant_ns, context):
    groups = snapshot_groups(snapshot, expected_groups)
    group = groups[selected_group]
    sample = clock_sample(group, context + " selected group")
    promise, _, until_ns = quarantine_fields(group, context + " selected group")
    if sample < until_ns:
        raise RunnerError(context + " accepted the restarted voter before its deadline")
    observation = group.get("observation")
    if group.get("observation_available") is not True or not isinstance(observation, dict):
        raise RunnerError(context + " selected group has no accepted observation")
    if observation.get("stable") is not True or observation.get("current_term_committed") is not True:
        raise RunnerError(context + " selected group has not caught up to a stable committed term")
    grant = group.get("promise")
    if not isinstance(grant, dict) or grant.get("has_record") is not True or \
            grant.get("active_known") is not True or grant.get("active") is not True:
        raise RunnerError(context + " restarted voter has no recorded accepted request")
    validate_request_binding(grant.get("request"), group, context + ".selected group")
    holder = group.get("holder")
    # The selected member may be a nonholder, so find the holder cut across the
    # complete set in the caller. This local check only requires a live promise
    # and a post-deadline monotonic sample.
    if promise.get("active_known") is not True or promise.get("active") is not True:
        raise RunnerError(context + " restarted voter promise is inactive")
    promise_until = _int(promise.get("promise_until_ns"),
                         context + ".selected group.promise_until_ns", 1)
    granted_at = promise.get("granted_at_ns")
    if granted_at is None:
        # PromiseBook records only its expiry. A valid grant sets expiry to the
        # same-voter checked sample plus MaxGrant, so subtraction proves the
        # grant sample without comparing clocks between members.
        granted_at = promise_until - max_grant_ns
    granted_at = _int(granted_at, context + ".selected group.granted_at_ns", 0)
    if granted_at < until_ns or granted_at > sample:
        raise RunnerError(context + " accepted the restarted voter before quarantine expiry")
    selected_member = _int(selected_member, context + ".selected_member", 1)
    if runtime_member(group, context + ".selected group") != selected_member:
        raise RunnerError(context + " selected member does not match runtime identity")
    return {
        "group_id": selected_group,
        "member_id": selected_member,
        "sample_ns": sample,
        "quarantine_until_ns": until_ns,
        "promise_request": promise.get("request"),
        "granted_at_ns": granted_at,
        "grant_timing_basis": "explicit granted_at_ns" if "granted_at_ns" in promise else "promise_until_ns - manifest max_grant_ns",
        "holder_available_on_restarted_node": bool(isinstance(holder, dict) and holder.get("available")),
    }


def manifest_node_id(manifest, context):
    groups = manifest.get("groups")
    if not isinstance(groups, list) or not groups:
        raise RunnerError(context + " has no groups")
    nodes = set()
    group_ids = []
    for index, group in enumerate(groups):
        if not isinstance(group, dict):
            raise RunnerError(context + " group " + str(index) + " is not an object")
        route = group.get("route")
        if not isinstance(route, dict):
            raise RunnerError(context + " group " + str(index) + " has no route")
        group_id = _string(route.get("group_id"), context + " group_id")
        if group_id in group_ids:
            raise RunnerError(context + " has duplicate group " + group_id)
        group_ids.append(group_id)
        member_id = _int(route.get("member_id"), context + ".member_id", 1)
        members = group.get("members")
        if not isinstance(members, list):
            raise RunnerError(context + " group " + group_id + " has no members")
        local = [member.get("node_id") for member in members
                 if isinstance(member, dict) and member.get("member_id") == member_id]
        if len(local) != 1 or not isinstance(local[0], str) or len(local[0]) != 32:
            raise RunnerError(context + " group " + group_id + " has no unique local node identity")
        nodes.add(local[0])
    if len(nodes) != 1:
        raise RunnerError(context + " maps to multiple local node identities")
    return next(iter(nodes)), group_ids


def manifest_authority_policy(manifest, context):
    """Return the exact enabled policy timing from a prepared node manifest."""
    policy = manifest.get("read_authority")
    if not isinstance(policy, dict) or policy.get("enabled") is not True:
        raise RunnerError(context + " has no enabled read-authority policy")
    voters = policy.get("voters")
    if not isinstance(voters, list) or sorted(voters) != list(EXPECTED_VOTERS):
        raise RunnerError(context + " does not retain the complete RF3 voter set")
    max_grant_ms = _int(policy.get("max_grant_millis"), context + ".max_grant_millis", 1)
    rate_ppm = _int(policy.get("clock_rate_ppm"), context + ".clock_rate_ppm", 0)
    rounding_ms = _int(policy.get("rounding_margin_millis"), context + ".rounding_margin_millis", 0)
    if rate_ppm >= 1_000_000:
        raise RunnerError(context + " clock rate is outside the conservative bound")
    max_grant_ns = max_grant_ms * 1_000_000
    numerator = 1_000_000 + rate_ppm
    denominator = 1_000_000 - rate_ppm
    # This is the same ceil((D*(1+rho))/(1-rho)) + margin formula used by
    # raftauthority.ReadAuthorityPolicy.QuarantineDuration.  Keep it in the
    # runner only to bind evidence to the retained manifest policy.
    scaled = (max_grant_ns * numerator + denominator - 1) // denominator
    return {
        "max_grant_ns": max_grant_ns,
        "quarantine_ns": scaled + rounding_ms * 1_000_000,
        "policy_version": _int(policy.get("policy_version"), context + ".policy_version", 1),
        "voters": list(voters),
        "policy": policy,
    }


def node_number(path):
    for part in Path(path).parts:
        if part.startswith("node-") and part[5:].isdigit():
            return int(part[5:])
    raise RunnerError("manifest path has no numeric node component: " + str(path))


def parse_process_inventory(inventory):
    for field in ("processes", "executables"):
        record = inventory.get(field)
        if not isinstance(record, dict) or record.get("exit_code") != 0:
            raise RunnerError("authoritative " + field + " inventory is unavailable")
    executable = {}
    for line in inventory.get("executables", {}).get("text", "").splitlines():
        fields = line.split("\t", 1)
        if len(fields) == 2 and fields[0].isdigit():
            executable[int(fields[0])] = fields[1]
    processes = {}
    for line in inventory.get("processes", {}).get("text", "").splitlines():
        fields = line.split(None, 3)
        if len(fields) == 4 and fields[0].isdigit():
            try:
                processes[int(fields[0])] = {
                    "pid": int(fields[0]), "ppid": fields[1],
                    "comm": fields[2], "args": fields[3],
                    "argv": shlex.split(fields[3]),
                }
            except ValueError:
                continue
    return executable, processes


def serve_targets(fixture, destination, inventory):
    targets = fixture.candidate_diagnostic_targets(destination, inventory, EXPECTED_NODES)
    executable, processes = parse_process_inventory(inventory)
    by_pid = {}
    for target in targets:
        pid = target["pid"]
        process = processes.get(pid)
        if executable.get(pid) != "/bench/candidate-vibedb-shard" or process is None:
            raise RunnerError("diagnostic target PID is not an RF3 shard: " + str(pid))
        argv = process["argv"]
        if len(argv) < 2 or argv[0] != "/bench/candidate-vibedb-shard" or argv[1] != "serve-node":
            raise RunnerError("target PID does not run serve-node: " + str(pid))
        if len(argv) != 5 or argv[2] != "-manifest" or argv[4] != "-reload-prepared-groups":
            raise RunnerError("target PID does not use the exact prepared serve-node argv: " + str(pid))
        manifest_path = argv[3]
        if not manifest_path.startswith("/data/") or ".." in Path(manifest_path).parts:
            raise RunnerError("target PID has an invalid manifest path")
        canonical = ["/bench/candidate-vibedb-shard", "serve-node", "-manifest",
                     manifest_path, "-reload-prepared-groups"]
        if argv != canonical:
            raise RunnerError("target PID has an unexpected serve-node argv")
        target = dict(target)
        target["manifest_path"] = manifest_path
        target["manifest_local_path"] = str(
            destination / "published" / "ready" / manifest_path.lstrip("/"))
        manifest_local = Path(target["manifest_local_path"])
        if not manifest_local.is_file():
            raise RunnerError("target manifest was not retained locally: " + manifest_path)
        target["manifest_sha256"] = hashlib.sha256(manifest_local.read_bytes()).hexdigest()
        target["serve_argv"] = canonical
        target["node_number"] = node_number(manifest_path)
        target["process_line"] = process["args"]
        target["ppid"] = process["ppid"]
        by_pid[pid] = target
    if len(by_pid) != EXPECTED_NODES or len({v["node_id"] for v in by_pid.values()}) != EXPECTED_NODES:
        raise RunnerError("serve-node targets do not cover three distinct nodes")
    return sorted(by_pid.values(), key=lambda value: value["node_id"])


def inventory_identity(targets, inventory):
    executable, processes = parse_process_inventory(inventory)
    result = {}
    for target in targets:
        process = processes.get(target["pid"])
        if process is None or executable.get(target["pid"]) != "/bench/candidate-vibedb-shard":
            raise RunnerError("inventory lost target PID " + str(target["pid"]))
        result[target["node_id"]] = {
            "pid": target["pid"],
            "node_id": target["node_id"],
            "manifest_path": target["manifest_path"],
            "serve_argv": list(target["serve_argv"]),
            "process_line": process["args"],
        }
    return result


def validate_other_nodes_unchanged(pre_targets, post_targets, pre_snapshots, post_snapshots, selected_node, expected_groups):
    pre_by_node = {target["node_id"]: target for target in pre_targets}
    post_by_node = {target["node_id"]: target for target in post_targets}
    if set(pre_by_node) != set(post_by_node):
        raise RunnerError("post-restart inventory changed the physical node set")
    unchanged = []
    for node_id in sorted(pre_by_node):
        if node_id == selected_node:
            continue
        pre = pre_by_node[node_id]
        post = post_by_node[node_id]
        if pre["pid"] != post["pid"] or pre["serve_argv"] != post["serve_argv"]:
            raise RunnerError("nonselected node process changed: " + node_id)
        pre_groups = snapshot_groups(pre_snapshots[node_id], expected_groups)
        post_groups = snapshot_groups(post_snapshots[node_id], expected_groups)
        rows = []
        for group_id in expected_groups:
            pre_group = pre_groups[group_id]
            post_group = post_groups[group_id]
            pre_inc = runtime_node_incarnation(pre_group, "pre " + node_id + " " + group_id)
            post_inc = runtime_node_incarnation(post_group, "post " + node_id + " " + group_id)
            pre_store = runtime_store_id(pre_group, "pre " + node_id + " " + group_id)
            post_store = runtime_store_id(post_group, "post " + node_id + " " + group_id)
            if pre_inc != post_inc or pre_store != post_store or \
                    pre_group.get("runtime_identity") != post_group.get("runtime_identity"):
                raise RunnerError("nonselected node runtime identity changed: " + node_id + " " + group_id)
            rows.append({"group_id": group_id, "node_incarnation": post_inc, "store_id": post_store})
        unchanged.append({"node_id": node_id, "pid": pre["pid"], "groups": rows})
    if len(unchanged) != EXPECTED_NODES - 1:
        raise RunnerError("expected exactly two unchanged physical nodes")
    return unchanged


def validate_selected_restart(pre_target, post_target, pre_snapshot, post_snapshot, expected_groups):
    if pre_target.get("serve_argv") != post_target.get("serve_argv") or \
            pre_target.get("manifest_path") != post_target.get("manifest_path") or \
            pre_target.get("executable") != post_target.get("executable"):
        raise RunnerError("selected voter restart changed its prepared serve-node argv")
    if (pre_target.get("manifest_sha256") is not None and
            pre_target.get("manifest_sha256") != post_target.get("manifest_sha256")):
        raise RunnerError("selected voter restart changed its retained manifest bytes")
    if pre_target["pid"] == post_target["pid"]:
        raise RunnerError("selected voter restart reused the old PID")
    pre_groups = snapshot_groups(pre_snapshot, expected_groups)
    post_groups = snapshot_groups(post_snapshot, expected_groups)
    changed = []
    for group_id in expected_groups:
        old = runtime_node_incarnation(pre_groups[group_id], "old selected " + group_id)
        new = runtime_node_incarnation(post_groups[group_id], "new selected " + group_id)
        old_runtime = dict(pre_groups[group_id].get("runtime_identity", {}))
        new_runtime = dict(post_groups[group_id].get("runtime_identity", {}))
        if old_runtime.get("node_incarnation") != old or new_runtime.get("node_incarnation") != new:
            raise RunnerError("selected voter runtime identity is malformed for " + group_id)
        old_runtime.pop("node_incarnation", None)
        new_runtime.pop("node_incarnation", None)
        if old_runtime != new_runtime:
            raise RunnerError("selected voter changed identity fields beyond its boot incarnation for " + group_id)
        if new <= old:
            raise RunnerError("selected voter did not receive a new boot incarnation for " + group_id)
        changed.append({"group_id": group_id, "old": old, "new": new})
    return {"old_pid": pre_target["pid"], "new_pid": post_target["pid"], "groups": changed}


def copy_from_container(fixture, container, remote, local):
    local.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    completed = fixture.run(["docker", "cp", container + ":" + remote, local],
                            check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if completed.returncode:
        raise RunnerError("docker cp failed for " + remote + ": " +
                          completed.stdout.decode(errors="replace"))
    return local


def copy_manifest_and_compare(fixture, container, target, destination, expected_sha256):
    """Hash the actual restart input and compare it with the retained manifest."""
    local = destination / ("restart-node" + str(target["node_number"]) + "-serve-rf3.vibejson")
    copy_from_container(fixture, container, target["manifest_path"], local)
    actual = hashlib.sha256(local.read_bytes()).hexdigest()
    if actual != expected_sha256:
        raise RunnerError("restarted voter loaded manifest bytes different from the prepared root")
    return {"path": target["manifest_path"], "sha256": actual,
            "expected_sha256": expected_sha256, "same_bytes": True}


def validate_runtime_binding(groups, target, expected_groups, context):
    """Bind each diagnostic runtime identity to its prepared node manifest."""
    manifest_path = target.get("manifest_local_path")
    if not isinstance(manifest_path, str):
        raise RunnerError(context + " has no retained local manifest")
    manifest = parse_json(Path(manifest_path))
    manifest_groups = {}
    local_node, manifest_group_ids = manifest_node_id(manifest, manifest_path)
    if local_node != target.get("node_id"):
        raise RunnerError(context + " manifest local node does not match target")
    if manifest_group_ids != expected_groups:
        raise RunnerError(context + " manifest group set changed")
    for item in manifest.get("groups", []):
        route = item["route"]
        manifest_groups[route["group_id"]] = route
    for group_id in expected_groups:
        group = groups[group_id]
        runtime = group.get("runtime_identity")
        route = manifest_groups[group_id]
        if not isinstance(runtime, dict):
            raise RunnerError(context + " " + group_id + " has no runtime identity")
        runtime_group = runtime.get("group")
        if not isinstance(runtime_group, dict):
            raise RunnerError(context + " " + group_id + " has no runtime group identity")
        for key in ("cluster_id", "cluster_incarnation", "topology_recovery_epoch", "shard_incarnation", "group_id"):
            if runtime_group.get(key) != route.get(key):
                raise RunnerError(context + " " + group_id + " changed prepared group identity " + key)
        for key in ("distribution", "shard", "allocation_generation", "member_id", "store_id"):
            if runtime.get(key) != route.get(key):
                raise RunnerError(context + " " + group_id + " changed prepared runtime identity " + key)
        runtime_node_incarnation(group, context + " " + group_id)
        _string(runtime.get("relation_manifest_digest"), context + " " + group_id + ".relation_manifest_digest")
    return True


def validate_snapshot_runtime_bindings(snapshots, targets, expected_groups, context):
    """Bind every current diagnostic cut to its retained serving manifest."""
    target_by_node = {target["node_id"]: target for target in targets}
    if set(snapshots) != set(target_by_node):
        raise RunnerError(context + " does not cover the current serving node set")
    for node_id, target in target_by_node.items():
        groups = snapshot_groups(snapshots[node_id], expected_groups)
        validate_runtime_binding(groups, target, expected_groups, context + " " + node_id)
    return True


def validate_startup_policy(startup, target, expected_groups, policy_rows, context):
    groups = startup_groups(startup, expected_groups)
    rows = policy_rows.get(target["node_id"])
    if not isinstance(rows, list) or len(rows) != len(expected_groups):
        raise RunnerError(context + " has no complete policy marker set")
    for index, group_id in enumerate(expected_groups):
        group = groups[group_id]
        if group.get("policy_version") != rows[index]["policy_version"] or \
                group.get("policy_digest") != rows[index]["policy_digest"]:
            raise RunnerError(context + " " + group_id + " policy does not match marker")


def validate_startup_value(value, target, expected_groups, label, policy_timing=None):
    if value.get("event") != "read_authority_startup":
        raise RunnerError("startup diagnostic is not a read_authority_startup event")
    if _int(value.get("pid"), "startup pid", 2) != target["pid"]:
        raise RunnerError("startup diagnostic PID does not match target")
    groups = startup_groups(value, expected_groups)
    validate_runtime_binding(groups, target, expected_groups, label + " startup")
    for group_id, group in groups.items():
        promise, at_ns, until_ns = quarantine_fields(group, label + " startup " + group_id)
        if promise.get("quarantine_configured") is not True or \
                promise.get("quarantine_known") is not True or promise.get("quarantine_active") is not True:
            raise RunnerError(label + " startup " + group_id + " is not actively quarantined")
        if policy_timing is not None and until_ns - at_ns != policy_timing["quarantine_ns"]:
            raise RunnerError(label + " startup " + group_id + " has the wrong policy quarantine duration")
        clock_sample(group, label + " startup " + group_id)
    return value


def copy_startup(fixture, container, target, destination, label, expected_groups,
                 policy_timing=None):
    local = destination / (label + "-node" + str(target["node_number"]) + ".json")
    copy_from_container(fixture, container, target["path"], local)
    return validate_startup_value(
        parse_json(local), target, expected_groups, label, policy_timing)


def wait_for_startup(fixture, container, target, destination, label, expected_groups,
                     timeout, policy_timing=None, disallowed_pids=()):
    """Wait for this boot's startup cut before the service-ready marker.

    The diagnostic path is installed before startup evidence is emitted, so a
    SIGUSR1 can be queued immediately after this function returns.  The
    previous supervisor's diagnostic file may still be present while the
    independent process is initializing; reject its PID rather than treating
    that stale record as evidence for the new boot.
    """
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)
    local = destination / (label + "-node" + str(target["node_number"]) + ".json")
    deadline = time.monotonic() + timeout
    disallowed = set(disallowed_pids)
    last = None
    while time.monotonic() < deadline:
        copied = fixture.run(["docker", "cp", container + ":" + target["path"], local],
                             check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if copied.returncode == 0:
            try:
                value = parse_json(local)
            except RunnerError:
                value = None
            if value is not None:
                pid = _int(value.get("pid"), label + " startup pid", 2)
                last = value
                if value.get("event") != "read_authority_startup" or pid in disallowed:
                    time.sleep(0.05)
                    continue
                observed = dict(target)
                observed["pid"] = pid
                return observed, validate_startup_value(
                    value, observed, expected_groups, label, policy_timing)
        time.sleep(0.05)
    raise RunnerError("did not capture a fresh startup event before the ready marker for " +
                      label + ": " + repr(last))


def copy_policy_markers(fixture, container, targets, expected_groups, destination, label):
    result = {}
    for target in targets:
        records = []
        for index, _ in enumerate(expected_groups):
            remote = "/data/vibe/node-" + str(target["node_number"]) + "/group-" + str(index) + "/read-authority.state.vibejson"
            local = destination / (label + "-node" + str(target["node_number"]) + "-group" + str(index) + ".json")
            try:
                copy_from_container(fixture, container, remote, local)
                value = parse_json(local)
            except RunnerError:
                raise RunnerError("read-authority policy marker missing: " + remote)
            if value.get("enabled") is not True:
                raise RunnerError("read-authority policy marker is disabled: " + remote)
            _int(value.get("policy_version"), remote + ".policy_version", 1)
            _string(value.get("policy_digest"), remote + ".policy_digest")
            voters = value.get("voters")
            if not isinstance(voters, list) or sorted(voters) != [1, 2, 3]:
                raise RunnerError("read-authority policy marker has unexpected voters: " + remote)
            records.append({
                "remote": remote, "policy_version": value["policy_version"],
                "policy_digest": value["policy_digest"], "voters": list(voters),
                "enabled": value["enabled"],
            })
        result[target["node_id"]] = records
    return result


def same_policy(before, after):
    def normalize(value):
        return {
            node: [(row["policy_version"], row["policy_digest"], tuple(row["voters"]), row["enabled"])
                   for row in rows]
            for node, rows in sorted(value.items())
        }
    return normalize(before) == normalize(after)


def process_pids(inventory, executable):
    executable_map, _ = parse_process_inventory(inventory)
    return sorted(pid for pid, path in executable_map.items() if path == executable)


def wait_pid_gone(fixture, container, pid, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        inventory = fixture.process_inventory(container)
        if pid not in process_pids(inventory, "/bench/candidate-vibedb-shard"):
            return inventory
        time.sleep(0.2)
    raise RunnerError("selected serve-node PID did not exit: " + str(pid))


def validate_final_servers(fixture, container, destination, expected_targets):
    """Require the exact three serving processes to remain alive at completion."""
    inventory = fixture.process_inventory(container)
    if process_pids(inventory, "/bench/candidate-vibedb"):
        raise RunnerError("cluster-dev supervisor reappeared during qualification")
    expected_by_node = {target["node_id"]: target for target in expected_targets}
    expected_pids = {target["pid"] for target in expected_targets}
    actual_pids = process_pids(inventory, "/bench/candidate-vibedb-shard")
    if actual_pids != expected_pids:
        raise RunnerError(
            "serving process inventory changed before qualification completion: " +
            repr({"expected": sorted(expected_pids), "actual": sorted(actual_pids)}))
    observed = serve_targets(fixture, destination, inventory)
    observed_by_node = {target["node_id"]: target for target in observed}
    if set(observed_by_node) != set(expected_by_node):
        raise RunnerError("final serving inventory changed its node set")
    for node_id, expected in expected_by_node.items():
        actual = observed_by_node[node_id]
        for key in ("pid", "manifest_path", "serve_argv", "executable"):
            if actual.get(key) != expected.get(key):
                raise RunnerError("final serving process changed " + key + " on " + node_id)
    return inventory, inventory_identity(observed, inventory)


def stop_supervisor_cleanly(fixture, container, process, destination, timeout):
    inventory = fixture.process_inventory(container)
    supervisors = process_pids(inventory, "/bench/candidate-vibedb")
    if len(supervisors) != 1:
        raise RunnerError("cluster-dev preparation did not have exactly one supervisor")
    pid = next(iter(supervisors))
    fixture.run(["docker", "exec", container, "kill", "-TERM", str(pid)])
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RunnerError("cluster-dev supervisor did not stop cleanly") from exc
    if process.returncode != 0:
        raise RunnerError("cluster-dev supervisor exited abnormally: " + str(process.returncode))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        inventory = fixture.process_inventory(container)
        candidate = process_pids(inventory, "/bench/candidate-vibedb")
        shards = process_pids(inventory, "/bench/candidate-vibedb-shard")
        if not candidate and not shards:
            return {"supervisor_pid": pid, "process_exit_code": process.returncode,
                    "children_stopped": True, "signal": "SIGTERM"}
        time.sleep(0.2)
    raise RunnerError("cluster-dev clean shutdown left a candidate process")


def acknowledged_update_progress(report):
    """Return proof of an acknowledged update plus a later active trial.

    ``rf3-sqlbench`` appends a result only after its operation-level checks and
    trial verification complete. A result and an active trial with a
    different identity therefore give us a compact, report-local proof that a
    mutation was acknowledged before the fault while the workload continued.
    """
    if not isinstance(report, dict):
        return None
    results = report.get("results")
    if not isinstance(results, list):
        return None
    completed = set()
    update_trials = []
    for result in results:
        if not isinstance(result, dict):
            continue
        key = (result.get("workload"), result.get("clients"), result.get("repetition"))
        completed.add(key)
        if (result.get("workload") == "update_existing" and
                result.get("errors") == 0 and result.get("verified") is True):
            operations = result.get("operations")
            if isinstance(operations, bool) or not isinstance(operations, int) or operations < 1:
                continue
            update_trials.append(result)
    if not update_trials:
        return None
    active = report.get("active_trial")
    if not isinstance(active, dict):
        return None
    if (not isinstance(active.get("workload"), str) or
            isinstance(active.get("clients"), bool) or not isinstance(active.get("clients"), int) or
            isinstance(active.get("repetition"), bool) or not isinstance(active.get("repetition"), int)):
        return None
    active_key = (active.get("workload"), active.get("clients"), active.get("repetition"))
    if active_key in completed:
        # This is the short "verifying" window for the completed trial. It
        # does not prove that the workload advanced past the acknowledged
        # mutation.
        return None
    return {
        "acknowledged_update_trials": len(update_trials),
        "acknowledged_update_operations": sum(result["operations"] for result in update_trials),
        "active_trial": dict(active),
    }


def read_remote_report(container, report_remote):
    completed = subprocess.run(["docker", "exec", container, "cat", report_remote],
                               check=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    if completed.returncode != 0:
        return None
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def require_client_active_trial(container, report_remote, client):
    if client.poll() is not None:
        raise RunnerError("SQL workload exited before selected fault: " + str(client.returncode))
    report = read_remote_report(container, report_remote)
    if report is None or not isinstance(report.get("active_trial"), dict):
        raise RunnerError("SQL workload was not active at selected fault")
    return dict(report["active_trial"])


def wait_report_acknowledged_update(container, report_remote, client, timeout):
    """Wait until one verified update is complete and a later trial is active."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if client.poll() is not None:
            raise RunnerError(
                "SQL workload exited before an acknowledged update and later active trial: " +
                str(client.returncode))
        report = read_remote_report(container, report_remote)
        if report is not None:
            last = report
            progress = acknowledged_update_progress(report)
            if progress is not None and client.poll() is None:
                return report, progress
        time.sleep(0.2)
    raise RunnerError(
        "SQL workload did not retain an acknowledged update with a later active trial: " +
        repr(last))


def target_snapshot(fixture, container, target, destination, label, prior_serial=0):
    """Signal one target and wait for its atomic acknowledged snapshot."""
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fixture.run(["docker", "exec", container, "kill", "-USR1", str(target["pid"])])
    deadline = time.monotonic() + 15
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    last = None
    while time.monotonic() < deadline:
        copied = fixture.run(["docker", "cp", container + ":" + target["path"], temporary],
                             check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if copied.returncode == 0:
            try:
                value = parse_json(temporary)
                if (value.get("event") == "snapshot" and value.get("pid") == target["pid"] and
                        value.get("node_id") == target["node_id"] and
                        isinstance(value.get("serial"), int) and value["serial"] > prior_serial):
                    os.replace(temporary, destination)
                    return value
                last = value
            except RunnerError:
                pass
        time.sleep(0.05)
    temporary.unlink(missing_ok=True)
    raise RunnerError("diagnostic snapshot did not acknowledge target " + target["node_id"] +
                      " (prior=" + str(prior_serial) + ", last=" + repr(last) + ")")


def snapshot_all(fixture, container, targets, destination, label, prior=None):
    prior = prior or {}
    values = {}
    for target in targets:
        path = destination / (label + "-node" + str(target["node_number"]) + ".json")
        values[target["node_id"]] = target_snapshot(
            fixture, container, target, path, label, prior.get(target["node_id"], 0))
    return values


def poll_quarantine(fixture, container, target, expected_groups, destination, timeout,
                    startup_quarantine=None, policy_timing=None, prior_serial=0,
                    label="quarantine-early"):
    deadline = time.monotonic() + timeout
    prior = prior_serial
    last = None
    while time.monotonic() < deadline:
        path = destination / (label + "-node" + str(target["node_number"]) + ".json")
        try:
            value = target_snapshot(fixture, container, target, path, label, prior)
            prior = value["serial"]
            last = value
            try:
                expected = startup_quarantine
                return value, validate_boot_quarantine(
                    value, expected_groups, "quarantine early",
                    expected_quarantine=expected, policy_timing=policy_timing)
            except RunnerError:
                pass
        except RunnerError:
            pass
        time.sleep(0.2)
    raise RunnerError("did not capture a same-boot blocked quarantine tick before deadline for " +
                      label + ": " + repr(last))


def client_command(table_names, args, phase, output, oracle):
    url = "postgresql://local@127.0.0.1:5432/vibedb?sslmode=disable"
    command = [
        "/bench/rf3-sqlbench", "-engine", "vibedb", "-url", url, "-urls", url,
        "-rows", str(args.rows), "-operations", str(args.operations),
        "-scans", str(args.scans), "-warmup", str(args.warmup),
        "-repetitions", str(args.repetitions), "-clients", args.clients,
        "-tables", ",".join(table_names), "-workloads", args.workloads,
        "-group-distribution", "uniform", "-skew-percent", "80",
        "-physical-nodes", "3", "-output", output, "-require-existing-tables",
        "-phase", phase,
    ]
    if oracle:
        command.extend(["-recovery-oracle", oracle])
    return command


def validate_run_report(path, args, table_names):
    report = parse_json(path)
    if report.get("status") != "complete" or report.get("verification_error"):
        raise RunnerError("point/update workload report is incomplete or unverified")
    config = report.get("config")
    if not isinstance(config, dict):
        raise RunnerError("point/update workload report has no config")
    for key, value in {
        "Rows": args.rows, "Operations": args.operations, "ScanOperations": args.scans,
        "Warmup": args.warmup, "Repetitions": args.repetitions, "Clients": args.clients,
        "Tables": table_names, "Workloads": [x.strip() for x in args.workloads.split(",") if x.strip()],
        "PhysicalNodes": 3, "EndpointCount": 1, "VerifyEveryTrial": True,
    }.items():
        if config.get(key) != value:
            raise RunnerError("workload report config differs for " + key)
    results = report.get("results")
    if not isinstance(results, list) or not results:
        raise RunnerError("workload report has no completed trials")
    seen = set()
    for result in results:
        if not isinstance(result, dict) or result.get("errors") != 0 or result.get("verified") is not True:
            raise RunnerError("workload report contains an unverified or failed trial")
        operations = result.get("operations")
        if isinstance(operations, bool) or not isinstance(operations, int) or operations < 1:
            raise RunnerError("workload report contains an invalid operation count")
        seen.add(result.get("workload"))
    required = set(x.strip() for x in args.workloads.split(",") if x.strip())
    if not required.issubset(seen):
        raise RunnerError("workload report omitted a required point/update workload")
    return {
        "status": report["status"], "results": len(results),
        "workloads": sorted(seen), "rows": config.get("Rows"),
        "acknowledged_update_trials": sum(
            1 for result in results if result.get("workload") == "update_existing"),
        "acknowledged_update_operations": sum(
            result.get("operations", 0) for result in results
            if result.get("workload") == "update_existing"),
    }


def validate_recovery_report(path):
    report = parse_json(path)
    if report.get("status") != "complete" or report.get("verification_error"):
        raise RunnerError("post-restart recovery report is incomplete")
    return {"status": report["status"], "results": len(report.get("results", []))}


def source_snapshot(fault, fixture, repo, destination, label):
    return fault.source_snapshot(fixture, repo, destination, label)


def build_candidate(fault, fixture, repo, destination, arch):
    gocache = os.environ.get("GOCACHE")
    if not gocache:
        raise RunnerError("run through /Users/thesyncim/.codex/bin/project-env so GOCACHE is scoped")
    fault.GOCACHE = Path(gocache)
    project_env = Path("/Users/thesyncim/.codex/bin/project-env")
    if not project_env.is_file():
        raise RunnerError("project-env wrapper is unavailable")
    original_build = fixture.build_binary

    def build_binary(source, package, output, env):
        build_env = dict(env)
        build_env.setdefault("CODEX_PROJECT_CACHE_ROOT", "/private/tmp/codex-project-cache")
        build_env.setdefault("CODEX_AGENT_ID", "authority-restart")
        fixture.run([project_env, "go", "build", "-mod=readonly", "-trimpath",
                     "-o", output, package], cwd=source, env=build_env)

    fixture.build_binary = build_binary
    try:
        return fault.build_candidate(fixture, repo, destination, arch)
    finally:
        fixture.build_binary = original_build


def copy_ready_manifests(fixture, container, destination, inventory):
    copied = fixture.copy_published_inventories(container, destination, inventory, "ready")
    if copied.get("failed"):
        raise RunnerError("prepared manifests could not all be retained")
    paths = []
    for remote in copied.get("copied", []):
        if re_match_serve_manifest(remote):
            paths.append(remote)
    node_paths = [path for path in paths if "/group-" not in path]
    if len(node_paths) != EXPECTED_NODES:
        raise RunnerError("prepared inventory did not retain exactly three node manifests")
    identities = {}
    expected = None
    policy_timing = None
    policy_fingerprint = None
    for remote in sorted(node_paths):
        local = destination / "published" / "ready" / remote.lstrip("/")
        manifest = parse_json(local)
        node_id, groups = manifest_node_id(manifest, remote)
        timing = manifest_authority_policy(manifest, remote)
        fingerprint = json.dumps(timing["policy"], sort_keys=True, separators=(",", ":"))
        if expected is None:
            expected = groups
            policy_timing = timing
            policy_fingerprint = fingerprint
        elif groups != expected:
            raise RunnerError("node manifests disagree on exact group order")
        elif fingerprint != policy_fingerprint:
            raise RunnerError("node manifests disagree on read-authority policy")
        if node_id in identities:
            raise RunnerError("node manifests duplicate node identity")
        identities[node_id] = {"remote": remote, "groups": groups}
    return {"copied": copied, "expected_groups": expected, "nodes": identities,
            "policy_timing": policy_timing}


def re_match_serve_manifest(path):
    return path.endswith("/serve-rf3.vibejson") and "/node-" in path


def launch_independent(fixture, container, targets, destination, logs, processes, timeout,
                       on_startup=None):
    for target in targets:
        log_path = destination / ("serve-node-" + str(target["node_number"]) + ".log")
        log = log_path.open("wb")
        logs.append(log)
        command = ["docker", "exec", container, *target["serve_argv"]]
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        target["launcher_argv"] = command
        target["log"] = log_path.name
    if on_startup is not None:
        # Startup evidence is emitted before the RF3 ready marker. Capture it
        # (and the first quarantined tick) while the short boot quarantine is
        # still observable; waiting for all ready markers here loses that
        # window on slower Docker hosts.
        for target in targets:
            on_startup(target)
    for target, process in zip(targets, processes[-len(targets):]):
        fixture.wait_for_marker(process, destination / target["log"], ["vibedb-shard RF3 ready"], timeout)
    fixture.wait_for_tcp_ports(container, [5432], timeout)


def stop_independent(fixture, container, processes, destination):
    return fixture.stop_processes(processes, container, destination)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new absolute evidence directory")
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--tables", default=DEFAULT_TABLES)
    parser.add_argument("--rows", type=int, default=1024)
    parser.add_argument("--operations", type=int, default=20000)
    parser.add_argument("--scans", type=int, default=64)
    parser.add_argument("--warmup", type=int, default=500)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--clients", default="1,8")
    parser.add_argument("--workloads", default=DEFAULT_WORKLOADS)
    parser.add_argument("--ready-timeout", type=int, default=180)
    parser.add_argument("--qualification-timeout", type=int, default=180)
    parser.add_argument("--client-timeout", type=int, default=900)
    parser.add_argument("--cpus", default="12")
    parser.add_argument("--memory", default="24g")
    parser.add_argument("--laboratory-read-authority", action="store_true",
                        help="required explicit opt-in for the lab-tagged read-authority binary")
    selected = parser.parse_args(argv)
    if not selected.laboratory_read_authority:
        parser.error("--laboratory-read-authority is required for this qualification")
    if selected.rows < 64 or selected.operations < 8 or selected.scans < 8 or selected.repetitions < 1:
        parser.error("rows/operations/scans/repetitions are too small for the qualification")
    tables = [part.strip() for part in selected.tables.split(",") if part.strip()]
    workloads = [part.strip() for part in selected.workloads.split(",") if part.strip()]
    if not tables or not workloads or not {"point_hit", "point_miss", "update_existing"}.issubset(workloads):
        parser.error("tables and workloads must include point_hit, point_miss, and update_existing")
    if len(workloads) != len(set(workloads)):
        parser.error("workloads must not repeat")
    if workloads.index("update_existing") == len(workloads) - 1:
        parser.error("update_existing must be followed by a later point/read trial")
    repo = selected.repo.resolve()
    fixture = load_fixture()
    fault = load_fault_helpers()
    destination = fixture.require_new_directory(selected.output)
    fixture.COMMAND_LOG = destination / "control-commands.jsonl"
    controls = destination / "controls"
    controls.mkdir(mode=0o700)
    for path in (Path(__file__), FIXTURE_PATH, FAULT_PATH):
        shutil.copy2(path, controls / path.name)
    manifest = {
        "schema": "vibedb.read-authority-restart-qualification/1",
        "status": "preparing", "diagnostic_only": True,
        "qualification_variant": "laboratory-read-authority-single-voter-restart",
        "laboratory_read_authority": True, "build_tag": LAB_BUILD_TAG,
        "runtime_image": RUNTIME,
        "fixture_contract": {
            "physical_nodes": EXPECTED_NODES, "fault_rows": 1,
            "supervisor_role": "prepare-and-seed-only; stopped cleanly before fault",
            "serving_role": "three exact independent serve-node argv from retained manifests",
            "dynamic_schema_append": False, "standard_authority_gate_changed": False,
            "timestamps": "same-boot/member monotonic evidence only; walltime controls orchestration",
        },
        "workload": {
            "tables": tables, "workloads": workloads, "rows": selected.rows,
            "operations": selected.operations, "scans": selected.scans,
            "warmup": selected.warmup, "repetitions": selected.repetitions,
            "clients": selected.clients, "verify_every_trial": True,
            "oracle": "client-side monotonic acknowledged update scores, exported after complete run",
        },
        "source": {}, "binary_sha256": {}, "events": [], "errors": [],
    }
    write_json(destination / "manifest.json", manifest)
    source_before = None
    container = None
    volume = None
    container_created = False
    supervisor = None
    client = None
    processes = []
    logs = []
    targets = []
    selected_fault = None
    try:
        source_before = source_snapshot(fault, fixture, repo, destination, "before")
        manifest["source"] = source_before
        arch = fixture.docker_architecture()
        manifest["docker_architecture"] = arch
        image = fixture.ensure_image(RUNTIME)
        manifest["runtime_image_inspection"] = image
        binaries, metadata, _ = build_candidate(fault, fixture, repo, destination, arch)
        manifest["binary_sha256"] = fixture.binary_hashes(binaries)
        manifest["build_metadata"] = metadata
        container = "vibedb-authority-restart-" + uuid.uuid4().hex[:12]
        volume = container + "-data"
        manifest["container"] = container
        manifest["volume"] = volume
        fixture.run(["docker", "volume", "create", volume])
        fixture.run(["docker", "run", "-d", "--name", container, "--cpus", selected.cpus,
                     "--memory", selected.memory, "--memory-swap", selected.memory,
                     "--mount", "source=" + volume + ",target=/data",
                     "--entrypoint", "sleep", image["Id"], "infinity"])
        container_created = True
        fixture.run(["docker", "cp", str(binaries), container + ":/bench"])
        schema_dir = fixture.schema_files(destination / "schema-input", tables)
        fixture.run(["docker", "cp", str(schema_dir), container + ":/bench/schema"])
        fixture.run(["docker", "exec", container, "mkdir", "-p", "/evidence", "/data/certs"])
        cluster_command = [
            "/bench/candidate-vibedb", "cluster", "dev", "--root", "/data/vibe",
            "--replicas", "3", "--physical-nodes", "3", "--node-log",
            "--pg-listen", "127.0.0.1:5432", "--shard-binary", "/bench/candidate-vibedb-shard",
            "--gateway-binary", "/bench/candidate-vibedb-gateway", "--diagnostics-on-exit",
        ]
        for table in tables:
            cluster_command.extend(["--table-schema", "/bench/schema/" + table + ".sql"])
        cluster_command.append("--read-authority")
        manifest["prepare_seed_argv"] = cluster_command
        prepare_log_path = destination / "cluster-dev-prepare.log"
        prepare_log = prepare_log_path.open("wb")
        logs.append(prepare_log)
        supervisor = subprocess.Popen(["docker", "exec", container, *cluster_command],
                                      stdout=prepare_log, stderr=subprocess.STDOUT)
        processes.append(supervisor)
        fixture.wait_for_marker(supervisor, prepare_log_path,
                                 ["VibeDB development RF3 physical cluster ready:"],
                                 selected.ready_timeout)
        fixture.wait_for_tcp_ports(container, [5432], selected.ready_timeout)
        setup = client_command(tables, selected, "setup", "/evidence/setup.json", None)
        setup_log_path = destination / "setup-client.log"
        with setup_log_path.open("wb") as setup_log:
            setup_result = fixture.run(["docker", "exec", container, *setup],
                                       check=False, stdout=setup_log, stderr=subprocess.STDOUT)
        if setup_result.returncode:
            raise RunnerError("untimed predeclared-schema SQL seed failed")
        manifest["events"].append({"event": "prepared-and-seeded", "utc": utc_now(),
                                   "setup_exit_code": setup_result.returncode,
                                   "schemas": tables})
        ready_inventory = fixture.process_inventory(container)
        fixture.save_inventory(destination, ready_inventory, "cluster-dev-ready")
        prepared = copy_ready_manifests(fixture, container, destination, ready_inventory)
        expected_groups = prepared["expected_groups"]
        policy_timing = prepared["policy_timing"]
        manifest["expected_groups"] = expected_groups
        manifest["prepared_nodes"] = prepared["nodes"]
        manifest["policy_timing"] = {
            "max_grant_ns": policy_timing["max_grant_ns"],
            "quarantine_ns": policy_timing["quarantine_ns"],
            "policy_version": policy_timing["policy_version"],
        }
        prepared_ready_targets = serve_targets(fixture, destination, ready_inventory)
        manifest["policy_before"] = copy_policy_markers(
            fixture, container, prepared_ready_targets,
            expected_groups, destination / "policy", "before")
        stop_record = stop_supervisor_cleanly(
            fixture, container, supervisor, destination, selected.ready_timeout)
        manifest["events"].append({"event": "supervisor-stopped-cleanly", "utc": utc_now(), **stop_record})
        supervisor = None
        # Rebuild targets from the retained exact argv and start the three
        # independent servers. The launch order is deterministic by node ID;
        # the command itself remains the manifest's exact serve-node argv.
        prepared_targets = []
        for node_id, node in sorted(prepared["nodes"].items()):
            manifest_path = node["remote"]
            local_manifest = destination / "published" / "ready" / manifest_path.lstrip("/")
            manifest_value = parse_json(local_manifest)
            prepared_targets.append({
                "node_id": node_id, "node_number": node_number(manifest_path),
                "manifest_path": manifest_path,
                "manifest_local_path": str(local_manifest),
                "path": "/data/vibe/node-" + str(node_number(manifest_path)) + "/rf3-diagnostics.json",
                "executable": "/bench/candidate-vibedb-shard",
                "serve_argv": ["/bench/candidate-vibedb-shard", "serve-node", "-manifest",
                               manifest_path, "-reload-prepared-groups"],
                "manifest_sha256": hashlib.sha256(local_manifest.read_bytes()).hexdigest(),
            })
            # Ensure the retained manifest used for launch remains a canonical
            # prepared node manifest, rather than a synthetic runner fixture.
            manifest_node_id(manifest_value, manifest_path)
        startup_before = {}
        early_snapshots = {}
        early_quarantine = {}
        stale_pids = {target["pid"] for target in prepared_ready_targets}

        def capture_initial_startup(target):
            observed, startup = wait_for_startup(
                fixture, container, target, destination / "startup", "initial",
                expected_groups, selected.ready_timeout, policy_timing,
                disallowed_pids=stale_pids)
            target.update(observed)
            startup_before[target["node_id"]] = startup
            value, proof = poll_quarantine(
                fixture, container, target, expected_groups, destination / "snapshots",
                DEFAULT_QUARANTINE_WAIT,
                startup_quarantine=startup_quarantine_map(startup, expected_groups),
                policy_timing=policy_timing,
                prior_serial=startup.get("serial", 0))
            early_snapshots[target["node_id"]] = value
            early_quarantine[target["node_id"]] = proof

        launch_independent(fixture, container, prepared_targets, destination, logs, processes,
                           selected.ready_timeout, on_startup=capture_initial_startup)
        independent_inventory = fixture.process_inventory(container)
        fixture.save_inventory(destination, independent_inventory, "independent-ready")
        targets = serve_targets(fixture, destination, independent_inventory)
        # Match target PID/path data to our retained manifest list. Startup
        # events and the first quarantine cuts were captured before the ready
        # marker by launch_independent's startup callback.
        by_node = {target["node_id"]: target for target in targets}
        for expected in prepared_targets:
            observed = by_node.get(expected["node_id"])
            if observed is None or observed["manifest_path"] != expected["manifest_path"] or \
                    observed["pid"] != expected.get("pid"):
                raise RunnerError("independent server did not use the prepared manifest")
        manifest["events"].append({"event": "independent-startup-captured", "utc": utc_now(),
                                   "targets": [{k: v for k, v in target.items() if k not in {"log"}}
                                               for target in targets],
                                   "startup_files": sorted(startup_before)})
        validate_snapshot_runtime_bindings(
            early_snapshots, targets, expected_groups, "initial quarantine snapshots")
        write_json(destination / "initial-quarantine.json", early_snapshots)
        manifest["events"].append({"event": "initial-quarantine-blocked", "utc": utc_now(),
                                   "same_boot_evidence": early_quarantine})
        initial_policy = copy_policy_markers(
            fixture, container, targets, expected_groups, destination / "policy", "independent")
        if not same_policy(manifest["policy_before"], initial_policy):
            raise RunnerError("independent startup changed the prepared authority policy")
        for target in targets:
            validate_startup_policy(
                startup_before[target["node_id"]], target, expected_groups,
                initial_policy, "initial startup policy")
        # Start verified point hits/misses and acknowledged updates before grant
        # selection so the row includes an ongoing SQL oracle workload.
        run_command = client_command(tables, selected, "run", "/evidence/run.json",
                                      "/evidence/client-oracle.json")
        manifest["client_argv"] = run_command
        client_log_path = destination / "client-run.log"
        client_log = client_log_path.open("wb")
        logs.append(client_log)
        client = subprocess.Popen(["docker", "exec", container, *run_command],
                                  stdout=client_log, stderr=subprocess.STDOUT)
        processes.append(client)
        _, pre_fault_workload = wait_report_acknowledged_update(
            container, "/evidence/run.json", client, selected.ready_timeout)
        manifest["events"].append({
            "event": "pre-fault-acknowledged-update",
            "utc": utc_now(),
            "proof": pre_fault_workload,
        })
        serials = {node: value.get("serial", 0) for node, value in early_snapshots.items()}
        selection_deadline = time.monotonic() + selected.qualification_timeout
        selected_fault = None
        selection_snapshots = {}
        while time.monotonic() < selection_deadline:
            selection_snapshots = snapshot_all(
                fixture, container, targets, destination / "snapshots", "selection", serials)
            serials = {node: value["serial"] for node, value in selection_snapshots.items()}
            candidates = grant_candidates(selection_snapshots, expected_groups)
            if candidates:
                validate_snapshot_runtime_bindings(
                    selection_snapshots, targets, expected_groups, "pre-fault selection snapshots")
                # Keep node-1 (the SQL endpoint) alive when another accepted
                # voter can be selected. This does not affect the grant proof.
                endpoint_node_id = next(
                    target["node_id"] for target in targets if target["node_number"] == 1)
                candidates.sort(key=lambda row: (
                    row["nonholder_node_id"] == endpoint_node_id, row["group_id"]))
                selected_fault = candidates[0]
                break
            time.sleep(0.25)
        if selected_fault is None:
            raise RunnerError("no exact accepted holder/nonholder request pair appeared")
        target_by_node = {target["node_id"]: target for target in targets}
        fault_target = target_by_node[selected_fault["nonholder_node_id"]]
        holder_target = target_by_node[selected_fault["holder_node_id"]]
        workload_at_fault = require_client_active_trial(
            container, "/evidence/run.json", client)
        pre_identity = inventory_identity(targets, independent_inventory)
        fault_record = {
            "event": "single-voter-restart",
            "group_id": selected_fault["group_id"],
            "holder": {"node_id": holder_target["node_id"], "member_id": selected_fault["holder_member_id"],
                        "pid": holder_target["pid"], "request": selected_fault["holder_request"],
                        "runtime_identity": selected_group_record(
                            selection_snapshots[holder_target["node_id"]],
                            selected_fault["group_id"], expected_groups)["runtime_identity"]},
            "selected_granting_voter": {"node_id": fault_target["node_id"],
                                         "member_id": selected_fault["nonholder_member_id"],
                                         "pid": fault_target["pid"],
                                         "manifest_path": fault_target["manifest_path"],
                                         "serve_argv": fault_target["serve_argv"],
                                         "request": selected_fault["nonholder_request"],
                                         "runtime_identity": selected_group_record(
                                             selection_snapshots[fault_target["node_id"]],
                                             selected_fault["group_id"], expected_groups)["runtime_identity"]},
            "accepted_voter_ids": selected_fault["accepted_voter_ids"],
            "workload_active_at_fault": workload_at_fault,
            "same_boot_pre_fault_snapshots": selection_snapshots,
            "pre_fault_inventory": pre_identity,
            "volume": volume, "durable_root": "/data/vibe",
        }
        manifest["events"].append(fault_record)
        write_json(destination / "pre-fault-selection.json", fault_record)
        # Kill only the accepted nonholder's serving PID. The supervisor is
        # already gone and the two other serving PIDs are never signalled.
        fixture.run(["docker", "exec", container, "kill", "-KILL", str(fault_target["pid"])])
        after_kill = wait_pid_gone(fixture, container, fault_target["pid"], selected.ready_timeout)
        fixture.save_inventory(destination, after_kill, "after-selected-kill")
        remaining = process_pids(after_kill, "/bench/candidate-vibedb-shard")
        if any(target["pid"] not in remaining for target in targets if target["node_id"] != fault_target["node_id"]):
            raise RunnerError("a nonselected serve-node PID exited during the selected fault")
        manifest["events"].append({"event": "selected-voter-killed", "utc": utc_now(),
                                   "pid": fault_target["pid"], "signal": "SIGKILL",
                                   "remaining_shard_pids": sorted(remaining)})
        # Restart with the same argv, same durable volume, and same policy.
        restart_log_path = destination / ("restart-node-" + str(fault_target["node_number"]) + ".log")
        restart_log = restart_log_path.open("wb")
        logs.append(restart_log)
        restart_argv = ["docker", "exec", container, *fault_target["serve_argv"]]
        restarted = subprocess.Popen(restart_argv, stdout=restart_log, stderr=subprocess.STDOUT)
        processes.append(restarted)
        # Startup evidence is emitted after SIGUSR1 registration and before the
        # RF3 ready marker. Use it to identify the replacement PID, then take
        # the overlap and quarantine cuts before gateway recovery/ready checks
        # consume the original holder's short remaining window.
        post_fault_target, startup_after = wait_for_startup(
            fixture, container, fault_target, destination / "startup", "restart",
            expected_groups, selected.ready_timeout, policy_timing,
            disallowed_pids={fault_target["pid"]})
        try:
            holder_after_restart = target_snapshot(
                fixture, container, holder_target,
                destination / "snapshots" / "holder-after-restart.json",
                "holder-after-restart",
                selection_snapshots[holder_target["node_id"]]["serial"])
            validate_snapshot_runtime_bindings(
                {holder_target["node_id"]: holder_after_restart}, [holder_target],
                expected_groups, "holder overlap snapshot")
            holder_overlap = validate_original_holder_live(
                holder_after_restart, expected_groups, selected_fault["group_id"],
                selected_fault["holder_request"], selected_fault["holder_member_id"],
                "holder after restarted voter startup")
        except RunnerError as exc:
            if "no longer exposes the original holder" in str(exc) or \
                    "sampled after its local expiry" in str(exc):
                raise MissedWindowError("original holder overlap was missed: " + str(exc)) from exc
            raise
        restart_quarantine_snapshot, restart_quarantine = poll_quarantine(
            fixture, container, post_fault_target, expected_groups,
            destination / "snapshots", DEFAULT_QUARANTINE_WAIT,
            startup_quarantine=startup_quarantine_map(startup_after, expected_groups),
            policy_timing=policy_timing,
            prior_serial=startup_after.get("serial", 0),
            label="quarantine-restart")
        fixture.wait_for_marker(restarted, restart_log_path, ["vibedb-shard RF3 ready"], selected.ready_timeout)
        fixture.wait_for_tcp_ports(container, [5432], selected.ready_timeout)
        post_inventory = fixture.process_inventory(container)
        fixture.save_inventory(destination, post_inventory, "post-restart-ready")
        post_targets = serve_targets(fixture, destination, post_inventory)
        post_by_node = {target["node_id"]: target for target in post_targets}
        observed_post_fault = post_by_node[fault_target["node_id"]]
        if observed_post_fault["pid"] != post_fault_target["pid"]:
            raise RunnerError("post-restart inventory changed the PID identified by startup evidence")
        post_fault_target = observed_post_fault
        restart_manifest = copy_manifest_and_compare(
            fixture, container, post_fault_target, destination / "published" / "restart",
            fault_target["manifest_sha256"])
        restart_policy = copy_policy_markers(
            fixture, container, post_targets, expected_groups, destination / "policy", "restart")
        if not same_policy(manifest["policy_before"], restart_policy):
            raise RunnerError("restart changed the durable authority policy")
        validate_startup_policy(
            startup_after, post_fault_target, expected_groups,
            restart_policy, "restart startup policy")
        post_serials = {}
        for target in post_targets:
            post_serials[target["node_id"]] = (
                restart_quarantine_snapshot["serial"] if target["node_id"] == fault_target["node_id"] else
                selection_snapshots[target["node_id"]]["serial"])
        post_snapshots = snapshot_all(
            fixture, container, post_targets, destination / "snapshots", "post-restart", post_serials)
        validate_snapshot_runtime_bindings(
            post_snapshots, post_targets, expected_groups, "post-restart snapshots")
        unchanged = validate_other_nodes_unchanged(
            targets, post_targets, selection_snapshots, post_snapshots,
            fault_target["node_id"], expected_groups)
        selected_restart = validate_selected_restart(
            fault_target, post_fault_target,
            selection_snapshots[fault_target["node_id"]],
            post_snapshots[fault_target["node_id"]], expected_groups)
        fault_record["restart"] = {
            "argv": restart_argv, "same_argv": restart_argv[3:] == fault_target["serve_argv"],
            "startup": startup_after, "quarantine_snapshot": restart_quarantine_snapshot,
            "quarantine": restart_quarantine,
            "manifest": restart_manifest,
            "original_holder_overlap": holder_overlap,
            "post_restart_inventory": inventory_identity(post_targets, post_inventory),
            "unchanged_nodes": unchanged, "new_incarnation": selected_restart,
            "policy_same": True,
        }
        manifest["events"].append({"event": "selected-voter-restarted", "utc": utc_now(),
                                   "pid": post_fault_target["pid"], "same_argv": True,
                                   "quarantine": restart_quarantine,
                                   "unchanged_nodes": unchanged,
                                   "new_incarnation": selected_restart})
        # Wait for a new accepted grant that includes this new incarnation. We
        # retain every final cut and require the deadline to have expired in the
        # selected member's monotonic clock before accepting the overlap.
        final_deadline = time.monotonic() + selected.qualification_timeout
        final_serials = {node: value.get("serial", 0) for node, value in post_snapshots.items()}
        final_snapshots = {}
        final_grant = None
        while time.monotonic() < final_deadline:
            final_snapshots = snapshot_all(
                fixture, container, post_targets, destination / "snapshots", "final", final_serials)
            final_serials = {node: value["serial"] for node, value in final_snapshots.items()}
            candidates = [candidate for candidate in grant_candidates(final_snapshots, expected_groups)
                          if candidate["nonholder_node_id"] == fault_target["node_id"] and
                          candidate["nonholder_member_id"] == selected_fault["nonholder_member_id"] and
                          candidate["group_id"] == selected_fault["group_id"]]
            if candidates:
                validate_snapshot_runtime_bindings(
                    final_snapshots, post_targets, expected_groups, "post-quarantine snapshots")
                candidate = candidates[0]
                try:
                    post_proof = validate_post_quarantine(
                        final_snapshots[fault_target["node_id"]], expected_groups,
                        candidate["group_id"], candidate["nonholder_member_id"],
                        policy_timing["max_grant_ns"],
                        "post-quarantine catchup")
                except RunnerError:
                    post_proof = None
                if post_proof is not None:
                    final_grant = candidate
                    break
            time.sleep(0.25)
        if final_grant is None:
            raise MissedWindowError(
                "restarted voter did not participate in a valid post-quarantine grant")
        fault_record["post_quarantine_grant"] = final_grant
        fault_record["post_quarantine_catchup"] = post_proof
        fault_record["post_quarantine_snapshots"] = final_snapshots
        write_json(destination / "post-quarantine.json", fault_record)
        if client is None:
            raise RunnerError("SQL workload process was not started")
        try:
            client.wait(timeout=selected.client_timeout)
        except subprocess.TimeoutExpired as exc:
            raise RunnerError("verified SQL workload exceeded its bounded timeout") from exc
        if client.returncode != 0:
            raise RunnerError("verified SQL workload exited nonzero: " + str(client.returncode))
        run_report_local = destination / "run-report.json"
        copy_from_container(fixture, container, "/evidence/run.json", run_report_local)
        oracle_local = destination / "client-oracle.json"
        copy_from_container(fixture, container, "/evidence/client-oracle.json", oracle_local)
        workload_summary = validate_run_report(run_report_local, selected, tables)
        oracle = parse_json(oracle_local)
        if oracle.get("version") != 1 or oracle.get("engine") != "vibedb" or oracle.get("tables") != tables:
            raise RunnerError("client oracle has the wrong identity")
        if oracle.get("rows") != selected.rows or not isinstance(oracle.get("scores"), list):
            raise RunnerError("client oracle does not retain acknowledged update state")
        recovery_command = client_command(tables, selected, "recovery", "/evidence/recovery.json",
                                           "/evidence/client-oracle.json")
        recovery_log_path = destination / "recovery.log"
        with recovery_log_path.open("wb") as recovery_log:
            recovery = fixture.run(["docker", "exec", container, *recovery_command],
                                   check=False, stdout=recovery_log, stderr=subprocess.STDOUT)
        if recovery.returncode:
            raise RunnerError("post-restart recovery oracle process failed")
        recovery_local = destination / "recovery-report.json"
        copy_from_container(fixture, container, "/evidence/recovery.json", recovery_local)
        recovery_summary = validate_recovery_report(recovery_local)
        final_inventory, final_identity = validate_final_servers(
            fixture, container, destination, post_targets)
        fixture.save_inventory(destination, final_inventory, "qualification-complete")
        fault_record["sql_oracle"] = {"run": workload_summary, "oracle": "client-oracle.json",
                                       "recovery": recovery_summary,
                                       "recovery_argv": recovery_command,
                                       "final_serving_identity": final_identity}
        manifest["fault"] = fault_record
        manifest["status"] = "complete"
    except BaseException as exc:
        manifest["status"] = "missed-window" if isinstance(exc, MissedWindowError) else "incomplete-or-failed"
        manifest["errors"].append(str(exc) or exc.__class__.__name__)
    finally:
        # Stop the client wrapper first, then only candidate serving executables.
        # The supervisor has already been stopped cleanly; it is never restarted
        # or SIGSTOPed by this runner.
        try:
            if client is not None and client.poll() is None:
                client.terminate()
                try:
                    client.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    client.kill()
                    client.wait(timeout=5)
            if container_created and container:
                forced = stop_independent(fixture, container, processes, destination)
                manifest["cleanup_forced_pids"] = forced
                evidence_copy = fixture.run(
                    ["docker", "cp", container + ":/evidence", destination / "raw"],
                    check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if evidence_copy.returncode:
                    raise RunnerError("could not retain container evidence: " +
                                      evidence_copy.stdout.decode(errors="replace"))
                stopped_inventory = fixture.process_inventory(container)
                parse_process_inventory(stopped_inventory)
                fixture.save_inventory(destination, stopped_inventory, "stopped")
        except BaseException as exc:
            manifest["status"] = "incomplete-or-failed"
            manifest["errors"].append("cleanup evidence: " + (str(exc) or exc.__class__.__name__))
        for log in logs:
            try:
                log.close()
            except OSError:
                pass
        if source_before is not None:
            try:
                source_after = source_snapshot(fault, fixture, repo, destination, "after")
                manifest["source_after"] = source_after
                manifest["source_stable"] = (
                    source_before.get("revision") == source_after.get("revision") and
                    source_before.get("patch_sha256") == source_after.get("patch_sha256") and
                    source_before.get("file_sha256") == source_after.get("file_sha256"))
                if not manifest["source_stable"]:
                    manifest["status"] = "incomplete-or-failed"
                    manifest["errors"].append("source changed during qualification")
            except BaseException as exc:
                manifest["status"] = "incomplete-or-failed"
                manifest["errors"].append("source snapshot after run: " + (str(exc) or exc.__class__.__name__))
        cleanup_failures = []
        if container_created and container:
            removed = fixture.run(["docker", "rm", "-f", container], check=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if removed.returncode:
                cleanup_failures.append(
                    "docker rm -f " + container + ": " +
                    removed.stdout.decode(errors="replace"))
        if volume:
            removed = fixture.run(["docker", "volume", "rm", volume], check=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if removed.returncode:
                cleanup_failures.append(
                    "docker volume rm " + volume + ": " +
                    removed.stdout.decode(errors="replace"))
        if cleanup_failures:
            manifest["status"] = "incomplete-or-failed"
            manifest["errors"].extend("cleanup resource: " + value for value in cleanup_failures)
        write_json(destination / "manifest.json", manifest)
    print(destination)
    print(json.dumps({"status": manifest.get("status"), "errors": manifest.get("errors"),
                      "fault": manifest.get("fault")}, sort_keys=True))
    return 0 if manifest.get("status") == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
