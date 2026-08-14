# Distributed sharding and replication

**Status:** future serving design. A bounded non-serving in-process Raft member
runtime, Multi-Raft host, and static post-auth ordinary-message frame/roster
boundary exist, but no peer authenticator, network transport, client routing,
serving replica protocol, distributed ownership, or cross-shard guarantee is
implemented today.

**Idea:** place independent durable collections behind stateless routers, give
each shard one fenced writer authority, and let an adaptive durability
controller choose and safely change its voter count and failure-domain
placement inside an explicit SLO envelope wherever replication is in use.
Keep the strongly consistent topology service out of the steady-state query
path, and move ranges with a copy/catch-up/verify/switch workflow. Scale
readers by adding replicas and writers by adding independently owned shards.

**Decision:** follow the Vitess separation of a cached, strongly consistent
control plane from the data plane, while retaining vibedb's canonical-root
storage model inside every shard. Sharding for write capacity and replication
for availability are separate requirements: a shard needs deterministic
placement, one fenced writer authority, an independently owned physical
partition, and a router, and that minimum does not itself require Raft or any
other replication protocol. This design's qualified default satisfies that
writer authority, and adds optional failover, by replicating each shard with
one embedded Raft group, multiplexed by a Multi-Raft scheduler, using a pinned
and audited version of [`etcd-io/raft`](https://github.com/etcd-io/raft); Raft
is the chosen implementation, not a hard prerequisite for sharding or for
running a shard leader-only. A one-range deployment registers one Runtime and
one group in the same Host path used at larger scale. Range splits create
additional independent groups; they do not extend one global consensus log
across independently writable ranges. The bounded Host schedules only runnable
groups and a dormant range can be removed to reclaim its slot, while topology
authorization and split generation fencing remain later serving contracts. A
shard may instead run leader-only under a
statically configured ownership epoch and no replication protocol at all (see
[Ownership, fencing, and failover](#ownership-fencing-and-failover)), and adopt
Raft later without a placement or routing change. Vibedb implements the
durable log, transport, snapshots, and state-machine integration, not another
consensus algorithm. Do not claim CockroachDB's global transaction or snapshot
contract until the separate gates for those features pass.

The proposed systems contribution is **evidence-carrying elastic
durability**. A planner may optimize voter cardinality and placement, but a
separate deterministic verifier must prove every acknowledgement quorum and
stable/joint Raft configuration against the policy's arbitrary correlated
failure sets. Replicated stable and transition contracts then linearize when a
protection promise changes. Thus the optimizer can be replaced, replayed, or
wrong without becoming the safety authority.

Automatic replica count by itself has prior art; consensus safety is also
deliberately not novel. The research hypothesis is this composition of
per-shard risk/cost optimization, proof-carrying configuration changes,
independent verification, and explicit promise activation over unmodified
Raft membership. It is not a priority claim. The controller must beat
contract-matched fixed and adaptive baselines under the preregistered Phase 6
gate before any novelty or efficiency claim is made.

Where a shard is Raft-replicated, its data Raft group is the only
steady-state write authority; where it instead runs leader-only, the single
fenced writer identified by its `OwnershipEpoch` fills that role. Either way,
the topology service owns range placement, desired membership, workflow
intent, and cached leader hints. For a Raft-replicated shard the topology
service does not mint a competing data-plane term or lease of its own — Raft's
term is already the write fence, and `OwnershipEpoch` is only derived from it.
For a leader-only shard there is no Raft term to derive from, so the topology
service (or static configuration) is exactly what assigns and advances
`OwnershipEpoch` directly, as described in
[Ownership, fencing, and failover](#ownership-fencing-and-failover); that
narrower role is not a second, independent epoch competing with the one
fencing primitive a shard has. Consequently an established RF3 shard can elect
a leader and continue through a topology outage while its data quorum remains
connected, although route, new membership, and resharding changes pause. A
previously committed protection plan may only pause safely or roll forward
from its committed plan revision and replicated state.

## Why this is a separate design

A macro-tablet is an in-file lexical subtree. `TabletID`, `LocalLeafID`, and
`BucketID` name storage inside one `StateRoot`; the proposed
[parallel tablet writers](parallel-tablet-writers.md) share that root and one
publisher. A distributed shard is different:

- it is a placement, replication, ownership, and failure unit;
- it has an independent root, journal, cache, and file lifetime;
- it owns a range in a logical keyspace rather than a subtree reference in
  another shard's root; and
- its failure cannot make another shard's root invalid.

This document therefore reserves *tablet* for the existing in-file structure
and *shard* for the distributed unit. A local tablet split remains one bounded
storage transaction. A distributed shard split is a long-running workflow.

The current authoritative atomic unit is one `durable.Collection.Update`.
Initially a shard is therefore identified by `(keyspace, collection, ShardID)`
and owns one physical collection partition. Co-sharded collections may share
placement, but a transaction that writes more than one collection remains
unsupported until a common shard-group commit protocol lands.

## Contract before mechanism

The first release target is a strong shard-local contract, not a transparent
CockroachDB clone.

| Operation or failure | Initial contract |
| --- | --- |
| point read from leader | current and linearizable with respect to acknowledged writes |
| point or batch write | atomic within one collection shard |
| acknowledged synchronous write | durable on the exact committed Raft configuration and `ProtectionEpoch` returned with the result |
| one crash or permanent disk loss | protection-specific: RF3/RF5 stay writable, RF2 preserves acknowledged data but stops writes, and RF1 may lose data |
| minority network partition | loses write availability; never gains a second leader |
| topology outage | cached routes and established Raft groups continue while their data quorums remain connected; new route and membership plans pause |
| replica read | explicitly stale at an advertised applied `CommitSequence`, or waits for a supplied session token |
| cross-shard read | one pinned local snapshot per shard under one `RoutingVersion`; not initially a common real-time snapshot |
| cross-shard write | rejected before any participant publishes |
| online reshard | copy, catch up, verify, fence, switch, reverse-stream, then retire |
| synchronous replica/profile change | eligible RF1↔RF3↔RF5 changes are autonomously planned inside policy; RF2 is manual; seed and verify asynchronously, reconfigure through Raft, then activate a new protection epoch |
| writer-node addition | split or move ranges to new single-leader shards without making one shard multi-leader |

### Replication profiles

`RF` counts Raft voters including the leader; it does not mean "followers in
addition to the leader." RF1, RF2, RF3, and RF5 are realized protection
states, not necessarily operator-selected static settings. Extra asynchronous
read replicas are Raft learners or independently verified non-voting copies.
They do not count toward RF and never satisfy a write acknowledgement or
election quorum until a committed membership change promotes them.

| Profile | Layout | Successful write | After one data-member loss |
| --- | --- | --- | --- |
| RF1 | one voter and one durable copy total | local log entry and root are durable; zero peer round trips | unavailable if the voter is lost; acknowledged data may be lost |
| RF2 | two voters | both voters durably replicate | acknowledged data remains on one copy, but writes stop until the intact voter returns or explicit unsafe recovery |
| RF3 | three voters | any two voters durably replicate | the remaining two preserve the committed log, elect a leader, and continue |
| RF5 | five voters | any three voters durably replicate | remains writable after two voter losses; domain-loss claims require a policy proof |

RF1 is therefore still supported: it is one member total, commits locally
without a replication network round trip, and has no database failover or
redundancy. Using the same one-voter Raft integration avoids a second storage
protocol and lets the controller strengthen it online when eligible capacity
appears. Leader-only operation is a first-class, indefinitely supportable
deployment shape, not a placeholder: RF1 realizes it by reusing the Raft
integration with one voter, and a shard may equally run leader-only with no
Raft group at all, fenced only by a statically configured `OwnershipEpoch`
(see [Ownership, fencing, and failover](#ownership-fencing-and-failover)).
Both satisfy the same fenced-writer requirement without added redundancy;
only the RF1 path reuses the Raft integration so a later strengthening needs
no separate protocol swap.

Failure claims require synchronous voters in independent declared failure
domains. Two files on one host do not constitute RF2 fault tolerance. Odd
voter counts are preferred because RF4 has the same one-failure write
availability as RF3 with a larger quorum; RF2 remains useful only as an
explicit two-copy, fail-closed point. The first implementation qualifies
RF1/RF2/RF3 before RF5, while its configuration representation and simulator
must not assume three is the maximum.

RF3 is the default production policy floor and the first profile promising
both no acknowledged-write loss and continued write availability after any
one independent data-member failure. RF2 is a lower-cost, fail-closed profile.
RF1 supports development, rebuildable data, bulk load, and explicitly
single-node deployments. A production policy may explicitly run RF1 or RF2,
but RF2 is operator-pinned and manual-only through Phase 6; RF1 may participate
in the separately authorized automatic RF1↔RF3 ladder. Either lower profile is
an explicit lifecycle acceptance of possible retrospective weakening for
existing data, not a hidden controller decision.
Low QPS, old age, or a "cold" label never implies that data is unimportant;
only an authenticated, persisted data-class policy may authorize a weaker
contract.

RF2, RF3, and RF5 use ordinary Raft majority commitment and leader election.
Joint configurations use the selected core's exact incoming-and-outgoing
quorum rules and are exposed as `Transitioning`, never mislabeled as a stable
RF profile. Vibedb
does not reinterpret election, log-matching, leader-completeness, or
configuration-change rules. It treats the selected Raft core as protocol code
and proves that its own persistence, transport, state-machine, and snapshot
integration satisfies that core's contract.

Losing the required data quorum is unavailable rather than silently lossy.
Losing the control-plane quorum pauses route, placement, and membership
changes but does not revoke an established data leader. Byzantine servers, a
compromised topology authority, and correlated loss of every durable copy are
outside the initial fault model.

### Durability Autopilot: evidence-carrying adaptive replication

Applications declare a versioned `DurabilityIntent`; they do not have to pick
one permanent replica count. Policy inheritance may start at cluster,
keyspace, collection, tenant, or range scope, but every shard receives one
fully resolved policy generation. Policy boundaries should align with shard
boundaries. Where they do not, the compiler deterministically takes the
strongest compatible floor and failure set, intersects residency and placement
requirements, and takes the tightest risk/RPO/RTO bound across all data in the
shard. An empty intersection is unsatisfiable: the shard fails closed or the
planner splits at the policy boundary; it never averages conflicting tenant
requirements.

| Intent field | Meaning |
| --- | --- |
| `ContractProfile`, `MaxVoters`, `AllowedTargets` | hard protection floor and cardinality envelope, initially drawn from RF1/RF2/RF3 and later RF5 |
| `MinWriteFaultTolerance` | independent voter failures after which writes must remain available; one requires at least RF3 |
| `RequiredFailureSets` | named correlated host/rack/zone/region scenarios that must retain data or remain writable |
| `MaxModeledAckLossRisk`, `MaxModeledWriteUnavailability` | upper-bound advisory objectives with an explicit horizon and confidence |
| `BackupRPO`, `BackupRTO` | disaster-recovery objectives; backups never count as live voters |
| `MaxRepairExposure`, `MaxTimeToProtect` | repair-window and protection-restoration objectives |
| `MaxCommitP99` | latency objective used only after safety and locality constraints |
| `CostBudget` | storage, transfer, and failure-domain budget |
| `VoterPlacement`, `Residency` | required distinct domains, jurisdictions, media, and capabilities |
| `DegradedWriteAction` | after exogenous loss, `fail-closed` or continue with exact degraded status; never authorizes planned weakening below the floor |
| `MinDwell`, `DownshiftWindow`, `Cooldown` | asymmetric anti-oscillation bounds |
| `RetiredCopyGrace` | how long a removed, tombstoned copy is retained for rollback and forensics |
| `AutomationMode` | `off`, `strengthen-only`, or explicitly authorized `full-elasticity` |

The policy compiler applies this precedence:

| Constraint class | Planner and serving rule |
| --- | --- |
| identity, authorization, residency, encryption, and corruption | absolute; reject or fail closed |
| the active stable contract, or an explicitly authorized active transition contract, plus required failure sets and the active operation epoch | hard; no planned configuration may violate them |
| exogenous member loss while the configured floor still exists | report `DEGRADED`; follow the predeclared `DegradedWriteAction` without changing membership |
| modeled risk, repair, backup, latency, and cost objectives | optimize and report violations; never trade away either hard class |

Modeled probabilities remain advisory until the Phase 6 calibration gate
passes. Even afterward, exceeding a calibrated risk budget can block
weakening or trigger strengthening, but satisfying it never overrides a hard
failure scenario.

The safe production template sets `ContractProfile=RF3`,
`MinWriteFaultTolerance=1`, and `AutomationMode=strengthen-only`; the
controller may choose RF3 or RF5. A single-node elastic template may set
`ContractProfile=RF1`, `MaxVoters=3`, and `full-elasticity`; it starts as
exactly one durable copy and automatically grows when independent stores
become eligible. Automatic downshift below RF3 is disabled unless the resolved
policy explicitly permits it. A manual RF pin is an override implemented by
setting equal floor and ceiling, not a separate mechanism.

The initial automatic transition graph is the availability ladder
`RF1 ↔ RF3 ↔ RF5`; direct RF1↔RF5 edges are rejected. RF2 is an explicitly
manual, fail-closed profile through Phase 6, so the planner never silently
passes through or leaves it. Adding an RF2 automatic edge requires its own
crash matrix and policy semantics before admission.
Each automatic edge seeds every learner first and then uses one qualified,
multi-change `ConfChangeV2` transition pinned to
`ConfChangeTransitionJointExplicit`; implicit/default joint transitions and
`AutoLeave` are forbidden. The accepted plan contains distinct enter-joint and
leave-joint steps. Immediately before each proposal, the executor obtains the
required fresh attestation and places the complete consume-once action binding
directly in `ConfChangeV2.Context`: current `ConfIndex`, expected resulting
`ConfState`, plan step, `OperationEpoch`, `TransitionID`,
`TopologyRecoveryEpoch`, certificate digest, and, for a weakening step, the
full signed permit and nonce. There is no separately banked preauthorization.

At ordered apply, the integration verifies that context against the current
replicated phase/index/epoch. A mismatch is deterministically persisted as an
application no-op and the integration does **not** call `ApplyConfChange`;
an empty `ConfChangeV2` is reserved for the real explicit leave-joint step and
must never represent rejection. A match consumes the nonce and calls
`ApplyConfChange` with the exact committed V2. The rejected/accepted result,
nonce state, resulting `ConfState`, and `AppliedSequence` form one
crash-replayed apply outcome. Every step that weakens the possible
acknowledgement/survivor quorums—including an explicit leave-joint—requires
its own fresh weakening permit.

The edge does not create an intermediate stable RF2 or RF4 configuration. If
the selected Raft core or release cannot execute and pass that exact explicit
joint edge, the controller rejects it instead of decomposing it into
one-member automatic changes.

Raft voters, asynchronous learners, and backup/PITR posture are three separate
control loops. Voters respond to the durability, write-availability, and
repair-exposure intent. Read QPS and read locality add or remove learners.
Write load triggers leader movement, split, or range movement rather than
inflating a consensus quorum. Backups reduce disaster-recovery exposure but
never count as live quorum members.

The controller combines:

- conservative device, host, rack, zone, and region hazard estimates with
  confidence bounds, hardware/rollout cohorts, and explicit
  correlated-failure scenarios;
- current configuration health, repair time, snapshot size, mutation rate,
  repair backlog, and reserved recovery bandwidth;
- verified backup/archive age, restore-time evidence, and corruption/scrub
  state;
- planned maintenance, software-version diversity, topology freshness, and
  failure-domain capacity;
- measured quorum latency and workload forecasts; and
- cost and data-residency constraints.

Unknown or stale risk evidence is pessimistic. A low recent failure count
alone never justifies weakening. For each eligible placement and voter count,
a hard solver expands every declared single or combined
host/rack/zone/region/cohort failure set and enumerates every possible
acknowledgement quorum. Every data-retention scenario must leave at least one
member of every such quorum, and every write-availability scenario must leave
the exact required Raft quorum. The solver also checks signed policy,
infrastructure-attested immutable locality, residency, store eligibility, disk
reserve, and every stable and intermediate `ConfState`. Workload telemetry and
predicted latency cannot satisfy this proof. Only candidates that pass enter
the optimizer.

The optimizer then computes conservative upper bounds for acknowledged-write
loss and write unavailability plus predicted repair exposure,
time-to-protect, backup RPO/RTO, quorum latency, migration cost, and steady
cost. It uses the deterministic lexicographic key
`(hard violation, normalized advisory-budget violation, steady cost,
commit p99, transition bytes, stable placement IDs)`. Thus any candidate
inside every budget is compared on cost before opportunistic extra protection,
and ties replay identically. Modeled probability is an auditable ranking
input, not a consensus proof or a promise that rare failures cannot occur.
Initial releases use it only for ranking; enforcing a probabilistic budget
requires holdout calibration and trace-replay gates.

Each decision has a bounded canonical plan and independently verified
survivability certificate:

```text
ProtectionDecision {
  DecisionSchemaVersion
  ClusterIncarnation, TopologyRecoveryEpoch
  ShardID, ShardIncarnation, and RaftGroupID
  OperationEpoch and AdaptationID
  TransitionKind, ActivePolicyGeneration, optional PendingPolicyGeneration
  active/target policy digests and authorizing principal
  FromProtectionEpoch and FromConfIndex
  TargetVoters and target failure domains
  ordered stable/joint ConfStates
  SurvivabilityCertificateDigest
  EvidenceDigest, estimator version, observation interval, confidence,
  and issuer freshness-attestation digest
  predicted risk/availability/latency/cost bounds
  backup cut, age, and repair-capacity reservation
  reason code and rollout deadline
}

SurvivabilityCertificate {
  CertificateSchemaVersion and VerifierVersion
  ClusterIncarnation, TopologyRecoveryEpoch
  ShardID, ShardIncarnation, RaftGroupID, OperationEpoch, and AdaptationID
  transition kind, active/target policy digests, and signed authorizing principal
  current ConfIndex and ordered candidate ConfStates
  canonical typed retention/write-availability scenario set
  signed StoreID, incarnation, locality, residency, and capability facts
  acknowledgement quorums and survivor result for every state/scenario
  reservation ID and exact topology, incident, and fact revisions
  verifier result and signature
}
```

The hard verifier is a small deterministic component independent from the
planner and risk estimator. It canonicalizes and recomputes the certificate;
it does not trust a planner-asserted `pass`. Ordered apply of
`BeginProtectionChange` runs the pinned verifier on every voter and records a
deterministic accept/reject result before any configuration proposal is
permitted. The executor also verifies policy and certificate signatures,
exact identities and epochs, configuration/topology revisions, store
incarnations, and reservations before begin, every promotion, and activation.
Those later commands carry a signed fact-revision permit that ordered apply
matches to the accepted plan.

A `WeakeningPermit` binds
`(ClusterIncarnation, TopologyRecoveryEpoch, ShardID, ShardIncarnation,
RaftGroupID, OperationEpoch, AdaptationID, ActivePolicyGeneration, optional
PendingPolicyGeneration, active/target policy digests, plan revision,
certificate digest, FromConfIndex, exact topology/incident/fact revisions,
PermitNonce)`. The control plane issues it only after a linearizable freshness
attestation and a new verifier run. Ordered apply verifies the signature and
exact tuple, rejects an already consumed nonce, and records nonce consumption
in the same entry that activates the weaker contract or authorizes the exact
named configuration step. A crash, retry, snapshot install, topology restore,
or group recovery therefore cannot replay it in another state or lineage.

Evidence age, decision deadlines, and signed freshness assertions are checked
before proposal, but no voter reads its local wall clock to decide ordered
apply. The safety decision is deterministic from replicated state and signed
inputs. Wall-clock expiry is only a conservative admission reason to obtain a
new attestation; it is not a divergent state-machine predicate. Topology
unavailability prevents a new attestation and therefore pauses weakening.
Stale evidence never degrades to a probabilistic guess.

VibeTopo stores only the bounded plan, current phase, certificate/evidence
digests, and archive reference. Full inputs, rejected candidates, and model
explanations go to an encrypted append-only audit archive with bounded
retention and decision rate. Before changing membership, the shard commits
that bounded generation-pinned plan and complete survivability certificate in
`BeginProtectionChange`. Its replicated state is either
`Stable(ActivePolicyGeneration, ContractProfile, ProtectionEpoch, EffectiveRF,
ConfIndex)` or
`Transitioning(AdaptationID, TransitionKind, FromPolicyGeneration,
FromContractProfile, TargetPolicyGeneration, TargetContractProfile, FromEpoch,
FromConfIndex, TargetRF, TransitionContractIndex, Phase)`.
`TransitionKind` is one of:

- `REALIZATION`: the active policy generation and contract stay unchanged;
  only the realized `EffectiveRF`/placement and final `ProtectionEpoch` move;
- `POLICY_STRENGTHEN`: an authenticated pending policy raises a hard promise,
  which remains pending until the realized configuration satisfies it; or
- `POLICY_WEAKEN`: an authenticated pending policy relaxes any hard promise
  and must pass the explicit old-quorum transition-contract cut.

Mixed policy changes containing any relaxation use `POLICY_WEAKEN`, but the
pending policy is not activated while one of its tightened hard constraints
is false. Before the weakening cut, the executor realizes every target
residency, placement, failure-set, and minimum-protection tightening while the
old policy and contract remain active, then commits
`PolicyTighteningPrepared` with a verifier proof that the current
configuration satisfies both policies. Target cardinality ceilings are
transition goals rather than a false claim about the still-stronger source
configuration. If the conjunction cannot be realized, the atomic update is
rejected and the policy authority must submit explicit ordered revisions; the
controller never invents them. Once reconfiguration starts, responses expose
`TRANSITIONING`, both source and target state, the active contract, and the
exact `ConfState`. A target `EffectiveRF` is never advertised as stable before
final activation.

Policy activation is replicated and separate from realization. A controller
never creates a policy generation. An authenticated user or policy authority
first commits a changed resolved policy as
`StagePolicy(PendingPolicyGeneration, resolvedIntent, authorization)`.
Ordinary commands carry `ActivePolicyGeneration`, and ordered apply rejects a
different generation. A same-policy `REALIZATION` has
`TargetPolicyGeneration=ActivePolicyGeneration`, no pending generation, and
never changes `ContractProfile`. A policy strengthening leaves the old active
contract advertised while the stricter generation is pending; callers may
require the pending minimum and fail closed until it is ready.

Policy weakening has an earlier, explicit semantic linearization point. Any
mixed tightening must already have its `PolicyTighteningPrepared` proof.
After the write fence and retained-voter cut are applied, but before any
removal or demotion proposal, the healthy old quorum commits and applies
`ActivateWeakeningContract(PendingPolicyGeneration, TargetContractProfile,
AdaptationID, WeakeningPermitDigest)`. Ordered apply reruns the hard verifier
and atomically makes the authorized weaker policy and contract active, clears
the pending generation, records `TransitionContractIndex`, and sets
`ProtectionStatus=TRANSITIONING`; the actual old configuration is still
stronger at this point and writes remain fenced. This is an explicit
under-promise authorized by the old contract's quorum, not a relabel after
members disappear. Every subsequent joint or stable configuration must
satisfy this active transition contract. The activation consumes a fresh
permit, and every later quorum-weakening configuration step, including
explicit leave-joint, consumes its own newly attested, revision-bound permit.

The final `ActivateProtection` is kind-specific. For `REALIZATION`, it keeps
the active policy and contract and installs only the exact stable
`EffectiveRF`/placement. For `POLICY_STRENGTHEN`, it atomically installs the
pending stronger policy and contract. For `POLICY_WEAKEN`, it converts the
already-active transition contract into a stable realized state. Every kind
clears transition state and increments `ProtectionEpoch`.

Stale controllers are rejected, and at most one protection transition and one
unapplied Raft configuration change exist per shard. The exact applied Raft
`ConfState`, including any joint configuration, always defines quorum.
Topology exposes desired and active policy generations separately. A
keyspace-wide policy update is reported fulfilled only after every covered
shard activates it, or returns the explicit set still
`UNSATISFIED_POLICY`/`TRANSITIONING`.

An autonomous realization upshift or policy-strengthening transition:

1. reserves distinct failure-domain capacity and adds never-reused member IDs
   as learners while the old profile remains advertised;
2. installs snapshot plus log tail and verifies every target through a named
   barrier;
3. promotes targets with the Raft core's supported joint or overlapping
   configuration protocol;
4. commits a `ProtectionPrepared` entry under the final configuration and
   waits until every target voter has durably replicated and applied it; and
5. commits `ActivateProtection` under the new quorum and increments
   `ProtectionEpoch`. A realization upshift retains the existing contract and
   exposes the higher exact `EffectiveRF`; a policy strengthening only then
   activates and advertises the stronger contract.

An automatic realization downshift or policy weakening is intentionally
slower and may briefly pause writes. It
requires a healthy old quorum, a long stable evidence window, retained-target
verification, no corruption, lag, ENOSPC, repair, reshard, restore, or feature
finalization, and advance authorization in the active policy. The shard
commits a write fence, waits for every retained voter to apply the same cut
and digest. `POLICY_WEAKEN` then commits `ActivateWeakeningContract` under the
old quorum; `REALIZATION` skips that command because its active floor was
already authorized and remains unchanged. Both consume a fresh permit with
the enter-joint removal proposal and another before any quorum-weakening
explicit leave-joint, change membership through Raft, commit and apply a final
configuration activation, and resume under the new `ProtectionEpoch`.
Removed copies stay tombstoned and non-serving for `RetiredCopyGrace`.
Reducing realized protection is retrospective for existing data, so every
affected protection epoch and any policy revision remain in audit history.

A downshift never reacts to an already failed, corrupt, lagging, or full voter
and is never a way to regain quorum. In particular, RF2 with one voter missing
cannot automatically become RF1. A fenced `REALIZATION` downshift may commit
and apply `AbortProtectionChange` under its unchanged policy, contract, and
`ConfState` only before any removal or demotion `ConfChange` is proposed and
after the Raft core reports no pending configuration change. A
`POLICY_WEAKEN` transition may use that same abort only before
`ActivateWeakeningContract`. After transition-contract activation but before
a removal proposal, cancellation may keep the stronger actual membership only
by committing
`FinalizeWeakeningWithoutMembershipChange`: this makes that exact
configuration stable under the already-active weaker contract, increments
`ProtectionEpoch`, and lifts the fence. Reinstating the old stronger promise
requires a new verified strengthening activation; a restart may not silently
restore it. Once a removal proposal is emitted, recovery rolls forward from
replicated state or performs a later reverse transition. It never relabels
topology. Unsafe forced recovery and cluster-incarnation changes are outside
the controller.

Target failure is explicit transition state, not an instruction to improvise.
Before any configuration proposal, a `REALIZATION` may use its legal abort, a
policy weakening may abort before transition-contract activation, and an
already-activated weaker policy may only finalize at the unchanged stronger
membership. It may then replan. After a proposal, it enters `WaitForQuorum`,
`ReplaceTarget`, `ReverseRequired`, or `UnsafeRecoveryRequired`. Emergency
repair runs as a nested phase under the same `OperationEpoch` and
`AdaptationID`; it is not a conflicting independent workflow. Choosing a new
member requires current quorum, topology availability, a never-reused ID, a
new capacity reservation and survivability certificate, and a replicated
monotonic plan amendment. Without those prerequisites the shard remains
fenced or unavailable. "Roll forward" is a safety direction, not a
termination guarantee.

Strengthening reacts faster than weakening. A cluster-wide scheduler reserves
control traffic and limits concurrent snapshots and membership changes per
store and failure domain. It ranks work by `ProtectionDebt`, the time integral
of a reason-coded debt vector: below-floor deficit, failure-domain survival
margin, rebuild ETA, corruption/scrub debt, backup debt, and capacity debt.
Hard deficits and the smallest survival margin rank first. Capacity pressure
may move or backpressure shards but never lowers RF below policy.
Debt components route to different verified actuators: below-floor or survival
debt seeds voters or moves placement; rebuild debt enters the critical repair
lane; corruption debt scrubs, quarantines, and uses only quorum-certified
sources; backup debt runs backup/PITR work; capacity debt moves, splits, or
requests stores. Backup or corruption debt never directly creates a voter, and
read/write load alone never changes RF.
If no eligible failure domain has capacity, the controller emits a bounded,
reason-coded demand to an optional external provisioner and reports
`UNSATISFIED_POLICY`; it does not count requested or merely registered capacity
until the store is authenticated, healthy, reserved, seeded, and verified.

No new plan starts without a linearizable policy read. During a topology
outage, stable groups keep their current configuration. A strengthening
transition may continue only when the complete target facts, reservation,
certificate, and remaining steps are already committed; otherwise it pauses.
A realization downshift may abort before removal because its contract never
changed. A policy weakening that has not activated its transition contract may
also abort; after that activation but before removal it may only stay fenced
or finalize the weaker promise at the unchanged stronger membership. After a
removal proposal either kind stays fenced and resumes from its signed
replicated plan when its required quorum exists. Only a `ConfChangeV2`
actually proposed before the outage may finish; an unproposed permit is not
banked and must be re-attested after recovery. An explicit leave-joint or
other weakening step that was not yet proposed waits for topology recovery.
Target replacement also waits for topology; the executor never assumes
progress. Metadata mirroring, plan amendment, and physical deletion wait for
topology recovery.
The controller itself is never needed for Raft election or ordinary writes
under an already active protection epoch.

#### Closest prior art and claim boundary

Automatic replica count is not new. The Phase 6 claim is limited to the
combination below and remains a research hypothesis:

| System | Existing contribution | Proposed distinction to evaluate |
| --- | --- | --- |
| Total Recall | maps a user availability target and predicted host availability to adaptive redundancy and repair | linearizable per-shard Raft voters, correlated-domain scenarios, and exact transition contracts rather than peer-to-peer file blocks |
| Tuba | adds, removes, moves, and changes replica roles within application consistency, locality, and cost constraints | hard quorum survivability certificates and Raft configuration activation rather than a broadly tunable geo-replication consistency model |
| Take Me to Your Leader | dynamically optimizes leader, quorum roles, and replica locations for workload latency | changes voter cardinality from a preauthorized protection envelope and accounts for repair/correlated-failure debt |
| Tiger | adapts erasure redundancy to measured device failure rates | replicated-state-machine availability and online membership rather than erasure-code width |

The planner, verifier, policy activation protocol, and evaluation—not the words
"adaptive replication"—must establish whether that combination is useful and
publishable. No "first" claim is made.

### Position relative to Vitess and CockroachDB

The intended split is Vitess-like routing and operations with a stronger,
explicit shard-replication contract. It is not CockroachDB-equivalent SQL.

| Property | Planned vibedb target | Typical Vitess shape | CockroachDB |
| --- | --- | --- | --- |
| routing | stateless cached router plus explicit shard key | VTGate plus Vindex/keyspace ranges | distributed SQL over automatically split ranges |
| write ownership | one fenced writer per shard (a Raft leader by default, or a leader-only writer under a static `OwnershipEpoch`) | one MySQL primary per shard | one leaseholder over a Raft-replicated range |
| acknowledged durability | autonomously realized RF1/RF3/RF5 inside an explicit protection envelope; RF2 manual-only | configured MySQL semi-sync or async replication | Raft quorum under configured survival policy |
| current read | shard leader after `ReadIndex` | primary tablet | leaseholder, with other modes explicitly selected |
| follower read | explicit eventual or `replica-at-least` | explicit tablet type and freshness policy | follower-read contract at a safe timestamp |
| cross-shard read initially | scatter over a declared vector of local cuts | scatter/gather without a universal transaction snapshot | one MVCC timestamp under the transaction contract |
| cross-shard write initially | rejected | optional modes including 2PC, with narrower isolation than serializable | distributed serializable transaction |
| resharding | VReplication-style copy/tail/verify/switch | VReplication workflows | automatic range split and rebalancing |

Exact claims are established by the gates in this document, not inherited
from either comparison system.

### Competitive hypothesis

The plausible winning lane is a locality-heavy workload whose transactions
and exact-index probes include the shard key. Its warm path is one router hop,
one leader, no topology lookup, and the existing canonical local read path;
independent shards can use independent writers. Replica reads can add
throughput when the caller accepts an explicit sequence or staleness contract.

That is a hypothesis, not a result. A synchronous write still pays follower
durability, and a workload dominated by global scans, globally serializable
transactions, or one hot shard key is not the intended advantage. "Beats
CockroachDB" is admissible only for a published matched-contract benchmark;
before Phase 7 it cannot describe the global SQL contract.

### Claims deliberately deferred

The initial contract does **not** claim:

- serializable transactions across shards or collections;
- a globally consistent timestamped snapshot;
- current reads from arbitrary replicas;
- active-active writers for one shard;
- automatic discovery of a correct application shard key;
- global lexical locality after hash-based placement;
- availability in both sides of a network partition;
- that an asynchronous replica protects an acknowledged write;
- a globally ordered change-data-capture stream or cross-shard changefeed; or
- multi-region survival without an explicit placement, latency, and disaster
  recovery contract.

Optional two-phase commit may later provide atomic cross-shard writes, but 2PC
alone does not provide serializable isolation or prevent fractured reads.
Those claims require a separate concurrency-control and retained-history
design.

## Terminology

The names are intentionally distinct from existing uses of generation and
epoch:

| Name | Meaning |
| --- | --- |
| `ClusterIncarnation` | never-reused identity for one whole-cluster data lineage |
| `ShardID` | stable distributed placement identity |
| `ShardIncarnation` | never-reused identity changed by isolated forced recovery or group restore |
| `TopologyRecoveryEpoch` | serving-metadata restore generation; does not rewrite live data identities |
| `RoutingVersion` | linearizable revision of one keyspace's route-manifest snapshot |
| `RouteGeneration` | monotonically increasing lineage version of one route interval |
| `DurabilityIntent` | inherited hard protection envelope plus latency, risk, and cost objectives |
| `ActivePolicyGeneration` | replicated policy generation currently enforced by ordered apply |
| `PendingPolicyGeneration` | staged generation; strengthening activates it at final protection activation, while weakening activates it at the fenced transition-contract cut before removal |
| `ContractProfile` | active stable or transition protection floor; changing it requires an authorized replicated activation |
| `TransitionContractIndex` | Raft index where an authorized weaker contract became active under the old quorum; absent for stable states and ordinary strengthening |
| `TransitionKind` | `REALIZATION`, `POLICY_STRENGTHEN`, or `POLICY_WEAKEN`; prevents an RF realization change from fabricating or implicitly changing policy |
| `ProtectionEpoch` | replicated activation generation for one realized stable protection state |
| `EffectiveRF` | voter count of the exact stable applied `ConfState`; joint states are `Transitioning` |
| `DesiredRF` | controller target from the active bounded plan; never used as current quorum |
| `ProtectionStatus` | `SATISFIED`, `DEGRADED`, `TRANSITIONING`, or `UNSATISFIED_POLICY`, reported independently from quorum availability |
| `ReplicaSetVersion` | Raft log index of the latest applied membership configuration |
| `OperationEpoch` | replicated generation and exclusive mode serializing protection, range, repair, restore, drain, and feature-finalization work |
| `ShardTerm` | externally exposed Raft term; never allocated by the topology service |
| `OwnershipEpoch` | typed write-fencing epoch checked by the shard service on every proposal; derived from `ShardTerm`/`ConfState` when Raft-replicated, or assigned by topology/static configuration for a leader-only shard |
| `CommitSequence` | Raft log index; may advance for no-op and configuration entries |
| `StateRoot.Generation` | local physical publication generation; never a network log position |
| `LastLogIndex` | highest contiguous Raft log index durably present on one member |
| `CommitIndex` | highest log index known committed by the Raft group |
| `AppliedSequence` | highest Raft index applied and published, including no-op and configuration entries that leave the canonical root unchanged |
| `SafeTime` | future timestamp below which a replica promises no earlier write can appear |
| `GCWatermark` | oldest root/log cut that may still be required |

External row identities and continuation state qualify the existing
`(BucketID, slot)` with `ShardID`. `BucketID` remains shard-local.

## Architecture

```text
application
    |
    v
VibeGate (stateless SQL/router/scatter-gather)
    |
    +-- shard A leader --- replica A1
    |                \---- replica A2
    |
    +-- shard B leader --- replica B1
    |                \---- replica B2
    |
    `-- shard C leader --- replica C1
                     \---- replica C2

VibeTopo (linearizable range metadata, desired placement, locks, and watches)
VibeFlow (Durability Autopilot plus copy/verify/switch/repair workflows)
```

The diagram shows RF3 shards. RF2 omits one synchronous replica and RF1 omits
both; RF5 adds two voters. The names describe roles, not required packages or
processes.

### VibeGate

Routers are stateless and horizontally replicated. Each caches:

- collection schemas and shard-key extractors;
- keyspace-ID ranges;
- every route's `RouteGeneration`, Raft group, voters, learners, and leader
  hint;
- `RoutingVersion`, active/pending `DurabilityIntent`, `ProtectionEpoch`,
  `EffectiveRF` or exact transition quorums, `DesiredRF`,
  `ProtectionStatus`, and `ReplicaSetVersion`; and
- query plans for single-shard and scatter execution.

VibeGate's cached "query plans" are routing-time artifacts — which shard or
shards a statement targets, and whether it is single-shard or scatter — not a
serialized distributed execution plan. The wire contract between a gateway
and a shard's leader or eligible replica is SQL text plus typed bound
parameters, the target `ShardID`, `RoutingVersion`, and `OwnershipEpoch`, and
the selected read policy; it never carries a serialized `durable`/planner
execution plan. Each shard parses and plans that SQL locally with vibedb's
ordinary parser and planner, so no second, frozen distributed plan format is
introduced anywhere in this design.

A steady-state single-shard operation performs no topology RPC. A stale route
may reach any cached voter. A non-leader rejects it with `NOT_LEADER` and a
bounded leader hint; a fenced or moved group returns the newer route lineage.
The router refreshes and retries only when the request's idempotency rules
allow it.

### VibeTopo

The first release supports the etcd v3 API through a conformance adapter for
its compare-and-swap, transaction, revision, watch-compaction, snapshot, and
recovery behavior. "etcd-like" is not a portable correctness contract. The
backend contains small, infrequently changing records:

```text
ClusterID
ClusterIncarnation
TopologyRecoveryEpoch
Keyspace {
    RoutingVersion
    schema version
    shard-key function and version
    signed durability-policy tree and generation
    routes[] {
        [lower keyspace ID, upper keyspace ID)
        RouteGeneration
        ShardID
        ShardIncarnation
        RaftGroupID
        ActivePolicyGeneration
        PendingPolicyGeneration
        TransitionKind
        ContractProfile
        TransitionContractIndex
        EffectiveRF
        DesiredRF
        ProtectionStatus
        ProtectionEpoch
        ReplicaSetVersion
        desired voters and learners
        cached leader ID and term hint
        workflow state
    }
}
```

Range descriptors are independently versioned records under one keyspace
revision, not one cluster-wide object. A split transaction compares the exact
source generations and atomically replaces them with bounded child
descriptors; unrelated keyspaces and disjoint workflows need not conflict.
Routers consume checksum-addressed, bounded serving snapshots plus revisioned
deltas. A watch gap, compaction, cancellation, or reconnect forces a
linearizable snapshot reload before later events are applied.

Other backends are future work and must pass the same adapter conformance. The
topology backend's consensus orders placement and route changes. It neither
elects data leaders nor appears in an established shard's write quorum.

No high-frequency load statistic belongs in the authoritative manifest.
Telemetry and scan advertisements are separately replaceable hints.

### VibeFlow

The workflow controller owns resumable operations:

- seed or replace a replica;
- strengthen or weaken a shard's durability profile;
- request a planned Raft leadership transfer;
- split, merge, and move shards;
- verify logical agreement;
- rebuild a lagged replica from a snapshot; and
- retire old data after the rollback and snapshot windows close.

Every transition is persisted before its side effects are considered complete.
Any controller instance may resume an interrupted workflow. Locks serialize
intent but are not part of the data-safety proof; committed Raft entries (or,
for a leader-only shard, its durably applied, `OwnershipEpoch`-fenced
`OperationState` records) and range generations are the durable fences.

Each shard also durably maintains
`OperationState(OperationEpoch, ExclusiveMode, TransitionID)`: replicated
through its Raft group where the shard is Raft-replicated, or persisted
locally under the shard's `OwnershipEpoch`-fenced writer where it is
leader-only. `BeginProtectionChange`, `BeginRangeTransition`, restore, repair,
drain, and feature finalization acquire it by ordered apply-time
compare-and-swap from `Idle`; their configuration changes, fences, imports,
and activation commands carry and recheck the same epoch and transition ID.
Completion or a legal pre-irreversible abort advances the epoch before
returning to `Idle`. Multi-shard workflows acquire groups in deterministic
`ShardID` order and release partial acquisitions before retrying. A topology
lock improves scheduling, but it cannot bypass this interlock.

## Routing and physical order

The routing decision and the local primary order must not be conflated.

### Selected default: hash the locality key

Each sharded collection declares an immutable shard-key extractor. For a
multi-tenant collection the default shape is:

```text
keyspaceID = H(tenantID)
localKey   = encode(tenantID, collection, primaryKey)
```

Shards own contiguous ranges of `keyspaceID`. Hashing distributes sequential
tenant or user identifiers while keeping one tenant's related rows and local
lexical scans together. The complete `localKey` remains authoritative inside
the shard and uses the ordered-tablet graph unchanged.

This choice has an explicit cost: a query without the shard key may contact
every candidate shard, and a global `ORDER BY primaryKey` performs a k-way
merge. It does not preserve the ordered-hybrid store's lexical order across
the complete cluster. The invariant that one shard's scan walks one canonical
representation remains intact; merging independent authoritative shard
streams is not base/delta reconciliation.

### Alternate raw-key range mode

A collection may instead route on the raw primary-key prefix. This preserves
targeted global lexical ranges and permits tablet-aligned movement when route
and tablet fences coincide. It also concentrates monotonically increasing
writes in the rightmost shard. An automatic splitter cannot make one hot key
or one sequential tail execute on multiple leaders.

The two modes are visible schema choices. The system never silently changes a
collection's routing function.

### Shard-key changes

Changing a row's shard key is a delete plus insert across shards. It is
rejected in the initial contract. A later distributed transaction may perform
it atomically; a reshard workflow may rewrite the whole collection under a new
routing function.

## Very large logical tables

One logical SQL table or durable collection may span any number of shards.
Each route owns one disjoint subset of its rows and stores that subset in an
independent physical collection/root; the schema and logical table identity
remain shared. The authoritative routing manifest grows with shard ranges,
not with rows, so a multi-terabyte table does not imply row-level topology
metadata.

The shard key determines whether the largest unit can keep splitting:

| Workload shape | Suitable route | Cost |
| --- | --- | --- |
| many independently sized tenants | `H(tenantID)` | one tenant stays colocated, but an exceptional tenant remains one-leader limited |
| one very large tenant with broad parallel access | a fixed high-cardinality virtual bucket derived from `(tenantID, primaryKey)` | writes spread, but tenant-wide queries fan out across buckets |
| one very large tenant dominated by ordered ranges | raw `(tenantID, primaryKey)` ranges | targeted range scans stay ordered, but a sequential tail may be hot |

Virtual buckets are stable schema identities; physical shard splits move
bucket ranges without rehashing every row. The system never guesses or changes
this choice automatically. A shard key that maps an entire huge tenant to one
value is intentionally unsplittable until an explicit reshard-key migration.

Large-table splits remain online and resumable: pin a source snapshot, copy the
selected ranges, tail logical changes, verify, briefly fence the moving
ranges, and switch one routing version. The workflow may run for hours or
days, but foreground copy/checksum bandwidth, retained-log bytes, and
source-plus-destination disk amplification stay bounded. If catch-up exceeds
those bounds, the destination is reseeded from a newer snapshot rather than
forcing unbounded retention.

A query without a usable shard key scatters under fan-out and memory limits
and merges authoritative local streams. If the exact candidate set exceeds an
admission bound, the query is rejected explicitly rather than returning a
partial result. A bulk mutation spanning shards is decomposed into retry-safe
shard-local batches and is not globally atomic before the
distributed-transaction phase.

## Replicated commit stream

The existing recovery journal is paired to one store, bounded, and recycled
after checkpoint. It may contribute its logical batch-record codec, but it is
not the distributed replica log.

Neither the recovery journal nor a shard's Raft log is the retained stream
that online split/move, backup positioning, and replication catch-up require.
A shard's Raft log, described next, is replication machinery: it is scoped to
one Raft group's lifetime, compacted behind the latest durable snapshot, and
absent entirely for a shard running leader-only without Raft. This design
therefore also needs a separate retained logical change stream, with stable
per-shard commit positions that outlive checkpoint, repack, and Raft log
compaction alike and that exist whether or not Raft replication is enabled for
the shard. It is the source of the change positions consumed by online
reshard (see [Online split, move, and merge](#online-split-move-and-merge))
and by backup/restore; its storage format, retention, and consumer contract
are specified separately and are not satisfied by tailing the Raft log
directly.

Each Raft-replicated shard has a separate bounded Raft write-ahead log with a
different lifetime. Raft owns terms, indexes, commitment, elections, and
configuration entries. A normal state-machine command contains:

```text
ClusterID
ClusterIncarnation
ShardID
ShardIncarnation
ReplicaSetVersion
ActivePolicyGeneration and ProtectionEpoch
client ID and client sequence
canonical request fingerprint
retry-home key
schema, routing, and route generations
ordered Put/Delete batch
command checksum and format version
```

The Raft envelope supplies `ShardTerm` and `CommitSequence`. The sequence is
independent of `StateRoot.Generation`: replicas may checkpoint at different
times and select different physical extents while publishing identical
documents. Configuration and no-op entries consume indexes without mutating
rows, but ordered application still advances and publishes `AppliedSequence`
with an unchanged canonical root.

The log retains an uncompacted suffix after the latest durable snapshot. A
configured byte and record ceiling keeps retention bounded. A lagging learner
or voter is flow-controlled and eventually receives a snapshot; it never
forces unbounded log growth. If a quorum is unavailable, proposal admission
stops before the reserved completion/checkpoint space is consumed.

Each member tracks `LastLogIndex`, `CommitIndex`, and `AppliedSequence`
separately from reader publication state. Uncommitted entries are never
reader-visible. A client result is never successful before its entry is
committed and applied on the responding leader.

### Raft integration and acknowledgement

The selected Raft core is deterministic protocol machinery, not a storage or
transport implementation. For every `Ready` batch, vibedb obeys this order:

1. Validate the request envelope and propose one deterministic command on the
   current leader. State-dependent success is decided by ordered application,
   not by an unreplicated preflight result.
2. Persist new entries, `HardState`, and any snapshot before sending messages
   that depend on them. A follower acknowledges append only after the same
   durability boundary.
3. Send Raft messages and let the core advance `CommitIndex` only under the
   quorum of the exact applied `ConfState`, including both majorities while
   joint consensus is active.
4. Apply committed entries exactly once and in order. The canonical mutation
   and its completion record publish atomically in the local state machine.
5. Return success only after the responding member has applied and published
   the committed entry. A concurrent step-down does not invalidate an already
   committed result.
6. Advance the core only in its required order; report snapshot transfer
   success or failure so a follower cannot remain silently stuck.

RF2 and RF3 acknowledgement therefore covers at least two durable voters, RF5
at least three, and RF1 only the leader. A failure-domain claim additionally
requires the certificate to prove that every possible acknowledgement quorum
spans the named domain combinations; Raft alone proves no such placement fact.
The core may pipeline and batch entries under bounded per-follower windows.
Vibedb never sends a dependent message before the corresponding durable state
and never exposes an uncommitted entry.

A timeout after proposal is an ambiguous client outcome: the entry may commit,
be overwritten while uncommitted, or commit after leadership changes. The
client retries through the replicated completion table rather than reasoning
from the transport error.

Every retryable command carries `(tenant, clientID, clientSequence)` and a
canonical request fingerprint. The same applied state-machine step atomically
records `{status, exact response or durable response-blob reference, digest}`
with the mutation. A repeated ID and matching fingerprint returns that exact
completion; a mismatched fingerprint is rejected. Completion state is included
in snapshots and moves with the request's deterministic retry-home range. A
split retains a source forwarder and transition certificate so an old retry
reaches that home even when re-executing the original batch would now span
shards.

Completion garbage collection requires an authenticated client acknowledgement
watermark or an expiring client epoch checked before execution. After its
declared retention contract expires, the service returns typed
`STALE_REQUEST` or `INDETERMINATE`; it never silently reapplies the request.
A leader-local cache may accelerate lookup but is never authoritative.

### Snapshots and log compaction

A Raft snapshot represents one fully applied committed cut and remains bound
to the same cluster, shard incarnation, group lineage, and `ConfState`. It is
for catch-up and compaction, not a portable backup. Its manifest binds:

```text
ClusterID and ClusterIncarnation
ShardID, ShardIncarnation, and Raft group ID
TopologyRecoveryEpoch and accepted topology signer/trust digest
recovery-manifest digest, operation-rebind records, and old-authority tombstones
last included index and term
exact Raft ConfState and ReplicaSetVersion
schema, routing, and route generations
active/pending durability policies and ContractProfile
complete Stable/Transitioning protection record, plan revision, certificate,
transition-contract index, and consumed permit nonces
ProtectionEpoch, OperationState, write fence, and prepared/activation barriers
canonical root and logical digest
completion table and client GC state
complete Inactive/Active/Frozen/Moved range lineage
permanent fences and move tombstones
import watermarks, retry forwarders, and transition certificates
format versions and encryption key ID
```

Creation stages and verifies the data, fsyncs it, then atomically publishes the
snapshot and manifest. Compaction removes entries at or below the included
index only after that publication is durable; entries after it remain the log
suffix. Installation verifies identity, term/index, configuration, checksums,
and formats, then crash-atomically swaps the state machine. It never regresses
term, vote, commit index, applied index, configuration,
`TopologyRecoveryEpoch`, accepted signer generation, or an authority/rebind
tombstone.

Startup restores snapshot, `HardState`, and suffix before the member may vote
or serve. Missing, rolled-back, or corrupt consensus state is voter amnesia:
the process is quarantined. It may rejoin with a never-reused member ID as a
learner only while the current `ConfState` still has quorum to authorize that
change. Otherwise recovery is the explicitly unsafe new-incarnation path.
Same `(term, index)` with different command bytes is corruption, not a digest
tie-break; the member fails closed.

### Replica application and verification

Replicas replay the ordinary mutation path into their own canonical graph.
They expose:

- durable and applied sequence watermarks;
- the current record-chain digest;
- a logical snapshot digest for verification; and
- replication lag in records, bytes, and wall time.

Snapshot transfer may use physical pages when formats and geometry match, but
logical agreement is authoritative. VDiff-style verification compares sorted
keys, exact values, index results, counts, and checksums at named cuts; root
byte equality is not required.

## Ownership, fencing, and failover

A route version, workflow lock, or owner field stored only in topology is not
data fencing. For a Raft-replicated shard, Raft election, term, log matching,
and quorum commitment are the write fence. VibeTopo remains authoritative for
which `ShardID` owns a key range, while the group's committed range state is
authoritative for whether that shard is `Inactive`, `Active`, `Frozen`, or
`Moved`.

Fencing itself is a typed contract, not only a Raft artifact. Every shard
exposes a monotonically increasing, typed `OwnershipEpoch` that the shard
service checks on every proposal independently of whatever mechanism advances
it. Where a shard is Raft-replicated, `OwnershipEpoch` is derived from, and
advances with, its `ShardTerm`/`ConfState` exactly as this section describes.
Where a shard instead runs leader-only with no replication protocol,
`OwnershipEpoch` is assigned and advanced directly by the topology service or
by static configuration, and the shard service rejects any request carrying a
stale epoch exactly as it would reject a stale Raft term. `OwnershipEpoch` is
therefore the fencing primitive both deployment shapes share; Raft election
and term are one qualified way to produce it, not the definition of it.

The current leader-only server implements only the local half of that future
contract. Before listening, it persists nonzero `OwnershipEpoch` and route
version high-waters beside the immutable shard-store identity and holds one
in-process claim until its connections drain. This prevents a stale restart or
second server over that exact open store. It does not elect an owner, expire a
lease, revoke another process or a copied store, or replace the replicated
authority required for automated failover.

All data-plane servers start non-serving. A voter accepts client-data proposals
only while it is the current Raft leader, its range state is `Active`, and the
request's route generation matches. Ordered apply rechecks the range state,
route generation, and transition ID so a mutation logged behind a
`FreezeRange` becomes a deterministic rejection rather than crossing the
fence. Authenticated control, import, repair, retry-forwarding, and tombstone
commands are separately authorized in the exact `Inactive`, `Active`,
`Frozen`, or `Moved` states their transition permits. Loss and reacquisition
of leadership does not revive an old in-flight invocation. It returns
`NOT_LEADER` or an indeterminate result; any command that committed is
discoverable through the completion table.

RF3 and RF5 voters elect a replacement through Raft when a quorum remains.
RF2 cannot elect or commit after either voter is lost; RF1 cannot fail over.
The topology leader hint may lag without affecting safety. A former leader
cannot commit new writes and cannot pass the linearizable-read barrier.

A planned transfer first catches the target voter up, then uses the Raft
core's leadership-transfer mechanism. Placement is published after the new
leader confirms authority. The optimization may fail or time out; ordinary
Raft election remains the recovery path. No workflow implements a second
promotion or log-adoption algorithm.

### Replica replacement and profile changes

Membership linearizes at a committed and applied Raft configuration entry, not
at a topology compare-and-swap. Multi-change protection edges use the selected
core's documented `ConfChangeTransitionJointExplicit` protocol; they never
use implicit `AutoLeave`. Operator-only one-member changes may use the core's
documented overlapping protocol. Consensus code remains unmodified:

1. While the existing configuration still has quorum, allocate a globally
   unique member ID that will never be reused and add it as a learner.
2. Install and verify a snapshot plus log tail through a named barrier.
3. Promote or remove members through the core's exact configuration proposal.
   Permit only one unapplied configuration change. Explicit enter-joint and
   leave-joint entries each carry the complete consume-once `Context` binding
   above. Report an incoming/outgoing joint configuration as `Transitioning`,
   including its exact quorums.
4. After the final configuration applies, derive `ReplicaSetVersion` from its
   log index. Commit a protection-activation entry under that final quorum and
   wait for every voter needed by the advertised failure contract to apply it.
5. Increment `ProtectionEpoch`, expose the stable `EffectiveRF`, and mirror
   voters, learners, policy, and transition state to topology.
6. Drain removed members and retain tombstones through every read, retry,
   snapshot, transition, backup, rollback, and forensic-retention obligation.

Strengthening a profile advertises the stronger contract only after its new
voters are caught up, verified, committed into the configuration, and have
applied the post-change activation barrier. Weakening requires explicit policy
authorization and exposes each intermediate configuration honestly. A learner
never acknowledges for quorum durability and never campaigns.

Normal transitions require the current configuration's quorum. In particular,
a surviving RF2 voter cannot commit removal of its lost peer or add a
replacement with a new ID. Normal recovery restarts that same voter with its
intact identity, `HardState`, WAL, and snapshot. If those are lost, recovery
invokes an explicitly unsafe disaster-recovery workflow after hard fencing.
Forced quorum recovery creates a new `ShardIncarnation` and group identity,
reports the exact possible data-loss boundary, and permanently rejects old
members and tokens. A full-cluster restore also creates a new
`ClusterIncarnation`. Neither is ordinary failover.

## Read contracts

Read freshness is selected per request rather than inferred from the endpoint:

Stable responses carry active `ContractProfile`, integer `EffectiveRF`,
`ProtectionEpoch`, and `ReplicaSetVersion`. During a joint or weakening
configuration they instead carry `EffectiveRF=null`,
`ProtectionStatus=TRANSITIONING`, the exact incoming and outgoing voter sets
and quorum rules, the active and target contract profiles, and the current
configuration index. A request may set `MinimumContractProfile`; it succeeds
only under a stable, satisfied active contract at least that strong.

| Mode | Source | Contract | Availability |
| --- | --- | --- | --- |
| `leader-current` | Raft leader | current and linearizable within the shard | initial |
| `replica-eventual` | any healthy read replica | latest locally applied root; no time bound | initial |
| `replica-at-least(token)` | replica or leader | `AppliedSequence` is at least the supplied session sequence | initial |
| `replica-bounded(maxAge)` | safe-time-qualified replica | result is no older than the declared wall-time bound | future safe-time phase |
| `as-of(timestamp)` | any replica retaining the required root | one exact timestamped shard snapshot | future safe-time phase |

### Leader-current

The default path uses Raft `ReadIndex` in quorum-safe mode. After the core
confirms the read index, the leader waits until
`AppliedSequence >= readIndex`, serializes with the apply/publication path, and
then reads the canonical root. A leadership belief or topology hint alone is
not a linearizable-read barrier.

A future zero-round-trip optimization may use a Raft quorum-supported leader
lease only with `CheckQuorum`, a proven bound on clock rate and process pause,
and an applied-index barrier captured by that lease. Clock uncertainty,
suspension, or leadership transfer falls back to `ReadIndex` or fails closed.
The topology backend's lease is never substituted for a Raft read lease.

### Eventual and session-consistent replicas

An eventual replica returns its highest fully applied canonical root. It never
serves an uncommitted or committed-but-unapplied tail. This mode may be arbitrarily
stale within operational retention limits; a router's wall-time lag threshold
is a placement policy, not a correctness guarantee.

A write response supplies a session token containing at least
`ClusterIncarnation`, `RoutingVersion`, `RouteGeneration`, `ShardID`,
`ShardIncarnation`, Raft group ID, `CommitSequence`,
`ActivePolicyGeneration`, `ProtectionEpoch`, `EffectiveRF`, and
`ProtectionStatus`, and `ReplicaSetVersion`. `ShardTerm` may be included as
diagnostic evidence but is not a validity equality requirement across leader
changes. A transition token substitutes the exact incoming/outgoing schema
above for `EffectiveRF`. For `replica-at-least(token)`, a replica first
requires the signed
`ClusterIncarnation`, `ShardID`, `ShardIncarnation`, and Raft group ID to match
exactly and the route lineage to remain compatible. It then serves only after
its `AppliedSequence` reaches the token's sequence. An identity or lineage
mismatch requires a valid transition certificate or rejects the token; an
unrelated group with a numerically larger index can never satisfy it.
Read-your-writes therefore selects a sufficiently applied replica, waits
within the caller's deadline, or routes to the leader.

Asynchronous read replicas may be added and removed at runtime after
snapshot-plus-tail verification. They do not change RF and do not protect an
acknowledged write unless a separate membership workflow promotes them into
the synchronous set.

### Session tokens across resharding

The public session token is opaque to callers: on the wire it is
undifferentiated bytes carrying a versioned, internal encoding of the fields
described in [Eventual and session-consistent replicas](#eventual-and-session-consistent-replicas).
Callers store, compare, and return it; they never parse or construct it. A
router that receives a token against a changed `RoutingVersion` does not
locally decide staleness — it forwards the token to the relevant shard
leader for validation or translation. The leader either proves the token's
visibility and serves or translates the request, or returns a typed
`token expired` or `token indeterminate` result; it never silently downgrades
an unprovable token to a stale read.

A split or move cannot discard a token merely because its route acquired a new
`ShardID`. Route activation publishes a durable transition certificate:

```text
source ClusterIncarnation, ShardID, ShardIncarnation, and Raft group ID
source RoutingVersion, RouteGeneration, and final CommitSequence
source final root/log digest and optional evidentiary term
target keyspace range, RouteGeneration, ShardID, ShardIncarnation, Raft group ID
target ImportedThrough(source lineage, source ShardID, source sequence)
target prepared AppliedSequence, root digest, and preparation-certificate hash
expiry
```

For a point read, the router selects the certificate covering the key. For a
range read, it builds the required target vector. A target may satisfy an old
token only when the certificate covers the token's source sequence and proves
that the target's applied activation root includes every source mutation for
its range through that cut. The target must also have locally committed and
applied an `Active` marker with the same transition ID, routing generation,
and preparation-certificate hash before serving. That marker derives its own
applied index; topology never predicts it. Target Raft indexes are unrelated
to source indexes; the `ImportedThrough` proof is the mapping between them,
including for a merge with multiple source logs.

Transition certificates and source cut metadata remain available through the
session-token and retry-deduplication windows. A token that cannot be proven or
has expired returns a typed token-expired/indeterminate result; the router
never silently treats it as an unqualified stale read.

### Future safe-time reads

Timestamp-bounded follower reads and globally consistent snapshots require:

- a leader-chosen hybrid clock timestamp inside the replicated command, with a
  declared maximum clock-offset and uncertainty contract;
- retained root history indexed by commit time;
- a replicated closed-time tuple
  `(timestamp, ShardTerm, CommitSequence, ReplicaSetVersion)` after which
  earlier writes are forbidden;
- transaction visibility rules across participants; and
- a cluster `GCWatermark`.

This is a separate delivery phase. Retaining roots disables otherwise-safe
in-place updates and delays extent reuse, so its write and space cost must be
measured rather than described as free.

`replica-bounded(maxAge)` requires a closed timestamp
`T >= now - maxAge` and waits until the replica has applied through the tuple's
Raft index. A write at or below `T` is rejected or timestamp-forwarded, and
the promise never regresses across leadership changes. `as-of(T)` selects the
latest root at or before `T` only after every involved shard has closed through
`T`. Once 2PC exists, unresolved prepared transactions also constrain safe
time. GC is bounded by active reads, cursors, backups, changefeeds, and
transition certificates, not wall time alone.

## Cross-shard scans

A baseline scatter query captures one immutable `RoutingVersion`, resolves the
intersecting shard set, and asks each shard to pin a local snapshot. Results
are:

- concatenated in route order only when routing order is also query order; or
- merged by the requested logical key otherwise.

The coordinator propagates the selected read mode to every participant.
`leader-current` still means one current cut per shard, not that all cuts
coexisted. `replica-eventual` and session-token reads produce an explicit
vector of potentially different sequences. Only the future common
`as-of(timestamp)` mode claims one distributed read timestamp.

The snapshot vector is stable for the query lifetime but does not claim that
all roots coexisted at one real-time instant. A global-snapshot mode waits for
the future safe-time phase.

The coordinator records the participating route set as
`(keyspace interval, RouteGeneration, ShardID, ShardIncarnation,
ReplicaSetVersion)` entries.
Before it emits a row, every participant must accept its recorded entry. A
`RoutingVersion` change outside those intervals does not invalidate the scan.
If a participating entry changes or any shard reports `MOVED`, the coordinator
releases all pins and restarts the whole query on one newer manifest; it never
combines partial results across different participating route sets. A route
switch keeps already pinned source snapshots readable until their leases
expire.

### Lightweight scan metadata

The route manifest contains only authoritative keyspace bounds. Shards may
advertise counts, index coverage, and zone summaries tagged with
`(ShardTerm, CommitSequence, root digest)`.

A coordinator may prune a shard only when the summary is certified for the
exact pinned cut and proves exclusion. Stale or missing metadata may change
cost and ordering but never the answer. Otherwise the shard is scanned.

### Pagination

A distributed cursor owns:

- cluster incarnation and routing version;
- shard IDs, shard incarnations, and Raft group IDs;
- pinned commit sequences;
- last logical key and tie-breaker;
- sort and predicate identity; and
- an expiry bounded by snapshot lease capacity.

The initial implementation uses server-side leased cursors rather than placing
an unbounded shard vector in a client token. The authoritative owner is one of
a bounded, consistently sharded set of internal RF3 cursor-home Raft groups
running through the same data-plane runtime, never a router or VibeTopo. A
client token contains only the cursor ID, cursor-home group/incarnation,
cursor epoch, next page sequence, query digest, principal binding, and MAC.
Any router forwards it to that group's current leader.

The cursor home commits the route vector and participant pin IDs before the
first row is returned, serializes each page advance through an idempotent
`(CursorID, pageSequence)` command, and retains the bounded response
digest/blob reference until retry expiry. Participant pins are renewable
leases tied to the cursor epoch; partial creation is released by a committed
abort or by lease expiry. Cursor-home leader failure elects normally and
resumes from replicated state. Router failure loses no authority. A
participant outage stalls that page rather than changing its cut, and loss of
the cursor-home quorum returns a typed retry/expiry result rather than
rehoming the same cursor. On expiry, participants release pins and the service
returns a typed restart error. Restarting begins strictly after the last
complete logical key under a fresh route and may only be offered for query
shapes whose duplicate/omission proof is explicit.

Long scans pin old roots and replication log cuts. Admission limits their
count, duration, and retained bytes.

## Online split, move, and merge

The active routing manifest itself remains non-overlapping throughout a
split, move, or merge: at every published `RoutingVersion`, a keyspace point
belongs to exactly one active range. All in-flight migration state — copy/tail
progress, fence and activation markers, and the transition certificates
described below — lives in a separate transition record keyed by
`transitionID`, never as a second, overlapping entry in the manifest. A reader
or router that only ever consults the published manifest therefore cannot
observe two active owners for one key; observing migration progress requires
consulting the transition record explicitly.

Every workflow is a persisted state machine:

```text
Prepare
  -> Copy
  -> Tail
  -> Verify
  -> SwitchReplicaReads
  -> FenceWrites
  -> CatchUp
  -> Activate
  -> Reverse
  -> Retire
```

The steps below are stated over each source and target's retained logical
change stream (its stable commit positions, unaffected by checkpoint, repack,
or Raft-log compaction) and its `OwnershipEpoch`, not over Raft particulars,
so the same workflow applies whether a range is Raft-replicated or
leader-only. Where a step names a "Raft group" or a Raft-derived cut, that is
the Raft-replicated case; a leader-only source or target substitutes its
single fenced writer authority, fenced by `OwnershipEpoch`, and its retained
stream's commit position for the corresponding Raft-group and Raft-index
reference. Neither substitution changes the copy/catch-up/verify/fence/
activate sequence, the transition record, or the certificates it produces.

### Split or move

1. Acquire a topology range intent, then acquire the replicated
   `OperationState` in deterministic shard order and pin its epoch, the
   shard-key extractor, schema, source route generations, and transition ID.
   Conflicting reshard, schema, restore, repair, drain, feature, or protection
   workflows are rejected at ordered apply.
2. Resolve the strongest compatible active policy across each source range.
   Allocate destination shards that preserve both the source's current
   `EffectiveRF` and its failure-domain proof, not merely its lower
   `ContractProfile` — a Raft group where the destination is replicated, or a
   single fenced writer under a freshly allocated `OwnershipEpoch` where it is
   leader-only — and leave their ranges `Inactive`. Any later reduction uses
   the ordinary authorized weakening protocol.
3. Pin a source snapshot at `copySequence` and copy rows selected by the
   destination ranges plus every retry-home completion record, exact response
   or response-blob reference, and client-GC state assigned to those ranges.
4. Stream later commands with durable
   `ImportOrigin=(source lineage, source ShardID, source sequence,
   operation ordinal)`. For every retained-stream commit position after
   `copySequence` and operation ordinal, emit either the relevant mutation or
   an authenticated skip/covered-interval proof, including any no-op and
   configuration entries the stream carries for a Raft-replicated source. A
   target advances its contiguous `ImportedThrough` watermark only after
   complete coverage through that source cut; origin tags also prevent
   reverse replication loops.
5. Verify exact source and target rows, indexes, completion state, and
   retry-home response references at named source cuts.
6. Optionally switch explicitly stale replica reads to exercise targets.
7. Commit and apply `FreezeRange(transitionID, bounds)` at every source —
   through its Raft group where replicated, or directly under the source's
   fenced writer where leader-only. The freeze entry's actual applied commit
   position (the Raft index when replicated, the retained-stream position
   when leader-only) is the final source cut; deterministic apply computes
   its canonical root digest and persists both in the freeze completion and
   transition certificate. The fence is permanent for that lineage and
   rejects stale-route writes even when topology is unavailable.
8. Apply every target through the source final cuts, verify, and commit a
   non-serving prepared-activation record containing those proofs.
9. In one topology transaction, compare the workflow and transition IDs plus
   the exact source route generations, consume a committed `FreezeRange`
   certificate from every source and byte-identical `PreparedActivation`
   proof from every target, and replace the routes with target descriptors
   carrying hashes of those proofs. The transaction yields the new
   `RoutingVersion`.
10. Commit
    `ActivateRange(RoutingVersion, RouteGeneration, transitionID, proofHash)`
    on each target. Ordered apply requires a byte-identical local preparation,
    derives and persists the actual activation index, and rejects any proof or
    generation mismatch. Until every affected target activates, the range may
    be unavailable but never has overlapping writers; sources remain frozen.
11. After all target activation markers are proven, commit
    `MarkMoved(transitionID, RoutingVersion, certificates)` in every frozen
    source. Before it applies, the source returns `FROZEN/RETRY`; afterward it
    returns `MOVED` and forwards retry-home requests through the certificate.
12. Reverse-stream tagged target changes to the moved sources during a bounded
    rollback window.
13. Retire source data only after rollback, snapshot, completion,
    transition, backup, and forensic-retention windows close.

A workflow may cancel only before any source `FreezeRange` commits. Once a
source freezes, recovery rolls forward to target activation. Rollback is then
another complete fenced cutover: freeze targets, drain reverse streams through
named watermarks, verify, compare-and-swap new route generations, activate the
old groups under the new lineage, and publish reverse transition certificates.
It never merely points topology back.

### Merge

A merge uses the same workflow with multiple source ranges and one target.
It is permitted only when combined capacity and load remain under the
post-merge hysteresis floor and the source intents are compatible. The target
uses their strongest combined active policy, greatest effective protection,
and intersection of residency constraints; an unsatisfiable combination
rejects the merge.

### Failure requirements

Fault injection covers a crash or topology outage after every transition and
every external side effect. Resuming the workflow must produce exactly one
write authority and one complete authoritative logical row set for each
range; redundant or rollback copies remain non-serving. Copy and tail
operations are idempotent by their full `ImportOrigin`; key alone is not
unique when one batch touches the same key more than once.

## Automatic placement, protection, and scaling

Automation has distinct actuators. Durability Autopilot changes voters and
failure-domain placement to meet protection intent. A read loop changes
learners. A capacity loop chooses *when* and *where* to split or move after a
human or schema has chosen the routing function. It never treats these as
interchangeable ways to reduce one overloaded metric.

Per-shard telemetry includes:

- live and allocated bytes;
- read, write, and scan CPU;
- QPS and bandwidth;
- p50/p95/p99 latency by operation;
- hot-key and keyspace-ID histograms;
- replica lag and retained log bytes;
- snapshot-pinned retired bytes; and
- disk and cache pressure.

Every store receives a never-reused `StoreID` and process incarnation through
mutually authenticated enrollment. Hierarchical region/zone/rack/host
locality, residency, media, and capability facts are infrastructure-attested
or administrator-approved and signed; changing an immutable hard fact requires
a new incarnation and drain. Self-reported capacity and load may influence
ranking but never prove failure-domain separation. Placement policy declares
voter, learner, and leader constraints plus the minimum separation for each
contract profile. Promotion and activation revalidate the signed facts.
Rebalancing may leave utilization uneven rather than violate survivability or
data-residency policy, and under-protection is reported independently from
current unavailability.

A split requires a sustained threshold, a candidate boundary that improves the
selected objective, sufficient destination capacity, and a cooldown since the
last placement change. The planner estimates the increase in scatter queries
before approving a boundary. Each evidence window is fenced to the exact shard
allocation, routing version, ownership epoch, and a contiguous collector
sequence: replayed or regressed windows are ignored and a missing window starts
a fresh evidence run. Boundary drift is measured from the run's first candidate,
and hot-point isolation requires the same exact point rather than merely the
same histogram bin. Move and merge decisions use separate thresholds to prevent
oscillation.

One hot row, one hot tenant, and a raw-key sequential tail are reported as
unsplittable rather than repeatedly resharded.

### Adding and removing capacity at runtime

Registering a node makes its CPU, disk, and failure domain available to the
placement controller; it does not make that node a concurrent writer for an
existing shard. To add writer capacity, VibeFlow creates destination members
at the source's active policy and current effective protection, seeds and
verifies them, then runs the online split or move workflow. Route activation
gives each destination shard exactly one active fenced writer — a Raft leader
where replicated, a leader-only writer under its own `OwnershipEpoch`
otherwise — so aggregate writer count rises with independently led shards. Durability Autopilot may
subsequently change those groups inside their envelope.

Phase 5 exposes this as an operator-controlled runtime operation. Phase 6 may
trigger it automatically from sustained load and size thresholds. Removing a
writer node first moves every owned shard and drains its replica/cursor/log
obligations; an abrupt unregister never changes membership or transfers
leadership.

Reader scaling is independent: seed an asynchronous replica, catch it up,
verify it, and add it to stale-read routing. Durability Autopilot changes a
shard among qualified voter profiles through the replicated protection
workflow. Every response exposes the current policy generation, protection
epoch, effective RF, and configuration index; neither loop silently changes
acknowledgement semantics.

Runtime writer scaling therefore works for a very large divisible table, but
never turns one indivisible hot shard key into multiple write authorities.

## Indexes, constraints, and transactions

Exact indexes and stable postings remain local to a collection shard. A query
with a shard key uses the local index. Without one, the router scatters the
index probe and merges exact results.

Local uniqueness is only sound if placement makes it global: every unique
key on a distributed table, including its primary key, must contain every
shard-key column. A shard-local unique index can only rule out a duplicate
among the rows it owns; if a unique or primary key omitted a shard-key
column, two rows in different shards could carry the same value and no local
index would ever see the conflict. This uniqueness-locality invariant is
checked when a table's placement is bound and is a precondition for the local
index guarantees above, independent of the excluded global-constraint cases
below.

The initial contract excludes:

- global unique constraints;
- foreign keys crossing shards;
- a lookup index whose row and target update must commit on different shards;
- sequences requiring one globally serialized counter; and
- atomic DDL mixed with data movement.

Small immutable reference collections may be copied to every shard through a
verified workflow. Mutable lookup indexes need either careful ordered locking
with repair or a distributed transaction; they are not ordinary local exact
indexes.

The local SQL layer provides explicit Read Committed, Repeatable Read/Snapshot,
and Serializable modes with first-committer-wins and crash-atomic multi-table
commits inside one database (conditional journal
records decided by one `txn.vtm` sync). Distribution does not upgrade those
semantics to a multi-shard atomic commit. Cross-shard atomicity remains a
separate track; see [multi-table-transactions.md](multi-table-transactions.md)
for the local contract and its named follow-ups.

## Bounded resources and backpressure

Every distributed resource has an open-time or cluster-policy bound:

- router route and plan cache;
- queued/proposed request count and bytes;
- replication log records and bytes;
- Raft peer in-flight records;
- snapshot-transfer bytes and workers;
- workflow concurrency per disk and node;
- deduplication records and retention;
- distributed cursor count and age;
- retained root and retired-extent bytes; and
- telemetry cardinality.

Crossing a bound backpressures, replaces a replica from a snapshot, rejects a
new cursor, or pauses a workflow. It never grows memory or disk without a
declared ceiling.

Admission is hierarchical by node, tenant, shard, and work class and charges
CPU, memory, disk space/I/O, network, fan-out, and retained-history cost.
Capacity is reserved in this order:

1. Raft heartbeats, votes, append completion, apply, and safety fences;
2. quorum-restoring rebuild and quorum-authoritative corruption repair, with a
   protected minimum service rate and bounded maximum share;
3. foreground reads and writes under per-tenant fairness; and
4. scrub, elastic copy, backup, verification, and future CDC work.

Critical repair may preempt elastic work and a bounded share of foreground
capacity, but never consensus traffic or its own verification. This reservation
is sized and fault-tested against the declared repair RTO.

Before proposal, the leader reserves enough log, completion-table, and disk
headroom to finish or safely reject the command. Client cancellation never
abandons a committed entry. Deadlines propagate through scatter and replica
RPCs; expired queued work is rejected before side effects. Typed overload
responses carry pushback, routers enforce retry budgets with jitter, and only
idempotent operations retry automatically. Hard disk watermarks reserve space
for quorum completion, snapshot/checkpoint publication, fencing, and repair.

## Production-readiness foundation

Passing the data protocol is necessary but insufficient. No distributed mode
is production-supported until the following operational contracts pass.
Phase 3 applies them to one shard, including shard backup/PITR and restore.
The keyspace-vector portions become mandatory for routed multi-shard
production in Phase 4.

### Security and tenancy

The shard key's `tenantID` is application data, not an authorization boundary.
The initial release supports one trusted administrative domain. Hostile
multi-tenancy requires a separate gate that derives immutable tenant identity
from the authenticated principal and enforces it in every point/range plan,
index, scatter, cursor, token, workflow, backup, and restore.

All client, router, data-node, workflow, backup, and topology connections use
mutual authentication and encrypted transport. Role policy defaults to deny:
routers cannot mutate topology, learners cannot vote, unauthenticated nodes
cannot join groups, and direct data-node access cannot bypass authorization.
Store-enrollment certificates bind never-reused identity and approved hard
locality/capability facts; self-reported workload telemetry is authenticated
but remains non-authoritative for safety.
Roots, Raft logs, snapshots, copy files, and backups use authenticated
encryption with versioned KMS keys and online rotation.

Session, cursor, completion, and transition tokens are signed, expiring,
tenant/principal-bound, and rotation-aware. Durable audit events cover
authentication and policy changes, membership and route edits, leadership
transfer, forced recovery, backup/restore, reshard cutover, and key rotation.
Row values, credentials, and token material are redacted by default.

### Backup, restore, and disaster recovery

Replication protects member failure; it does not protect operator error,
logical corruption, or total quorum loss. Before production:

- every shard supports full plus bounded incremental backup from one fully
  applied committed cut;
- a backup manifest binds cluster incarnation, shard/range lineage, schema,
  route and configuration versions, durability policy and protection epoch,
  Raft cut, root/log digests, completion state, format versions, encryption
  key IDs, and wrapped DEK metadata, never raw keys;
- a keyspace backup pins one participating route set and records an explicit
  vector of shard cuts; it does not claim a common timestamp before safe time;
- incremental history archives only committed, applied logical entries, never
  a member's uncommitted local WAL suffix; it starts a new full backup when the
  separately bounded remote chain expires;
- portable restore verifies every manifest, row/index digest, route interval,
  and placement constraint, imports logical state into fresh cluster, shard,
  group, and member identities, emits a new Raft genesis snapshot rather than
  rewriting the old consensus snapshot, and remains non-serving until
  complete;
- portable backup preserves the last stable active policy, protection epoch,
  and bounded decision/audit digests. It records in-flight protection state for
  diagnosis but never resumes old member IDs, reservations, permits, or
  transitions; restore recompiles policy and reconciles fresh non-serving
  groups before activation; and
- published RPO/RTO, immutable offsite retention, KMS redundancy, and recurring
  measured full-restore drills are release gates.

Topology, schema, security policy, and data are backed up together but restored
through separate verified steps. Live leaders, leases, cursors, watches, or
in-flight workflow side effects are never revived from backup. Forced
single-shard recovery first hard-fences old endpoints, inventories surviving
cuts, records the exact possible data-loss boundary, requires explicit
authorization, creates a new `ShardIncarnation`, and permanently rejects the
old group. A full-cluster restore additionally creates a new
`ClusterIncarnation`, Raft group/member identities, PKI trust generation, and
serving-endpoint generation. The restored cluster cannot become serving until
the recovery runbook has revoked the old node, client, topology-signer, and
automation credentials; fenced the old data/control endpoints and discovery
records; invalidated old reservations and permits; and verified those fences
from an independent network. If an old authority cannot be fenced, the
restored cluster uses a disjoint endpoint and trust domain and the old endpoint
is treated as still live, not silently superseded.

### Versioning and rolling upgrades

Handshakes and persisted manifests carry `WireVersion`,
`RecordFormatVersion`, `SnapshotFormatVersion`, and supported feature ranges.
New commands or semantics are emitted only after every voter that may
acknowledge or lead can decode and deterministically apply them. Activation is
a committed feature-version entry; unknown safety-critical commands stop a
member rather than being skipped.

The release defines component skew, upgrade order, promotion eligibility, and
rollback/finalization rules. Every persisted workflow state is versioned.
N/N+1 must either resume it at every transition or block upgrade while it is
active. Failover, membership change, snapshot install, resharding, and restore
are tested at each supported mixed-version cut.

The policy compiler, decision, survivability-certificate, evidence, verifier,
and estimator schemas are independently versioned. Exactly one decision and
verifier version is active for new plans. N+1 runs in shadow on recorded N
inputs before activation; voters that cannot decode and validate the complete
protection record are ineligible for promotion. Every transition phase defines
N/N+1 resume behavior, and downgrade is blocked after a newer protection
record or policy feature is finalized.

### Scrubbing and authoritative repair

Reads verify page/record checksums, and background jobs periodically scrub
logical rows, indexes, roots, Raft logs, and snapshots at named committed
cuts. Hierarchical digests localize damage. Automatic voter repair requires a
matching majority of the exact applied `ConfState` at the same cut—two matching
RF3 voters or three matching RF5 voters—or an equivalent snapshot-plus-log
replay proof anchored in such a majority. The controller quarantines and
reseeds only the non-matching member. RF2 disagreement, RF3/RF5 without a
matching majority, voter amnesia, and same-term/index byte disagreement become
unavailable pending explicit recovery; role alone never makes one copy
authoritative.

Snapshot sources carry quorum/configuration proof and remain ineligible until
post-install verification and scrub succeed. RF1 has no independent repair
authority after its only voter is lost; only that identity with intact durable
state or explicit restore/unsafe recovery can continue. Learners, retired
copies, topology role labels, and backup fragments never form a live repair
quorum. Repair has reserved disk and I/O capacity, separate throttles, and
bounded forensic retention for corrupt artifacts.

### Control-plane recovery and observability

Topology-only restore advances `TopologyRecoveryEpoch` without changing
`ClusterIncarnation` or any live `ShardIncarnation`. It invalidates every watch
and route cache, rotates the topology signer, revokes the old signer and
automation credentials, and invalidates every old reservation, certificate,
fact attestation, and permit. Established data serving may continue during
reconciliation, but control changes remain frozen.

The recovery inventory happens before a group adopts the new epoch. A stable
group, or a reversible transition, first remains stable or takes its legal
old-epoch abort/finalization. An irreversible transition—an emitted membership
change, activated weakening contract, frozen range, prepared target, or
committed route switch—must either finish its already-accepted replicated plan
before epoch adoption or commit
`AdoptTopologyRecoveryEpochAndRebind(newEpoch, newSignerDigest,
recoveryManifestDigest, TransitionID, oldPlanDigest, oldProofDigests,
exactPhase)`. That command requires the current data quorum and offline
recovery-root authorization. Ordered apply reruns the pinned verifier and
accepts it only when it changes no policy, contract, voter/range target, cut,
configuration, or safety decision; it merely binds the exact replicated
operation and its continuation proofs to the new epoch and consumes the old
authority. Multi-shard rebinds acquire groups in deterministic order and the
recovery manifest becomes publishable only after every participant records
the same transition digest. If the proof or quorum is missing, the operation
stays fenced or unavailable.

Before accepting any new route or membership command, every published live
group commits either the plain adoption command while stable or that atomic
adopt-and-rebind command. Ordered apply then rejects every control command,
route proof, or permit with the old epoch or signer.

The recovered topology inventories each Raft group's committed membership,
range lineage, `Inactive/Active/Frozen/Moved` state, protection state,
transition certificates, and workflow state. Topology publication resumes only
after that inventory proves exactly-once route coverage, every published group
has adopted the new epoch, old topology endpoints and cached discovery records
are fenced, and any prepared or frozen transition is completed or carries its
verified rebind proof; `ConfState` alone is insufficient. Topology restore is
injected before and after every protection, membership, freeze, prepare,
route-switch, activation, and move-marker cut. A full-cluster data restore
instead creates fresh cluster, shard, group, member, PKI, and endpoint
identities.
Backend leader failure, watch compaction/gaps, quota/ENOSPC, bounded
serving-snapshot size, and snapshot restore are conformance tests at the
declared maximum shard count.

Bounded aggregate metrics expose contract/effective/desired RF counts,
protection status and reason, time below intent, debt by component, evidence
freshness/confidence, transition phase/age, bytes remaining/ETA, cooldown,
budget saturation, calibration drift, and change success/abort/reverse counts.
General metrics never label by `ShardID`, `AdaptationID`, tenant, or request.
The explain/status API and bounded-retention audit events provide per-shard
active/pending policy, survival margin by domain, chosen/rejected candidates,
certificate/verifier versions, configuration index, plan revision, and
reservation; structured logs and sampled traces carry correlation IDs.

Release alerts and tested runbooks cover under-protection, no admissible
placement, policy/`ConfState` mismatch, stale evidence, stuck or repeatedly
churning transitions, repair-SLO breach, leader loss, commit-to-apply lag,
digest divergence, topology revision/watch age, admission delay, disk/log/root
headroom, backup age/validation, certificate/KMS expiry, calibration drift,
and recovery RPO/RTO.

## Delivery plan

Each phase is independently useful and must pass before the next one broadens
the claim. Phase 3 is the first single-shard production candidate, Phase 4 the
first statically routed multi-shard production candidate, Phase 5 adds online
range workflows, and Phase 6 adds autonomous protection and placement.

### Phase 0 — contract and deterministic model

Status: the first executable foundation is implemented. The pinned core,
synchronous `Ready` driver, pure command/completion codecs, bounded logical
store/state machine, canonical trace, and RF1/RF3 crash/network simulator are
present. Protection-policy transitions, range workflows, topology revision
refinement, snapshot/WAL behavior, exhaustive history generation, and the full
gate below remain open; Phase 0 is therefore not complete.

- Freeze the terms, failure model, acknowledgement point, and unsupported
  operations.
- Select, pin, license-audit, and threat-model one production Raft core. Record
  every configuration option and unsupported extension. **Done for the core
  selection and configuration:** see [Raft core selection and threat
  model](raft-core-selection.md); executable integration qualification remains
  part of this phase's gate.
- Build an executable model of the integration boundary: `Ready` persistence,
  outbound messages, committed apply/publication, `ReadIndex`, snapshots,
  configuration changes, active/pending protection policy, verifier
  certificates, `OperationEpoch`, completion GC, range fences, and topology
  revisions.
  **Foundation present:** `Ready`, outbound-message, committed-entry,
  publication, `ReadIndex`, configuration, crash/restart, and logical snapshot
  ports are executable. Configuration context/topology authorization,
  duplicate command re-proposal, frozen completion-table integration, verified
  snapshot data, and the policy/range/topology portions remain open.
- Build a seeded deterministic simulator that runs the production state
  machine model, storage adapter, transport, timers, and workflow model under
  process, network, disk, clock, and topology faults. Each later phase plugs
  its production components into the same harness and adds trace-refinement
  checks.
  **Foundation present:** event order, logical time, scenario identity,
  message loss/duplication/reordering, partitions, logical disk outcomes, and
  process restart are byte-replayable from a complete canonical trace around
  the actual pinned core. Deterministic RNG and queue primitives exist, but a
  package-level seed-to-trace runner, physical WAL/snapshot faults, and
  production component refinement remain follow-ups.
- Prove or model-check one leader per term, committed-entry retention,
  configuration quorum overlap, range-fence exclusivity, profile contracts,
  and linearizable current reads.

**Gate:** exhaustive bounded histories satisfy ownership safety,
profile-specific acknowledged-write retention, per-shard linearizability, and
termination only under the declared liveness assumptions: an eventually
synchronous healed network, fair scheduling, bounded healthy-disk I/O, no
continuing configuration or range change, and one stable quorum for the
required interval. Every simulator failure is exactly replayable from its
complete trace; seed-only regeneration remains an open Phase-0 gate. Later
implementation phases must show that production traces refine the model at the
named protocol boundaries.

### Phase 1 — durable Raft substrate

- Add a versioned Raft WAL for entries, `HardState`, configuration, and
  snapshot manifests under `internal/storeio`.
- Add command and completion codecs with cluster incarnation, shard identity,
  canonical request fingerprints, and route generations.
- Put versioned AEAD envelopes, key IDs, wrapped-key metadata, and
  rotation-compatible headers in WAL, root, snapshot, and backup-capable
  formats from their first version.
- Implement crash-atomic snapshot creation/install and bounded log compaction.
- Export and deterministically apply commands through `durable.Collection`.
- Reuse record body encoding only where it remains independently versioned
  from the recovery journal.

**Gate:** byte-exact codec tests; crash injection around every persistence and
manifest-swap boundary; corrupt/truncated/reordered rejection; anti-amnesia
checks; snapshot-plus-suffix differential equality; idempotent replay; bounded
full-log behavior; and no store read-path change.

### Phase 2 — static RF1, RF2, and RF3 shards

- Embed one Raft group per shard behind a bounded Multi-Raft scheduler. Map
  RF1/RF2/RF3 to one/two/three voters and read replicas to learners.
- Bound every per-group mailbox and reserve fair heartbeat, vote, persistence,
  apply, and snapshot capacity so a hot group or catch-up stream cannot starve
  another group. Enforce maximum groups per node and per-peer flow control.
- Extend the static post-auth ordinary frame into mutually authenticated
  network transport, batching, flow control,
  persistence-before-message ordering, ordered apply, `ReadIndex`, replicated
  completions, eventual/session reads, and snapshot transfer.
- Add shard full/incremental backup and restore into fresh identities.
- Keep placement static while allowing ordinary Raft leader election.
- Expose last-log/commit/applied indexes and logical verification.

**Gate:** every single crash and I/O fault cut reopens to a profile-legal
prefix; RF1 loses no acknowledged write across a voter restart, RF2 loses no
acknowledged write after either single member loss but stops, and RF3 retains
a writable data quorum after any one member loss. Current reads pass
Porcupine; retained retries return the exact response; fingerprint conflicts,
acknowledged GC watermarks, expired client epochs, `STALE_REQUEST`, and
`INDETERMINATE` never reapply a command. Slow peers, snapshot storms, and the
maximum group count cannot starve elections or exhaust an unbounded resource.
Logical backup and fresh-identity restore are correct; published remote
RPO/RTO is a Phase 3 gate.

### Phase 3 — topology, membership, and operations

- Implement and qualify one topology backend adapter, paged serving snapshots,
  revisioned range descriptors, watch-gap reload, and topology backup/restore.
- Add learner seeding, planned leadership transfer, the Raft core's supported
  configuration-change protocol, operator-driven RF1/RF2/RF3 transitions,
  and the exact direct multi-change joint RF1↔RF3 edge used by automation.
  Add `Stable`/`Transitioning` protection epochs, placement constraints,
  draining, tombstones, and forced-recovery fencing. Qualify RF5 and RF3↔RF5
  as a separate extension required before Phase 6.
- Add the deterministic policy compiler, active/pending policy protocol,
  survivability-certificate verifier, operation interlock, and explainable
  operator plans; the probabilistic estimator remains Phase 6.
- Add periodic scrubbing, quorum-authoritative repair, version/feature gates,
  audit, hierarchical admission, and the production observability contract.
- Complete the single-shard security, upgrade, encrypted remote backup,
  restore, and disaster-recovery gates in the production-readiness foundation.

**Gate A — first single-shard production candidate:** the partition matrix
proves one writable leader and minority writes stop; RF3 elections preserve
every acknowledged record; RF1 and RF2 fail closed when their quorums are
absent; stale routers and former leaders cannot publish.
A crash at every membership and protection transition leaves one exact Raft
`ConfState` authoritative, RF2 loss never shrinks to RF1, and stronger
protection is not advertised before its target-voter barrier. Stale policy,
certificate, fact-revision, and operation-epoch commands reject
deterministically. A mixed tightening/weakening policy cannot activate until
`PolicyTighteningPrepared` proves every target hard constraint; incompatible
conjunctions reject, and crash/outage on both sides of that cut never exposes
an unmet active constraint. The direct RF1↔RF3 `ConfChangeV2` edge, both joint
majorities, every intermediate state, and crash/partition before and after
joint entry, leave, and activation pass this gate without a stable RF2 stop.
With the complete topology quorum unavailable, an
established group continues writes and `ReadIndex` reads while its data quorum
is healthy, but no policy stage, new plan, freshness attestation, membership
proposal, or route change begins. Strengthening pauses before proposal;
a realization downshift may abort before removal; policy weakening aborts only
before transition-contract activation and otherwise finalizes the weaker
promise at the unchanged stronger membership. No restart or outage silently
restores the old stronger contract. An already-proposed Raft configuration and
only that exact context-bound step may apply from the accepted replicated
plan. An unproposed permit is never banked. No next quorum-weakening step
starts without its distinct fresh permit: loss of topology after enter-joint
can therefore leave the shard safely fenced in joint consensus until
recovery. Test the outage before and after every fence,
transition-contract activation, authorization, joint-configuration
proposal/apply, explicit joint leave, prepared barrier, final activation, and
abort cut. Topology leader failure, watch compaction, ENOSPC, and restore do
not create data authority.
Repeat every protection cut with a topology restore: an irreversible operation
must finish its accepted plan or pass exact verifier-checked epoch rebind
before new control commands.
The composite production gate covers mTLS/default-deny authorization, at-rest
key rotation, durable redacted audit, N/N+1 finalize/rollback, immutable
offsite restore, forced-recovery fencing, scrub disagreement, admission
fairness, alerts, and measured runbooks. RF3 failover and repair meet the
declared RTO under 2x foreground load.

**Gate B — RF5 prerequisite for Autopilot:** operator-driven RF3↔RF5,
every joint configuration, two-voter failure combinations, matching-majority
repair, mixed-version cuts, and post-change activation pass the same matrix
without weakening Gate A.

### Phase 4 — static multi-shard routing

- Add shard-key schema, keyspace-ID routing, route caching, typed `MOVED`
  responses, single-shard plans, and scatter reads.
- Add keyspace backup as a routing-version-pinned vector of shard cuts, without
  claiming one common timestamp.
- Add server-side leased distributed cursors with bounded pins and explicit
  expiry/restart semantics.
- Keep placement operator-defined.
- Qualify local-key order and global merge semantics.

**Gate:** route intervals have no gaps or overlap; every key has exactly one
active shard across stale-route retries; heap/durable/single-shard/distributed
query differentials agree; vector backup restores every interval exactly once;
cursor expiry, router restart, shard crash, and retained-byte limits neither
omit nor duplicate a promised page. Topology is absent from the steady query
path. With the complete topology quorum unavailable and the cached RF3 leader
dead, cached voters elect, routers probe cached members, and unchanged-range
writes plus `ReadIndex` reads continue. Cold routers, every route change, and
every new membership plan fail closed. This gate reuses Phase 3's
protection-phase outage matrix and adds routed-cache reload, stale-leader, and
cold-router cases.

### Phase 5 — online workflows

- Add replica seeding, origin-tagged filtered copy/tail, per-source import
  watermarks, VDiff, committed source fences, prepared target activation,
  atomic route switch, reverse stream, and retirement.
- Support split and move first; merge second.
- Compose the Phase 3 membership primitives with operator-controlled node
  addition, removal, drain, and range movement; do not implement a second
  replica-change protocol.

**Gate:** crash after every workflow action resumes safely; concurrent mutation
and scan differentials show no missing or duplicate row; source deletion never
precedes every retention cut; pre-cutover session tokens translate to proven
target cuts or fail explicitly; old retries rendezvous with one completion
home; backup includes each moving interval once; topology restore at every
freeze/route-switch/activation cut either completes the accepted plan or
verifier-rebinds its byte-identical transition and reconstructs exactly one
owner; every destination preserves source policy/effective protection and
every merge either compiles the strongest compatible intent or rejects;
foreground p99 stays within the declared resharding budget.

### Phase 6 — Durability Autopilot and automatic placement

- Add load collection, histograms, split-point selection, capacity placement,
  runtime node registration, throttling, hysteresis, cooldowns, and
  unsplittable-hotspot reporting.
- Add the hard failure-scenario solver, versioned risk estimator, protection
  debt scheduler, explain API, bounded decision certificates, per-domain
  transition budgets, and separate voter, learner, and split/move loops.
- Roll out through shadow recommendation, bounded strengthen-only canary, and
  explicitly authorized full-elastic stages. Each has a predeclared soak
  interval, shard/failure-domain blast-radius cap, error/churn/pause/latency
  budget, emergency disable, and controller-version rollback. Disabling the
  controller stops new plans but never abandons an emitted configuration
  change.
- Stale evidence, topology outage, upgrade finalization, restore, reshard,
  repair, drain, or forced recovery freezes discretionary decisions.
- Automate only profiles, placements, and workflows already qualified under
  operator initiation.

**Shadow gate:** replay identical traces against fixed RF3/RF5, a Total
Recall-style availability/repair controller, a Tuba-style
min/max/locality/cost controller, a Take-Me-to-Your-Leader-style
fixed-fault-tolerance role/placement optimizer, naive threshold auto-RF,
Anna-style load-selective replication, and an offline future-knowing oracle.
Each named online baseline is an adapted decision-policy ablation over the
same candidate placements and voter counts, hard contract and failure
scenarios, signed facts, hardware/capacity, workloads, and Raft transition
executor. Native differences in consistency or storage engines are not scored
as controller differences.
Use uniform, Zipf, shifting-hotspot, growth, and repair-backlog workloads. The
hard verifier must independently reproduce every recommendation. On
time-split held-out failure traces, each enforced probabilistic bound's
one-sided upper confidence limit must meet the declared horizon/confidence;
otherwise risk remains advisory and weakening is disabled.

**Strengthen-only canary gate:** inject disk, host, rack, zone, region,
rollout-cohort, partition, corruption, stalled repair, ENOSPC,
stale/malformed workload telemetry, forged locality certificates, topology
loss, and controller/leader crash at every RF1↔RF3 and RF3↔RF5 cut, including
concurrent backup, reshard, upgrade, repair, and tenant demand. Signed hard
facts reject forgery; workload telemetry may affect ranking only. No
intermediate configuration violates the floor, no learner counts as a voter,
and RF2 remains manual-only. The canary exits only after its declared soak with
zero safety/policy violations and all time-to-protect, RTO, p99, pause, churn,
and blast-radius budgets met.

**Full-elastic gate:** repeat the matrix in both directions, stress
correlation, MTTR, repair-bandwidth, and estimator misspecification, and prove
stale coverage automatically disables weakening. No weakening occurs outside
signed policy or without a fresh permit. Before held-out traces are revealed,
the evaluation preregisters an adaptive-benefit margin and non-inferiority
margins. At an equal hard contract, the one-sided 95% confidence bound must
show at least that strict reduction in normalized steady-state
storage-plus-network cost against the best named online baseline, while write
p99, unavailability, time-to-protect, and RPO violations remain within their
non-inferiority margins. It must also meet a preregistered nontrivial regret
bound against the offline oracle. Pareto non-domination alone is insufficient:
cloning fixed RF3 cannot pass. Controlled skew and growth must avoid
oscillation, capacity oversubscription, or unbounded catch-up; adding eight
independent shards reaches at least 75% of ideal throughput scaling on matched
hardware.

### Phase 7 — stronger distributed semantics

These are separate subprojects rather than a condition for initial sharding:

1. timestamped root history, `SafeTime`, and `GCWatermark`;
2. consistent global read snapshots, common-timestamp backup cuts, and
   stateless resumable pagination;
3. atomic 2PC with replicated participant and decision recovery;
4. serializable cross-shard concurrency control;
5. shard-group atomicity across co-located collections; and
6. global constraints and lookup indexes.

Each new mode keeps the shard-local fast path structurally unchanged and
publishes its additional latency, storage, and availability contract.

## Qualification matrix

Correctness precedes competitive measurement.

### Safety and recovery

- crash the Raft leader, each voter/learner, topology leader, and workflow
  controller at every
  protocol boundary;
- crash before/after entry, `HardState`, and snapshot persistence; dependent
  message send; commit advance; apply/publication; response; snapshot manifest
  swap; configuration apply; and range activation;
- run the crash and partition matrix independently for RF1, RF2, RF3, RF5,
  every joint state, and each permitted profile transition;
- drop, duplicate, delay, reorder, and partition every message class;
- inject torn records, lost writes, sync failures, ENOSPC, and corrupt
  snapshots;
- exercise stale routes, `ReadIndex` retry, duplicate/fingerprint-conflicting
  requests, leadership loss/reacquisition, and late former-leader recovery;
- carry session tokens and cursors across every split/move cutover and
  retention expiry;
- exercise ambiguous proposals, repeated elections, learner promotion,
  configuration change, voter amnesia, stale snapshots, and replica
  replacement at every persisted transition;
- reject malformed/stale `ConfChangeV2.Context` as an application no-op
  without calling `ApplyConfChange`; prove an empty V2 appears only as an
  authorized explicit leave-joint;
- check generated protocol histories with a linearizability checker such as
  Porcupine and run partition-oriented workload histories through Elle-style
  anomaly analysis;
- exercise mixed-version rolling upgrade, schema-version skew, and downgrade
  rejection;
- hold old snapshots through write, failover, split, rollback, and retirement;
- restore topology alone under a fresh `TopologyRecoveryEpoch`, restore data
  under fresh data incarnations, and prove old members, watches, routes,
  tokens, and workflows cannot revive;
- after every epoch adoption/rebind, snapshot and compact its log entry, then
  restart and install that snapshot on another member; the epoch, signer,
  recovery/rebind proof, and old-authority tombstones never regress;
- exercise forged/expired/cross-tenant tokens, certificate and key rotation,
  backup corruption, KMS loss, retry storms, and 2x overload with one failed
  voter; and
- verify exact rows, indexes, order, and root/log reclamation after recovery.

The executable model is the protocol oracle. Seeded deterministic simulation
exercises the production integration, while store crash tests remain the
oracle for physical roots.

### Performance and scale

Publish separate lanes for:

- one shard and 1/8/64 clients;
- 1/2/8/32 independent shards;
- RF1, RF2, RF3, and RF5 synchronous voter profiles;
- zero, one, and three additional asynchronous read replicas;
- local, cross-AZ, and injected-latency placement;
- leader-current, replica-eventual, replica-at-least, bounded-stale, and as-of
  reads where implemented;
- shard-key point work, tenant range scans, and all-shard scans;
- steady state, replica catch-up, reshard, and failover; and
- uniform, Zipf, sequential-tail, and single-hot-key distributions.

Report throughput at a fixed p99 SLO, p50/p95/p99/max, errors and retries,
CPU/op, allocation, network bytes, device bytes, live/allocated disk, replica
lag, forced checkpoints, and recovery time.

Competitive CockroachDB and Vitess rows must match:

- acknowledgement and replica count;
- availability-zone topology and RTT;
- isolation and read freshness;
- indexes, constraints, and transaction shape;
- corpus, shard key, distribution, and client placement; and
- resharding/failure activity.

An RF1 or asynchronous result beside an RF3 synchronous competitor is labeled
as a different product contract, not a database-engine win.

Structural gates:

1. no topology RPC on a warm single-shard query;
2. exactly one router-to-leader hop for a routed operation;
3. zero peer round trips for RF1 and one parallel follower-round-trip latency
   with the required quorum acknowledgements for RF2/RF3/RF5;
4. bounded queues, logs, snapshots, cursors, and workflow concurrency;
5. no reader-visible log, delta, or merge representation inside a shard;
6. no single-shard storage/read regression outside measured noise; and
7. at least 75% ideal throughput scaling from one to eight independent shards
   before an automatic-scaling claim.

### Scale-model versus release-scale qualification

Passing the matrix above at small scale proves the state machine, fencing,
and recovery invariants; it does not by itself prove behavior on a logical
table or database in the 100+ TB range. This design tracks two qualification
tiers and never conflates them:

- **Continuous scale-model gates** run in CI or scheduled engineering
  validation using many small shards: thousands of small manifests/ranges,
  repeated split/move/merge storms, ownership-epoch and route churn, and
  topology-outage-and-reload cycles. These prove orchestration and safety
  invariants at low cost and high repetition, not storage behavior at full
  size.
- **Release qualification** exercises a full 100+ TB generated/imported
  logical table or database across qualified shards on declared hardware, with
  documented cost, duration, and recovery envelope. It is a release-qualification
  event, not a routine CI gate.

A scale-model pass never substitutes for a completed release-qualification
run, and a public 100+ TB claim is never inferred from scale-model results
alone; it requires a completed release-qualification run.

## Honest limits

This architecture moves coordination out of the common case; it does not make
coordination disappear.

- The topology service is a consensus system even when vibedb does not
  implement its consensus protocol.
- RF2/RF3/RF5 synchronous replica durability costs at least one network round
  trip; RF1 has no redundant copy.
- RF1 cannot survive voter loss, and RF2 cannot continue writes after one
  member loss without restoring quorum or explicitly unsafe recovery.
- Fresh reads do not scale across asynchronous followers.
- One hot shard key remains one-leader limited.
- Hash distribution trades global lexical locality for balanced writers.
- Cross-shard scans pay fan-out, merge, and snapshot-retention costs.
- Automatic resharding cannot repair a semantically wrong shard key cheaply.
- Durability Autopilot cannot predict the future. Its probabilistic model may
  rank safe candidates but never replaces deterministic failure-domain floors,
  and bad evidence may cause extra cost or fail-closed unavailability.
- Automatic weakening changes the protection of old as well as new data; it
  therefore remains opt-in, slow, and auditable.
- Strong global snapshots and transactions add history, coordination, and
  failure-recovery state.
- Operational correctness—backup, upgrades, placement, throttling, repair, and
  observability—is part of the database, not deployment polish.

## References and precedent

These sources inform the architecture. `etcd-io/raft` is the selected and
[pinned protocol-core dependency](raft-core-selection.md); the papers are not
invitations to create another consensus implementation. Adaptive replication,
placement, and repair all have close prior art, so this design claims only a
proposed combination, not "the first," without a formal literature and patent
review:

- [PacificA](https://www.microsoft.com/en-us/research/wp-content/uploads/2008/02/tr-2008-25.pdf)
  describes a primary/backup replicated log coordinated by an external
  configuration manager and leases. Its all-active-backup prepare rule does
  not justify RF3 two-of-three commitment.
- [`etcd-io/raft`](https://github.com/etcd-io/raft) and its
  [Go API](https://pkg.go.dev/go.etcd.io/raft/v3) provide the production-proven
  deterministic core, persistence ordering, membership, `ReadIndex`,
  pipelining, flow control, and compaction contract.
- [Raft](https://raft.github.io/raft.pdf), the
  [Raft dissertation](https://www.web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf),
  and
  [Viewstamped Replication Revisited](https://www.cs.princeton.edu/courses/archive/fall19/cos418/papers/vr-revisited.pdf)
  provide comparable term/view-change and quorum-intersection models.
- [Paxos Made Live](https://research.google.com/archive/paxos_made_live.html)
  documents the engineering gap between a consensus proof and a production
  replicated system.
- [RIFL](https://web.stanford.edu/~ouster/cgi-bin/papers/rifl.pdf) defines the
  unique request, durable completion, retry rendezvous, and safe-GC
  requirements behind the completion table.
- [FoundationDB simulation testing](https://apple.github.io/foundationdb/testing.html)
  demonstrates deterministic whole-system fault simulation with replayable
  seeds.
- [Porcupine](https://github.com/anishathalye/porcupine) and
  [Elle](https://github.com/jepsen-io/elle) provide practical history-checking
  techniques for linearizability and transactional anomalies.
- [Vitess topology service](https://vitess.io/docs/25.0/concepts/topology-service/)
  describes cached consistent metadata outside the steady query path.
- [Vitess replication](https://vitess.io/docs/archive/21.0/reference/features/mysql-replication/)
  documents asynchronous and semi-synchronous primary/replica durability.
- [Vitess VReplication](https://vitess.io/docs/25.0/reference/vreplication/vreplication/)
  specifies resumable copy, catch-up, verification, routing switch, and
  journaling.
- [Vitess backup and restore](https://vitess.io/docs/25.0/user-guides/operating-vitess/backup-and-restore/managing-backups/)
  provides the per-shard backup/PITR operational precedent.
- [Vitess distributed transactions](https://vitess.io/docs/24.0/reference/features/distributed-transaction/)
  distinguishes shard-local ACID, best-effort multi-shard commits, and atomic
  2PC without full isolation.
- [PlanetScale sharding](https://planetscale.com/docs/vitess/sharding) and
  [Vindexes](https://planetscale.com/docs/vitess/sharding/vindexes) document
  explicit shard-key selection and keyspace-ID routing.
- [CockroachDB replication](https://www.cockroachlabs.com/docs/v26.2/architecture/replication-layer)
  and [transaction layer](https://www.cockroachlabs.com/docs/v26.2/architecture/transaction-layer/)
  define the stronger quorum and distributed-transaction comparison contract.
- [CockroachDB survival goals](https://www.cockroachlabs.com/docs/stable/multiregion-survival-goals),
  [TiDB placement policies](https://docs.pingcap.com/tidb/stable/placement-rules-in-sql/),
  and [Scylla tablets](https://docs.scylladb.com/manual/stable/architecture/tablets.html)
  are baselines for policy-derived replica placement and automatic movement.
- [Total Recall](https://www.usenix.org/conference/nsdi-04/total-recall-system-support-automated-availability-management)
  and [Tuba](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/ardekani)
  are the direct availability-target and constrained automatic
  replica-reconfiguration baselines.
- [Anna](https://dsf.berkeley.edu/jmh/papers/anna_vldb_19.pdf) is the
  load-driven selective-replication baseline; its coordination model and
  consistency contract differ from a linearizable Raft group.
- Google's
  [availability study](https://research.google.com/pubs/archive/36737.pdf),
  [online storage-configuration optimization](https://research.google/pubs/take-me-to-your-leader-online-optimization-of-distributed-storage-configurations/),
  and
  [Tiger](https://research.google/pubs/tiger-disk-adaptive-redundancy-without-placement-restrictions/)
  motivate correlated-failure, workload, repair-time, and uncertainty-aware
  decisions rather than an independent-disk replica-count heuristic.
- [Carbonite](https://www.usenix.org/legacy/event/nsdi06/tech/full_papers/chun/chun.pdf)
  provides the repair-exposure and bounded replica-maintenance baseline.
- The [Spanner paper](https://research.google.com/archive/spanner-osdi2012.pdf)
  defines applied-consensus and prepared-transaction constraints on safe time.
- Fischer, Lynch, and Paterson,
  [Impossibility of Distributed Consensus with One Faulty Process](https://www.cs.cornell.edu/courses/cs614/2003sp/papers/FLP85.pdf),
  gives the failure-model boundary behind ownership changes.
