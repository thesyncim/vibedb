# Distributed feature ledger

> [!CAUTION]
> VibeDB is unreleased development software. Any commit may break APIs, commands, wire
> protocols, or disk formats. Use documentation and binaries from the exact same commit, and
> store only disposable or independently recoverable data. A **Yes** below does not mean
> supported, production-ready, or compatible with another commit. Consult [current
> status](status.md) for known failing tests and defects.

> [!NOTE]
> Generated from `internal/featurestate.Distributed`. Change the manifest, not this file.

## How to read this ledger

Every feature has four independent stages. The adjacent contract is the claim; the label alone
is not. This ledger describes only the exact commit that generated it.

- **Yes** — The manifest makes the complete stage claim written in its contract and cites
  evidence.
- **Partial** — Only the subset stated in the contract is present or qualified; the contract
  names material gaps.
- **No** — The manifest makes no positive claim for that stage; the contract states the
  current boundary.

| Stage | Question answered |
| --- | --- |
| Primitive | Does the underlying code, codec, or protocol exist? |
| Integrated | Does a repository path compose and use it? |
| Development command | Does a checked-in command or CLI construct the path? |
| Qualification | Has the stated contract passed the cited fault or performance gate? |

A qualification **Yes** requires the fault or benchmark gate named by its contract; ordinary
correctness tests remain **Partial**. A development-command **Yes** means a checked-in command
constructs the path, not that the feature is released.

## Features

<details>
<summary><strong>Static shard routing and scatter reads</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

The catalog, planner, router, merge path, and bounded fanout exist.

_Evidence:_ [E1](#evidence-1), [E2](#evidence-2)

**Integrated — Yes**

The gateway executor sends SQL to leader-only shard services and merges complete results.

_Evidence:_ [E3](#evidence-3), [E4](#evidence-4)

**Development command — Yes**

vibedb-gateway serve and vibedb-shard serve expose this static-ownership path.

_Evidence:_ [E5](#evidence-5), [E6](#evidence-6)

**Qualification — Partial**

Correctness, stale-catalog, admission, and merge tests exist. There is no external kill or
scaling benchmark gate for this command path.

_Evidence:_ [E3](#evidence-3), [E7](#evidence-7)


</details>

<details>
<summary><strong>Authenticated and authorized service transport</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

TLS 1.3 profiles bind node identity, traffic class, trust roots, and limits. Canonical vibejson
policies bind exact principals and generations to fixed capabilities.

_Evidence:_ [E8](#evidence-8), [E9](#evidence-9)

**Integrated — Yes**

The gateway authorizes complete request semantics and forwards the exact client authority.
Shards independently check it while requiring a delegate-capable gateway.

_Evidence:_ [E10](#evidence-10), [E11](#evidence-11)

**Development command — Yes**

The gateway and static shard commands require TLS plus an authorization policy unless the
operator selects explicit loopback development plaintext. The RF3 shard command is always
authenticated.

_Evidence:_ [E5](#evidence-5), [E6](#evidence-6), [E12](#evidence-12)

**Qualification — Partial**

Tests cover complete parsed SQL semantics, identity, generation rotation, confused-deputy
refusal, connection bounds, and allocation contracts. Real TCP TLS gates cover client-to-gateway
handshake churn and gateway-to-shard independent delegate and forwarded-principal checks. An
external shard process gate sustains requests across a directional partition and healing,
rotates its certificate generation while revoking the old stream, and rejects both a rogue
gateway certificate and a confused-deputy request. TestGatewayDurableRF3ExternalProcessRecovery
adds the checked-in development gateway command with distinct gateway principals and native
mutual TLS across client, gateway, catalog, request-ledger, and two data RF3 groups. Hot
authorization is allocation-free; the steady-stream benchmark keeps the standard crypto/tls
per-record read allocation floor separate and visible. This remains Partial because the complete
process gate does not combine certificate-rotation and confused-deputy faults across the whole
gateway command path.

_Evidence:_ [E13](#evidence-13), [E14](#evidence-14), [E15](#evidence-15), [E16](#evidence-16), [E17](#evidence-17), [E18](#evidence-18), [E19](#evidence-19), [E20](#evidence-20), [E21](#evidence-21), [E22](#evidence-22)


</details>

<details>
<summary><strong>Coherent multi-shard reads</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

Bounded static read fences establish one scoped vector cut. RF3 exact-key batch reads return an
explicit sorted observation vector. General SQL SELECT plans use independent leader ReadIndex
cuts but expose no public observation vector; none of these contracts claims one global MVCC
timestamp.

_Evidence:_ [E23](#evidence-23), [E24](#evidence-24), [E25](#evidence-25)

**Integrated — Yes**

The static gateway acquires all fences before fanout. In replicated-catalog mode the ordinary
SQL planner resolves each pinned physical target through ReplicatedSQLTransport, follows its RF3
leader, executes after ReadIndex, and reuses bounded target/scatter merge. The narrower RF3
reader folds exact-key statements by group, bounds parallel reads and response bytes, and
refuses every partial result.

_Evidence:_ [E23](#evidence-23), [E25](#evidence-25), [E24](#evidence-24)

**Development command — Yes**

The query operation is mode-dependent: explicit development/static catalog mode uses the static
shard transport, while replicated-catalog mode sends supported general SELECT plans through RF3.
RF3 supports bounded targeted/scatter reads, projection, global order/limit, and mergeable
aggregates; global-index read and repartition-exchange plans are refused, and the public query
response exposes no observation vector. read_batch separately serves ordered multi-table and
multi-group exact-primary-key reads with an explicit vector.

_Evidence:_ [E26](#evidence-26), [E27](#evidence-27), [E28](#evidence-28)

**Qualification — Partial**

Static lease, cleanup, stale-refusal, and pinned-snapshot tests exist. RF3 general-SQL tests
exercise targeted/scatter SELECTs, global order, aggregate merge, leader routing, internal cut
accounting, and all-or-nothing failure. RF3 read_batch tests prove same-group multi-relation
cuts, cross-group vectors, all-or-nothing byte admission, and bounded execution across 65
groups. Mandatory external process gates cover the exact-key lane across a bidirectional Raft
peer-network partition with the isolated process live, a killed leader, delayed response,
gateway replacement, and rolling recovery while bounding failover, p99, RSS, storage, WAL, and
exact public wire bytes; they do not qualify general RF3 SQL, so the row remains Partial.

_Evidence:_ [E29](#evidence-29), [E30](#evidence-30), [E31](#evidence-31), [E32](#evidence-32), [E33](#evidence-33)


</details>

<details>
<summary><strong>Bounded distributed metrics</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

Fixed-width counters and gauges observe proposal commands and bytes, authoritative commit-index
advancements and committed entries, applied entries, persisted Ready steps, snapshot apply,
ReadIndex completion, Raft faults, checkpoints, WAL, backup, snapshot transfer, replica actions,
split control, and target bootstrap. Group and node cuts use no unbounded labels.

_Evidence:_ [E34](#evidence-34), [E35](#evidence-35)

**Integrated — Yes**

The authenticated shard-control service returns one exact group/member cut or one node-stage
aggregate. The gateway fixes the complete catalog directory, refreshes it with bounded workers
and deadlines, publishes through seqlocks, and computes saturating lock-free aggregates without
request-path sample storage. Split and move controller loops update fixed atomics from their
actual pass results.

_Evidence:_ [E36](#evidence-36), [E37](#evidence-37), [E38](#evidence-38), [E39](#evidence-39)

**Development command — Yes**

serve-rf3 and the cold target bootstrap mux install the fixed 80-byte request and 408-byte
topology-authorized response. A replicated vibedb-gateway metrics response directly encodes
process routing counters, cluster aggregate, overflow, bounded member and node cuts, and
configured split/move controller progress with vibejson. Telemetry never authorizes routing,
membership, split, move, cleanup, or acknowledgement.

_Evidence:_ [E40](#evidence-40), [E41](#evidence-41), [E42](#evidence-42), [E43](#evidence-43)

**Qualification — Partial**

Known defect: a nonzero-group request can panic when Service was constructed with a Provider
that does not also implement GroupProvider. Group-serving configurations must supply
GroupProvider. Tests otherwise cover exact group identity, node-stage framing, authentication
before read, corruption, duplicate and wrong-member refusal, saturating aggregation, actual
controller-pass observation, and canonical direct encoding. Benchmarks check the counter hot
path, codec, cached snapshot, controller snapshot, and gateway encoding allocation contracts at
zero allocations. There is no mandatory external slow-scrape, member-churn, very-wide-directory,
or long-running telemetry-overhead gate. ready_persisted is not a quorum counter. The
authoritative commit counters do not measure quorum latency or bytes. Leadership, exact split
and move phase, latency histograms, total network, and physical device writes are not exported.

_Evidence:_ [E44](#evidence-44), [E45](#evidence-45), [E46](#evidence-46), [E47](#evidence-47), [E48](#evidence-48), [E49](#evidence-49), [E50](#evidence-50)


</details>

<details>
<summary><strong>Distributed clock model and skew resilience</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

RF3 order and reads use Raft term, index, applied index, and ReadOnlySafe. Transaction recovery
advances bounded replicated pulses. Execution-pin leases and hot-shard cooldown use replicated
progress fences instead of elapsed time.

_Evidence:_ [E51](#evidence-51), [E52](#evidence-52), [E53](#evidence-53)

**Integrated — Yes**

Static and RF3 transaction recovery require an ordered pulse sequence before abort. The durable
request lifecycle binds one clockless controller epoch and applied-index lease to the complete
program. Pressure evidence uses catalog generations and authority revisions.

_Evidence:_ [E54](#evidence-54), [E55](#evidence-55), [E56](#evidence-56)

**Development command — Yes**

The RF3 command path uses quorum order, applied-index reads, vector cuts, logical recovery
pulses, and the checked-in durable request service's applied-index execution pins. Local time
controls only TLS validity, network/context deadlines, retry scheduling, catalog-session
deadline construction, and the separate static read-fence lane.

_Evidence:_ [E57](#evidence-57), [E40](#evidence-40), [E58](#evidence-58)

**Qualification — Partial**

A bounded Linux command matrix composes independently injected peer UTC steps and TLS validity,
logical-pulse restart recovery, two-group leader isolation and exact transaction retry, real
process suspend/resume, former-leader refusal, foreground failover latency, and the checked-in
serve-rf3 kill/partition pressure gate. Skips fail the matrix and evidence bytes are bounded.
Live database-process UTC is not changed, and arbitrary static read-fence suspension/overrun
remains unqualified, so this is not a global timestamp or unrestricted clock-fault claim.

_Evidence:_ [E59](#evidence-59), [E60](#evidence-60), [E61](#evidence-61), [E62](#evidence-62), [E63](#evidence-63)


</details>

<details>
<summary><strong>Byte-bounded distributed transactions</strong> — Primitive:Yes · Integrated:Yes · Command:Partial · Qualification:Partial</summary>

**Primitive — Yes**

Compact inline and root-bound paged coordinator manifests implement prepare, decision, apply,
release, retry, and bounded recovery without a participant-count contract.

_Evidence:_ [E64](#evidence-64), [E65](#evidence-65)

**Integrated — Yes**

ExecBatch and global-index writes select the inline fast path or stream segmented manifests
through the authenticated shard protocol and paged recovery.

_Evidence:_ [E66](#evidence-66), [E67](#evidence-67)

**Development command — Partial**

Static vibedb-gateway serve can invoke the transaction engine behind one single-base-owner exec
when independently placed index maintenance adds participants. It does not expose general
multi-statement or cross-base-shard static exec_batch; public exec_batch is reserved for
authenticated durable RF3 and refuses every unsequenced fallback.

_Evidence:_ [E57](#evidence-57), [E68](#evidence-68), [E69](#evidence-69)

**Qualification — Partial**

Known defect: journal compaction omits coordinator recovery-pulse records, so reopen after
compaction can reset pulse state. Do not rely on that path for recovery authority. Library-level
tests prove a real 65-shard segmented transaction, readback, outcome-unknown handling,
multi-page restart, malformed-page refusal, idempotency, recovery, and failure atomicity. A
public static-listener test proves exec_batch cannot reach that engine through an unsequenced
fallback. No checked-in static command path exists to qualify under external kill, partition, or
scaling gates.

_Evidence:_ [E70](#evidence-70), [E71](#evidence-71), [E72](#evidence-72), [E73](#evidence-73), [E69](#evidence-69)


</details>

<details>
<summary><strong>Fused multi-shard RF3 transaction orchestration</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

Fresh replicated operations atomically combine coordinator begin with its local prepare, remote
stage with prepare, and participant apply or abort with release. Inline and greedily packed
manifests remain byte-bounded without a participant-count contract.

_Evidence:_ [E74](#evidence-74), [E75](#evidence-75)

**Integrated — Yes**

The SQL lowerer can read one exact-key old row, evaluate supported computed UPDATE assignments
once, and seal the canonical postimage plus exact old-value CAS into the participant program.
The durable request runner streams that owned program, builds exact fused commands, stages their
bytes before proposal, and recovers from replicated ReadIndex witnesses without re-evaluating
SQL. Aggregate execution-pin epochs fence takeover locally at every participant while one
home-group proof admits each persisted wave.

_Evidence:_ [E76](#evidence-76), [E77](#evidence-77), [E78](#evidence-78), [E79](#evidence-79)

**Development command — Yes**

vibedb-gateway accepts only authenticated structured issuer identities for RF3 exec_batch,
lowers complete-document or canonical column-list INSERT rows plus supported exact-key
UPDATE/DELETE across one or more tables and global-index relations, including computed top-level
declared-column UPDATE assignments, performs fused durable admission, and returns the terminal
result with an authenticated ACK capability. One multi-row INSERT may span RF3 groups. There is
no process-local request registry or legacy RF3 fallback.

_Evidence:_ [E80](#evidence-80), [E81](#evidence-81), [E26](#evidence-26)

**Qualification — Partial**

State-machine, SQL lowering, schedule, durable lifecycle, and exact-retry gates cover canonical
flat INSERT encoding, cross-shard multi-row INSERT, retained computed-update postimages and
old-value CAS, planner-error replay of an authenticated retained program, atomic transitions,
bounded reclamation, concurrent gateways, and the fused 2P+1 decision/apply schedule without a
participant-count contract. Real in-process RF3 gates prove two independently led data groups, a
dedicated request-ledger group, isolation, exact hidden-command retry, replacement-gateway
terminal replay, ACK recovery, replica convergence, and former-leader refusal.
TestGatewayDurableRF3ExternalProcessRecovery adds three shard processes that each host catalog,
request-ledger, and two data RF3 groups, native mutual TLS, distinct replacement-gateway
principals and route seeds, a stopped voter, killed leaders, lost terminal and ACK responses,
exact replay, and rolling voter restarts. Its workload does not exercise computed UPDATE
expressions and its byte bounds omit total Raft and network traffic, so the row remains Partial.

_Evidence:_ [E82](#evidence-82), [E83](#evidence-83), [E84](#evidence-84), [E85](#evidence-85), [E86](#evidence-86), [E87](#evidence-87), [E61](#evidence-61), [E88](#evidence-88), [E89](#evidence-89), [E21](#evidence-21)


</details>

<details>
<summary><strong>RF3 transaction recovery reads</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

A closed hidden-state reader provides exact coordinator and participant lookup, paged manifest
access, and bounded resumable active-coordinator scans without a participant-count contract.

_Evidence:_ [E90](#evidence-90), [E91](#evidence-91)

**Integrated — Yes**

The dedicated transaction-recovery capability, leader-only ReadIndex path, native shard
protocol, and leader-aware gateway executor share exact byte, row, applied-index, and
serving-fence bounds. The durable distributed runner consumes those reads while replaying its
sealed participant stream.

_Evidence:_ [E92](#evidence-92), [E93](#evidence-93)

**Development command — Yes**

vibedb-shard serve-rf3 installs the authenticated recovery reader. vibedb-gateway retries
structured requests from replicated ledger state, reuses the sealed program after admission, and
exposes terminal ACK plus bounded collection. Recovery authority is replicated; the command
installs no same-process registry or periodic process-memory sweep.

_Evidence:_ [E40](#evidence-40), [E94](#evidence-94), [E95](#evidence-95)

**Qualification — Partial**

Real RF3 tests prove leader-only recovery, replacement-leader continuity, isolated-former-leader
refusal, hidden-commit recovery across two groups, terminal replay through a replacement
gateway, and lost committed ACK recovery through a new ledger leader. Durable fault tests cover
bounded state, duplicate convergence, ACK tombstones, and restart at every lifecycle cut.
TestGatewayDurableRF3ExternalProcessRecovery now exercises the checked-in child-process gateway
replacement against real catalog, request-ledger, and two-data-group RF3, including terminal and
ACK response loss, ledger-leader kill, exact replay, collection, and every voter restart. Its
byte evidence is limited to exact public client request/response wire bytes and snapshot payload
bytes, not total network traffic, so the row remains Partial.

_Evidence:_ [E96](#evidence-96), [E61](#evidence-61), [E88](#evidence-88), [E97](#evidence-97), [E21](#evidence-21)


</details>

<details>
<summary><strong>Durable RF3 request ledger</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

A replicated request grammar owns paged plans, pending waves, terminal results,
acknowledgements, bounded collection, issuer lanes, contiguous issuer high-water, and logical
execution pins. Catalog metadata stores adjacent immutable ledger-home ranges with exact RF3
route authority.

_Evidence:_ [E98](#evidence-98), [E99](#evidence-99), [E52](#evidence-52)

**Integrated — Yes**

The typed service selects the catalog-persisted ledger home, seals and streams a logical
transaction recipe, recovers lifecycle CAS operations from RF3 state, fences every transaction
wave with one execution-pin epoch, derives stable ACK authority, and collects only contiguous
GC-complete issuer sequences. The ledger system recovery record region retains a 16 MiB plus
119-sector (16,838,144-byte) compatibility floor, which covers the current 514-entry
maximum-key-width conditional record; its owner seals the greater of that floor and the region
derived from its actual frozen limits. The 18,360,832-byte hard parser ceiling bounds every
owner but is not the ledger's configured allocation. User-relation journals retain their
separate 16 MiB plus 34-sector contract.

_Evidence:_ [E100](#evidence-100), [E101](#evidence-101), [E102](#evidence-102), [E103](#evidence-103), [E104](#evidence-104), [E105](#evidence-105), [E106](#evidence-106)

**Development command — Yes**

vibedb cluster dev provisions a dedicated request-ledger group and shared ACK authority.
runServe constructs the RF3 ledger client, catalog-bound topology, execution-pin sessions,
distributed runner, replicated issuer authority, strict structured exec_batch service, and ACK
collector. Missing durable authority fails startup; no legacy fallback is installed.

_Evidence:_ [E107](#evidence-107), [E108](#evidence-108), [E109](#evidence-109)

**Qualification — Partial**

Internal tests prove catalog topology round trips, bounded wide-plan replay, logical-pin
fencing, concurrent-gateway convergence, ACK and GC recovery, issuer high-water restart, and
strict public wire identity. Real in-process gates add a three-voter ledger, two data groups,
full SQL execution, terminal response loss, replacement-gateway replay, authenticated ACK
refusal, lost committed ACK recovery, and write-free completed retry.
TestGatewayDurableRF3ExternalProcessRecovery now supplies the checked-in child-process gate:
three shard processes each host catalog, request-ledger, and two data RF3 groups; distinct
gateway principals and route seeds recover lost terminal and ACK responses across a stopped
voter, killed leaders, and rolling voter restarts over native mutual TLS. It bounds p99, RSS,
allocated storage, WAL allocation, exact public client request/response wire bytes, and snapshot
payload bytes, not total Raft or total network bytes, so the row remains Partial.

_Evidence:_ [E110](#evidence-110), [E111](#evidence-111), [E112](#evidence-112), [E113](#evidence-113), [E88](#evidence-88), [E21](#evidence-21)


</details>

<details>
<summary><strong>Global exact index routing and maintenance</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

Catalog metadata, independent index placement, exact lookup, lifecycle fencing, write expansion,
and canonical key/before/after mutation-image capture exist.

_Evidence:_ [E114](#evidence-114), [E115](#evidence-115), [E116](#evidence-116)

**Integrated — Yes**

Static planning can select a global index. Static UPDATE and DELETE, including computed UPDATE
assignments, capture canonical images on the base shard, bind old and postimage digests around
the original SQL, and order all index deletes before puts. Static and RF3 writes add
independently placed index participants without forcing the base row onto the index shard. RF3
exact-key direct and computed updates retain a canonical postimage plus exact old-value CAS;
global indexes are derived from that postimage, and a same-key locator refresh uses one
digest-compare replacement.

_Evidence:_ [E117](#evidence-117), [E118](#evidence-118), [E119](#evidence-119), [E76](#evidence-76)

**Development command — Yes**

The static gateway command consumes ready global-index metadata, exposes exact lookup, and
maintains computed-update indexes through one single-base-owner exec whose index writes may add
transaction participants. The durable RF3 exec_batch lane and gateway pgwire autocommit lane
lower ready unique and non-unique index maintenance for exact-key whole-document, direct-column,
and supported computed declared-column updates into relation-aware transaction participants. RF3
RETURNING remains fenced.

_Evidence:_ [E5](#evidence-5), [E117](#evidence-117), [E76](#evidence-76), [E120](#evidence-120)

**Qualification — Partial**

Local tests cover static capture without publication, computed simultaneous updates,
malformed-image refusal, base/postimage digest guards, unique swaps, cross-statement
delete-before-put ordering, RF3 retained computed postimages, index derivation, same-key locator
replacement, routing, lifecycle, and rollback. A mandatory external process gate churns
cross-hosted unique and non-unique RF3 indexes with two base tables across partition, leader
kill, gateway replacement, exact retry, ACK/GC, and all-voter reopen, but it uses whole-document
updates rather than computed expressions. The static computed path also lacks an external fault
gate, and required Ubuntu RF3 evidence is not yet recorded, so this remains Partial.

_Evidence:_ [E121](#evidence-121), [E122](#evidence-122), [E123](#evidence-123), [E124](#evidence-124), [E125](#evidence-125), [E126](#evidence-126), [E85](#evidence-85), [E127](#evidence-127), [E128](#evidence-128)


</details>

<details>
<summary><strong>Atomic multi-relation replicated apply</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

One replicated command can bind and atomically apply dense JSON and global-index relation
batches with one result.

_Evidence:_ [E129](#evidence-129), [E130](#evidence-130)

**Integrated — Yes**

The replicated state machine validates the relation manifest and commits all relation targets
with its system state.

_Evidence:_ [E131](#evidence-131), [E132](#evidence-132)

**Development command — Yes**

vibedb-shard prepare-rf3 creates and serve-rf3 opens an exact replicated SQL/apply bundle.
vibedb-gateway can lower exact-key mutations for multiple RF3 base-table relations into the same
participant without table or SQL strings entering Raft.

_Evidence:_ [E133](#evidence-133), [E40](#evidence-40), [E76](#evidence-76)

**Qualification — Partial**

Deterministic apply, replay, malformed-command, failure-atomic, multi-table lowering, RF3
global-index lowering, exact same-key locator replacement, and deterministic transaction-refusal
normalization tests exist. A mandatory external process gate issues 60 insert, update, and
delete mutations over two tables, two data groups, and four cross-hosted index relations while
injecting a bidirectional Raft peer-network partition with the isolated process live, leader
kill, gateway replacement, exact outcome-unknown retry, ACK/GC, and every-voter reopen. Exact
final base cardinality and stale/new index visibility are checked, but the updates are
whole-document rather than computed. Qualification remains Partial until Ubuntu records three
unskipped runs.

_Evidence:_ [E134](#evidence-134), [E127](#evidence-127), [E135](#evidence-135), [E136](#evidence-136), [E128](#evidence-128)


</details>

<details>
<summary><strong>RF3 proposal serving and exact retry</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

Bounded proposal admission, Raft quorum, durable committed apply, settlement waiters, and
request identity exist.

_Evidence:_ [E137](#evidence-137), [E138](#evidence-138)

**Integrated — Yes**

Authenticated peer runtime, replicated shard service, and native gateway executor form a
complete internal RF3 path.

_Evidence:_ [E139](#evidence-139), [E140](#evidence-140)

**Development command — Yes**

vibedb-shard prepare-rf3 atomically creates one stable three-voter member root. serve-rf3 opens
either that singleton manifest or 1..64 retained group bundles, routes each group through one of
1..64 deterministic execution lanes (default 8), shares one authenticated per-peer transport
across all lanes, and serves the authenticated native endpoint. With -reload-prepared-groups,
SIGHUP may append already prepared groups from the same manifest while preserving retained
bundles, process configuration, and roster; removal, replacement, enrolled targets,
identity/path reuse, drift, and more than 64 groups are refused. Group-scoped snapshot and
replica-control services support multi-group enrolled targets over shared physical listeners.
The manifest bound is per process, not a transaction participant or cluster-wide shard limit.

_Evidence:_ [E133](#evidence-133), [E12](#evidence-12), [E141](#evidence-141)

**Qualification — Partial**

Preparation and manifest gates prove restartable artifact publication, overwrite refusal,
canonical multi-group parsing, group-scaled serving bounds, explicit power-of-two lane counts,
and append-only reload validation. A checked-in composition gate with three processes proves
retained-state opening, mutual TLS, natural election, authenticated reads, and clean process
shutdown. Internal gates additionally enumerate all eight three-voter reachability masks, prove
majority commit and minority fail-closed behavior, benchmark execution-lane scaling and
hot-group isolation, and cover follower catch-up, response loss, exact retry, and
acknowledged-result survival. TestGatewayDurableRF3ExternalProcessRecovery adds a checked-in
multi-group process gate: each of three shard processes hosts four independent RF3 groups while
stopped and killed voters, exact terminal and ACK retries, gateway replacement, rolling
restarts, and hard p99, RSS, allocation, public-client-wire, and snapshot-payload bounds are
exercised. It does not measure total network bytes. Qualification remains Partial because
external live-SIGHUP reload, exhaustive quorum/apply cuts, and 64-group process scaling remain
absent.

_Evidence:_ [E142](#evidence-142), [E143](#evidence-143), [E144](#evidence-144), [E145](#evidence-145), [E146](#evidence-146), [E147](#evidence-147), [E62](#evidence-62), [E148](#evidence-148), [E149](#evidence-149), [E150](#evidence-150), [E151](#evidence-151), [E152](#evidence-152), [E21](#evidence-21)


</details>

<details>
<summary><strong>Development cluster and Kubernetes test tooling</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

A local RF1 development/no-HA or RF3 process orchestrator, deterministic Helm-free Kubernetes
manifest renderer, and secure disposable-cluster authority generator exist.

_Evidence:_ [E153](#evidence-153), [E154](#evidence-154), [E155](#evidence-155)

**Integrated — Yes**

The local command generates credentials, policy, WAL and shared ACK material, portable schema
witnesses, distinct retained catalog, request-ledger, and data roots, a strict canonical
replica-control inventory, and a private mutable route-seed path for the gateway. Every serving
process has one unambiguous authenticated NodeID/control listener. RF3 supervises three voters
per role plus one gateway and enables catalog-authorized hot-shard split admission. It publishes
no replica-move candidates because the dev topology has no certified cold target host. RF1
supervises one no-HA member per role without a gateway. Catalog genesis stores one immutable
proof atomically with the first head and witness; the route seed advances independently of
immutable genesis and only from authenticated certified heads. The Kubernetes lane composes
stable DNS, PVCs, disruption budgets, shard and gateway StatefulSets, and a scale-zero learner
bootstrap template.

_Evidence:_ [E107](#evidence-107), [E154](#evidence-154)

**Development command — Yes**

vibedb cluster dev --replicas 1&#124;3 starts or resumes the same three-role topology. RF1 has
three single-voter role processes and no gateway. RF3 has nine retained members with distinct
NodeIDs across independent catalog, request-ledger, and data groups plus one gateway; it passes
generated authenticated split-only replica-control and hot-shard capacity manifests plus a
private route-seed path derived by appending `.route-seed` to the catalog path. vibedb-operator
bootstrap, render, and prepare provide crash-resumable test authority, deterministic Kubernetes
manifests, and idempotent ordinal preparation. The renderer is not a reconciliation watch-loop,
and Kubernetes DNS is discovery rather than leader or topology authority.

_Evidence:_ [E156](#evidence-156), [E157](#evidence-157), [E158](#evidence-158), [E159](#evidence-159)

**Qualification — Partial**

Tests prove canonical local-cluster resume, three independent apply roles and NodeIDs, canonical
replica-control generation, portable replica schema witnesses, ledger-only capacity, data-only
table publication, explicit RF1/RF3 validation, production-policy compatibility, child reaping,
distinct loopback endpoints, deterministic Kubernetes output, secure bootstrap recovery, and
injection rejection. One mandatory Linux gate drives authenticated durable pressure through the
zero-config process topology and requires a serving child split under hard resource bounds. A
separate no-skip three-worker Kind gate now requires stable DNS, ten retained PVC identities,
one acknowledged write, every RF3 and gateway ordinal rolling restart, exact terminal retry, row
visibility, and hard p99, maximum-latency, RSS, apparent-storage, and WAL bounds. The claim
remains Partial until these new gates pass CI. There is no retained prior-binary fixture or
negotiated mixed-format protocol, so no honest rolling-format compatibility gate can be
constructed yet; the Kubernetes lane also does not prove gateway HA, involuntary partitions,
cloud volumes, or production PKI lifecycle.

_Evidence:_ [E160](#evidence-160), [E161](#evidence-161), [E162](#evidence-162), [E163](#evidence-163), [E164](#evidence-164), [E165](#evidence-165), [E166](#evidence-166), [E167](#evidence-167), [E168](#evidence-168)


</details>

<details>
<summary><strong>Automatic Raft WAL retention</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

Checkpoint-bound WAL generation preparation, authenticated selection, publication, settlement,
and old-generation replacement exist.

_Evidence:_ [E169](#evidence-169), [E170](#evidence-170)

**Integrated — Yes**

Each RF3 runtime captures and prepares generation authority on its owner lane, builds one
immutable candidate on a bounded background worker, revalidates the checkpoint before
publication, and retries post-selection settlement without blocking Raft progress.

_Evidence:_ [E171](#evidence-171), [E172](#evidence-172)

**Development command — Yes**

serve-rf3 enables checkpoint-driven WAL generation maintenance on a fixed logical cadence and
settles an authenticated selecting generation before runtime adoption after restart.

_Evidence:_ [E173](#evidence-173), [E171](#evidence-171)

**Qualification — Partial**

Repeated generation, idle, restart, selected-generation recovery, and blocked-build progress
tests exist. A mandatory external RF3 crash loop restarts every replica across three cycles and
enforces retained-WAL growth, retained/live ratio, waiter, RSS, descriptor, p99, and
maximum-latency bounds. Qualification remains Partial because the external gate covers one fixed
three-voter crash-loop profile rather than exhaustive WAL or device fault cuts or mixed-build
upgrades.

_Evidence:_ [E174](#evidence-174), [E175](#evidence-175)


</details>

<details>
<summary><strong>Bounded online storage compaction</strong> — Primitive:Yes · Integrated:Yes · Command:No · Qualification:Partial</summary>

**Primitive — Yes**

The durable engine migrates one immutable generation through authenticated adaptive staging
extents, rebuilds exact indexes, conditionally swaps the serving root, and retires source,
catalog, scratch, chain, and manifest extents without a separate destination collection file.

_Evidence:_ [E176](#evidence-176), [E177](#evidence-177)

**Integrated — Yes**

Collection lifecycle owns explicit crash resume, exact-root reopen, and source retirement.
Compaction is single-flight and rejects checkpoint-group ownership. Checkpoint-group seeding
cannot attach during an active compaction, and reservation, growth, and retirement recheck
ownership.

_Evidence:_ [E176](#evidence-176)

**Development command — No**

The public low-level durable API exposes explicit online compaction. The RF3 preparation and
serving commands do not select whole-file compaction as an automatic maintenance policy.

**Qualification — Partial**

Crash/reopen, opaque and overflow values, exact-index rebuild, atomic retirement, hard
apparent/allocated/accounted-payload amplification, and foreground-write p99 gates exist. The
accounted device payload omits direct 4 KiB manifest-slot rewrites and is not exact physical
device write amplification. There is no checked-in RF3 compaction-under-load gate.

_Evidence:_ [E178](#evidence-178), [E179](#evidence-179), [E180](#evidence-180), [E181](#evidence-181), [E182](#evidence-182), [E183](#evidence-183)


</details>

<details>
<summary><strong>Live RF3 backup export</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

A canonical certificate binds the complete catalog inventory to exact per-group Raft cuts and
verified snapshot artifacts. The bounded repository streams operation-scoped drafts and
publishes the certificate last without a second artifact copy.

_Evidence:_ [E184](#evidence-184), [E185](#evidence-185), [E186](#evidence-186)

**Integrated — Yes**

The backup controller reads one immutable catalog inventory, resolves and rechecks each
authenticated group leader, obtains target-free linearizable exports, persists the complete
vector, and conditionally advances one catalog-RF3 operation. Backup capability is independent
of data, topology, membership, and serving authority.

_Evidence:_ [E187](#evidence-187), [E188](#evidence-188), [E189](#evidence-189)

**Development command — Yes**

vibedb-shard serve-rf3 exposes the authenticated target-free backup service. vibedb-gateway
serve can open a bounded server-local repository and accepts exact canonical backup and
backup_status requests with stable idempotency identity. It rejects static catalog, plaintext,
relative paths, incomplete replica control, and missing backup authority.

_Evidence:_ [E40](#evidence-40), [E5](#evidence-5), [E190](#evidence-190)

**Qualification — Partial**

Canonical grammar, capability isolation, exact inventory, corruption, wrong-size/hash, partition
deadline, repository crash boundaries, partial-draft cleanup, external exit before certificate
publication, and no-second-copy collection tests exist. The deterministic exporter reports
logical artifact bytes separately from its two scans. There is no mandatory checked-in
multi-process foreground-load, leader-loss, retention-release, or restore-readback gate, so
qualification remains Partial.

_Evidence:_ [E191](#evidence-191), [E192](#evidence-192), [E193](#evidence-193), [E194](#evidence-194), [E195](#evidence-195)


</details>

<details>
<summary><strong>Fresh-identity RF3 restore activation</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

Canonical restore operations bind complete certified backup inventories to fresh cryptographic
RF3 group, member, node, and store identities. Authority-free imports support exact singleton
and base-plus-global-index relation bundles. The sealed schema set binds every group schema,
fresh generation-one catalog, and exact policy. Catalog import discards all source rows and
installs only fresh head, witness, genesis, and restore-policy rows. Root witnesses and
activation permits bind the complete target set without copying source serving authority.

_Evidence:_ [E196](#evidence-196), [E197](#evidence-197), [E198](#evidence-198), [E199](#evidence-199), [E200](#evidence-200)

**Integrated — Yes**

Restore validates the source artifact against the source machine schema and independently
derives the fresh target machine manifest, rather than reusing the logical SQL digest. A bounded
source-verifying rehash computes unchanged row images under fresh validation profiles. The
catalog uses its fresh projection instead. The gateway installs every exact group, publishes a
one-time target-catalog RF3 CAS witness through a narrowly fenced activation-only path,
separately observes it with ReadIndex, and installs the complete transient per-replica grant
vector. Dedicated restore_activate authority, authenticated current-incarnation binding, and
immutable restored-root fencing remain required after restart.

_Evidence:_ [E201](#evidence-201), [E202](#evidence-202), [E203](#evidence-203), [E204](#evidence-204), [E205](#evidence-205), [E206](#evidence-206), [E207](#evidence-207), [E208](#evidence-208)

**Development command — Yes**

vibedb-operator restore-group constructs three exact non-serving roots from a supplied
authenticated operation, artifact, and canonical target-schema set. adopt-restore invokes
certified staged-WAL adoption and publishes the manifest last. serve-rf3 enforces immutable
restored-root fencing and authenticates transient grants. vibedb-gateway restore-activate
consumes one bounded canonical manifest, requires the sealed fresh target catalog, resumes the
complete root set, uses real target-catalog RF3 sessions, and installs every serving grant.
Operation assembly still uses explicit builder APIs, and production PKI provisioning is not
included.

_Evidence:_ [E209](#evidence-209), [E210](#evidence-210), [E40](#evidence-40), [E211](#evidence-211), [E212](#evidence-212)

**Qualification — Partial**

Tests cover fresh identity, canonical corruption refusal, complete-root certification, every
catalog and serving publication cut, exact schema and relation-manifest binding, source-catalog
sanitation, staged-WAL adoption, dedicated authorization, response-loss settlement, and
process-local grant fencing. A mandatory external process gate boots six real serve-rf3
processes for independent catalog and base/global-index data groups, invokes the actual
restore-activate command, proves pre-grant and missing-marker refusal, observes real
target-catalog RF3 activation with ReadIndex, verifies fresh catalog and restored base/index
data, preserves an acknowledged write across leader SIGKILL, and requires re-observation and
regrant after restart under aggregate latency, RSS, storage, and WAL bounds. Qualification
remains Partial because cross-build schema migration and production certificate provisioning
remain unsupported.

_Evidence:_ [E213](#evidence-213), [E214](#evidence-214), [E215](#evidence-215), [E216](#evidence-216), [E217](#evidence-217), [E218](#evidence-218), [E219](#evidence-219), [E220](#evidence-220)


</details>

<details>
<summary><strong>Raft linearizable and follower point reads</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

Leader reads use ReadIndex. Follower reads require explicit applied-position and serving fences.
Serving probes read a fixed-width durable authorization fence without acquiring collection
snapshots or scanning hidden rows. Successful reads preserve the admitted request fence at the
returned applied cut instead of taking a post-read probe.

_Evidence:_ [E221](#evidence-221), [E222](#evidence-222), [E223](#evidence-223), [E224](#evidence-224)

**Integrated — Yes**

Replicated table profiles bind a public table and one scalar string/number ordered placement key
to exact routes. The gateway follows leaders, refreshes a definitely stale catalog route once,
and can select a sufficiently applied follower for the explicit follower contract. Composite and
tenant-path placement keys remain absent.

_Evidence:_ [E225](#evidence-225), [E226](#evidence-226)

**Development command — Yes**

vibedb-gateway serves canonical point get requests through the authenticated RF3 native pool
when it consumes the replicated catalog. Linearizable reads use ReadIndex. Monotonic follower
reads require the exact RouteID and applied index. The point API never falls back to SQL;
replicated-catalog query uses a separate RF3 SELECT transport.

_Evidence:_ [E227](#evidence-227), [E228](#evidence-228)

**Qualification — Partial**

Command-boundary tests cover canonical decoding, typed results, authorization, no SQL fallback,
coalesced catalog refresh, follower selection, route mismatch, serving fences, successful-read
no-post-probe behavior, duplicate-release-safe aggregate response reservations, direct
zero-allocation response streaming, and blocked-client write timeout.
TestGatewayDurableRF3ExternalProcessRecovery issues public-gateway linearizable point reads
while one shard process is SIGSTOP-partitioned, requires two-voter leaders for all four groups,
verifies post-commit readback, and includes the reads in bounded failover and foreground p99
evidence. Qualification remains Partial because external follower-staleness latency and
exhaustive partition cuts remain absent.

_Evidence:_ [E229](#evidence-229), [E230](#evidence-230), [E231](#evidence-231), [E21](#evidence-21)


</details>

<details>
<summary><strong>Learner promotion, removal, and leader transfer</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

ConfChange, learner roles, durable promotion evidence, removal, and leader transfer exist in the
Raft runtime.

_Evidence:_ [E232](#evidence-232), [E233](#evidence-233)

**Integrated — Yes**

The durable move controller owns learner add, snapshot/catch-up waits, promotion, conditional
leader transfer, ownership publication, two catalog-drain fences, source removal, retirement,
grant finalization, and restart resume. Certified failures across independent groups are
admitted as one atomic catalog operation set before any learner action begins.

_Evidence:_ [E234](#evidence-234), [E235](#evidence-235), [E236](#evidence-236)

**Development command — Yes**

serve-rf3 exposes authenticated membership, observation, snapshot-source, ownership, and
retirement control. vibedb-gateway starts the resumable move controller when a strict
replica-control manifest is present.

_Evidence:_ [E40](#evidence-40), [E237](#evidence-237), [E5](#evidence-5)

**Qualification — Partial**

Deterministic action tests cover the complete ordered lifecycle and real hosts cover
authenticated transfer with continued apply. A mandatory Linux CI gate runs three physical
voters, one shared-listener cold target, and one checked-in gateway command across two
independent groups; it requires atomic move-set discovery, snapshot bootstrap, catch-up,
promotion, catalog publication, source removal, controller SIGKILL/restart, certified cleanup,
non-rejoin, and hard admission, p50/p99/max, network, storage, WAL, RSS, and cleanup bounds. The
claim remains Partial until that new gate passes CI; failure replacement correctly retains the
already-elected live leader, while planned live-source moves exercise conditional leader
transfer separately.

_Evidence:_ [E238](#evidence-238), [E239](#evidence-239), [E240](#evidence-240)


</details>

<details>
<summary><strong>Online snapshot artifact transfer</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

A bounded authenticated repository, resumable chunk protocol, descriptor identity, and artifact
verification exist.

_Evidence:_ [E241](#evidence-241), [E242](#evidence-242)

**Integrated — Yes**

Artifact production, authenticated transport, crash-safe empty-learner activation,
exact-incarnation adoption, Multi-Raft host addition, catch-up observation, and later promotion
are composed by the durable move controller.

_Evidence:_ [E243](#evidence-243), [E234](#evidence-234)

**Development command — Yes**

serve-rf3 exposes authenticated bounded, group-scoped source-control and snapshot-data
listeners. bootstrap-rf3 receives, verifies, installs, and resumes one or more cold learners
through one physical NodeID/control listener before reopening every installed group through the
ordinary serving command.

_Evidence:_ [E40](#evidence-40), [E244](#evidence-244), [E245](#evidence-245)

**Qualification — Partial**

Resume, disconnect, corruption, bounds, TLS rotation, activation-seam fault settlement,
post-Host-add rejection reopen, exact-incarnation retry, and chunk benchmarks exist. Target
artifacts use a crash-safe authenticated publish-to-delete transition after learner
certification. Completed source exports are released only after the durable target-install
witness. Abandoned stages require a canonical catalog-RF3 cancellation witness with the exact
operation, step, artifact, source owner epoch, expired lease revision, target incarnation, and
schema/replica generations. A bounded gateway scheduler routes that witness to the exact source,
and repository tests cover staged and published crash cuts, reopen, idempotent replay,
slow-transfer protection, byte ceilings, and retained-byte failure. A Linux-only external
multi-process gate now spans two group-scoped learner transfers over shared physical listeners,
atomic catalog admission, real catalog cancellation, authenticated source routing, repository
rename and unlink crash cuts, target/source/gateway restart, exact cleanup convergence, and hard
latency, RSS, network, WAL, and storage bounds. CI requires three unskipped passes;
qualification remains Partial until that gate passes there.

_Evidence:_ [E246](#evidence-246), [E247](#evidence-247), [E248](#evidence-248), [E249](#evidence-249), [E240](#evidence-240), [E250](#evidence-250), [E251](#evidence-251)


</details>

<details>
<summary><strong>Automatic split and replica-move execution</strong> — Primitive:Yes · Integrated:Yes · Command:Partial · Qualification:Partial</summary>

**Primitive — Yes**

Durable split intent, source capture, immutable artifact construction, tail translation,
ownership seal, child staging, RF3 readiness, catalog publication, retained pruning, move plans,
failure authorization, and replica-move execution exist. Child image and global-index placement
accumulators provide constant-size cut proofs without rescanning relations at cutover.

_Evidence:_ [E252](#evidence-252), [E253](#evidence-253), [E254](#evidence-254), [E255](#evidence-255), [E256](#evidence-256), [E234](#evidence-234)

**Integrated — Yes**

For the base-relation split path, the catalog RF3 journal and shard-local durable runtime
reconstruct source and child observations, exact action grants, plan admission, capture,
artifact, stage, tail, seal, activation, publication, and retained pruning. Pure planning
precedes catalog admission; the serving controller persists a pending preparation step before
remote child preparation and settles exact receipts before runtime admission. Restart reuses
committed intent and receipts without preparing activated or terminal operations again.
Global-index relation snapshot partitioning, tail replay, and retained pruning are not
integrated. The replica-move path composes failure evidence, candidate selection, membership
grants, snapshot bootstrap, catch-up, promotion, catalog drains, removal, and cleanup.

_Evidence:_ [E257](#evidence-257), [E258](#evidence-258), [E259](#evidence-259), [E260](#evidence-260), [E261](#evidence-261), [E262](#evidence-262)

**Development command — Partial**

With a strict replica-control manifest, vibedb-gateway reconciles split operations and durable
replica moves, and the development path provides hot-split intake. serve-rf3 uses exact
per-group child registries and a durable process-wide admission bound. Child SQL preparation
supports authenticated base/local/global bundles and partial-schema restart; gateway
split_sources carries actual source SQL, local-index definitions, immutable placement, and exact
per-source templates with no fallback. Online globally indexed split plans still fail closed:
the current artifact and tail path is base-only, and retained pruning rejects index relations.
First composed serving-split qualification and repeated descendant discovery, fencing, and
restart integration remain pending. Retained-source ownership fencing remains enforced. There is
no general operator split CLI.

_Evidence:_ [E263](#evidence-263), [E264](#evidence-264), [E265](#evidence-265), [E5](#evidence-5), [E266](#evidence-266), [E267](#evidence-267), [E40](#evidence-40), [E268](#evidence-268), [E237](#evidence-237), [E269](#evidence-269)

**Qualification — Partial**

Internal crash matrices cover replicated-journal recovery, durable source capture and seal,
child stage and tail retry, publication-before-prune, and post-cutover ownership fencing. Tests
cover exact group registry selection and shared operation admission, quorum readiness, O(1)
child seal/activation, and constant-size global-index ownership proof. Committed-preparation
tests cover lost receipts, forged cursors, and restart after receipts before settlement.
Mandatory Linux process gates define split-under-load leader-kill, partition, reopen, p99, RSS,
storage, WAL, exact public-wire, and snapshot-payload requirements; current failing or pending
runs do not qualify the first serving split or repeated descendants. The initial partition is
one bounded scan, and crash recovery may audit the sealed image. Qualification remains Partial
until required unskipped Ubuntu runs pass. Range-scan routing proof remains absent.

_Evidence:_ [E270](#evidence-270), [E271](#evidence-271), [E272](#evidence-272), [E273](#evidence-273), [E274](#evidence-274), [E275](#evidence-275), [E276](#evidence-276), [E277](#evidence-277), [E278](#evidence-278), [E279](#evidence-279), [E280](#evidence-280), [E162](#evidence-162)


</details>

<details>
<summary><strong>Replicated catalog and distributed DDL</strong> — Primitive:Yes · Integrated:Yes · Command:Yes · Qualification:Partial</summary>

**Primitive — Yes**

A dedicated RF3 catalog authority provides linearizable catalog heads, an atomic immutable
generation-one proof, sealed authenticated head receipts, crash-safe mutable route-seed staging
and promotion, and bounded resumable operation records. Exact schema rollout primitives prepare
immutable shard bundles, bind per-group receipts, authorize one catalog cut, activate it, drain
the prior generation, and support pre-activation abort.

_Evidence:_ [E281](#evidence-281), [E282](#evidence-282), [E283](#evidence-283), [E284](#evidence-284), [E285](#evidence-285)

**Integrated — Yes**

Route-seed control installation performs one attested catch-up read before serving. After
installation, every subsequent authenticated catalog read and publication crosses one
certified-head observer before holder exposure. Byte-identical heads avoid disk I/O; newer
exact-self-route heads promote live. Any catalog self-route change durably stages the candidate,
seals authority, signals shutdown, and requires quiescence plus old-session Retire, Release,
journal destruction, and candidate promotion before restart. Catalog publication, topology
journals, schema rollout records, shard installers, and authenticated control services retain
exact catalog, group, relation-manifest, and contract digests.

_Evidence:_ [E286](#evidence-286), [E287](#evidence-287), [E288](#evidence-288), [E289](#evidence-289), [E40](#evidence-40), [E290](#evidence-290)

**Development command — Yes**

Replicated gateway mode requires an immutable -catalog genesis and a distinct private
-catalog-route-seed. On a certified catalog self-route change, runServe drains public and
control users, settles the old native session, promotes the staged seed, and exits with the
typed restart-required error for supervisor restart; startup resumes every pending/journal crash
cut. vibedb cluster dev automatically derives the private route-seed path by appending
`.route-seed` to the catalog path. serve-rf3 installs the authenticated schema control service.
Startup authenticates an exact committed source N+1 transition, opens only a fenced recovery
handle, settles local catalog publication, closes the source, and opens the target before
runtime adoption. The experimental schema-rollout command conditionally publishes one exact
catalog successor; it is not a general SQL DDL endpoint or a completed repeated-rollout
lifecycle.

_Evidence:_ [E291](#evidence-291), [E292](#evidence-292), [E5](#evidence-5), [E293](#evidence-293), [E107](#evidence-107), [E40](#evidence-40), [E294](#evidence-294), [E290](#evidence-290)

**Qualification — Partial**

Catalog tests cover quorum publication, leader loss, sealed-receipt rejection, same-route live
promotion, self-route-change staging, preauthorized-mutator refusal, path aliasing, near-maximum
heads, and startup recovery. The external durable RF3 gate adds distinct gateway route seeds and
catalog-voter failure. Initial route helpers distinguish logical SQL and exact machine
manifests. Schema tests cover activation, restart, pre-activation abort, mixed-old/new refusal,
authenticated control, installer reopen, command-plan validation, and leader-loss recovery.
Directory publication and retry sync fixes are implemented; local normal and race tests cover
exact committed-source recovery and settlement before runtime adoption, while the physical Linux
startup gate remains unqualified. The schema digest caller audit, post-drain replacement of
write-once rollout artifacts, and trusted retained-identity rollover for repeated DDL remain
incomplete. No external self-route-change handoff, rolling mixed-build, or SQL DDL rollback gate
exists.

_Evidence:_ [E295](#evidence-295), [E296](#evidence-296), [E297](#evidence-297), [E298](#evidence-298), [E299](#evidence-299), [E300](#evidence-300), [E301](#evidence-301), [E302](#evidence-302), [E303](#evidence-303), [E304](#evidence-304), [E21](#evidence-21), [E305](#evidence-305), [E306](#evidence-306), [E307](#evidence-307), [E308](#evidence-308), [E309](#evidence-309)


</details>

<details>
<summary><strong>Hot-shard detection and rebalancing</strong> — Primitive:Yes · Integrated:Yes · Command:Partial · Qualification:Partial</summary>

**Primitive — Yes**

Bounded pressure selection, failure-domain placement, split planning, and replica-move selection
exist.

_Evidence:_ [E310](#evidence-310), [E311](#evidence-311)

**Integrated — Yes**

Routed requests feed bounded per-allocation recorders. A collector publishes canonical pressure
cuts through catalog RF3. A clockless controller qualifies sustained pressure, selects either a
split or replica move, and hands one idempotent admission to the existing operation journals.

_Evidence:_ [E312](#evidence-312), [E56](#evidence-56), [E313](#evidence-313)

**Development command — Partial**

With -hot-shard-capacity and an authenticated replica-control manifest, vibedb-gateway records
routed pressure, publishes canonical catalog-RF3 cuts, and submits idempotent split or certified
replica-move work. Exact source SQL, local indexes, immutable placement, and separate
logical/machine schema digests support schema-bundle preparation, not completed global-index
split execution. Serving startup restores certified adopted groups from a bounded live
inventory. Catalog admission precedes remote preparation and local tests cover exact
source/catalog fences; relation-aware global-index snapshot/tail/prune, repeated descendant
capture, and gateway source-discovery qualification remain incomplete. External split and
restart qualification is still required.

_Evidence:_ [E314](#evidence-314), [E315](#evidence-315), [E5](#evidence-5), [E107](#evidence-107)

**Qualification — Partial**

Deterministic tests cover sustained-hotness qualification, logical cooldown, clock-skew refusal,
failure domains, capacity and retained-memory bounds, zero-allocation foreground intake, and
byte-identical outcome-unknown retry. Mandatory Linux external-process gates drive real
multi-table and global-index mutations through the checked-in gateway command into catalog-RF3
pressure, one automatic certified replica move across leader loss, response partition, and
process reopen, and one zero-config terminal split. They enforce hard p99, RSS,
allocated-storage, WAL-allocation, request-count, exact public client request/response
wire-byte, and snapshot-payload-byte ceilings; they do not measure total network traffic.
Qualification remains Partial until the mandatory unskipped Ubuntu evidence is recorded.

_Evidence:_ [E316](#evidence-316), [E317](#evidence-317), [E318](#evidence-318), [E319](#evidence-319), [E280](#evidence-280), [E162](#evidence-162)


</details>

## Evidence index

Evidence is deduplicated across features. Links point to source or executable tests in this
repository.

<details>
<summary>319 unique source and test references</summary>

1. <a id="evidence-1"></a>[gateway/catalog.go](../gateway/catalog.go) — `Snapshot`
2. <a id="evidence-2"></a>[gateway/executor.go](../gateway/executor.go) — `Executor`
3. <a id="evidence-3"></a>[gateway/e2e_test.go](../gateway/e2e_test.go) — `TestE2EFanoutShapes`
4. <a id="evidence-4"></a>[shardservice/server.go](../shardservice/server.go) — `Server`
5. <a id="evidence-5"></a>[internal/gatewayruntime/standalone_flags.go](../internal/gatewayruntime/standalone_flags.go) — `runServe`
6. <a id="evidence-6"></a>[cmd/vibedb-shard/main.go](../cmd/vibedb-shard/main.go) — `runServe`
7. <a id="evidence-7"></a>[gateway/executor_test.go](../gateway/executor_test.go) — `TestExecutorFailClosedOnShardError`
8. <a id="evidence-8"></a>[internal/servicetls/server.go](../internal/servicetls/server.go) — `Server`
9. <a id="evidence-9"></a>[internal/serviceauthz/policy.go](../internal/serviceauthz/policy.go) — `Policy`
10. <a id="evidence-10"></a>[gateway/client_tls.go](../gateway/client_tls.go) — `ServeAuthorizedClients`
11. <a id="evidence-11"></a>[shardservice/server.go](../shardservice/server.go) — `ServeAuthorizedConn`
12. <a id="evidence-12"></a>[cmd/vibedb-shard/serve_rf3.go](../cmd/vibedb-shard/serve_rf3.go) — `runServeRF3`
13. <a id="evidence-13"></a>[internal/serviceauthz/sql_test.go](../internal/serviceauthz/sql_test.go) — `TestSQLCapabilityRequiresCompleteParsedSemantics`
14. <a id="evidence-14"></a>[gateway/client_tls_test.go](../gateway/client_tls_test.go) — `TestClientTLSAuthenticatesAuthorizesRotatesAndSeparatesALPN`
15. <a id="evidence-15"></a>[gateway/client_tls_qualification_test.go](../gateway/client_tls_qualification_test.go) — `TestAuthorizedClientTLSNetworkChaosAndThroughputGate`
16. <a id="evidence-16"></a>[gateway/client_tls_qualification_test.go](../gateway/client_tls_qualification_test.go) — `BenchmarkAuthorizedClientTLSRequest`
17. <a id="evidence-17"></a>[gateway/shard_tls_qualification_test.go](../gateway/shard_tls_qualification_test.go) — `TestAuthenticatedShardBoundaryRotationAndConfusedDeputyFault`
18. <a id="evidence-18"></a>[gateway/shard_tls_qualification_test.go](../gateway/shard_tls_qualification_test.go) — `TestAuthenticatedGatewayShardHotAuthorizationAllocationFree`
19. <a id="evidence-19"></a>[gateway/shard_tls_qualification_test.go](../gateway/shard_tls_qualification_test.go) — `BenchmarkAuthenticatedGatewayShardStream`
20. <a id="evidence-20"></a>[gateway/shard_tls_process_qualification_test.go](../gateway/shard_tls_process_qualification_test.go) — `TestAuthenticatedGatewayShardProcessPartitionRotationAndDeputyFaults`
21. <a id="evidence-21"></a>[internal/gatewayruntime/durable_rf3_external_process_test.go](../internal/gatewayruntime/durable_rf3_external_process_test.go) — `TestGatewayDurableRF3ExternalProcessRecovery`
22. <a id="evidence-22"></a>[shardservice/authorization_test.go](../shardservice/authorization_test.go) — `TestShardAuthorizationRejectsConfusedDeputyAndSeparatesRoles`
23. <a id="evidence-23"></a>[gateway/read_snapshot.go](../gateway/read_snapshot.go) — `snapshotFanout`
24. <a id="evidence-24"></a>[gateway/replicated_sql_read.go](../gateway/replicated_sql_read.go) — `ReadSQLBatch`
25. <a id="evidence-25"></a>[gateway/replicated_query.go](../gateway/replicated_query.go) — `ReplicatedSQLTransport`
26. <a id="evidence-26"></a>[internal/gatewayruntime/serve.go](../internal/gatewayruntime/serve.go) — `newReplicatedCatalogGateway`
27. <a id="evidence-27"></a>[gateway/replicated_query.go](../gateway/replicated_query.go) — `QuerySQL`
28. <a id="evidence-28"></a>[internal/gatewayruntime/data_sql_batch.go](../internal/gatewayruntime/data_sql_batch.go) — `buildNativeSQLBatchReadRequest`
29. <a id="evidence-29"></a>[shardservice/read_fence_test.go](../shardservice/read_fence_test.go) — `TestReadFenceLeaseExpiresAndWakesWriter`
30. <a id="evidence-30"></a>[gateway/replicated_query_test.go](../gateway/replicated_query_test.go) — `TestRF3SQLReusesTargetingScatterGlobalOrderAndAggregate`
31. <a id="evidence-31"></a>[internal/gatewayruntime/data_sql_batch_test.go](../internal/gatewayruntime/data_sql_batch_test.go) — `TestReadBatchWireSixtyFiveGroupsIsBoundedAndUncapped`
32. <a id="evidence-32"></a>[internal/gatewayruntime/data_sql_batch_test.go](../internal/gatewayruntime/data_sql_batch_test.go) — `TestReadBatchWireExactByteBoundAndIntentNeverReturnPartialValues`
33. <a id="evidence-33"></a>[internal/gatewayruntime/read_batch_rf3_external_process_test.go](../internal/gatewayruntime/read_batch_rf3_external_process_test.go) — `TestGatewayReadBatchRF3ExternalProcessChaos`
34. <a id="evidence-34"></a>[internal/raftservice/progress_metrics.go](../internal/raftservice/progress_metrics.go) — `ProgressMetrics`
35. <a id="evidence-35"></a>[internal/servicemetrics/service.go](../internal/servicemetrics/service.go) — `StageMetricsSnapshot`
36. <a id="evidence-36"></a>[internal/servicemetrics/service.go](../internal/servicemetrics/service.go) — `Service`
37. <a id="evidence-37"></a>[gateway/distributed_metrics.go](../gateway/distributed_metrics.go) — `DistributedMetrics`
38. <a id="evidence-38"></a>[gateway/distributed_metrics.go](../gateway/distributed_metrics.go) — `DistributedMetricsAggregate`
39. <a id="evidence-39"></a>[internal/gatewayruntime/controller_metrics.go](../internal/gatewayruntime/controller_metrics.go) — `gatewayControllerMetrics`
40. <a id="evidence-40"></a>[cmd/vibedb-shard/serve_rf3.go](../cmd/vibedb-shard/serve_rf3.go) — `servePreparedRF3`
41. <a id="evidence-41"></a>[cmd/vibedb-shard/rf3_metrics.go](../cmd/vibedb-shard/rf3_metrics.go) — `rf3MetricsProvider`
42. <a id="evidence-42"></a>[internal/gatewayruntime/serve_metrics.go](../internal/gatewayruntime/serve_metrics.go) — `writeGatewayDistributedMetrics`
43. <a id="evidence-43"></a>[internal/gatewayruntime/serve_metrics.go](../internal/gatewayruntime/serve_metrics.go) — `writeGatewayControllerMetrics`
44. <a id="evidence-44"></a>[internal/servicemetrics/service.go](../internal/servicemetrics/service.go) — `Serve`
45. <a id="evidence-45"></a>[internal/raftservice/progress_metrics_test.go](../internal/raftservice/progress_metrics_test.go) — `TestProgressMetricsCountsExistingOwnerSeamsExactly`
46. <a id="evidence-46"></a>[internal/servicemetrics/service_test.go](../internal/servicemetrics/service_test.go) — `TestMetricsNodeStageSnapshotIsAuthenticatedAndCanonical`
47. <a id="evidence-47"></a>[gateway/distributed_metrics_test.go](../gateway/distributed_metrics_test.go) — `TestDistributedMetricsAuthenticatedExactGroupRefresh`
48. <a id="evidence-48"></a>[gateway/distributed_metrics_test.go](../gateway/distributed_metrics_test.go) — `BenchmarkDistributedMetricsSnapshotInto`
49. <a id="evidence-49"></a>[internal/gatewayruntime/controller_metrics_test.go](../internal/gatewayruntime/controller_metrics_test.go) — `TestGatewayControllerMetricsObserveActualPasses`
50. <a id="evidence-50"></a>[internal/gatewayruntime/serve_metrics_test.go](../internal/gatewayruntime/serve_metrics_test.go) — `BenchmarkWriteGatewayDistributedMetrics`
51. <a id="evidence-51"></a>[internal/raftmodel/config.go](../internal/raftmodel/config.go) — `NewConfig`
52. <a id="evidence-52"></a>[internal/executionpin/command.go](../internal/executionpin/command.go) — `Command`
53. <a id="evidence-53"></a>[internal/distributedtxn/journal.go](../internal/distributedtxn/journal.go) — `Journal`
54. <a id="evidence-54"></a>[gateway/recovery.go](../gateway/recovery.go) — `recoverCoordinator`
55. <a id="evidence-55"></a>[gateway/replicated_request_execution_context.go](../gateway/replicated_request_execution_context.go) — `BuildDurableRequestExecutionPinBinding`
56. <a id="evidence-56"></a>[internal/hotshard/controller.go](../internal/hotshard/controller.go) — `Controller`
57. <a id="evidence-57"></a>[internal/gatewayruntime/serve.go](../internal/gatewayruntime/serve.go) — `execRequest`
58. <a id="evidence-58"></a>[internal/rafttransport/identity.go](../internal/rafttransport/identity.go) — `PeerTLS`
59. <a id="evidence-59"></a>[internal/rafttransport/clock_fault_matrix_test.go](../internal/rafttransport/clock_fault_matrix_test.go) — `TestPeerTLSIndependentUTCStepMatrix`
60. <a id="evidence-60"></a>[gateway/recovery_test.go](../gateway/recovery_test.go) — `TestRecoveryManifestMissingPageRequiresLogicalPulsesAcrossRestart`
61. <a id="evidence-61"></a>[internal/raftservice/owner_rf3_multigroup_transaction_test.go](../internal/raftservice/owner_rf3_multigroup_transaction_test.go) — `TestTwoRealRF3GroupsExecuteFusedTwoTargetTransactionAcrossLeaderIsolation`
62. <a id="evidence-62"></a>[internal/raftservice/process_rf3_test.go](../internal/raftservice/process_rf3_test.go) — `TestRF3NativeServingThreeProcessRecoveryEvidence`
63. <a id="evidence-63"></a>[cmd/vibedb-shard/serve_rf3_fault_process_test.go](../cmd/vibedb-shard/serve_rf3_fault_process_test.go) — `TestServeRF3ShippedFaultHarness`
64. <a id="evidence-64"></a>[internal/distributedtxn/manifest.go](../internal/distributedtxn/manifest.go) — `ManifestBuilder`
65. <a id="evidence-65"></a>[gateway/transaction.go](../gateway/transaction.go) — `executeTransaction`
66. <a id="evidence-66"></a>[gateway/transaction_manifest.go](../gateway/transaction_manifest.go) — `stageTransactionCoordinator`
67. <a id="evidence-67"></a>[gateway/recovery.go](../gateway/recovery.go) — `recoverManifestCoordinator`
68. <a id="evidence-68"></a>[gateway/executor.go](../gateway/executor.go) — `Exec`
69. <a id="evidence-69"></a>[internal/gatewayruntime/serve_test.go](../internal/gatewayruntime/serve_test.go) — `TestServeGatewayStaticWriteSurface`
70. <a id="evidence-70"></a>[internal/distributedtxn/journal.go](../internal/distributedtxn/journal.go) — `PulseCoordinator`
71. <a id="evidence-71"></a>[internal/distributedtxn/journal_compact.go](../internal/distributedtxn/journal_compact.go) — `compactedBytesLocked`
72. <a id="evidence-72"></a>[gateway/segmented_e2e_test.go](../gateway/segmented_e2e_test.go) — `TestSegmentedExecBatchAcross65RealShardServers`
73. <a id="evidence-73"></a>[gateway/segmented_e2e_test.go](../gateway/segmented_e2e_test.go) — `TestSegmentedCoordinatorResponseLossAndRestartBoundaries`
74. <a id="evidence-74"></a>[internal/distributedtxn/replicated_codec.go](../internal/distributedtxn/replicated_codec.go) — `ReplicatedOperation`
75. <a id="evidence-75"></a>[internal/replicatedstate/transaction_apply.go](../internal/replicatedstate/transaction_apply.go) — `planCoordinatorBeginPrepare`
76. <a id="evidence-76"></a>[gateway/replicated_sql_transaction.go](../gateway/replicated_sql_transaction.go) — `planReplicatedSQLTransaction`
77. <a id="evidence-77"></a>[gateway/replicated_request_transaction_runner.go](../gateway/replicated_request_transaction_runner.go) — `DurableRequestDistributedRunner`
78. <a id="evidence-78"></a>[gateway/replicated_request_lifecycle_runner.go](../gateway/replicated_request_lifecycle_runner.go) — `RunWave`
79. <a id="evidence-79"></a>[gateway/replicated_transaction_protocol.go](../gateway/replicated_transaction_protocol.go) — `replicatedTransactionCommandEncoder`
80. <a id="evidence-80"></a>[gateway/durable_sql_request_executor.go](../gateway/durable_sql_request_executor.go) — `Execute`
81. <a id="evidence-81"></a>[internal/gatewayruntime/durable_request_adapter.go](../internal/gatewayruntime/durable_request_adapter.go) — `ExecBatch`
82. <a id="evidence-82"></a>[gateway/replicated_sql_transaction_test.go](../gateway/replicated_sql_transaction_test.go) — `TestReplicatedSQLFlatInsertUsesCanonicalRuntimeDocuments`
83. <a id="evidence-83"></a>[gateway/replicated_sql_transaction_test.go](../gateway/replicated_sql_transaction_test.go) — `TestReplicatedSQLFlatInsertRoutesAcrossDataShards`
84. <a id="evidence-84"></a>[gateway/replicated_sql_transaction_test.go](../gateway/replicated_sql_transaction_test.go) — `TestReplicatedSQLComputedUpdateRetainsCanonicalExactCAS`
85. <a id="evidence-85"></a>[gateway/replicated_sql_transaction_test.go](../gateway/replicated_sql_transaction_test.go) — `TestReplicatedSQLComputedPostimageIsOwnedByDurableLogicalProgram`
86. <a id="evidence-86"></a>[gateway/durable_sql_request_executor_test.go](../gateway/durable_sql_request_executor_test.go) — `TestDurableSQLComputedUpdateRetryRecoversRetainedProgramAfterReevaluationError`
87. <a id="evidence-87"></a>[internal/replication/transaction_perf_contract_test.go](../internal/replication/transaction_perf_contract_test.go) — `TestReplicatedTransactionEncodedSchedulePerformanceTargets`
88. <a id="evidence-88"></a>[internal/raftservice/owner_rf3_request_ledger_test.go](../internal/raftservice/owner_rf3_request_ledger_test.go) — `TestTwoGatewayDurableSQLRF3RecoversTerminalAndAckAcrossLeaderPartitions`
89. <a id="evidence-89"></a>[gateway/replicated_request_ledger_fault_test.go](../gateway/replicated_request_ledger_fault_test.go) — `TestDurableRequestReplacementRecoversEveryReplicatedBoundary`
90. <a id="evidence-90"></a>[internal/replicatedstate/transaction_recovery_read.go](../internal/replicatedstate/transaction_recovery_read.go) — `TransactionRecoveryReadInto`
91. <a id="evidence-91"></a>[internal/replicatedstate/transaction_recovery_read.go](../internal/replicatedstate/transaction_recovery_read.go) — `TransactionRecoveryReadRequest`
92. <a id="evidence-92"></a>[gateway/replicated_transaction_recovery.go](../gateway/replicated_transaction_recovery.go) — `ReadTransactionRecovery`
93. <a id="evidence-93"></a>[gateway/replicated_request_transaction_runner.go](../gateway/replicated_request_transaction_runner.go) — `recoverManifestDescriptor`
94. <a id="evidence-94"></a>[gateway/durable_sql_request_executor.go](../gateway/durable_sql_request_executor.go) — `Replay`
95. <a id="evidence-95"></a>[gateway/replicated_request_service.go](../gateway/replicated_request_service.go) — `Acknowledge`
96. <a id="evidence-96"></a>[internal/raftservice/owner_rf3_transaction_test.go](../internal/raftservice/owner_rf3_transaction_test.go) — `TestRF3TransactionRecoveryReadIsLeaderOnlyAndSurvivesGatewayReplacement`
97. <a id="evidence-97"></a>[gateway/replicated_request_ledger_fault_test.go](../gateway/replicated_request_ledger_fault_test.go) — `TestDurableRequestLostResponseConvergesWhenAnotherGatewayAdvancesAhead`
98. <a id="evidence-98"></a>[internal/requestledger/command.go](../internal/requestledger/command.go) — `Command`
99. <a id="evidence-99"></a>[gateway/replicated_request_ledger.go](../gateway/replicated_request_ledger.go) — `DurableRequestLedgerTopology`
100. <a id="evidence-100"></a>[gateway/replicated_request_service.go](../gateway/replicated_request_service.go) — `NewDurableRequestService`
101. <a id="evidence-101"></a>[gateway/replicated_request_lifecycle_runner.go](../gateway/replicated_request_lifecycle_runner.go) — `NewDurableRequestLifecycleRunner`
102. <a id="evidence-102"></a>[gateway/replicated_request_issuer_collector.go](../gateway/replicated_request_issuer_collector.go) — `NewDurableIssuerHighwaterCollector`
103. <a id="evidence-103"></a>[sql/driver/replicated_sidecars.go](../sql/driver/replicated_sidecars.go) — `canonicalReplicatedApplySidecarsForLimits`
104. <a id="evidence-104"></a>[sql/driver/replicated_sidecars.go](../sql/driver/replicated_sidecars.go) — `ReplicatedUserRecoveryJournalBytes`
105. <a id="evidence-105"></a>[internal/storeio/recovery_journal.go](../internal/storeio/recovery_journal.go) — `RecoveryJournalMaxCapacityBytes`
106. <a id="evidence-106"></a>[internal/storeio/recovery_journal_conditional_test.go](../internal/storeio/recovery_journal_conditional_test.go) — `TestRecoveryJournalReplicatedLedgerConditionalCeiling`
107. <a id="evidence-107"></a>[cmd/vibedb/cluster_dev.go](../cmd/vibedb/cluster_dev.go) — `ensureDevCluster`
108. <a id="evidence-108"></a>[internal/gatewayruntime/durable_request_runtime.go](../internal/gatewayruntime/durable_request_runtime.go) — `newReplicatedDurableRuntime`
109. <a id="evidence-109"></a>[internal/gatewayruntime/serve.go](../internal/gatewayruntime/serve.go) — `handleConnPolicyDurable`
110. <a id="evidence-110"></a>[gateway/replicated_request_ledger_catalog_test.go](../gateway/replicated_request_ledger_catalog_test.go) — `TestRequestLedgerTopologyCatalogRoundTripAndExactRouteBinding`
111. <a id="evidence-111"></a>[gateway/replicated_request_ledger_fault_test.go](../gateway/replicated_request_ledger_fault_test.go) — `TestDurableRequestConcurrentGatewaysConvergeOnOneOutcome`
112. <a id="evidence-112"></a>[gateway/replicated_request_issuer_collector_test.go](../gateway/replicated_request_issuer_collector_test.go) — `TestDurableIssuerHighwaterCollectorResolvesOutcomeUnknownAndRestart`
113. <a id="evidence-113"></a>[internal/raftservice/owner_rf3_request_ledger_test.go](../internal/raftservice/owner_rf3_request_ledger_test.go) — `TestTwoGatewayRequestLedgerRF3RecoversUnknownCreateAcrossLeaderPartition`
114. <a id="evidence-114"></a>[gateway/index_metadata.go](../gateway/index_metadata.go) — `IndexMetadata`
115. <a id="evidence-115"></a>[gateway/global_index.go](../gateway/global_index.go) — `GlobalIndexProgram`
116. <a id="evidence-116"></a>[sql/driver/mutation_capture.go](../sql/driver/mutation_capture.go) — `CaptureMutationImagesInto`
117. <a id="evidence-117"></a>[gateway/executor.go](../gateway/executor.go) — `captureIndexedMutation`
118. <a id="evidence-118"></a>[gateway/writer.go](../gateway/writer.go) — `bindGlobalIndexCapture`
119. <a id="evidence-119"></a>[gateway/transaction.go](../gateway/transaction.go) — `sortTransactionTargets`
120. <a id="evidence-120"></a>[gateway/upsert_conflict_test.go](../gateway/upsert_conflict_test.go) — `TestPostgreSQLRF3PreparesComputedUpdateAndKeepsReturningFenced`
121. <a id="evidence-121"></a>[sql/driver/mutation_capture_images_test.go](../sql/driver/mutation_capture_images_test.go) — `TestMutationImageCaptureEvaluatesComputedUpdateOnceAndPublishesNothing`
122. <a id="evidence-122"></a>[shardservice/mutation_capture_images_test.go](../shardservice/mutation_capture_images_test.go) — `TestMutationCaptureWireReturnsExactImagesWithoutPublication`
123. <a id="evidence-123"></a>[gateway/global_index_test.go](../gateway/global_index_test.go) — `TestComputedUpdateGlobalIndexCaptureUsesShardPostimage`
124. <a id="evidence-124"></a>[gateway/global_index_test.go](../gateway/global_index_test.go) — `TestComputedUpdateGlobalIndexCaptureOrdersUniqueSwapDeletesBeforePuts`
125. <a id="evidence-125"></a>[gateway/global_index_test.go](../gateway/global_index_test.go) — `TestStaticTransactionOrdersGlobalIndexDeletesAcrossStatements`
126. <a id="evidence-126"></a>[gateway/replicated_sql_transaction_test.go](../gateway/replicated_sql_transaction_test.go) — `TestReplicatedSQLComputedUpdateDerivesGlobalIndexFromRetainedPostimage`
127. <a id="evidence-127"></a>[internal/replicatedstate/relation_bundle_test.go](../internal/replicatedstate/relation_bundle_test.go) — `TestGlobalDigestCompareProvesSameLocatorAndReplacesExactOldLocator`
128. <a id="evidence-128"></a>[internal/gatewayruntime/durable_rf3_multirelation_chaos_process_test.go](../internal/gatewayruntime/durable_rf3_multirelation_chaos_process_test.go) — `TestGatewayDurableRF3MultiRelationChaosProcess`
129. <a id="evidence-129"></a>[internal/replicatedstate/relation_bundle.go](../internal/replicatedstate/relation_bundle.go) — `RelationCollection`
130. <a id="evidence-130"></a>[internal/replication/command.go](../internal/replication/command.go) — `RelationBatchView`
131. <a id="evidence-131"></a>[internal/replicatedstate/machine.go](../internal/replicatedstate/machine.go) — `Machine`
132. <a id="evidence-132"></a>[internal/replicatedstate/relation_bundle_test.go](../internal/replicatedstate/relation_bundle_test.go) — `TestRelationBundleAtomicBaseLocalAndGlobalIndexApply`
133. <a id="evidence-133"></a>[cmd/vibedb-shard/prepare_rf3.go](../cmd/vibedb-shard/prepare_rf3.go) — `runPrepareRF3`
134. <a id="evidence-134"></a>[internal/replicatedstate/relation_bundle_test.go](../internal/replicatedstate/relation_bundle_test.go) — `TestRelationBundleCheckpointCrashPhasesNeverRecoverSkew`
135. <a id="evidence-135"></a>[internal/replicatedstate/transaction_apply_integration_test.go](../internal/replicatedstate/transaction_apply_integration_test.go) — `TestTransactionPrepareNormalizesMutationRefusalsToExactConflictVote`
136. <a id="evidence-136"></a>[gateway/replicated_sql_transaction_test.go](../gateway/replicated_sql_transaction_test.go) — `TestReplicatedSQLTransactionRoutesReadyGlobalIndexAsIndependentRF3Target`
137. <a id="evidence-137"></a>[internal/raftservice/owner.go](../internal/raftservice/owner.go) — `Owner`
138. <a id="evidence-138"></a>[internal/raftserve/registry.go](../internal/raftserve/registry.go) — `Waiter`
139. <a id="evidence-139"></a>[internal/raftservice/peer.go](../internal/raftservice/peer.go) — `AuthenticatedPeerRuntime`
140. <a id="evidence-140"></a>[gateway/replicated_native.go](../gateway/replicated_native.go) — `ReplicatedExecutor`
141. <a id="evidence-141"></a>[cmd/vibedb-shard/rf3_reload_groups.go](../cmd/vibedb-shard/rf3_reload_groups.go) — `reloadPreparedRF3Groups`
142. <a id="evidence-142"></a>[cmd/vibedb-shard/prepare_rf3_test.go](../cmd/vibedb-shard/prepare_rf3_test.go) — `TestPrepareRF3PublishesCompleteRestartableMemberAndReopensExactly`
143. <a id="evidence-143"></a>[cmd/vibedb-shard/rf3_manifest_test.go](../cmd/vibedb-shard/rf3_manifest_test.go) — `TestParseRF3ManifestCanonicalMultiGroupBundles`
144. <a id="evidence-144"></a>[cmd/vibedb-shard/serve_rf3_test.go](../cmd/vibedb-shard/serve_rf3_test.go) — `TestRF3ExecutionLaneCountIsExplicitPowerOfTwo`
145. <a id="evidence-145"></a>[cmd/vibedb-shard/serve_rf3_test.go](../cmd/vibedb-shard/serve_rf3_test.go) — `TestRF3MultiGroupServingLimitsCoverManifestBound`
146. <a id="evidence-146"></a>[cmd/vibedb-shard/rf3_reload_groups_test.go](../cmd/vibedb-shard/rf3_reload_groups_test.go) — `TestRF3ReloadOnlyAppendsIndependentPreparedGroups`
147. <a id="evidence-147"></a>[cmd/vibedb-shard/serve_rf3_process_test.go](../cmd/vibedb-shard/serve_rf3_process_test.go) — `TestServeRF3ShippedCompositionThreeProcesses`
148. <a id="evidence-148"></a>[internal/raftservice/owner_rf3_test.go](../internal/raftservice/owner_rf3_test.go) — `TestAuthenticatedThreeVoterServingPutSurvivesLeaderLossAndExactRetry`
149. <a id="evidence-149"></a>[internal/raftservice/owner_rf3_multigroup_transaction_test.go](../internal/raftservice/owner_rf3_multigroup_transaction_test.go) — `TestRF3AllThreeVoterQuorumCutsFailClosedOrCommit`
150. <a id="evidence-150"></a>[internal/multiraft/lanes_test.go](../internal/multiraft/lanes_test.go) — `BenchmarkExecutionLanesScaling`
151. <a id="evidence-151"></a>[internal/multiraft/lanes_test.go](../internal/multiraft/lanes_test.go) — `BenchmarkExecutionLanesSixtyFourGroups`
152. <a id="evidence-152"></a>[internal/multiraft/lanes_test.go](../internal/multiraft/lanes_test.go) — `BenchmarkExecutionLanesHotShardIsolation`
153. <a id="evidence-153"></a>[cmd/vibedb/cluster_dev.go](../cmd/vibedb/cluster_dev.go) — `runClusterDev`
154. <a id="evidence-154"></a>[internal/kubeoperator/render.go](../internal/kubeoperator/render.go) — `Render`
155. <a id="evidence-155"></a>[internal/kubeoperator/bootstrap.go](../internal/kubeoperator/bootstrap.go) — `Bootstrap`
156. <a id="evidence-156"></a>[cmd/vibedb/main.go](../cmd/vibedb/main.go) — `run`
157. <a id="evidence-157"></a>[cmd/vibedb-operator/main.go](../cmd/vibedb-operator/main.go) — `bootstrap`
158. <a id="evidence-158"></a>[cmd/vibedb-operator/main.go](../cmd/vibedb-operator/main.go) — `render`
159. <a id="evidence-159"></a>[cmd/vibedb-operator/main.go](../cmd/vibedb-operator/main.go) — `prepare`
160. <a id="evidence-160"></a>[cmd/vibedb/cluster_dev_test.go](../cmd/vibedb/cluster_dev_test.go) — `TestDevClusterManifestResumeIsCanonicalAndDoesNotReprovision`
161. <a id="evidence-161"></a>[cmd/vibedb/cluster_dev_test.go](../cmd/vibedb/cluster_dev_test.go) — `TestInitializeDevClusterEmitsThreeIndependentApplyRoles`
162. <a id="evidence-162"></a>[internal/gatewayruntime/hot_shard_dev_process_test.go](../internal/gatewayruntime/hot_shard_dev_process_test.go) — `TestGatewayZeroConfigDevPressureCompletesReplicatedSplit`
163. <a id="evidence-163"></a>[cmd/vibedb/cluster_dev_test.go](../cmd/vibedb/cluster_dev_test.go) — `TestDevRequestLedgerPrepareProfileMatchesCatalogHomeAndKeepsCatalogDisabled`
164. <a id="evidence-164"></a>[cmd/vibedb/cluster_dev_test.go](../cmd/vibedb/cluster_dev_test.go) — `TestDevReplicatedTableProfileUsesPortableSchemaAcrossReplicaLocalStores`
165. <a id="evidence-165"></a>[cmd/vibedb/cluster_dev_test.go](../cmd/vibedb/cluster_dev_test.go) — `TestDevCatalogPublishesOnlyThePortableDataTableProfile`
166. <a id="evidence-166"></a>[internal/kubeoperator/bootstrap_test.go](../internal/kubeoperator/bootstrap_test.go) — `TestBootstrapRecoversEveryPublicationCut`
167. <a id="evidence-167"></a>[internal/kubeoperator/render_test.go](../internal/kubeoperator/render_test.go) — `TestRenderRF3GoldenAndSafetyContract`
168. <a id="evidence-168"></a>[cmd/vibedb-kube-qualify/main.go](../cmd/vibedb-kube-qualify/main.go) — `runClient`
169. <a id="evidence-169"></a>[internal/raftstore/generation_activate.go](../internal/raftstore/generation_activate.go) — `GenerationActivation`
170. <a id="evidence-170"></a>[internal/raftmember/generation_driver.go](../internal/raftmember/generation_driver.go) — `WALGenerationDriverOptions`
171. <a id="evidence-171"></a>[internal/raftmember/generation_driver.go](../internal/raftmember/generation_driver.go) — `ConfigureWALGeneration`
172. <a id="evidence-172"></a>[internal/raftmember/generation_driver_test.go](../internal/raftmember/generation_driver_test.go) — `TestRuntimeWALGenerationBuildDoesNotBlockRaftProgress`
173. <a id="evidence-173"></a>[cmd/vibedb-shard/serve_rf3.go](../cmd/vibedb-shard/serve_rf3.go) — `rf3WALGenerationIntervalTicks`
174. <a id="evidence-174"></a>[internal/raftmember/generation_driver_test.go](../internal/raftmember/generation_driver_test.go) — `TestRuntimeWALGenerationDriverRepeatedCompactionAndRestart`
175. <a id="evidence-175"></a>[cmd/vibedb-shard/wal_retention_process_qualification_test.go](../cmd/vibedb-shard/wal_retention_process_qualification_test.go) — `TestServeRF3WALRetentionCrashQualification`
176. <a id="evidence-176"></a>[store/durable/store_file_online_compact.go](../store/durable/store_file_online_compact.go) — `CompactOnline`
177. <a id="evidence-177"></a>[internal/storeio/generation_migration_manifest.go](../internal/storeio/generation_migration_manifest.go) — `GenerationMigrationManifest`
178. <a id="evidence-178"></a>[store/durable/store_file_online_compact_test.go](../store/durable/store_file_online_compact_test.go) — `TestOnlineCompactionManifestAndChainedExtentSurviveReopen`
179. <a id="evidence-179"></a>[store/durable/store_file_online_compact_owner_test.go](../store/durable/store_file_online_compact_owner_test.go) — `TestCompactOnlineSingleFlight`
180. <a id="evidence-180"></a>[store/durable/store_file_online_compact_owner_test.go](../store/durable/store_file_online_compact_owner_test.go) — `TestCheckpointGroupSeedRejectsActiveOnlineCompaction`
181. <a id="evidence-181"></a>[store/durable/store_file_online_compact_owner_test.go](../store/durable/store_file_online_compact_owner_test.go) — `TestOnlineCompactionPublicationHelpersRejectCheckpointGroupOwner`
182. <a id="evidence-182"></a>[store/durable/store_file_online_compact_amplification_unix_test.go](../store/durable/store_file_online_compact_amplification_unix_test.go) — `TestCompactOnlineHardAmplificationGates`
183. <a id="evidence-183"></a>[store/durable/store_file_online_compact_amplification_unix_test.go](../store/durable/store_file_online_compact_amplification_unix_test.go) — `TestCompactOnlineForegroundWriteP99Bound`
184. <a id="evidence-184"></a>[internal/clusterbackup/certificate.go](../internal/clusterbackup/certificate.go) — `Certificate`
185. <a id="evidence-185"></a>[internal/clusterbackup/repository.go](../internal/clusterbackup/repository.go) — `BackupRepository`
186. <a id="evidence-186"></a>[internal/clusterbackup/live_collect.go](../internal/clusterbackup/live_collect.go) — `CollectLive`
187. <a id="evidence-187"></a>[gateway/backup_operation.go](../gateway/backup_operation.go) — `BackupCatalogCut`
188. <a id="evidence-188"></a>[internal/clusterbackup/source_export.go](../internal/clusterbackup/source_export.go) — `ExportLinearizableGroupCut`
189. <a id="evidence-189"></a>[internal/gatewayruntime/backup_operator.go](../internal/gatewayruntime/backup_operator.go) — `gatewayBackupOperatorRuntime`
190. <a id="evidence-190"></a>[internal/gatewayruntime/serve_backup.go](../internal/gatewayruntime/serve_backup.go) — `gatewayBackupWireRequest`
191. <a id="evidence-191"></a>[internal/gatewayruntime/serve_backup_test.go](../internal/gatewayruntime/serve_backup_test.go) — `TestGatewayBackupRequestUsesDedicatedAuthorityAndCanonicalResponse`
192. <a id="evidence-192"></a>[internal/clusterbackup/certificate_test.go](../internal/clusterbackup/certificate_test.go) — `TestCertificateCanonicalRoundTripAndCompleteCatalogInventory`
193. <a id="evidence-193"></a>[internal/clusterbackup/repository_test.go](../internal/clusterbackup/repository_test.go) — `TestRepositoryCrashBoundariesNeverExposePartialPublicationOrReleasedBackup`
194. <a id="evidence-194"></a>[internal/clusterbackup/live_service_test.go](../internal/clusterbackup/live_service_test.go) — `TestLiveClientPartitionFailsWithinConfiguredDeadline`
195. <a id="evidence-195"></a>[internal/clusterbackup/live_collect_test.go](../internal/clusterbackup/live_collect_test.go) — `TestCollectLiveExternalExitBeforeCertificateRecoversNoBackup`
196. <a id="evidence-196"></a>[internal/clusterrestore/operation.go](../internal/clusterrestore/operation.go) — `Operation`
197. <a id="evidence-197"></a>[internal/clusterrestore/seal.go](../internal/clusterrestore/seal.go) — `SealFreshOperation`
198. <a id="evidence-198"></a>[internal/restoreservice/installer.go](../internal/restoreservice/installer.go) — `GroupInstaller`
199. <a id="evidence-199"></a>[internal/kubeoperator/restore.go](../internal/kubeoperator/restore.go) — `RestoreGroup`
200. <a id="evidence-200"></a>[gateway/restore_catalog_projection.go](../gateway/restore_catalog_projection.go) — `RestoreCatalogProjection`
201. <a id="evidence-201"></a>[gateway/restore_activation.go](../gateway/restore_activation.go) — `ActivateRestore`
202. <a id="evidence-202"></a>[gateway/replicated_restore_catalog.go](../gateway/replicated_restore_catalog.go) — `ReplicatedRestoreCatalog`
203. <a id="evidence-203"></a>[internal/clusterrestore/serving_grant.go](../internal/clusterrestore/serving_grant.go) — `ServingGrant`
204. <a id="evidence-204"></a>[shardservice/restore_serving_control.go](../shardservice/restore_serving_control.go) — `RestoreServingGate`
205. <a id="evidence-205"></a>[internal/kubeoperator/restore_bootstrap.go](../internal/kubeoperator/restore_bootstrap.go) — `RestoreBootstrapOperation`
206. <a id="evidence-206"></a>[cmd/vibedb-shard/restore_serving_rf3.go](../cmd/vibedb-shard/restore_serving_rf3.go) — `rf3RestoreCatalogPreparingAuthority`
207. <a id="evidence-207"></a>[sql/driver/replicated_restore_manifest.go](../sql/driver/replicated_restore_manifest.go) — `ReplicatedRelationManifestForBinding`
208. <a id="evidence-208"></a>[internal/replicatedstate/restore_manifest.go](../internal/replicatedstate/restore_manifest.go) — `RehashSnapshotArtifact`
209. <a id="evidence-209"></a>[cmd/vibedb-operator/restore.go](../cmd/vibedb-operator/restore.go) — `restoreGroup`
210. <a id="evidence-210"></a>[cmd/vibedb-shard/adopt_restored_rf3.go](../cmd/vibedb-shard/adopt_restored_rf3.go) — `adoptRestoredRF3Member`
211. <a id="evidence-211"></a>[internal/gatewayruntime/restore_activate.go](../internal/gatewayruntime/restore_activate.go) — `runRestoreActivate`
212. <a id="evidence-212"></a>[internal/gatewayruntime/restore_activate_manifest.go](../internal/gatewayruntime/restore_activate_manifest.go) — `parseGatewayRestoreManifest`
213. <a id="evidence-213"></a>[internal/clusterrestore/controller_process_linux_test.go](../internal/clusterrestore/controller_process_linux_test.go) — `TestActivationExternalProcessRecoversEveryPublicationCutWithinBounds`
214. <a id="evidence-214"></a>[internal/restoreservice/installer_linux_test.go](../internal/restoreservice/installer_linux_test.go) — `TestGroupInstallerBuildsAndRecoversThreeAuthorityFreeRoots`
215. <a id="evidence-215"></a>[internal/kubeoperator/restore_bundle_linux_test.go](../internal/kubeoperator/restore_bundle_linux_test.go) — `TestRestoreReplicaFactoryBindsExactBundle`
216. <a id="evidence-216"></a>[internal/kubeoperator/restore_catalog_projection_test.go](../internal/kubeoperator/restore_catalog_projection_test.go) — `TestRestoreCatalogProjectionIsOperationBoundAndExcludesSourceAuthority`
217. <a id="evidence-217"></a>[sql/driver/replicated_restore_projection_test.go](../sql/driver/replicated_restore_projection_test.go) — `TestReplicatedRestoreProjectionDropsSourceRowsAndResumes`
218. <a id="evidence-218"></a>[cmd/vibedb-shard/adopt_restored_rf3_test.go](../cmd/vibedb-shard/adopt_restored_rf3_test.go) — `TestRestoredRosterMatchesOnlyAuthenticatedTargets`
219. <a id="evidence-219"></a>[gateway/replicated_restore_catalog_test.go](../gateway/replicated_restore_catalog_test.go) — `TestReplicatedRestoreCatalogSettlesResponseLossByLinearizableRead`
220. <a id="evidence-220"></a>[cmd/vibedb-shard/restore_rf3_process_test.go](../cmd/vibedb-shard/restore_rf3_process_test.go) — `TestRestoredRF3ExternalProcessServingAndFailover`
221. <a id="evidence-221"></a>[internal/raftservice/owner.go](../internal/raftservice/owner.go) — `ReadPoint`
222. <a id="evidence-222"></a>[internal/raftservice/owner.go](../internal/raftservice/owner.go) — `syncCommandFenceFromSnapshot`
223. <a id="evidence-223"></a>[internal/multiraft/host.go](../internal/multiraft/host.go) — `ReadIndex`
224. <a id="evidence-224"></a>[shardservice/replicated_server.go](../shardservice/replicated_server.go) — `replicatedReadState`
225. <a id="evidence-225"></a>[gateway/replicated_data_read.go](../gateway/replicated_data_read.go) — `ReplicatedDataReader`
226. <a id="evidence-226"></a>[gateway/replicated_table.go](../gateway/replicated_table.go) — `ResolveReplicatedTableKey`
227. <a id="evidence-227"></a>[internal/gatewayruntime/serve.go](../internal/gatewayruntime/serve.go) — `handleConnPolicy`
228. <a id="evidence-228"></a>[gateway/replicated_data_read.go](../gateway/replicated_data_read.go) — `Read`
229. <a id="evidence-229"></a>[internal/gatewayruntime/data_handler_test.go](../internal/gatewayruntime/data_handler_test.go) — `TestHandleConnDataDispatchesRF3ReadWithoutSQLFallback`
230. <a id="evidence-230"></a>[gateway/replicated_data_read_test.go](../gateway/replicated_data_read_test.go) — `TestReplicatedDataReaderLinearizableRefreshesNotLeader`
231. <a id="evidence-231"></a>[internal/raftservice/owner_rf3_query_probe_test.go](../internal/raftservice/owner_rf3_query_probe_test.go) — `TestRF3SQLReadUsesAcceptedFenceWithoutPostProbe`
232. <a id="evidence-232"></a>[internal/multiraft/host.go](../internal/multiraft/host.go) — `ProposeConfChange`
233. <a id="evidence-233"></a>[internal/multiraft/host.go](../internal/multiraft/host.go) — `TransferLeader`
234. <a id="evidence-234"></a>[internal/rebalanceexec/executor.go](../internal/rebalanceexec/executor.go) — `ExecuteReplicaMove`
235. <a id="evidence-235"></a>[internal/rebalanceexec/controller.go](../internal/rebalanceexec/controller.go) — `SubmitSet`
236. <a id="evidence-236"></a>[internal/rebalanceexec/controller.go](../internal/rebalanceexec/controller.go) — `RunPass`
237. <a id="evidence-237"></a>[internal/gatewayruntime/replica_health_controller.go](../internal/gatewayruntime/replica_health_controller.go) — `startGatewayReplicaControllers`
238. <a id="evidence-238"></a>[internal/rebalanceexec/executor_test.go](../internal/rebalanceexec/executor_test.go) — `TestExecutorMapsExactMembershipSnapshotWaitAndDrainActions`
239. <a id="evidence-239"></a>[internal/multiraft/host_leader_transfer_real_test.go](../internal/multiraft/host_leader_transfer_real_test.go) — `TestThreeRealHostsTransferLeaderThroughAuthenticatedTransportAndContinueApply`
240. <a id="evidence-240"></a>[internal/gatewayruntime/replica_replacement_process_test.go](../internal/gatewayruntime/replica_replacement_process_test.go) — `TestGatewayAutomaticReplicaReplacementProcesses`
241. <a id="evidence-241"></a>[internal/snapshottransfer/repository.go](../internal/snapshottransfer/repository.go) — `Repository`
242. <a id="evidence-242"></a>[internal/snapshottransfer/service.go](../internal/snapshottransfer/service.go) — `Receiver`
243. <a id="evidence-243"></a>[internal/snapshottransfer/learner_install.go](../internal/snapshottransfer/learner_install.go) — `InstallPublishedLearner`
244. <a id="evidence-244"></a>[cmd/vibedb-shard/bootstrap_rf3.go](../cmd/vibedb-shard/bootstrap_rf3.go) — `bootstrapPreparedRF3`
245. <a id="evidence-245"></a>[cmd/vibedb-shard/bootstrap_rf3_groups.go](../cmd/vibedb-shard/bootstrap_rf3_groups.go) — `bootstrapPreparedRF3Groups`
246. <a id="evidence-246"></a>[internal/snapshottransfer/source_provider_test.go](../internal/snapshottransfer/source_provider_test.go) — `TestRetainedSourceProviderExportsAndObservesAfterReopen`
247. <a id="evidence-247"></a>[internal/snapshottransfer/source_control_test.go](../internal/snapshottransfer/source_control_test.go) — `TestSourceControlClientRoutesExactReplicatedAbandonment`
248. <a id="evidence-248"></a>[internal/snapshottransfer/abandonment_test.go](../internal/snapshottransfer/abandonment_test.go) — `TestRepositoryRecoversEveryAbandonmentNamespacePhase`
249. <a id="evidence-249"></a>[internal/rebalanceexec/abandonment_test.go](../internal/rebalanceexec/abandonment_test.go) — `TestAbandonmentSchedulerCrashRestartAndByteGateDoNotSkipWitness`
250. <a id="evidence-250"></a>[internal/snapshottransfer/learner_install_test.go](../internal/snapshottransfer/learner_install_test.go) — `TestInstallPublishedLearnerRetriesExactIncarnationAfterHostBoundary`
251. <a id="evidence-251"></a>[internal/snapshottransfer/transfer_test.go](../internal/snapshottransfer/transfer_test.go) — `BenchmarkSnapshotServiceChunk`
252. <a id="evidence-252"></a>[internal/splitcontroller/replicated_executor.go](../internal/splitcontroller/replicated_executor.go) — `AdmitReplicatedPlan`
253. <a id="evidence-253"></a>[internal/splitcontroller/local_source_actions.go](../internal/splitcontroller/local_source_actions.go) — `LocalSourceActions`
254. <a id="evidence-254"></a>[internal/splitcontroller/local_child_actions.go](../internal/splitcontroller/local_child_actions.go) — `LocalChildActions`
255. <a id="evidence-255"></a>[internal/rangesplit/stage_image.go](../internal/rangesplit/stage_image.go) — `childStageImageAccumulator`
256. <a id="evidence-256"></a>[internal/replicatedstate/relation_placement_accumulator.go](../internal/replicatedstate/relation_placement_accumulator.go) — `GlobalIndexPlacementProof`
257. <a id="evidence-257"></a>[internal/splitcontroller/committed_preparation.go](../internal/splitcontroller/committed_preparation.go) — `CommittedPlanPreparer`
258. <a id="evidence-258"></a>[internal/splitcontroller/committed_preparation_test.go](../internal/splitcontroller/committed_preparation_test.go) — `TestServingPreparationRequiresCommittedIntentAndResumesLostReceipt`
259. <a id="evidence-259"></a>[internal/splitcontroller/local_observation_provider.go](../internal/splitcontroller/local_observation_provider.go) — `LocalPlanObservationProvider`
260. <a id="evidence-260"></a>[internal/splitcontroller/composite_shard_executor.go](../internal/splitcontroller/composite_shard_executor.go) — `CompositeShardActionExecutor`
261. <a id="evidence-261"></a>[internal/splitcontroller/controller_service.go](../internal/splitcontroller/controller_service.go) — `NewServingControllerService`
262. <a id="evidence-262"></a>[internal/rebalanceexec/controller.go](../internal/rebalanceexec/controller.go) — `Controller`
263. <a id="evidence-263"></a>[internal/rangesplit/artifact.go](../internal/rangesplit/artifact.go) — `WriteChildArtifacts`
264. <a id="evidence-264"></a>[internal/rangesplit/tail.go](../internal/rangesplit/tail.go) — `TranslateTailEntry`
265. <a id="evidence-265"></a>[internal/splitcontroller/retained_prune_proposer.go](../internal/splitcontroller/retained_prune_proposer.go) — `NewRF3RetainedPruneProposerForPlan`
266. <a id="evidence-266"></a>[internal/gatewayruntime/split_controller_runtime.go](../internal/gatewayruntime/split_controller_runtime.go) — `gatewayServingSplitRuntime`
267. <a id="evidence-267"></a>[internal/gatewayruntime/hot_split_factory.go](../internal/gatewayruntime/hot_split_factory.go) — `gatewayHotSplitFactory`
268. <a id="evidence-268"></a>[cmd/vibedb-shard/rf3_split_serving.go](../cmd/vibedb-shard/rf3_split_serving.go) — `rf3SplitServingRuntime`
269. <a id="evidence-269"></a>[internal/replicatedstate/relation_bundle.go](../internal/replicatedstate/relation_bundle.go) — `GlobalIndexProfile`
270. <a id="evidence-270"></a>[internal/splitcontroller/committed_preparation_test.go](../internal/splitcontroller/committed_preparation_test.go) — `TestCommittedPreparationRejectsForgedReservedCursorBeforeEffects`
271. <a id="evidence-271"></a>[internal/splitcontroller/committed_preparation_test.go](../internal/splitcontroller/committed_preparation_test.go) — `TestCommittedPreparationCrashAfterReceiptsBeforeCompletionRecord`
272. <a id="evidence-272"></a>[cmd/vibedb-shard/rf3_group_child_preparer_test.go](../cmd/vibedb-shard/rf3_group_child_preparer_test.go) — `TestRF3GroupChildRegistrySelectionBindsExactGroupProfileAndPaths`
273. <a id="evidence-273"></a>[cmd/vibedb-shard/rf3_group_child_preparer_test.go](../cmd/vibedb-shard/rf3_group_child_preparer_test.go) — `TestRF3GroupChildPreparationHasOneGlobalOperationBound`
274. <a id="evidence-274"></a>[internal/splitcontroller/local_source_actions_test.go](../internal/splitcontroller/local_source_actions_test.go) — `TestLocalSourceActionsRecoverCaptureAndPublishImmutableArtifacts`
275. <a id="evidence-275"></a>[internal/splitcontroller/local_source_seal_test.go](../internal/splitcontroller/local_source_seal_test.go) — `TestLocalSourceSealAndCutoverCertificateSurviveRestart`
276. <a id="evidence-276"></a>[internal/splitcontroller/reconcile_test.go](../internal/splitcontroller/reconcile_test.go) — `TestChildActionsRequireMonotonicExactEvidence`
277. <a id="evidence-277"></a>[internal/rangesplit/stage_image_incremental_test.go](../internal/rangesplit/stage_image_incremental_test.go) — `TestChildStageSealDoesNotScanRows`
278. <a id="evidence-278"></a>[internal/splitcontroller/global_index_cut_test.go](../internal/splitcontroller/global_index_cut_test.go) — `TestGlobalIndexCutUsesCanonicalUniqueAndNonUniquePlacementAtBoundary`
279. <a id="evidence-279"></a>[internal/splitcontroller/execute_test.go](../internal/splitcontroller/execute_test.go) — `TestPublishBeforePruneCrashMatrixNeverLosesOrDoubleRoutesRows`
280. <a id="evidence-280"></a>[internal/gatewayruntime/hot_shard_mutation_process_test.go](../internal/gatewayruntime/hot_shard_mutation_process_test.go) — `TestGatewayHotShardMutationProcesses`
281. <a id="evidence-281"></a>[gateway/replicated_catalog_authority.go](../gateway/replicated_catalog_authority.go) — `ReplicatedCatalogAuthority`
282. <a id="evidence-282"></a>[gateway/replicated_catalog_route_seed.go](../gateway/replicated_catalog_route_seed.go) — `ReplicatedCatalogRouteSeedState`
283. <a id="evidence-283"></a>[gateway/replicated_catalog_route_seed.go](../gateway/replicated_catalog_route_seed.go) — `StageReplicatedCatalogRouteSeedAfter`
284. <a id="evidence-284"></a>[gateway/schema_rollout.go](../gateway/schema_rollout.go) — `PrepareSchemaRollout`
285. <a id="evidence-285"></a>[internal/schemainstall/installer.go](../internal/schemainstall/installer.go) — `Installer`
286. <a id="evidence-286"></a>[gateway/replicated_catalog_route_seed.go](../gateway/replicated_catalog_route_seed.go) — `InstallReplicatedCatalogRouteSeed`
287. <a id="evidence-287"></a>[gateway/replicated_catalog_route_seed.go](../gateway/replicated_catalog_route_seed.go) — `CompleteQuiescedHandoff`
288. <a id="evidence-288"></a>[gateway/schema_rollout_controller.go](../gateway/schema_rollout_controller.go) — `NewSchemaRolloutController`
289. <a id="evidence-289"></a>[internal/schemainstall/control.go](../internal/schemainstall/control.go) — `NewControlService`
290. <a id="evidence-290"></a>[internal/gatewayruntime/schema_rollout_admin.go](../internal/gatewayruntime/schema_rollout_admin.go) — `executeGatewaySchemaRollout`
291. <a id="evidence-291"></a>[cmd/vibedb-shard/schema_startup_recovery.go](../cmd/vibedb-shard/schema_startup_recovery.go) — `openRF3RetainedApply`
292. <a id="evidence-292"></a>[sql/driver/schema_catalog_source_recovery.go](../sql/driver/schema_catalog_source_recovery.go) — `OpenReplicatedShardStoreWithSchemaSourceTransition`
293. <a id="evidence-293"></a>[internal/gatewayruntime/serve.go](../internal/gatewayruntime/serve.go) — `recoverReplicatedCatalogRouteSeedStartup`
294. <a id="evidence-294"></a>[internal/gatewayruntime/command.go](../internal/gatewayruntime/command.go) — `run`
295. <a id="evidence-295"></a>[internal/replicatedstate/schema_source_recovery_test.go](../internal/replicatedstate/schema_source_recovery_test.go) — `TestSchemaSourceRecoveryAuthenticatesCommittedCheckpoint`
296. <a id="evidence-296"></a>[internal/replicatedstate/schema_source_recovery_test.go](../internal/replicatedstate/schema_source_recovery_test.go) — `TestSchemaRetiredSourceRejectsServingOperations`
297. <a id="evidence-297"></a>[cmd/vibedb-shard/schema_startup_recovery_test.go](../cmd/vibedb-shard/schema_startup_recovery_test.go) — `TestRF3SchemaStartupSettlementFailsClosedAtEveryBoundary`
298. <a id="evidence-298"></a>[cmd/vibedb-shard/schema_startup_recovery_linux_test.go](../cmd/vibedb-shard/schema_startup_recovery_linux_test.go) — `TestRF3SchemaStartupSettlesCommittedSourceBeforeRuntimeAdoption`
299. <a id="evidence-299"></a>[sql/driver/replicated_manifest_test.go](../sql/driver/replicated_manifest_test.go) — `TestInitialReplicatedRelationManifestMatchesServingIdentity`
300. <a id="evidence-300"></a>[internal/replicatedstate/initial_manifest_test.go](../internal/replicatedstate/initial_manifest_test.go) — `TestInitialJSONRelationManifestMatchesPreparedCollection`
301. <a id="evidence-301"></a>[gateway/replicated_catalog_authority_test.go](../gateway/replicated_catalog_authority_test.go) — `TestReplicatedCatalogRouteSeedTrackerPersistsSameRouteBeforeHolderPublish`
302. <a id="evidence-302"></a>[gateway/replicated_catalog_authority_test.go](../gateway/replicated_catalog_authority_test.go) — `TestReplicatedCatalogRouteSeedTrackerCompletesLiveBindingHandoff`
303. <a id="evidence-303"></a>[gateway/replicated_catalog_authority_test.go](../gateway/replicated_catalog_authority_test.go) — `TestReplicatedCatalogRouteSeedLockedCheckRejectsPreauthorizedWaiter`
304. <a id="evidence-304"></a>[internal/gatewayruntime/catalog_route_seed_test.go](../internal/gatewayruntime/catalog_route_seed_test.go) — `TestRecoverReplicatedCatalogRouteSeedStartupSettlesExactOldBinding`
305. <a id="evidence-305"></a>[gateway/schema_rollout_test.go](../gateway/schema_rollout_test.go) — `TestSchemaRolloutPrepareActivateExactCatalog`
306. <a id="evidence-306"></a>[gateway/schema_rollout_process_test.go](../gateway/schema_rollout_process_test.go) — `TestSchemaRolloutExternalProcessLeaderLossAndMixedGenerationRecovery`
307. <a id="evidence-307"></a>[internal/schemainstall/installer_test.go](../internal/schemainstall/installer_test.go) — `TestInstallerCrashReopenAuthorizationActivationAndDrain`
308. <a id="evidence-308"></a>[internal/gatewayruntime/schema_rollout_admin_test.go](../internal/gatewayruntime/schema_rollout_admin_test.go) — `TestGatewaySchemaRolloutManifestRequiresCanonicalVibeJSON`
309. <a id="evidence-309"></a>[internal/raftservice/controlplane_catalog_rf3_test.go](../internal/raftservice/controlplane_catalog_rf3_test.go) — `TestReplicatedCatalogAuthorityRF3QuorumReplayAndControllerRestart`
310. <a id="evidence-310"></a>[internal/topologyscheduler/admission.go](../internal/topologyscheduler/admission.go) — `SelectSplits`
311. <a id="evidence-311"></a>[internal/topologyscheduler/replica_move.go](../internal/topologyscheduler/replica_move.go) — `SelectReplicaMoves`
312. <a id="evidence-312"></a>[internal/hotshard/collector.go](../internal/hotshard/collector.go) — `Collector`
313. <a id="evidence-313"></a>[internal/hotshard/operation_sink.go](../internal/hotshard/operation_sink.go) — `OperationSink`
314. <a id="evidence-314"></a>[internal/gatewayruntime/hot_shard_runtime.go](../internal/gatewayruntime/hot_shard_runtime.go) — `gatewayHotShardRuntime`
315. <a id="evidence-315"></a>[internal/gatewayruntime/hot_shard_runtime.go](../internal/gatewayruntime/hot_shard_runtime.go) — `runPressurePass`
316. <a id="evidence-316"></a>[internal/hotshard/controller_test.go](../internal/hotshard/controller_test.go) — `TestControllerQualifiesHotShardAndRetriesByteIdenticalAdmission`
317. <a id="evidence-317"></a>[internal/hotshard/controller_test.go](../internal/hotshard/controller_test.go) — `TestControllerClockSkewCannotAdvanceReplicatedEvidence`
318. <a id="evidence-318"></a>[internal/gatewayruntime/hot_shard_runtime_test.go](../internal/gatewayruntime/hot_shard_runtime_test.go) — `TestGatewayHotShardPressurePassCreatesExactEnrolledReplicaMove`
319. <a id="evidence-319"></a>[internal/gatewayruntime/hot_shard_shipped_e2e_test.go](../internal/gatewayruntime/hot_shard_shipped_e2e_test.go) — `TestGatewayHotShardForegroundP99Overhead`

</details>

## Regenerate

1. Change `internal/featurestate/manifest.go` and cite production code or an executable test.
2. Run `go generate ./internal/featurestate`.
3. Run `go test ./internal/featurestate`. The test rejects stale output, invalid state
   transitions, duplicate rows, and missing evidence files.

See the [embedded capability matrix](capabilities.md) for local entry points and [distributed
operations](operations/distributed.md) for runtime behavior.
