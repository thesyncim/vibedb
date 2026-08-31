# Roll out one RF3 schema generation

> [!CAUTION]
> Schema rollout is a development-only, same-build control path. Plans, bundles,
> state files, protocol bytes, and recovery rules can change or break at any
> commit. This is not a general DDL service, a rolling-version upgrade mechanism,
> or a production online-schema-change guarantee.

`vibedb-gateway schema-rollout` coordinates one exact catalog transition across
every replica of every changed RF3 group. Each changed shard moves from schema
generation `N` to exactly `N+1`; skipping generations is rejected by the serving
installer.

## What this command is—and is not

The command consumes a sealed target catalog plus prebuilt, replica-local
canonical SQL catalog bundles. It does not accept arbitrary SQL and does not
discover, backfill, or invent a target schema on its own.

Public pgwire DDL, the internal exact-cut bundle builder, and this rollout
installer are separate layers:

- the internal builder can prepare selected schema artifacts under explicit
  route/cut authority;
- the rollout installs already prepared artifacts and advances the global
  catalog; and
- the public DDL coordinator remains an experimental, separately qualified path.

Do not treat successful local `CREATE INDEX`, `DROP INDEX`, `TRUNCATE`, or other
DDL tests as proof that arbitrary RF3 DDL is supported end to end. In particular,
RF3 SQL `CREATE UNIQUE INDEX` has no coordinated uniqueness proof and is rejected.

## Authority and prerequisites

You need all of the following from the same build:

- an authenticated RF3 catalog and shard fleet;
- the gateway's replicated-catalog route seed, stable client/retry identity,
  durable session journal, and acknowledgement key;
- TLS peer identities and a policy granting the gateway `schema` capability;
- a replica-control manifest covering every target node;
- one canonical target global catalog that is the exact successor of the
  current catalog;
- one canonical bundle for each replica of every changed group; and
- the exact apply-contract digest implemented by the fleet.

The gateway derives group identity, allocation generation, source/target schema
generation, and relation-manifest digests from authenticated old and target
catalogs. A plan cannot override them. Each changed group has exactly three
distinct replica plans. Unchanged groups have no bundle entry.

The command refuses plaintext mode. A schema-capable principal does not thereby
gain data, topology, membership, backup, or restore-activation authority.

## Prepare the plan

The rollout plan is strict canonical `vibejson`. It contains:

- one nonzero 32-byte operation ID as lowercase hexadecimal;
- the absolute path to the target catalog; and
- for each changed replica, its node/member identity, absolute bundle path, and
  exact apply-contract digest.

Whitespace variants, reordered or duplicate fields, trailing bytes, relative
paths, substituted bundles, malformed identities, mixed apply contracts, missing
replicas, duplicate receipts, or a stale source cut fail closed. The plan is
bounded to 4 MiB and each bundle to 64 MiB.

A prepared bundle is not serving authority. The shard materializes and verifies
it away from the live generation and returns an installation receipt. Retain
every receipt and operation file; do not rename, delete, or hand-edit installer
journals to work around a conflict.

## Run the rollout

Use the same replicated-catalog, TLS, authorization, peer, and stable-session
options as `vibedb-gateway serve`, plus the control manifest and plan:

```text
vibedb-gateway schema-rollout \
  <replicated-catalog-session-and-TLS-options> \
  -replica-control-manifest /etc/vibedb/replica-control.vibejson \
  -schema-rollout-plan /var/lib/vibedb/rollouts/catalog-next.vibejson
```

The command prints target catalog generation, replicated operation revision,
and total elapsed time only after completion.

## Two state machines

Do not use the catalog operation state as a substitute for each replica's local
installation state.

### Replicated catalog journal

| State | Meaning | Recovery rule |
| --- | --- | --- |
| `Planned` | Exact target intent and folded prepared-group root are recorded | May be cancelled before authorization |
| `Running` | Catalog authorized shard activation; this is the no-return boundary | Rollback is refused; finish forward |
| `Complete` | Every required replica reported active and the target catalog head was conditionally published | Exact replay is idempotent |
| `Cancelled` | A still-planned operation was aborted | It cannot later cross into running |

### Replica-local installer journal

| State | Meaning | Serving effect |
| --- | --- | --- |
| `Prepared` | Immutable target artifact and installation digest are durable | None |
| `Authorized` | Exact catalog authorization is durable; committing the RF3 transition is a separate observed action before activation | The old local generation remains live |
| `Active` | The exact prepared generation has been atomically selected locally | New-generation serving is locally possible, subject to catalog/routing fences |
| `Drained` | Target catalog is complete and an exact proof says old execution pins are released | Old generation may be reclaimed |

The only valid local sequence is
`Prepared → Authorized → Active → Drained`. Every transition is a revisioned
compare-and-swap. An error may be outcome-unknown; recovery rereads the journal
and observes physical state before retrying an irreversible action.

## Ordered protocol

1. Validate the base/target catalog pair and every replica request.
2. Prepare and authenticate every replica-local bundle without changing live
   serving state.
3. Fold the three replica receipts per changed group into bounded group evidence.
4. Record the catalog operation as `Planned`.
5. Advance it to `Running`. From this point, abort is forbidden.
6. Send the exact authorization to every replica; each commits the same RF3
   transition, activates its pinned target, and reports `Active`.
7. Only after all required replicas are active, conditionally publish the target
   global catalog and mark the operation `Complete`.
8. Separately prove old catalog leases and execution pins are gone, then move
   replicas to `Drained` and reclaim only predecessor state.

There can be a recovery interval in which some replicas are locally active while
the global catalog still names the old generation. This is expected and fenced;
it is why `Running` cannot be rolled back.

## Recover an interrupted rollout

Rerun the same command with the byte-identical plan, operation ID, catalog
session identity, retry home, and durable journals.

- Before `Running`, catalog authority may cancel the plan.
- At or after `Running`, always finish forward. A replacement controller settles
  catalog compare-and-swap and shard outcome-unknown results by observation.
- If RF3 committed the exact transition but the local SQL catalog was not
  published before a crash, startup authenticates the retained command and
  proofs, fences the predecessor, finishes the target publication, then opens
  the runtime.
- A merely prepared/uncommitted target leaves the old schema active.
- Ambiguous commands, a non-neutral WAL suffix, stale generation, changed bundle,
  mixed contract, missing authorization, or conflicting lineage fail closed.
- Old files remain until exact drain. Never delete `.schema-*` lineage,
  activation, membership, target-catalog, or journal files manually.

## Successive changes

One rollout authorizes one `N → N+1` transition. Bounded lineage records allow
the implementation to recognize an exactly drained predecessor, but that is not
a promise of an arbitrary repeated-rollout service. Before planning another
change, require the previous catalog operation to be complete, prove every
affected replica drained, build a new exact successor from the now-current
catalog, and requalify the sequence on the pinned build.

## Limits and claim boundary

Current hard ceilings include 64 concurrent gateway replica operations, 8 shard
installer operations, 64 MiB per bundle, 16 retained shard artifacts, 1 GiB of
artifact storage, and 256 shard rollout journal records. These are refusal
bounds, not recommended capacity or latency targets.

Tests cover exact preparation, mixed-generation recovery, outcome-unknown
catalog writes, refusal to roll back after authorization, and selected
same-build restart cuts. They do not establish:

- arbitrary PostgreSQL DDL compatibility;
- zero-pause or zero-overhead changes;
- bounded filesystem latency on every device;
- every crash/power-loss point;
- mixed-version or rolling-fleet compatibility; or
- safe repeated production migrations.

## Source map

| Boundary | Source |
| --- | --- |
| Catalog intent, authorization, completion, and abort | [`gateway/schema_rollout.go`](../../gateway/schema_rollout.go) |
| Fan-out, resume, and drain coordinator | [`gateway/schema_rollout_controller.go`](../../gateway/schema_rollout_controller.go) |
| Plan construction from exact receipts | [`gateway/schema_ddl_plan.go`](../../gateway/schema_ddl_plan.go), [`cmd/vibedb-gateway/schema_rollout_admin.go`](../../cmd/vibedb-gateway/schema_rollout_admin.go) |
| Local states, requests, authorization, and drain proof | [`internal/schemainstall/types.go`](../../internal/schemainstall/types.go) |
| Durable local CAS journal | [`internal/schemainstall/journal.go`](../../internal/schemainstall/journal.go) |
| Prepare/activate/drain observation | [`internal/schemainstall/installer.go`](../../internal/schemainstall/installer.go), [`internal/schemainstall/artifacts.go`](../../internal/schemainstall/artifacts.go) |
| RF3 exact-successor validation and physical activation | [`cmd/vibedb-shard/schema_install_rf3.go`](../../cmd/vibedb-shard/schema_install_rf3.go) |
| Committed-before-publication restart recovery | [`cmd/vibedb-shard/schema_install_recovery.go`](../../cmd/vibedb-shard/schema_install_recovery.go), [`cmd/vibedb-shard/schema_startup_recovery.go`](../../cmd/vibedb-shard/schema_startup_recovery.go) |
