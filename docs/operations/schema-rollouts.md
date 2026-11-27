# Roll out one RF3 schema generation

[Documentation](../README.md) / [Operations](README.md) / Schema rollouts

`vibedb-gateway schema-rollout` advances every replica of each changed RF3
group from schema generation `N` to `N+1` using a sealed target catalog and
prebuilt canonical SQL catalog bundles. It does not accept arbitrary SQL or
backfill schemas (RF3 `CREATE UNIQUE INDEX` is rejected).

## Prerequisites and plan

Requires exact same build, authenticated RF3 catalog/shards (no plaintext),
gateway `schema` capability, replica-control manifest, exact successor target
catalog, and a canonical `vibejson` rollout plan (≤4 MiB; bundles ≤64 MiB each)
binding a 64-hex operation ID, absolute target catalog path, and per-replica
node/member IDs, bundle paths, and apply-contract digests.

```sh
vibedb-gateway schema-rollout \
  <replicated-catalog-session-and-TLS-options> \
  -replica-control-manifest /etc/vibedb/replica-control.vibejson \
  -schema-rollout-plan /var/lib/vibedb/rollouts/catalog-next.vibejson
```

## Protocol and state machines

1. Validate base/target catalogs and prepare every replica bundle off-path
   (`Prepared`).
2. Fold 3 replica receipts per changed group and record catalog operation
   `Planned` (cancellable before `Running`).
3. Advance catalog operation to `Running` (**point of no return**—rollback is
   refused).
4. Authorize replicas (`Authorized`), commit the RF3 transition, and activate
   locally (`Active`).
5. Publish the target global catalog (`Complete`), then prove old execution
   pins released (`Drained`) before reclaiming predecessor artifacts.

## Recover an interrupted rollout

Rerun the exact same command with byte-identical plan, operation ID, catalog
session identity, and durable journals. Never delete `.schema-*` lineage or
journal files manually.

- Before `Running`, the plan may be cancelled.
- At or after `Running`, finish forward. A replica that crashed after RF3
  commit completes local publication automatically on startup.
- Wait for `Complete` and full replica `Drained` before planning another
  `N+1 → N+2` rollout.

## Limits

Hard bounds: 64 concurrent gateway replica ops, 8 shard installer ops, 64 MiB
per bundle, 256 retained artifacts / 1 GiB artifact storage, 256 shard rollout
journal records.

## Source map

- [gateway/schema_rollout.go](../../gateway/schema_rollout.go) · [schema_rollout_controller.go](../../gateway/schema_rollout_controller.go)
- [internal/schemainstall/installer.go](../../internal/schemainstall/installer.go) · [cmd/vibedb-shard/schema_install_rf3.go](../../cmd/vibedb-shard/schema_install_rf3.go)
