# Upgrades and compatibility

[Documentation](../README.md) / [Operations](README.md) / Upgrades

VibeDB supports exactly one build per cluster. There is no rolling upgrade, no
downgrade, no mixed-version operation, and no on-disk format migration. This
page states the rule, how the build check behaves, and the only safe way to
move data to a new build. The project-wide statement is on the
[stability page](../status.md).

## The rule

- Every process in a cluster (every node, frontend, gateway, and helper that
  touches its state) runs binaries built from the same commit.
- Durable state is reopened only by the build that wrote it.
- Backups, restore artifacts, operation journals, and manifests are read only
  by the build that produced them. Restore is not a cross-build path.

## How builds are checked

After TLS, internal service streams exchange a fixed 104-byte build preface.
It carries opaque wire and disk grammar identifiers plus capability bits. A
peer is admitted only if both grammar identifiers match exactly and each side
satisfies the other's required capabilities. The identifiers are equality
tokens, not version numbers; there is no ordering and no fallback decoder.

The check is weaker than "same commit":

- A code change that alters a wire or disk format without regenerating the
  grammar manifest leaves the identifiers unchanged. Two such builds admit
  each other and can then misread data. This has happened; see the warning in
  the [protocol reference](../reference/protocols.md#tls-build-gate-and-control-protocols).
- Not every durable open path runs the disk-adoption gate. A successful low
  level open does not prove compatibility.
- Some formats (the local launcher manifest, restore operations) carry their
  own format numbers and reject older layouts outright.

Treat the commit, not the preface, as the compatibility unit.

## Move data to a new build

There is no in-place procedure. Choose one of these:

1. **Recreate and reload.** Export your data through your application (for
   example SQL `SELECT` through the old cluster), start a fresh cluster on the
   new build in a new root, and load it again. This is the only path that does
   not depend on cross-build decoding.
2. **Stay on the old build.** Keep the old binaries, commit, and state
   together until you can reload.

Before either, preserve a restorable copy of the old state with the old build:
[embedded backup](embedded-backup.md) for embedded databases, or a stopped,
complete copy of every root for a development cluster.

## Local launcher specifics

- The launcher rejects a root whose `cluster.vibejson` is not the exact
  current manifest format, with
  `cluster dev: vibedb: invalid local development cluster`. Start a fresh
  root on the new build.
- The launcher resolves `vibedb-shard` beside itself, then on `PATH`. A stale
  `vibedb-shard` on `PATH` can start with a new `vibedb`. Build all binaries
  into one directory, or pass `--shard-binary`.
- A root prepared with a read-authority marker must not be reopened by a
  build that cannot interpret it; see [local cluster](local-cluster.md#read-authority-policy).

## Limitations

- No release artifacts, version numbers, or changelog of format changes exist.
- No tool reports which build wrote a root.
- Kubernetes images use the mutable tag `vibedb:kube-qualification`; the
  qualification does not pin an image digest.

## Source map

| Concern | Source |
| --- | --- |
| Grammar identity and capability admission | [internal/buildgate/profile.go](../../internal/buildgate/profile.go), [internal/buildgate/preface.go](../../internal/buildgate/preface.go) |
| Unreleased restart boundary test | [internal/buildgate/rolling_restart_test.go](../../internal/buildgate/rolling_restart_test.go) |
| Launcher manifest validation | [cluster_dev.go](../../cmd/vibedb/cluster_dev.go) |
