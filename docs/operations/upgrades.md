# Upgrades and compatibility

[Documentation](../README.md) / [Operations](README.md) / Upgrades

VibeDB currently requires **one exact build per cluster**. Rolling mixed-build
upgrades, downgrades, cross-build restore, and in-place format migration are
not yet qualified end-to-end.

## Build checks and pre-flight verification

- **Wire preface (`internal/buildgate`):** Internal TLS streams exchange a
  104-byte build preface with wire/disk grammar digests and capability bits;
  mismatches fail closed. Treat the exact Git commit (not just the preface) as
  the compatibility unit.
- **Version window (`internal/clusterversion`):** Inspect the running binary's
  protocol version window and validate peer compatibility before any rollout:

  ```sh
  vibedb cluster version --json
  vibedb cluster version --check-peers 1.0,1.1 --active 1.0 --json
  ```

  Prometheus `/metrics` also exports `vibedb_cluster_binary_version_info` and
  `vibedb_cluster_active_version_info`.

## Moving data to a new build

1. Back up the existing state with the old build ([embedded](embedded-backup.md)
   or stopped full cluster copy).
2. Export logical data via your application (e.g. SQL `SELECT`), stand up a
   fresh cluster/root on the new build, and reload.
3. Build all binaries (`vibedb`, `vibedb-shard`, `vibedb-gateway`,
   `vibedb-verify`) into one directory so a stale `vibedb-shard` on `PATH` is
   never mixed with a newer launcher.

## Source map

- [internal/buildgate/profile.go](../../internal/buildgate/profile.go) · [internal/clusterversion/](../../internal/clusterversion/) · [cmd/vibedb/cluster_version.go](../../cmd/vibedb/cluster_version.go)
