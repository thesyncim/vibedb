# vibedb

An embedded JSON document database in pure Go, built on
[vibejson](https://github.com/thesyncim/vibejson): one mutable engine with
an ordered tablet primary, canonical in-memory frames, O(1) snapshots,
tombstone-free deletes, and explicit durability contracts from
buffered-visible through power-safe.

Extracted from the vibejson repository on 2026-07-27; that repository's
history carries every design decision and measurement to this point. The
project is pre-v1: APIs and the on-disk format change without migration.

- Design authority: [docs/ordered-hybrid-store.md](docs/ordered-hybrid-store.md)
  and its companions in [docs/](docs/).
- Measured competitive results: [bench/competitive/RESULTS.md](bench/competitive/RESULTS.md).
- The engine plan of record: [docs/unification.md](docs/unification.md).
