# Execution prompt — PR 1a

Paste-ready execution prompt for the first PR in the distributed-routing
implementation ladder described in
[`vitess-compatible-routing.md`](vitess-compatible-routing.md).

---

Implement **PR 1a — Frozen placement scalars and tuple codec** from:

```text
docs/design/vitess-compatible-routing.md
```

Read the full design and complete the required reconnaissance first, including:

```text
query/
sql/
store/
store/durable/
internal/orderedkey/
internal/conformance/
pgwire/
cmd/
docs/design/distributed-sharding.md
docs/design/recovery-journal.md
docs/design/hybrid-mutations.md
docs/design/parallel-tablet-writers.md
docs/design/sql-surface.md
docs/durability.md
CONTRIBUTING.md
```

Scope is only:

- closed placement scalar set: `String` and exact `Number`;
- frozen canonical scalar encoding;
- frozen ordered tuple codec;
- immutable golden vectors;
- fuzz tests;
- benchmarks;
- written format specification.

Pre-resolved semantics:

- binary UTF-8 strings;
- exact number canonicalization matching current group/join equality;
- `5`, `5.0`, `5e0`, and `50e-1` encode identically;
- `-0` encodes identically to `0`;
- bool, timestamp, Any, nested, object, and array values are rejected.

Forbidden:

- keyspace/manifests;
- domains/mappers/routes;
- SQL integration;
- networking;
- topology;
- Raft;
- change streams;
- Vitess dependencies.

Never regenerate or edit committed golden vectors to make a failing test pass.
A vector mismatch is a stop condition.

Run:

```bash
go vet ./...
go test -count=1 -timeout=25m ./...
go test -count=1 -race -timeout=25m ./...
```

plus targeted fuzz smoke tests and benchmarks.

Final report:

- implementation map;
- changed files;
- exact frozen format;
- tests/commands;
- fuzz/race results;
- benchmark/allocation results;
- unresolved risks;
- design deviations;
- confirmation that no forbidden scope or golden-vector rewriting occurred;
- exact PR 1b boundary.
