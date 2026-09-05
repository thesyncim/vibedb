# Historical documentation audit: 215fb05

[Qualification records](README.md)

This is the validation record retained from an earlier documentation audit of
`215fb05cf89359b6a3cc0ba94793a16131426163`. Results below describe that audit and
the explicitly named later SIMD probe. They are not the test status of current
`main`. Use the CI run and evidence for the revision you are evaluating.

## Validation record

Validation was incremental across the rewrite and its `main` merges; this is
the complete claim, not a blanket green-build assertion.

| Check | Result | Boundary |
| --- | --- | --- |
| `go test -p=1 -timeout=25m ./...` | **Incomplete** | An earlier run reached `store/durable` and was externally terminated; no complete serial root run finished after the final `main` merge |
| `go build ./...`; `go vet ./...` | Passed after the final merge | Root module at the audited commit above |
| Final-main changed packages | Passed after the final merge | `internal/multiraft`, `internal/raftmember`, `internal/raftservice`, `internal/replicatedstate`, `shardservice`, and `cmd/vibedb-shard` |
| Focused packages | Passed before the final probe/process-test merge | `gateway`, `sql/driver`, both gateway/shard commands, `sql`, `query`, `pgwire`, conformance, feature-state, build-gate, unsafe-audit, and service-authorization suites |
| Other focused packages and integrations | Passed during the audit | Core storage, Raft, benchmark tooling, and hermetic client modules; these do not replace the incomplete root run |
| `go test ./store/durable -timeout=30m` | **Timed out** | `TestFileStorePointReplayDoesNotExhaustRetirementCapacity` was active; that test passed alone in 51 seconds, but the package has no complete result |
| PostgreSQL 18.6 upstream corpus | Not run | The approval set remains empty; see [PostgreSQL compatibility status](../status.md#postgresql-compatibility-status) |
| Live `psql` and Java/JDBC gates | Not run | Require explicit local dependencies and flags |
| Linux fault, `/proc`, direct-I/O, and Kubernetes Kind lanes | Not run | This audit ran on Darwin |

CI workflow presence does not prove branch protection or a mandatory release
gate. Raw CI artifacts are evidence for one run, not a benchmark publication or
support claim.

A focused SIMD check on 2026-09-04 found that
`GOEXPERIMENT=simd go test -race -gcflags=all=-d=checkptr=2 ./internal/storeio -run '^TestCompactStreamAdaptiveAlphabetSelection$' -count=1`
fails its alphabet point allocation assertion (`1` allocation, expected `0`)
on clean commit `ea8665045570df3b7d3a3f9aee4d8d9bc4dba0ae` as well as the
packed-counter SIMD change. The ordinary compact-codec tests pass; this is a
limitation of the combined instrumented validation, not a passing full-suite
claim.
