# Coding conventions

[Documentation](../README.md) / [Developer guide](README.md)

These are the patterns the existing code follows and reviewers expect. Tests
enforce most of them. Each convention names the reason and an example to copy.

## Errors

- **Package sentinels.** Declare exported failure classes as package-level
  `Err…` variables with a `package: message` text, for example
  `raftservice.ErrIngressFull = errors.New("raftservice: owner ingress is full")`.
  The root module has almost a thousand of them. Document on each sentinel
  what a caller may do next, especially whether a retry is safe.
- **Wrap with `%w`; test with `errors.Is` and `errors.As`.** Callers branch
  on the class, never on message text. Add context with
  `fmt.Errorf("%w: detail", ErrX)`.
- **Typed errors carry data and still unwrap to a sentinel.**
  `gateway.DurableSQLAbortError` keeps the replicated `ResultCode` and
  `Unwrap`s to `ErrDurableSQLAborted`. Callers can then use `errors.Is` for
  the class and `errors.As` for the detail.
- **Separate "not admitted" from "outcome unknown".** A refusal that proves
  nothing reached a log (for example `ErrDurableSQLNotAdmitted`,
  `ErrIngressFull`, `ErrServingFence`) is a different sentinel from one that
  means "it may have committed" (`ErrOutcomeUnknown` in `raftservice`,
  `replicaaction`, `schemainstall`, and `snapshottransfer`). Never collapse
  the two into one error.
- **Fail closed on unsupported environments.** A missing capability returns a
  sentinel, for example `storeio.ErrStrictAllocationUnsupported` off Linux.
  It never returns a weaker success. Tests may skip on that sentinel, and
  their qualification gate must turn the skip into a failure.
- **Keep shutdown failures.** Use `internal/serviceerrors` so a component's
  real failure is not replaced by the cancellation that followed it.

## Bounded resources

Every queue, buffer, cache, retained history, and wire message has an explicit
item bound, byte bound, or both. The code checks the bound at admission and
returns a named error when the bound is reached:

- Configuration structs expose `Max…Items` and `Max…Bytes` fields (for example
  `raftservice.Limits`). Constructors reject zero, negative, or over-ceiling
  values against package `AbsoluteMax…` constants with an `ErrInvalid…`
  error. An unbounded configuration is a constructor error, not a default.
- Budgets that serve different callers are independent. In `raftservice`,
  pending proposals, pending reads, and peer ingress each have their own
  budget, so client load cannot block Raft peer traffic.
- Decoders check lengths against a fixed ceiling before they allocate.
  Examples are `splitcapture.MaxPortableSpecBytes` and
  `replicatedstate.MaxPointReadBatchBytes`.
- Do not size a bound from what is present at startup. Groups, nodes, and
  sessions change at run time: a node joins, a replica is adopted, a split
  doubles the groups. A table sized at boot fails later on the path that
  changed the count. Derive the bound from a declared maximum, or grow it
  under an explicit bound.
- Qualification tests measure the result as RSS growth, storage growth, WAL
  growth, file-descriptor growth, and snapshot bytes. Workflows reject values
  over fixed ceilings.

## Exact-once identity

The rules are in [exactly-once writes](../design/exactly-once-writes.md#the-rule).
For code, this means:

- Mint a request identity once, before the first send, and store it with the
  canonical request bytes.
- After a possible admission (a lost response, cancellation, or leader
  change), retry the **same identity and bytes**. Never build a new command
  because a connection closed.
- Mint a new identity only after a typed pre-admission refusal.
- Treat a transport-frame retry and a request re-drive as separate layers.
- Tests must include the lost-response case. Commit, drop the reply, retry,
  and assert that the original outcome is returned exactly once.

## Fences and generations

Requests carry the generations they were planned against: allocation, schema,
route, ownership, and term. The receiving side rejects a mismatch with a
fence error; it does not serve against newer state. When you add a field that
affects routing or visibility, add it to the fence and add a stale-identity
test. See the [distributed protocol row](../../CONTRIBUTING.md#match-evidence-to-the-change)
of the evidence table.

## Wait on conditions, not time

Controllers and tests wait for a durable, observable condition, such as an
applied index, a published generation, or a `safe_to_stop` report, with a
deadline. They do not sleep a fixed time and hope the state converged. Use
`testing/synctest` for timer logic; see the
[testing strategy](testing-strategy.md#virtual-time-with-synctest).

## Allocation budgets

- Hot paths state their allocation contract in the doc comment ("allocation-free
  in steady state") and prove it with `testing.AllocsPerRun`. The
  `internal/buildgate` preface codec, for example, asserts zero allocations
  over 1,000 runs.
- Follow the `Append…(dst []byte, …) ([]byte, error)` shape for encoders, so
  callers can reuse buffers. Reuse scratch space owned by the caller, the
  session, or the index instead of allocating per call.
- `bench/gate` fails a pull request on any `allocs/op` increase, or on a
  `B/op` increase over 5%, for its curated benchmarks. Time is never gated.
- An allocation or performance cost from a correctness fix is acceptable. Name
  it as a follow-up, measure it, and do not trade correctness for it.

## Platform code

Split platform behavior by file suffix: `_linux.go`, `_darwin.go`,
`_windows.go`, and a `_other.go` fallback with a `//go:build` constraint that
covers the rest. The fallback must fail closed, as the `strict_allocation`
files do. Cross-compile jobs build `linux/386` and three Windows targets, so
code must also compile for 32-bit and Windows.

SIMD kernels need a portable fallback and a differential test against it. See
[SIMD](../simd.md).

## Unsafe code

A production `unsafe` import must follow the contract in
[UNSAFE.md](../../UNSAFE.md). After adding or removing one, regenerate the
inventory:

```sh
go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update
```

The inventory test fails the `core` shard when the list is stale.

## Generated and canonical files

Regenerate the build manifest, feature ledger, capability matrix, competitive
coverage, and unsafe inventory from their source. Never edit them by hand.
The commands are in [Contributing](../../CONTRIBUTING.md#generated-contracts).
Canonical encodings (request ledger, replication envelopes, and result
formats) are byte-exact. Change them only together with their golden vectors
and the [format reference](../format.md).

## Comments

Package comments state the responsibility and the bound, as `go list -f
'{{.Doc}}'` shows. Comments on functions and types explain the invariant and
why it holds, not the mechanics. `internal/kubeoperator` and
`internal/splitcapture` have no package comment yet; add one when you change
them.

## Source map

- [internal/raftservice/owner.go](../../internal/raftservice/owner.go) (sentinels and `Limits`)
- [gateway/durable_sql_request_executor.go](../../gateway/durable_sql_request_executor.go) (typed abort error)
- [internal/storeio/strict_allocation.go](../../internal/storeio/strict_allocation.go)
- [internal/buildgate/buildgate_test.go](../../internal/buildgate/buildgate_test.go) (allocation assertions)
- [bench/gate/README.md](../../bench/gate/README.md)
