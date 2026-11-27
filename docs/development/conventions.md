# Coding conventions

[Documentation](../README.md) / [Developer guide](README.md)

Patterns enforced by tests and code review across VibeDB.

## Errors and identity

- **Package sentinels:** Export `Err...` sentinels with `"pkg: message"` text,
  wrap with `%w`, and branch with `errors.Is`/`errors.As` (never match error
  strings). Typed errors carrying metadata must `Unwrap()` to their sentinel.
- **Separate pre-admission refusal from unknown outcome:** Never collapse
  `ErrDurableSQLNotAdmitted`/`ErrServingFence`/`ErrIngressFull` into
  `ErrOutcomeUnknown`. Use `internal/serviceerrors` to preserve real shutdown
  errors.
- **Exact-once retries:** Mint request identity once before first send; after
  possible admission, retry the **exact same identity and bytes**
  ([exactly-once writes](../design/exactly-once-writes.md#retry-contract)).
- **Fences:** Any new routing/visibility coordinate must be included in the
  command fence with a stale-identity rejection test.

## Resource bounds and concurrency

- **Explicit admission bounds:** Every queue, cache, history, and wire decoder
  enforces explicit `Max*Items` / `Max*Bytes` limits (checking lengths before
  allocating). Constructors reject zero/negative/over-ceiling bounds with
  `ErrInvalid*`. Keep client and Raft peer budgets independent.
- **Do not size from boot state:** Groups, nodes, and sessions grow at runtime;
  size from declared ceilings or grow under an explicit cap.
- **Wait on conditions, not time:** Wait on durable/observable barriers with a
  deadline; use `testing/synctest` for timer/retry logic
  ([testing strategy](testing-strategy.md#core-testing-rules)).

## Allocations, platform code, and `unsafe`

- **Hot-path allocations:** Prove zero/bounded allocations with
  `testing.AllocsPerRun`; prefer `Append*(dst []byte, ...)` encoders;
  `bench/gate` rejects any `allocs/op` increase or `>5%` `B/op` increase.
- **Platform splits:** Use `_linux.go`, `_darwin.go`, `_windows.go`, and a
  fail-closed `_other.go` fallback (CI cross-compiles `linux/386` and Windows).
  SIMD kernels require a portable fallback and differential test ([SIMD](../simd.md)).
- **`unsafe`:** Follow [UNSAFE.md](../../UNSAFE.md) and update the inventory
  with `go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update`.
- **Generated files:** Never hand-edit generated blocks; regenerate via
  [Contributing](../../CONTRIBUTING.md#generated-contracts).

## Source map

- [internal/raftservice/owner.go](../../internal/raftservice/owner.go) · [gateway/durable_sql_request_executor.go](../../gateway/durable_sql_request_executor.go) · [internal/buildgate/buildgate_test.go](../../internal/buildgate/buildgate_test.go)
