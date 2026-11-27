# Developer guide

[Documentation](../README.md) / Developer guide · [Contributing](../../CONTRIBUTING.md)

Engineering reference for modifying, testing, and debugging VibeDB.

| Guide | Covers |
| --- | --- |
| [Repository map](repository-map.md) | Package ownership, commands, internal layers, and nested modules |
| [Build and test](build-and-test.md) | Make targets, CI shards, race/SIMD lanes, Linux-only gates, Docker from macOS |
| [Testing strategy](testing-strategy.md) | Unit, differential, fuzz, crash-point, `raftsim`, `synctest`, and process gates |
| [Debugging distributed failures](debugging.md) | CI evidence, `go test -json`, `SIGUSR1` diagnostics, retaining failed state |
| [Coding conventions](conventions.md) | Error sentinels, bounded admission, exact-once retries, allocations, `unsafe` |

## Ground rules

- Go 1.27 (`go.mod`) with `GOEXPERIMENT=simd` (default in `make` and CI; set
  explicitly for raw `go` commands).
- Use the standard Go build cache (`go env GOCACHE`); never create a per-task
  or in-worktree build cache.
- Process qualifications skip unless their `VIBEDB_*_E2E=1` gate is set, and
  most are Linux-only (always pass `-count=1` and check for skips).
- Format 0 is edited in place: breaking wire/disk changes replace the contract
  across codecs, golden fixtures, and docs ([Contributing](../../CONTRIBUTING.md#match-evidence-to-the-change)).
