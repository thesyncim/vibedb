# Developer guide

[Documentation](../README.md) / Developer guide

These pages are for engineers who change VibeDB. They cover how the repository
is laid out, how to build and test it, how the test layers fit together, how to
debug a failed multi-process run, and the conventions reviewers expect. Read
[Contributing](../../CONTRIBUTING.md) first for the change checklist and the
evidence each kind of change needs.

| Page | Use it to |
| --- | --- |
| [Repository map](repository-map.md) | Find the package that owns a behavior, and which packages are commands, libraries, or nested modules. |
| [Build and test](build-and-test.md) | Run make targets, reproduce a CI shard, run race and Linux-only tests, and use a Linux container from macOS. |
| [Testing strategy](testing-strategy.md) | Choose between unit, differential, fuzz, deterministic simulation, `synctest`, process, fault-injection, and qualification tests. |
| [Debugging distributed failures](debugging.md) | Read CI evidence, collect `VIBEDB_RF3_DIAGNOSTIC` records, keep failed state, and narrow a failure to its root cause. |
| [Coding conventions](conventions.md) | Follow the error, resource-bound, identity, allocation, and unsafe-code rules that the tests enforce. |

Related records:

- [Qualification workflows and records](../qualification/README.md): every
  qualification workflow, what it proves, and its runner.
- [Benchmark reports](../benchmarks/README.md) and
  [performance methodology](../performance.md).
- [CI performance investigation](../ci-performance.md): the 2026-09-04
  measurement behind the current CI sharding.
- [Unsafe-code boundary](../../UNSAFE.md), [source provenance](../provenance.md),
  and [security policy](../../SECURITY.md).

## Ground rules

- The toolchain is Go 1.27 (`go.mod`). Repository scripts and CI build with
  `GOEXPERIMENT=simd`; raw `go` commands need it set explicitly.
- Many durable and multi-process tests run only on Linux. A macOS pass with
  those tests filtered in can be a silent "no tests to run". See
  [Linux-only tests](build-and-test.md#linux-only-tests).
- Process qualifications are skipped unless their environment gate is set.
  CI rejects a skipped qualification; a local run must check for `SKIP` too.
- VibeDB is unreleased. Changing a wire or disk grammar replaces the current
  contract. It does not add a compatibility reader; see
  [change the development disk format](../../CONTRIBUTING.md#change-the-development-disk-format).
