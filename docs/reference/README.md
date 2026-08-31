# Reference

> [!CAUTION]
> This reference describes one unreleased source snapshot. APIs, commands,
> wire/disk formats, defaults, and hard limits may change at any commit. Use the
> matching docs and binary with disposable or recoverable data; these values are
> not service-level objectives.

Read the [stability and current-status page](../status.md) before treating a
reference value as a contract. Reference pages answer exact lookup questions;
start with a tutorial or API guide if you are learning the system.

| Need | Reference | Scope |
| --- | --- | --- |
| Find a binary, flag, default, or exit behavior | [Command-line tools](cli.md) | Checked-in commands and their development-only operating boundaries |
| Check syntax, semantics, or an unsupported statement | [SQL dialect](sql.md) | The VibeDB SQL subset shared by its SQL adapters |
| Inspect a frame, identity, error, or retry rule | [Development protocols](protocols.md) | pgwire plus internal service protocols; not a compatibility promise |
| Look up a default or hard bound | [Defaults and limits](limits.md) | Source-defined values, grouped by subsystem |
| Inspect the current storage grammar | [Current disk format](../format.md) | One development format, not a migration catalog |
| Compare embedded entry points | [Executable embedded capabilities](../capabilities.md) | Generated conformance cases for native, SQL, and embedded pgwire |
| Trace distributed implementation evidence | [Generated distributed feature ledger](../distributed-feature-state.md) | Primitive, integrated, development-command, and qualification stages |

For lifecycle and error-handling rules, use the relevant [API
guide](../api/README.md); for procedures, use the [operations
guides](../operations/README.md).
