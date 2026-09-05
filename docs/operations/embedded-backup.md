# Back up an embedded database

[Documentation](../README.md) / [Operations](README.md) / Embedded backup

Copy the complete database directory after a successful close, then verify
and reopen a separate restored copy with the same build. This procedure is
for a database opened through the native facade. For running RF3 groups, use
[distributed backup and restore](backup-restore.md).

## Before you copy

Stop application writes and release query results, sessions, transactions,
and snapshots. Call `db.Close()` and check its result. If close reports an
error, resolve it using the [native lifecycle contract](../api/native.md)
before treating the files as a completed backup source.

A database directory can contain collection files, recovery journals, and a
transaction decision log. Copying one apparent data file or copying while a
writer is active does not preserve that recovery unit. Writer locks coordinate
cooperating processes; they do not make a filesystem copy coherent.

## Create a copy

Build the verifier from the same checkout used to build the application:

```sh
mkdir -p ./bin
GOEXPERIMENT=simd go build -o ./bin/vibedb-verify ./cmd/vibedb-verify
```

With the application stopped and successfully closed, adapt the source path
and choose an absent backup destination:

```sh
source_dir=/absolute/path/to/closed-database
backup_dir=/absolute/path/to/new-backup
(test ! -e "$backup_dir" && cp -pR "$source_dir" "$backup_dir")
./bin/vibedb-verify verify "$backup_dir"
```

The copy command refuses an existing destination. Preserve permissions and
protect the backup under the same access policy as the source. Store the
application revision, build settings, configuration, and any external keys
beside your backup inventory.

## Test a restore

1. Copy the backup into another absent directory. Keep the backup itself closed.
2. Run `vibedb-verify verify` on that restored directory and inspect all findings.
3. Configure an isolated instance of the same application build to open it.
4. Read known keys and verify application invariants, including indexed queries
   and related records committed together.
5. Close the verification instance and inspect its close result.

File verification checks structural consistency. Application reads establish
whether the restored state contains the data you expected. Record both results
before replacing a failed application instance.

## If verification fails

Preserve the source and backup. Recheck that the source was closed, the whole
directory was copied, and the verifier matches the writer build. Use
[offline verification and salvage](verification.md) to investigate remaining
findings. Salvage can omit unprovable data, so its output needs a separate
application-level review.

## Source map

- [Facade ownership and close](../../vibedb.go).
- [Lifecycle tests](../../vibedb_lifecycle_internal_test.go).
- [Offline verifier](../../cmd/vibedb-verify/main.go).
- [Database transaction recovery](../../store/durable/).
