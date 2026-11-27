# Back up an embedded database

[Documentation](../README.md) / [Operations](README.md) / Embedded backup

Back up an embedded database by closing it cleanly, copying the entire
directory as one unit (`.vjc` primaries, `.rjournal` siblings, `txn.vtm`), and
verifying the copy with `vibedb-verify` from the exact same build. For RF3
clusters, see [RF3 backup and restore](backup-restore.md).

## Copy and verify

1. Release all query results, sessions, transactions, and snapshots; call
   `db.Close()` and confirm `CloseCompleted(err)` with no persistence error.
2. Build `vibedb-verify` from the same revision and copy into an absent path:

   ```sh
   GOEXPERIMENT=simd go build -o ./bin/vibedb-verify ./cmd/vibedb-verify
   test ! -e "$backup_dir" && cp -pR "$source_dir" "$backup_dir"
   ./bin/vibedb-verify verify "$backup_dir"
   ```

3. Expect `summary txn_log=... findings=0` and `result ok` (exit 0). Record the
   writer commit alongside the backup.

## Test a restore

Copy the backup to a fresh directory, verify it with `vibedb-verify verify`,
open it with the same application build, validate key/index reads, and close
cleanly. If verification fails, preserve both source and backup and follow
[offline verification and salvage](verification.md).

## Limitations

- No online or incremental backup API for embedded databases; writers must
  close first.
- Backups reopen only with the exact build that wrote them ([upgrades](upgrades.md)).

## Source map

- [vibedb.go](../../vibedb.go) · [cmd/vibedb-verify/main.go](../../cmd/vibedb-verify/main.go)
