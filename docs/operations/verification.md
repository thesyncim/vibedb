# Verify, salvage, and repack data

`vibedb-verify` is an offline tool for store files and database directories.

> **CAUTION:** Stop all writers before you use this tool. Use a quiescent file,
> a quiescent directory, or a copy. The verifier does not take the writer lock.
> A concurrent writer can retire and reuse data while the tool reads it.

## Build the tool

```bash
go build -o ./bin/vibedb-verify ./cmd/vibedb-verify
```

## Verify one store file

```bash
./bin/vibedb-verify verify ./data/users.vdb
```

The tool prints one line for each finding. It then prints page counts, a
machine-readable summary, and `result ok` or `result fail`.

The command exits with status 0 only when the report has no finding.

## Verify a database directory

```bash
./bin/vibedb-verify verify ./data
```

Directory verification checks transaction-decision and recovery-journal
pairing. It reports missing participants, identity mismatches, torn decisions,
same-epoch records with no decision, and other in-doubt conditions.

Directory verification reads `txn.vtm` and collection journals. It does not
append, sync, recycle, remove, or repair them.

## Salvage a damaged store

```bash
./bin/vibedb-verify salvage ./users.vdb ./users-salvaged.vdb
```

Salvage scans recoverable primary leaves and writes a new portable store. It
reports scanned leaves, retained buckets and documents, skipped overflow or
duplicate data, and the output size.

The output path must not exist. Keep the source unchanged until you validate
the new file.

Salvage is a data-recovery operation. It can omit data that it cannot prove is
valid. Compare the result with an application-level source of truth.

## Repack a healthy store

```bash
./bin/vibedb-verify repack ./users.vdb ./users-repacked.vdb
```

Repack opens the source through the normal snapshot read path. It writes rows
in lexical order to a new portable store. This restores clustered scan
locality and removes free-space churn.

The source needs read and write access because normal open can take a lock and
complete a pending rollback. The output path must not exist.

Repack supports the inline-primary format only.

## Safe replacement procedure

1. Stop every process that can open the database.
2. Make a backup of the primary file and its recovery-journal sibling.
3. Run `verify` against the stopped source.
4. Run `salvage` or `repack` to a new path.
5. Run `verify` against the new file.
6. Test the new file with application-level checks.
7. Replace the data only with a filesystem procedure that preserves the
   primary and journal pairing rules.

Do not overwrite the source with the tool output. The commands use exclusive
output creation to prevent this mistake.

## Implementation references

- `cmd/vibedb-verify/main.go`
- `store/durable/store_file_verify.go`
- `store/durable/store_file_repack.go`
- `cmd/vibedb-verify/main_test.go`
