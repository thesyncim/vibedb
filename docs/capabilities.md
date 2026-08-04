# Database capability conformance

This table is not hand-authored product prose. Its rows come from
`internal/conformance.Cases`; native, `database/sql`, and pgwire adapter tests
execute those same case IDs, and a golden test rejects any table that differs
from the manifest.

“Atomic unit” means the named call, statement, protocol batch, or explicit
transaction is visible wholly or not at all. It does not turn several native
point calls into a transaction. SQL tables currently use the fixed zero-value
durability contract (`DurabilitySync` with the power-safe recovery journal);
the wider durability matrix is exposed by the native collection API.

SQL fixtures are pre-materialized only to keep DDL and initial file publication
outside each mutation assertion. Indexed steady-state mutations and explicit
transactions then enter the same bounded `Collection.Update` publication path
as the native batch cases.

<!-- capability-matrix:start -->
| Executable case | Entry point | Indexing | Tables | Transaction | Keys | Operations | Durability / publication | Result | Atomic unit |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `native-point-unindexed` | native | unindexed | one table | autocommit | one key | insert<br>update<br>delete | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; one Put/Delete call is one publication | yes; rejected sibling rolls back all participants |
| `native-point-indexed` | native | indexed | one table | autocommit | one key | insert<br>update<br>delete | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; the primary row and exact postings publish together | yes; rejected sibling rolls back all participants |
| `native-point-mixed-one-unindexed` | native | unindexed | one table | autocommit | one key | mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; separate publications; mixed point operations on one key are separate publications; use Collection.Update for an atomic group | no |
| `native-point-mixed-one-indexed` | native | indexed | one table | autocommit | one key | mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; separate publications; each point publication maintains exact postings; use Collection.Update for an atomic group | no |
| `native-point-multiple-unindexed` | native | unindexed | one table | autocommit | multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; separate publications; separate point calls are not a transaction | no |
| `native-point-multiple-indexed` | native | indexed | one table | autocommit | multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; separate publications; separate point calls each maintain exact postings but are not atomic as a group | no |
| `native-batch-unindexed` | native | unindexed | one table | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem | success; Collection.Update publishes one logical failure-atomic cut; topology preparation may advance Generation without changing rows | yes; rejected sibling rolls back all participants |
| `native-batch-fence-lane` | native | unindexed | one table | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | async COW / power-safe<br>sync chain-fence / power-safe | documented error: `durable.ErrPrimaryBatchUnsupportedLane` | no |
| `native-batch-indexed` | native | indexed | one table | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem | success; primary rows and exact postings publish in the same logical batch cut; topology preparation may advance Generation without changing either | yes; rejected sibling rolls back all participants |
| `native-batch-indexed-fence-lane` | native | indexed | one table | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | async COW / power-safe<br>sync chain-fence / power-safe | documented error: `durable.ErrPrimaryBatchUnsupportedLane` | no |
| `native-database-txn-unindexed` | native | unindexed | multiple tables | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-journal / power-safe<br>buffered-journal / filesystem | success; Database.Update / vibedb.Update: sync-journal and buffered-journal are crash-atomic after K prepare syncs + decision sync; buffered-journal durability precedes visibility; Memory profile is visibility-atomic with no crash dimension | yes; rejected sibling rolls back all participants |
| `native-database-txn-indexed` | native | indexed | multiple tables | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-journal / power-safe<br>buffered-journal / filesystem | success; primary rows and exact postings publish together across every participant; sync-journal and buffered-journal are crash-atomic after K prepare syncs + decision sync | yes; rejected sibling rolls back all participants |
| `native-database-txn-unsupported-lane` | native | unindexed | multiple tables | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | documented error: `durable.ErrDatabaseTransactionUnsupportedLane`; buffered-volatile, async-COW, and sync chain-fence refuse multi-collection commits; the facade Buffered profile maps to the same typed refusal | no |
| `database-sql-autocommit-unindexed-one` | database/sql | unindexed | one table | autocommit | one key | insert<br>update<br>delete | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back all participants |
| `database-sql-autocommit-unindexed-multiple` | database/sql | unindexed | one table | autocommit | multiple keys | insert<br>delete | fixed SQL default: sync-journal / power-safe | success; one multi-row INSERT or matching DELETE | yes; rejected sibling rolls back all participants |
| `database-sql-autocommit-unindexed-multiple-update-error` | database/sql | unindexed | one table | autocommit | multiple keys | update | fixed SQL default: sync-journal / power-safe | documented error: `driver.ErrUpdatePrimaryKey`; one constant whole-document replacement cannot preserve several distinct primary keys; use BEGIN with one replacement per key | no |
| `database-sql-autocommit-unindexed-mixed-error` | database/sql | unindexed | one table | autocommit | one key<br>multiple keys | mixed | fixed SQL default: sync-journal / power-safe | documented error: `*sql.ParseError (only one statement may be parsed at a time)`; N/A as one database/sql Exec: it accepts one statement; use BEGIN for an atomic mixed group | no |
| `database-sql-autocommit-indexed-one` | database/sql | indexed | one table | autocommit | one key | insert<br>update<br>delete | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back all participants |
| `database-sql-autocommit-indexed-multiple` | database/sql | indexed | one table | autocommit | multiple keys | insert<br>delete | fixed SQL default: sync-journal / power-safe | success; one multi-row INSERT or matching DELETE | yes; rejected sibling rolls back all participants |
| `database-sql-autocommit-indexed-multiple-update-error` | database/sql | indexed | one table | autocommit | multiple keys | update | fixed SQL default: sync-journal / power-safe | documented error: `driver.ErrUpdatePrimaryKey`; one constant whole-document replacement cannot preserve several distinct primary keys; use BEGIN with one replacement per key | no |
| `database-sql-autocommit-indexed-mixed-error` | database/sql | indexed | one table | autocommit | one key<br>multiple keys | mixed | fixed SQL default: sync-journal / power-safe | documented error: `*sql.ParseError (only one statement may be parsed at a time)`; N/A as one database/sql Exec: it accepts one statement; use BEGIN for an atomic mixed group | no |
| `database-sql-transaction-unindexed` | database/sql | unindexed | one table | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back all participants |
| `database-sql-transaction-indexed-one` | database/sql | indexed | one table | explicit transaction | one key | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back all participants |
| `database-sql-transaction-indexed-multiple` | database/sql | indexed | one table | explicit transaction | multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back all participants |
| `database-sql-transaction-multi-table-unindexed` | database/sql | unindexed | multiple tables | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; fixed SQL default sync-journal: crash-atomic after K prepare syncs + decision sync | yes; rejected sibling rolls back all participants |
| `database-sql-transaction-multi-table-indexed` | database/sql | indexed | multiple tables | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; fixed SQL default sync-journal: crash-atomic after K prepare syncs + decision sync | yes; rejected sibling rolls back all participants |
| `database-sql-transaction-savepoint` | database/sql | unindexed | one table<br>multiple tables | explicit transaction with savepoints | one key | mixed | fixed SQL default: sync-journal / power-safe | success; SAVEPOINT / ROLLBACK TO / RELEASE restore staged overlays without publishing | yes |
| `pgwire-autocommit-unindexed` | pgwire | unindexed | one table | autocommit | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; one simple-query message uses the shared SQL transaction overlay | yes; rejected sibling rolls back all participants |
| `pgwire-autocommit-indexed-one` | pgwire | indexed | one table | autocommit | one key | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; one simple-query message uses the shared SQL transaction overlay | yes; rejected sibling rolls back all participants |
| `pgwire-autocommit-indexed-multiple` | pgwire | indexed | one table | autocommit | multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; multi-key update and mixed use several statements in one implicit simple-query transaction | yes; rejected sibling rolls back all participants |
| `pgwire-transaction-unindexed` | pgwire | unindexed | one table | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back all participants |
| `pgwire-transaction-indexed-one` | pgwire | indexed | one table | explicit transaction | one key | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back all participants |
| `pgwire-transaction-indexed-multiple` | pgwire | indexed | one table | explicit transaction | multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back all participants |
| `pgwire-transaction-multi-table-unindexed` | pgwire | unindexed | multiple tables | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; fixed SQL default sync-journal: crash-atomic after K prepare syncs + decision sync | yes; rejected sibling rolls back all participants |
| `pgwire-transaction-multi-table-indexed` | pgwire | indexed | multiple tables | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; fixed SQL default sync-journal: crash-atomic after K prepare syncs + decision sync | yes; rejected sibling rolls back all participants |
| `pgwire-transaction-savepoint` | pgwire | unindexed | one table<br>multiple tables | explicit transaction with savepoints | one key | mixed | fixed SQL default: sync-journal / power-safe | success; SAVEPOINT / ROLLBACK TO / RELEASE; ROLLBACK TO recovers a failed transaction to status T | yes |
| `pgwire-transaction-serialization-failure` | pgwire | unindexed | multiple tables | explicit transaction | one key | update | fixed SQL default: sync-journal / power-safe | documented error: `SQLSTATE 40001`; first-committer-wins conflict on a multi-table write set surfaces as serialization_failure; no participant publishes | no |
<!-- capability-matrix:end -->

## How the result is checked

Successful atomic rows also run a rejected sibling. A preflight sibling
rejection must leave every participant collection's prior generation, primary
rows, document count, and exact-index answers unchanged. Explicit SQL tests additionally force a rejected
commit; database/sql proves first-committer-wins conflict rollback, while pgwire
proves failed-transaction status and `COMMIT`-as-rollback. The general atomic
promise is about logical rows and postings: bounded topology preparation may
publish a representation-only generation before retrying a batch. Documented-
error rows require the named Go error, no logical change, and a still-usable
collection or session.

Durability crash qualification is stricter than this semantic table. Journal
tests cover whole-record/torn-record, append, sync, and synced-before-publish
windows; device tests cover each data write, ordering barrier, alternate-root
write, final sync, torn root, and space failure. Those suites establish the
publication primitives' before-or-after cuts.
`TestFilePrimaryIndexedBatchCheckpointCrashBoundary` composes those device
faults with a multi-key indexed batch and validates both primary rows and exact
postings against the same before-or-after oracle.
