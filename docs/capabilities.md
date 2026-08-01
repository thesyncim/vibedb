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
| Executable case | Entry point | Indexing | Transaction | Keys | Operations | Durability / publication | Result | Atomic unit |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `native-point-unindexed` | native | unindexed | autocommit | one key | insert<br>update<br>delete | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; one Put/Delete call is one publication | yes; rejected sibling rolls back |
| `native-point-indexed` | native | indexed | autocommit | one key | insert<br>update<br>delete | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; the primary row and exact postings publish together | yes; rejected sibling rolls back |
| `native-point-mixed-one-unindexed` | native | unindexed | autocommit | one key | mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; separate publications; mixed point operations on one key are separate publications; use Collection.Update for an atomic group | no |
| `native-point-mixed-one-indexed` | native | indexed | autocommit | one key | mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; separate publications; each point publication maintains exact postings; use Collection.Update for an atomic group | no |
| `native-point-multiple-unindexed` | native | unindexed | autocommit | multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; separate publications; separate point calls are not a transaction | no |
| `native-point-multiple-indexed` | native | indexed | autocommit | multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem<br>async COW / power-safe<br>sync chain-fence / power-safe | success; separate publications; separate point calls each maintain exact postings but are not atomic as a group | no |
| `native-batch-unindexed` | native | unindexed | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem | success; Collection.Update publishes one logical failure-atomic cut; topology preparation may advance Generation without changing rows | yes; rejected sibling rolls back |
| `native-batch-fence-lane` | native | unindexed | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | async COW / power-safe<br>sync chain-fence / power-safe | documented error: `durable.ErrPrimaryBatchUnsupportedLane` | no |
| `native-batch-indexed` | native | indexed | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | sync-journal / power-safe<br>buffered-volatile / power-safe<br>buffered-volatile / filesystem<br>buffered-journal / power-safe<br>buffered-journal / filesystem | success; primary rows and exact postings publish in the same logical batch cut; topology preparation may advance Generation without changing either | yes; rejected sibling rolls back |
| `native-batch-indexed-fence-lane` | native | indexed | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | async COW / power-safe<br>sync chain-fence / power-safe | documented error: `durable.ErrPrimaryBatchUnsupportedLane` | no |
| `database-sql-autocommit-unindexed-one` | database/sql | unindexed | autocommit | one key | insert<br>update<br>delete | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back |
| `database-sql-autocommit-unindexed-multiple` | database/sql | unindexed | autocommit | multiple keys | insert<br>delete | fixed SQL default: sync-journal / power-safe | success; one multi-row INSERT or matching DELETE | yes; rejected sibling rolls back |
| `database-sql-autocommit-unindexed-multiple-update-error` | database/sql | unindexed | autocommit | multiple keys | update | fixed SQL default: sync-journal / power-safe | documented error: `driver.ErrUpdatePrimaryKey`; one constant whole-document replacement cannot preserve several distinct primary keys; use BEGIN with one replacement per key | no |
| `database-sql-autocommit-unindexed-mixed-error` | database/sql | unindexed | autocommit | one key<br>multiple keys | mixed | fixed SQL default: sync-journal / power-safe | documented error: `*sql.ParseError (only one statement may be parsed at a time)`; N/A as one database/sql Exec: it accepts one statement; use BEGIN for an atomic mixed group | no |
| `database-sql-autocommit-indexed-one` | database/sql | indexed | autocommit | one key | insert<br>update<br>delete | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back |
| `database-sql-autocommit-indexed-multiple` | database/sql | indexed | autocommit | multiple keys | insert<br>delete | fixed SQL default: sync-journal / power-safe | success; one multi-row INSERT or matching DELETE | yes; rejected sibling rolls back |
| `database-sql-autocommit-indexed-multiple-update-error` | database/sql | indexed | autocommit | multiple keys | update | fixed SQL default: sync-journal / power-safe | documented error: `driver.ErrUpdatePrimaryKey`; one constant whole-document replacement cannot preserve several distinct primary keys; use BEGIN with one replacement per key | no |
| `database-sql-autocommit-indexed-mixed-error` | database/sql | indexed | autocommit | one key<br>multiple keys | mixed | fixed SQL default: sync-journal / power-safe | documented error: `*sql.ParseError (only one statement may be parsed at a time)`; N/A as one database/sql Exec: it accepts one statement; use BEGIN for an atomic mixed group | no |
| `database-sql-transaction-unindexed` | database/sql | unindexed | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back |
| `database-sql-transaction-indexed-one` | database/sql | indexed | explicit transaction | one key | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back |
| `database-sql-transaction-indexed-multiple` | database/sql | indexed | explicit transaction | multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back |
| `pgwire-autocommit-unindexed` | pgwire | unindexed | autocommit | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; one simple-query message uses the shared SQL transaction overlay | yes; rejected sibling rolls back |
| `pgwire-autocommit-indexed-one` | pgwire | indexed | autocommit | one key | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; one simple-query message uses the shared SQL transaction overlay | yes; rejected sibling rolls back |
| `pgwire-autocommit-indexed-multiple` | pgwire | indexed | autocommit | multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success; multi-key update and mixed use several statements in one implicit simple-query transaction | yes; rejected sibling rolls back |
| `pgwire-transaction-unindexed` | pgwire | unindexed | explicit transaction | one key<br>multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back |
| `pgwire-transaction-indexed-one` | pgwire | indexed | explicit transaction | one key | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back |
| `pgwire-transaction-indexed-multiple` | pgwire | indexed | explicit transaction | multiple keys | insert<br>update<br>delete<br>mixed | fixed SQL default: sync-journal / power-safe | success | yes; rejected sibling rolls back |
<!-- capability-matrix:end -->

## How the result is checked

Successful atomic rows also run a rejected sibling. A preflight sibling
rejection must leave the prior generation, primary rows, document count, and
exact-index answers unchanged. Explicit SQL tests additionally force a rejected
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
