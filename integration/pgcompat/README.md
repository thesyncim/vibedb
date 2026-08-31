# Upstream PostgreSQL compatibility suite

This lane runs VibeDB against PostgreSQL's own unmodified regression SQL and
expected output. The source is pinned to PostgreSQL `REL_18_6` at commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7`; no PostgreSQL test is copied into
or maintained by this repository.

It answers two different questions:

- The report shows missing SQL behavior and semantic/output differences across
  the upstream corpus. A mismatch is a compatibility gap, not a test skip.
- `approved-tests.txt` is the ratchet. Once an upstream script is byte-for-byte
  compatible, listing its name makes any later difference fail the lane.

The full PostgreSQL regression suite is a behavior suite, not an ISO SQL
certification. PostgreSQL describes it as a comprehensive test set for its SQL
implementation, covering standard operations and PostgreSQL extensions. It is
therefore the canonical external reference for a PostgreSQL-compatible surface.
VibeDB still documents its supported SQL subset separately. See PostgreSQL's
[regression-test documentation](https://www.postgresql.org/docs/18/regress.html).

## Run it

The smoke run needs Go, Git, and `psql`:

```sh
integration/pgcompat/run-postgres-regression.sh --suite smoke
```

Run every test in PostgreSQL's upstream schedule:

```sh
integration/pgcompat/run-postgres-regression.sh \
  --suite full \
  --per-test-timeout 30
```

Each invocation uses a fresh disposable VibeDB catalog and creates a new
timestamped evidence directory. `report.md` is the dashboard, `results.tsv` is
machine-readable, and `diffs/` contains the upstream expected-vs-VibeDB output
for each nonmatching script. Set `VIBEDB_PG_CORPUS_DIR` to reuse an existing
checkout. CI uses the digest-pinned PostgreSQL 18.6 `psql` image recorded in
`postgres.env` and uploads the complete report.

Unapproved semantic mismatches remain observational so the report can expose
the whole compatibility frontier. A timeout, a client/server failure, or a
regression in an approved exact test always makes the command fail.

The harness deliberately keeps PostgreSQL's statements, ordering, psql echo
mode, and expected files intact. It only serializes the schedule and gives each
script a hard timeout so unsupported or pathological features cannot consume
the entire CI job.
