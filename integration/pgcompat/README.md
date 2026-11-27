# PostgreSQL upstream regression frontier

[Client guide](../../docs/api/pgwire.md)

Compares VibeDB's `pgwire` server against unmodified PostgreSQL `REL_18_6`
(`724edf9bde9d356724ad384a2e196edc3c9f80f7`) regression scripts.

> **Status:** `approved-tests.txt` currently has **0 approved tests**. Semantic
> mismatches are recorded without failing the run; only timeouts, client
> failures, or regressions in approved tests exit `1`.

## Run

Requires Bash, Git, Go 1.27+, `timeout`/`gtimeout`/Perl, and Docker (for the
digest-pinned `psql` 18.6 image in `postgres.env`) or a local `psql`:

```sh
source integration/pgcompat/postgres.env
VIBEDB_PSQL_IMAGE="$PSQL_IMAGE" \
  integration/pgcompat/run-postgres-regression.sh \
  --suite smoke --per-test-timeout 30 --output "$(pwd)/pgcompat-evidence"
```

Use `--suite full` for the complete `parallel_schedule`, `--corpus DIR` to
reuse a verified clone, or `--tests a,b` for local debugging only (never cite
custom `--tests` as smoke/full evidence).

## Artifacts and approval ratchet

Each run writes `report.md`, `results.tsv`, `results/*.out`, `diffs/*.diff`,
`error-signatures.txt`, `summary.env`, `server.log`, and `psql-version.txt`
(ignored by Git; uploaded for 30 days by nightly `postgresql-compatibility.yml`).

Add a script name to `approved-tests.txt` only after reviewing an exact match
to lock it in as a non-regression contract.
