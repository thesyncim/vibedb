# PostgreSQL upstream regression frontier

[Client guide](../../docs/api/pgwire.md)

> **Development status:** VibeDB's SQL and pgwire behavior is incomplete and can
> change or break at any commit. This harness is a development compatibility
> probe, not PostgreSQL certification, a supported-version promise, or a stable
> release gate.

## Current status: zero approved tests

**`approved-tests.txt` currently contains 0 approved upstream tests.** There is
no checked-in compatibility percentage or durable result report.

That has an important consequence: an ordinary semantic/output mismatch is
recorded but does **not** fail the command. With the current empty ratchet, a run
can exit successfully even if every completed script is marked `mismatch`.
Only a timeout, a psql/client failure, or regression of a selected approved test
causes status 1.

The harness compares VibeDB with PostgreSQL's unmodified regression inputs at:

- tag `REL_18_6`;
- commit `724edf9bde9d356724ad384a2e196edc3c9f80f7`; and
- the digest-pinned PostgreSQL 18.6 client image in `postgres.env` when the
  caller enables image mode.

This is separate from the pgclient integration test that pins and asserts stock
`psql` 18.4.

## What the harness does

For one invocation it:

1. clones or reuses the exact pinned PostgreSQL source and verifies its commit;
2. starts one disposable, loopback-only VibeDB pgwire server with trust
   authentication;
3. runs either the fixed 21-script smoke selection or every test in the pinned
   upstream `parallel_schedule`, serially, against that shared fresh catalog;
4. feeds upstream SQL to `psql` unchanged with fixed locale/session settings;
5. compares output byte-for-byte with the canonical expected file or one of
   PostgreSQL's accepted `_0` through `_9` alternatives; and
6. writes a report, machine-readable results, diffs, error signatures, and a
   summary.

An exact row establishes only exact output for that script, client, revision,
and harness configuration. The upstream regression corpus is a behavior suite,
not ISO SQL certification, and VibeDB's supported SQL subset remains narrower.
See PostgreSQL's [regression-test documentation](https://www.postgresql.org/docs/18/regress.html).

## Requirements

- Bash, Git, Go 1.27, and standard Unix text tools
- network access for the first corpus clone
- either Docker for the pinned client image or a local `psql`
- GNU `timeout`, `gtimeout`, or Perl for per-script deadlines

Docker image mode is the reproducible path used by CI. A local `psql` is accepted
and its version is recorded, but it is not pinned by the harness and should be
treated as exploratory evidence.

## Run a pinned smoke suite

From the repository root on a Linux host with Docker:

```bash
source integration/pgcompat/postgres.env
VIBEDB_PSQL_IMAGE="$PSQL_IMAGE" \
  integration/pgcompat/run-postgres-regression.sh \
  --suite smoke \
  --per-test-timeout 30 \
  --output "$(pwd)/pgcompat-evidence"
```

Run the complete pinned upstream schedule:

```bash
source integration/pgcompat/postgres.env
VIBEDB_PSQL_IMAGE="$PSQL_IMAGE" \
  integration/pgcompat/run-postgres-regression.sh \
  --suite full \
  --per-test-timeout 30 \
  --output "$(pwd)/pgcompat-evidence"
```

Reuse a verified corpus checkout with `--corpus DIR` or
`VIBEDB_PG_CORPUS_DIR=DIR`. The script still rejects a commit mismatch.

For local exploratory work only:

```bash
PSQL=/absolute/path/to/psql \
  integration/pgcompat/run-postgres-regression.sh --suite smoke
```

Use `--tests a,b,c` only for debugging. It overrides suite selection, may omit
upstream schedule prerequisites, and the current generated report still prints
the separate `--suite` label. Do not present a custom-selection report as smoke
or full-suite evidence.

## Read the evidence

Every invocation creates a timestamped directory beneath `--output`:

| File | Meaning |
| --- | --- |
| `report.md` | Human-readable row table and error summary |
| `results.tsv` | `exact`, `mismatch`, `timeout`, or `client-failure` per script |
| `results/*.out` | Actual psql output |
| `diffs/*.diff` | Smallest expected-versus-actual diff for nonexact rows |
| `error-signatures.txt` | Aggregated observed `ERROR:` lines |
| `summary.env` | Counts and pinned PostgreSQL revision |
| `server.log` | Disposable VibeDB server output |
| `psql-version.txt` | Client version actually used |

Default artifacts are ignored by Git. The scheduled/manual GitHub workflow
uploads them for 30 days and appends the report to the job summary; it does not
publish a persistent in-repository compatibility matrix and is not triggered by
pull requests.

The report records the VibeDB `HEAD` revision but not dirty-tree state or a tree
digest. Do not publish local evidence from an uncommitted checkout as if it were
reproducible.

## Approval ratchet

Approval is a checked-in name in `approved-tests.txt`, not a runtime reviewer
action. Add a name only after an exact result has been reviewed and is intended
to become a nonregression contract.

- A full run requires every approved name to execute and remain exact.
- Smoke or explicit-test runs check only approved names they selected; approvals
  outside that selection are ignored.
- With the current empty file, there is no semantic compatibility ratchet.
