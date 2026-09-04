# Fused-node SQL comparison method

`run-fused-node-comparison.py` controls the structural fused-node comparison.
It records evidence and failures; it does not automatically approve promotion
or claim the full 2x CockroachDB objective. Actual timing must wait for runtime
correctness review and a quiet measurement host.

The pinned parent is `37a171521459199b8cea9fe5f3ad50ce0b325597`, including the
reviewed read-only-cut and no-write-transaction fixes. The candidate must be a
different commit containing those fixes. References are resolved once and exact
commits are checked out into detached server worktrees. The client source must
be clean and receives a third detached worktree. The caller's checkout is never
reset. Go 1.27 server/client builds, with `GOEXPERIMENT=simd` and read-only module
resolution, finish before fixture startup. One `rf3-sqlbench` binary from
`integration/pgclient` supplies the workload and verification for every engine.
Revisions, status, patches, binary hashes, control-script copies and command
failures are retained. No builds, tests or profile processing run during timing.

CockroachDB is v26.3.1 at the digest in the runner. Its binary is extracted
before startup. The Linux runtime image is resolved once and all fixtures use
that image ID. CockroachDB retains RF3, inter-node TLS, a 512 MiB cache and a
512 MiB SQL memory allowance per node. Both products use ordinary loopback
trust/plaintext SQL connections. No durability setting is disabled.

Every engine, cell and AB/BA ordering gets a fresh container, named data volume
and evidence directory. The default ceiling is 12 CPUs and 24 GiB for the
entire container, including every server, the supervisor and shared client.
Swap is disabled. Actual Docker CPU, memory and memory/swap limits are checked;
cgroup and VM facts are retained. These are single-host diagnostics with three
or six physical-node identities, not independent machines.

The parent has nine legacy shard role processes and node logs plus a standalone
gateway. The candidate has three or six fused serving processes and node logs.
Both VibeDB fixtures also have a development supervisor. Serving counts use
exact `/proc/PID/exe` identities, not truncated Linux `comm` names or command-line
substring matches. Retained manifests separately prove storage IDs, node-log
paths and a distinct coherent three-node roster for every measured table.
Process counts, physical-node identities and replication factor remain separate.

Run the existing five-workload C1/C8 matrix after the candidate and shared client
are committed and reviewed:

```sh
python3 scripts/bench/run-fused-node-comparison.py /absolute/new/evidence \
  --candidate-ref <candidate-commit> --matrix base \
  --vibedb-sql-ports 5432 --order both
```

`--order both` runs fresh fixtures in two exact engine sequences. With CRDB
included, `parent-first` is `[parent, candidate, crdb]` and `candidate-first`
is `[crdb, candidate, parent]`; without CRDB, the sequences are
`[parent, candidate]` and `[candidate, parent]`. The labels describe the
relative parent/candidate order, while the manifest records each exact sequence
and the planned runs use that same sequence. The baseline uses one SQL entrypoint
for every engine. Defaults are primary-key hit, primary-key miss, ordered 64-row
range, 16-group aggregate and existing-row update; C1/C8; 8,192 rows; 20,000
point/update operations; 2,000 range/group operations; 1,000 serial warmups before
each trial; and three repetitions. Repetitions inside a fixture preserve and
verify accumulated update state.

The full matrix adds four tables, uniform/skewed distributions and mixed traffic
at three and six candidate physical nodes:

```sh
python3 scripts/bench/run-fused-node-comparison.py /absolute/new/evidence \
  --candidate-ref <candidate-commit> --matrix all --physical-nodes 3,6 \
  --groups 4 --distributions uniform,skewed --endpoint-modes single,per-node \
  --multigroup-workloads mixed_read_update --multigroup-clients 8 \
  --vibedb-sql-ports 5432,5433,5434,5435,5436,5437 --order both
```

Single-entrypoint three-node multigroup cells retain a matched parent comparison.
The parent receives one initial `--table-schema`; the shared client enrolls
further tables through existing public `CREATE TABLE`. The candidate receives
one `--table-schema PATH` per table and the client requires those tables to exist.
An untimed `-phase setup` invocation seeds the data before `-phase run` measures
it. `setup.json` and `setup-client.log` record setup separately. Post-setup
inventories must prove distinct data groups for every table before timing starts.

Separate `-frontends` cells use one endpoint per physical node. The candidate
receives `--pg-listens addr1,addr2,...`; single-entrypoint cells use `--pg-listen
addr`. Candidate and CockroachDB clients use the same round-robin rule: endpoint
index is client index modulo endpoint count. C8 on six endpoints assigns
2,2,1,1,1,1 clients. There is no leader-aware routing or preferential placement;
published leader and range inventories are retained. The parent has only one
standalone frontend and no six-physical-node fixture. Those specific cells get
explicit `unsupported` records; they are not replaced by extra replicas or
fabricated rates. Candidate/CRDB scaling cells remain separate diagnostics.

Read keys, table selection and mixed read/update choices use independent
deterministic hash streams. Read keys are selected with replacement, avoiding
the previous stride's correlation between client index and key buckets. The
selection version is reported; the validator does not equate current sampling
with archived stride-based reports. Skewed selection assigns the configured hot
percentage (default 80%) to table zero and spreads the remainder over the others.
Updates use disjoint per-client keys in every table; mixed reads exclude current
clients' update keys. This makes no hot-key contention claim. Each operation's
returned fields are checked, and every stored field in every table is checked
before measurement and after each trial.

Latency includes statement construction, SQL execution, response handling and
per-operation oracle checks. Elapsed time covers the closed-loop worker interval
and its bookkeeping. Connections, serial warmup, full-table verification,
diagnostic snapshots, percentile computation and serialization are outside the
timer. Throughput is successful operations divided by elapsed time; p50/p95/p99
use nearest-rank order statistics over every sample, including failures. Failed
trials never contribute to a complete cell's median.

Schema-2 reports retain an RFC3339Nano UTC anchor, monotonic start offsets,
ordinal, client, endpoint, group, table and operation identity. Validation checks
exact sample counts/order, deterministic assignments, errors, throughput,
percentiles, sample ends within elapsed time and nonoverlap within each client.
Atomic checkpoints retain completed raw trials before verification and before
the next trial starts. Early connection or verification failure still leaves a
structured report. Abrupt death leaves the last checkpoint and an unfinished
trial marker. Samples still only in the killed process's memory cannot be
recovered; that trial stays incomplete and supplies no valid throughput result.

Ready candidates take acknowledged SIGUSR1 snapshots around every timed trial.
Each exact candidate PID/executable and `serve-node` manifest is bound to its
local node ID and node-root `rf3-diagnostics.json`. After warmup, the client
signals those processes and awaits matching PID/node IDs and advancing serials
before starting the timer. It repeats after recording elapsed time. The parent
is never sent SIGUSR1. Exact bytes and hashes are archived per trial; reports
retain cumulative counter/histogram deltas with integer precision. Gauges stay
as raw snapshots. Deltas include background activity between snapshots and do
not claim to count only timed operations. Offsets, identities, serials, archived
bytes and deltas are validated. A missing snapshot retains the measured raw
trial as failed and unverified. Servers retain only bounded latest snapshots.

Evidence includes raw logs, command failures, failed/unfinished/unsupported run
records, process/listener inventories, Docker/cgroup facts, manifests,
diagnostic archives, CRDB ranges, logical payload counts, and apparent/allocated
file bytes. Endpoint labels contain host:port; URL credentials are redacted from
control evidence and client errors. Available evidence survives setup, readiness
and verification failure. Failed container removal stops subsequent measurement
so an old fixture cannot consume resources in the next cell.

The validator remains compatible with archived reports lacking the new fields:

```sh
python3 scripts/bench/summarize-crdb-sql.py \
  /absolute/evidence/baseline-c1-c8/parent-first/parent/report.json \
  /absolute/evidence/baseline-c1-c8/parent-first/crdb/report.json
```

The registered gate remains at least 1.25x parent geometric-mean throughput
across the existing matrix, no per-cell median throughput regression over 5%,
and no per-cell median p99 regression over 10%, reproduced in both orderings.
Every required comparison must be complete, with demonstrable multigroup benefit
and all correctness/failure gates passing. Failed, unsupported, selectively
repeated or uncertain comparisons cannot count as passing evidence. This tranche
gate does not replace the full at-least-2x CockroachDB objective.
