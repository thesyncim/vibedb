# Fused-node SQL comparison method

[Benchmark methods](README.md)

`scripts/bench/run-fused-node-comparison.py` compares legacy multi-process RF3
(parent) vs fused physical-node RF3 (candidate, 3 or 6 nodes) and CockroachDB
v26.3.1 (pinned by digest, RF3, inter-node TLS, 512 MiB cache + 512 MiB SQL
memory/node) in isolated Docker containers (12 CPUs, 24 GiB RAM, swap off).

## Run

```sh
# Base 5-workload C1/C8 matrix (both orderings):
python3 scripts/bench/run-fused-node-comparison.py /abs/new/evidence \
  --candidate-ref <commit> --matrix base --vibedb-sql-ports 5432 --order both

# Full 3/6-node, 4-table, uniform/skewed, single/per-node endpoint matrix:
python3 scripts/bench/run-fused-node-comparison.py /abs/new/evidence \
  --candidate-ref <commit> --matrix all --physical-nodes 3,6 \
  --groups 4 --distributions uniform,skewed --endpoint-modes single,per-node \
  --multigroup-workloads mixed_read_update --multigroup-clients 8 \
  --vibedb-sql-ports 5432,5433,5434,5435,5436,5437 --order both

# Summarize pair:
python3 scripts/bench/summarize-crdb-sql.py <report-a.json> <report-b.json>
```

## Protocol and gate

- **Workloads:** PK hit, PK miss, ordered 64-row range, 16-group aggregate,
  existing-row update, and `mixed_read_update` (defaults: 8,192 rows, 20k
  point/update ops, 2k range/group ops, 1k warmup, 3 reps, C1/C8).
- **Verification:** Shared `rf3-sqlbench` binary (`integration/pgclient`)
  checks every returned field and full table contents after each trial;
  candidates capture `SIGUSR1` `rf3-diagnostics.json` deltas around each trial.
- **Promotion gate:** ≥1.25× parent geomean throughput, ≤5% per-cell median
  throughput regression, and ≤10% per-cell median p99 regression in both
  engine orderings with zero failed/unverified trials.
