# RF3 SQL comparison method

[Benchmark methods](README.md)

`scripts/bench/run-crdb-sql-comparison.py` compares VibeDB RF3 against
CockroachDB v26.3.1 (pinned by digest) inside a single 12-CPU / 24 GiB Linux
Docker container on the same native volume with loopback SQL and inter-node TLS
enabled (durability intact on both engines).

## Run

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /abs/new/evidence-dir
python3 scripts/bench/run-crdb-sql-comparison.py /abs/new/indexed-evidence \
  --indexes pack-leading --rows 10000000 --timeout 4h --no-verify-every-trial --seed-batch 64
```

Use `--order crdb-first` to reverse engine order.

## Workloads and verification

- **Schema:** PK-only (`id`, `bucket`, `score`, 256-byte `payload`; default
  8,192 rows, up to 10M) or `--indexes pack-leading|pack-nonleading` adding
  `shared`/`a`/`b` and two compound indexes. CockroachDB runs `ANALYZE` and
  verifies RF3 ranges before timing.
- **Closed-loop workloads (C1 & C8, 3 reps):** PK hit, PK miss, 64-row range,
  16-group count/sum aggregate, and disjoint-key computed updates (20k
  point/update ops, 2k range/group ops, 1k warmup) via unnamed PostgreSQL
  extended Parse/Bind/Execute.
- **Verification:** Checks every returned row and verifies full table contents
  after each trial; never discards failed trials or retries unknown writes.
  Single-host diagnostic baseline only (not multi-host or scale-out proof).
