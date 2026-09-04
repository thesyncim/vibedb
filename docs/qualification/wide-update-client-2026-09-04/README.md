# Wide-key client verification

Client/validator revision: `ba6070a2e37676418a6ba62684d0b80b20366980`.
The clean shared client checkout is `/private/tmp/vibedb-fused-wide-measure-client`.
These are unit and protocol-fixture results, not live database timings.

The Go 1.27 SIMD client race suite passes. Its protocol fixture checks both
engines' result formats, partial client stripes, warmup updates, repeated
trials, reads of updated rows, full key coverage and a corrupted final row.
All 40 Python tests pass. The two `*-regression-before.log` files retain
failures demonstrating that legacy-schema omission and insufficient operations
previously bypassed the new workloads' report checks.

Reproduce from the client checkout:

```sh
# In integration/pgclient:
GOEXPERIMENT=simd go test -race ./cmd/rf3-sqlbench -count=1

# In the repository root:
python3 -m unittest scripts/bench/test_run_fused_node_comparison.py scripts/bench/test_summarize_crdb_sql.py
```

`sha256.json` records the five unmodified review logs. No database files,
runtime manifests, credentials or server logs are included.
