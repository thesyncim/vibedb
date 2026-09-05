# Horizontal execution assessment plan

Status: plan only. This file contains no measurements or performance claim.

Freeze the two source identities before starting the campaign:

- `M` is the exact latest-main commit under review.
- `S` is the exact final-production commit under review.
- `CLIENT_SOURCE` is a clean immutable VibeDB checkout used to build the one
  shared `rf3-sqlbench` client for every arm.

The first pair measures cumulative change from `M` to `S` with the production
feature disabled (`S-off`, the default). The second pair uses `S` for both
immutable refs and enables `--read-authority` only on the after arm (`S-off` to
`S-on`). The runner permits equal refs only for a non-empty after-arm argument,
records that provenance, and verifies equal hashes for the independently built
same-ref server binaries.

Each output path below must be a new absolute directory. Each invocation runs
both matched orders, with a fresh fixture and volume for every arm, and retains
the CockroachDB oracle and VibeDB topology/durability validation. Run the two
physical-node counts as separate invocations:

```bash
cd /private/tmp/vibedb-horizontal

REPO=/private/tmp/vibedb-horizontal
CLIENT_SOURCE="<CLEAN_IMMUTABLE_CLIENT_SOURCE>"
M="<LATEST_MAIN_COMMIT>"
S="<FINAL_PRODUCTION_COMMIT>"
WORKLOADS=point_hit,point_miss,range_32,range_64,range_256,group_16,update_existing,mixed_read_update

run_matrix() {
  output="$1"
  before="$2"
  after="$3"
  nodes="$4"
  after_arg="${5:-}"
  extra_args=()
  if [ -n "$after_arg" ]; then
    extra_args+=("--after-arg=$after_arg")
  fi
  python3 scripts/bench/run-distributed-read-comparison.py "$output" \
    --repo "$REPO" --client-source "$CLIENT_SOURCE" \
    --baseline-ref "$before" --candidate-ref "$after" \
    --workloads "$WORKLOADS" --clients 1,8 --groups 16 \
    --physical-nodes "$nodes" --rows 8192 --operations 20000 \
    --scans 2000 --warmup 1000 --repetitions 3 \
    --cpus 12 --memory 24g "${extra_args[@]}"
}

# Cumulative M -> S-off.
run_matrix /private/tmp/vibedb-assessment-m-to-s-off-3n "$M" "$S" 3
run_matrix /private/tmp/vibedb-assessment-m-to-s-off-6n "$M" "$S" 6

# Same-binary S-off -> S-on. The only arm difference is this exact argv token.
run_matrix /private/tmp/vibedb-assessment-s-off-to-s-on-3n "$S" "$S" 3 --read-authority
run_matrix /private/tmp/vibedb-assessment-s-off-to-s-on-6n "$S" "$S" 6 --read-authority
```

The matrix covers C1/C8, 16 logical groups, N3/N6 physical-node fixtures,
seven named read/write workloads (`point_hit`, `point_miss`, `range_32`,
`range_64`, `range_256`, `group_16`, `update_existing`) and the explicit mixed
read/update control. Every workload has three repetitions. Setup, seeding,
verification, readiness, topology inventories, diagnostics and teardown stay
outside the timed operation phase; failed or incomplete oracle/durability
validation disqualifies that cell from any later analysis.

This is a fixed-resource single-host diagnostic: the six physical-node arm is
six processes sharing one Docker host, an aggregate 12-CPU/24-GiB ceiling, and
one loopback SQL frontend. It does not establish independent-machine scaling,
network scaling, peak per-query CPU/RSS, failure recovery, or a CockroachDB
performance ratio. Do not publish a performance claim from this plan until the
retained manifests, reports, oracle checks, durability/topology evidence and
both orderings have been independently reviewed.
