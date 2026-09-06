# Online scale in and out qualification

[Benchmark reports](README.md) / Online scale in and out qualification

This method qualifies a real Linux process cluster through a physical-node
scale cycle. The fixture starts with three serving nodes, enrolls an empty
fourth node, moves application and internal RF3 groups while authenticated
traffic continues, then drains and stops an original node and verifies the
three-node cluster after the stop. It repeats the physical 3-to-4-to-3 wave
three times with fresh target identities while retaining two survivor
frontends. It also restarts the controller and target during migration and
retries the same operation IDs. A process skip is a qualification failure in
CI.

The foreground workload uses a bounded calibration with the same mixed
open-loop SQL/native workload, selects one sustainable offered rate, and keeps
that rate fixed for baseline, migration, and after windows. Latency is measured
from scheduled arrival to validated response, including queueing and retries.
Samples are assigned to a phase by scheduled arrival, so migration requests
cannot move into a quiet post-migration window. Surviving gateways and SQL
sessions stay connected for the full wave. A connection associated with the
retiring frontend must keep `safe_to_stop` false until it gracefully completes
and disconnects. The fixture retains an exact `(table, key, version, value,
request)` acknowledgement set and verifies every unique acknowledgement after
the stop.

## Evidence format

The test writes one bounded TSV file per run. The first rows identify the
schema and terminal phase:

```text
schema	vibedb.seamless-scale-in-out	1
result	pass
phase	terminal_post_stop_verified
```

Topology rows record `physical_before`, `physical_peak`, `physical_after`,
`application_groups_moved`, `internal_groups_moved`, actual phase/window
counts, retiring references, and before/after group inventory digests. These
counts and digests come from the authenticated status response and are not
derived from a final node count.
Required marker rows are `empty_target_at_enrollment`,
`controller_restarted`, `target_restarted`, `duplicate_operation_stable`,
`post_restart_operation_recovered`, `safe_to_stop`, `node_stopped`,
`acknowledged_data_intact`, and `no_skipped_success`; each must be `1` for a
passing qualification.

Each phase contains actual window samples. The strict gate requires at least
five complete 10-second baseline windows, three complete during windows, three
complete after windows, and at least 10,000 scheduled samples in every
reported phase. Each phase has these workload rows:

| Metric | Meaning |
| --- | --- |
| `start_ns`, `end_ns`, `duration_ns` | Wall-clock phase span and active measurement duration. |
| `scheduled`, `started`, `completed`, `requests` | Open-loop arrivals and lifecycle counts. |
| `successes` | Requests with a complete, validated response. |
| `errors`, `timeouts`, `missed`, `retries` | Failed, timed out, unscheduled, and retried work. |
| `acknowledged_writes` | Writes acknowledged by the database. |
| `verified_reads` | Reads that found and validated an acknowledged row. |
| `p50_ns`, `p95_ns`, `p99_ns` | Request latency quantiles in nanoseconds. |
| `max_pause_ns` | Largest observed completion pause in the phase window. |
| `completion_gap_ns`, `queue_lag_p99_ns` | Worst completion gap and p99 scheduler lag. |
| `offered_rate_milli`, `throughput_milli` | Offered and successful requests per second multiplied by 1,000. |
| `acknowledgement_digest`, `verification_digest` | Sorted exact-set digests; they must match. |

The phase is valid only when scheduled, started, completed, and request counts
agree; every request succeeds; errors, timeouts, and missed arrivals are zero;
and acknowledged rows are conserved. The continuity gate bounds during/after
completion gaps by the larger of 1.25 times the matched baseline gap and two
schedule intervals, plus 1 ms. Budget rows record `throttled_calls`,
`throttled_bytes`, `peak_active`, and `max_active`. Both throttled counters
must be positive: a migration that fits in its initial burst does not exercise
node-wide pacing.

## Bounds

Latency and throughput are compared with the measured baseline. Defaults are
configurable for the runner with these environment variables:

| Variable | Default |
| --- | ---: |
| `VIBEDB_SCALE_DURING_P50_PPM`, `...P95_PPM`, `...P99_PPM` | 1,050,000; 1,100,000; 1,150,000 |
| `VIBEDB_SCALE_AFTER_P50_PPM`, `...P95_PPM`, `...P99_PPM` | 1,050,000; 1,100,000; 1,150,000 |
| `VIBEDB_SCALE_LATENCY_FLOOR_NS` | 100,000 |
| `VIBEDB_SCALE_DURING_THROUGHPUT_PPM` | 990,000 |
| `VIBEDB_SCALE_AFTER_THROUGHPUT_PPM` | 990,000 |
| `VIBEDB_SCALE_MAX_PAUSE_NS` | 100,000,000 |

A latency bound is `phase <= baseline * multiplier / 1,000,000 + floor`.
Throughput must remain at least `baseline * multiplier / 1,000,000`.
Any bound override marks the run diagnostic-only and cannot satisfy strict CI
qualification. The conservation, no-skip, continuity, and pacing checks remain
unconditional.

The raw TSV is retained with the CI run. It is evidence for the exact source
revision, process layout, workload window, and bound environment used to
produce it; it does not substitute for a benchmark comparison against another
database.

The required Linux gate is defined in
`.github/workflows/seamless-scale-in-out.yml`. It runs
`TestSeamlessScaleInOutProcessQualification` with
`VIBEDB_SEAMLESS_SCALE_E2E=1`, rejects skipped or incomplete JSON test output,
and checks every strict marker and terminal evidence row before uploading the
TSV.
