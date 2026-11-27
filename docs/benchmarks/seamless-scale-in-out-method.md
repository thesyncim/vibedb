# Online scale in and out qualification

[Benchmark methods](README.md) · [Qualification workflows](../qualification/README.md)

Qualifies `TestSeamlessScaleInOutProcessQualification` on Linux
(`.github/workflows/seamless-scale-in-out.yml`, `VIBEDB_SEAMLESS_SCALE_E2E=1`)
through three 3 → 4 → 3 physical-node scaling waves under open-loop SQL/native
traffic while restarting both the controller and target mid-migration and
retrying identical operation IDs.

## Workload and conservation

- 16 open-loop workers (each holding SQL + mTLS native sessions); calibrates at
  1,200 req/s (fallback 1,000 req/s on missed arrivals; fixed 400 req/s under
  `shared-runner`).
- Zero errors, timeouts, or missed arrivals permitted; exact
  `(table, key, version, value, request)` acknowledgement digest must match
  post-stop verification.
- Retiring-frontend connections must hold `safe_to_stop=false` until
  gracefully drained; both migration `throttled_calls` and `throttled_bytes`
  must be positive.

## Evidence format (schema 2 TSV)

Requires ≥5 baseline, ≥3 during, and ≥3 after 10-second windows (≥10,000
samples/phase), plus markers `empty_target_at_enrollment`,
`controller_restarted`, `target_restarted`, `duplicate_operation_stable`,
`post_restart_operation_recovered`, `safe_to_stop`, `node_stopped`,
`acknowledged_data_intact`, and `no_skipped_success` all equal to `1`.

During-migration windows intersecting 10 s after an injected restart go to
`during_recovery` (10 s pause/p99 ceiling); all other windows go to
`during_steady`.

## Bounds (`dedicated` defaults)

| Variable | Default |
| --- | ---: |
| `VIBEDB_SCALE_DURING_P50_PPM`, `...P95_PPM`, `...P99_PPM` | 1,050,000 / 1,100,000 / 1,150,000 |
| `VIBEDB_SCALE_AFTER_P50_PPM`, `...P95_PPM`, `...P99_PPM` | 1,050,000 / 1,100,000 / 1,150,000 |
| `VIBEDB_SCALE_LATENCY_FLOOR_NS` / `...MAX_PAUSE_NS` | 100,000 / 100,000,000 |
| `VIBEDB_SCALE_DURING_THROUGHPUT_PPM`, `...AFTER_THROUGHPUT_PPM` | 990,000 / 990,000 |

Setting any `VIBEDB_SCALE_*` override marks the run non-strict diagnostic-only.
See [seamless-scale profiles](../qualification/README.md#seamless-scale-profiles)
for `shared-runner` CI thresholds.
