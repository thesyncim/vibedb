# Rank-affine codec proof and independent integration review

Research source: [research_test.go.txt](research_test.go.txt), retained outside the production test suite. Copy it to internal/storeio/compact_rank_affine_research_test.go to rerun the opt-in census.
Base HEAD: `837c68bfe14d6197159561e675c1386b982a1fa1`; the parent concurrently added the production rank-affine implementation. This agent edited only that new research test and temporary reports, ran only focused correctness/space tests, and made no commit or production-source edit.

## Result

The scalar redundancy estimate was correct, but the initial physical-space estimate was too optimistic before page rounding. Both deterministic 100,000-row corpora reconstruct exactly. The measured current production emitter, with its own replanning, saves **10.040161% of the low-cardinality primary image** and **2.364066% of the high-cardinality primary image**. These images include all emitted graph extents and the existing 16 KiB mutable prefix. They exclude journals, Raft, redo, retired generations, allocator history, and filesystem metadata.

No latency benchmark was run by this agent. Package test elapsed times below are execution records, not performance comparisons or evidence of no regression.

### Original leaf boundaries: complete modeled replacement payloads

| Corpus | Leaves | Old payload | New payload | Payload saved | Old leaf extents | New leaf extents | Extent saved |
|---|---:|---:|---:|---:|---:|---:|---:|
| Low | 25 | 895,569 B | 729,339 B | 166,230 B / 18.561384% | 905,216 B | 802,816 B | 102,400 B / 11.312217% |
| High | 104 | 6,801,696 B | 6,641,940 B | 159,756 B / 2.348767% | 6,815,744 B | 6,811,648 B | 4,096 B / 0.060096% |

All original cuts are printed in the raw output. Low full leaves remain capped at 4,096 rows and shrink from 36,864 to 32,768 bytes. High full leaves retain their 65,536-byte extent after the replacement; only the last fixed-cut leaf crosses a 4 KiB boundary. This is why payload reduction alone is an inadequate disk-space claim.

### Real production emitter and production leaf replanning

The test separately invokes `PlanPrimaryGraph` and `BuildPlannedPrimaryGraphToSink`. It emits complete before/after graphs into the existing in-memory page sink, opens every page with its checksum and geometry, opens every primary leaf with its actual reference/identity, and compares every key and canonical document. The before graph receives retained payloads from explicit frozen legacy encoder/planner functions copied from `837c68bf`, so enabling the production writer cannot contaminate baseline leaf cuts or bytes.

| Quantity | Low before | Low production | High before | High production |
|---|---:|---:|---:|---:|
| Leaves | 25 | 25 | 104 | 102 |
| Payload bytes | 895,569 | 729,339 | 6,801,696 | 6,638,962 |
| Leaf extent bytes | 905,216 | 802,816 | 6,815,744 | 6,651,904 |
| All graph pages | 30 | 30 | 109 | 107 |
| Graph extent bytes | 1,003,520 | 901,120 | 6,914,048 | 6,750,208 |
| Primary with 16 KiB prefix | 1,019,904 | 917,504 | 6,930,432 | 6,766,592 |
| Primary savings | — | **102,400 B / 10.040161%** | — | **163,840 B / 2.364066%** |
| Rank-affine streams | 0 | 150 | 0 | 612 |

Graph metadata remains 98,304 bytes in both before/after graphs: 73,728 bytes of catalog pages and three 8,192-byte routing/locator/anchor pages. Adding the mutable prefix gives 114,688 bytes outside leaves. Production cuts are printed separately from fixed cuts. RF3 would replicate these primary savings three times, but the percentage of the whole database depends on the other layers and is not measured here.

### Exact per-path attribution at original cuts

| Corpus/path | Existing kind | Existing bytes | Affine bytes | Saved bytes |
|---|---|---:|---:|---:|
| Low `/id` | DeltaPack | 85,440 | 2,550 | 82,890 |
| Low `/name` | PrefixInt | 86,415 | 3,075 | 83,340 |
| High `/id` | DeltaPack | 89,550 | 10,608 | 78,942 |
| High `/name` | PrefixInt | 93,606 | 12,792 | 80,814 |

Each bare integer descriptor costs 34 bytes; each quoted `user-` descriptor costs 41 bytes. The reduction is 1.662300 B/row low and 1.597560 B/row high. Every other path and the keys remain byte-identical in the modeled payload. The complete per-path old/new kinds, counts, eligibility and sizes reconcile exactly to the payload delta. High-cardinality `/note` alone still consumes 3,956,682 bytes: affine factoring does not remove actual distinct string data.

## What the proof covers

The opt-in gate is `VIBEDB_RANK_AFFINE_RESEARCH=1`. The independent model uses private `VRA1` / `0xfe` tags, rejected by production readers, and actual serialized descriptors with the existing PrefixInt renderer. Stream count is the FULL leaf count; decoding passes physical rank. Shapes, local ordinals, posting slots and overflow references retain their distinct meanings. All unchanged templates, rank checkpoints, slot maps, summaries, keys, framing and section directories are charged.

Admission proves every value against a checked base/step, not just endpoints or a sampled prefix; it also proves affixes and exact decimal spelling/padding. Endpoint range checks cover all physical ranks including other shapes and overflow rows. Rejected candidates keep their old stream. Constant and singleton candidates are declined; small-dictionary preference is retained. The model does more parsing/re-rendering than a production planner should, intentionally for proof.

The tests cover descending and gapped ranks, shuffled and almost-affine data, ordinal-versus-rank mismatch, nonintegral slopes, duplicate/out-of-range ranks, affix/padding mismatches, decimal-width crossings, exact 20-digit leading-zero spellings, the reproduced 20-digit overflow sequence, uint64 and int64 boundaries, extrapolated-base/endpoint failure, and fixed-width full-domain overflow. Independent malformed descriptor tests cover flags, counts, widths, directories, truncation, extra bytes, invalid affixes, MinInt64 step, zero step and overflow; the actual-kind reader is checked too. A real page with a recomputed valid checksum and a shape-local count substituted for the full leaf count fails admission.

Mixed fixtures preserve every key, value, nonidentity posting slot and overflow reference. Replacement can retain unaffected affine columns; insertion/deletion in the middle and shuffled physical order correctly lose affine eligibility in the chosen fixture. This demonstrates a coverage limitation, not a corruption workaround. The optional/sparse event fixture also shows exact sequences/timestamps under different field names and shape-dependent bases, but remains a synthetic correctness fixture rather than a measured external workload.

Actual-kind integration tests independently verify point reads, repeat/sparse scans, exact value lengths, spelling and number equality, all integer order predicates, intervals, extrema, native integer projections, grouped integers/SUM inputs, resolved holes, certified numeric multi-shape replacement, exact reopened patch readback, and preservation of the old immutable page. Shape membership uses different affine bases across shapes and a sparse shape interspersed around 64-row boundaries. The initial string-certificate patch fixture was corrected to numeric because the established certificate intentionally declines string changes; this was not a production defect.

## Bounded review findings and remaining qualification

No remaining wrong-coordinate call or full-domain arithmetic safety defect was found in the inspected final parent implementation. The parent fixed the multiline certified-patch coordinate call while integration was in progress. The review identified `conservativeIntegerStreamBytes(stream.count)` as an overly large bound for rank streams; the parent changed it to the owning `entry.rows` and verified count association.

The current planner reuses already parsed prefix integers, attempts rank-affine after rejecting local affine form, requires at least 64 shape rows, and avoids constructing the discarded packed PrefixInt on success. Its base derivation and endpoint arithmetic are checked before multiplication. The parent is preserving unquoted negative-number native semantics by declining that emitter case; exact byte rendering alone is insufficient to claim equivalent numeric fast paths.

Writer/patch costs still need paired measurement. Existing dictionary, front, alphabet, FOR and delta candidates are generally measured or built before prefix admission. Failed almost-affine candidates add rank checks; this should be included explicitly in insertion/update benchmarks. Patch extraction currently scans all leaf ranks to collect a changed shape. Its cache retains only the immediately preceding shape, so an A/B/A sequence of changed-column groups can scan A twice. Grouping patch work by shape, or enumerating existing packed shape matches, is a tractable follow-up if this appears in measurements. Do not add speculative complexity before measuring the actual cost.

The strict user requirement of no slower reads, updates or inserts remains unproven. Needed qualification includes eligible and rejected data, cold and hot point reads, indexed/sparse/native scans and groups, sustained insert/update/checkpoint work, arbitrary deletions/reordering, large values, old snapshots and whole RF3 accounting. This experiment establishes bytes and correctness for its cases, not deployment readiness, migration safety or broad workload coverage.

## Reproduction artifacts

- `/private/tmp/vibedb-rank-affine-research-correctness.sh` and `.txt`: initial independent correctness proof, PASS, package elapsed 0.341 s.
- `/private/tmp/vibedb-rank-affine-research-census.sh` and `.txt`: initial original-cut census, PASS, package elapsed 2.423 s.
- `/private/tmp/vibedb-rank-affine-research-actual.sh`: final opt-in focused command covering the independent proof, actual-kind integration and both censuses.
- `/private/tmp/vibedb-rank-affine-research-actual-repacked.txt`: final focused suite, **PASS**, package elapsed 4.363 s; both 100k corpora have exact readback before and after production replanning.

Commands use `GOEXPERIMENT=simd`, `CODEX_AGENT_ID=astra-max-primary-format`, the repository's `scripts/project-env` wrapper and the explicitly authorized shared `GOCACHE=/Users/thesyncim/Library/Caches/go-build`. No latency benchmark, broad suite or disk workload was run by this agent.
