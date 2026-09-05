# Add wide-key update and mixed-read workloads

The existing `update_existing` workload repeatedly updates one key per client.
`mixed_read_update` uses the same update keys and reads other keys. Preserve
those workload definitions and their historical results. They measure a small
hot set and do not establish performance for writes spread across a table.

Add explicitly named `update_uniform` and `mixed_uniform` workloads. Client
`c` owns keys whose integer ID modulo the client count equals `c`. Select a key
uniformly within that stripe using a deterministic hash stream independent of
table and read/write selection. This covers the full table across clients,
including row counts not divisible by concurrency. It avoids concurrent
writes to the same key and makes no contention claim.

Uniform updates use the same increment statement as existing updates. Uniform
mixed traffic reads and updates keys in the same owned stripe. A client's
serial operations can verify every returned field, including the exact score
after its prior updates, without locks in the benchmark oracle. Warmup updates
must advance that oracle too. Keep complete table verification after every
trial, strict errors, and the same PostgreSQL protocol for both engines.

Use one reviewed, clean, shared client revision for both VibeDB revisions and
CockroachDB. Preserve operation ordinals, client IDs, repetition and workload
configuration so key selection can be reconstructed. The validator must accept
the new workload names while rejecting incorrect operation classification,
sample counts or assignment. Existing reports remain valid under their original
contracts; do not pool distinct matrices or change historical labels.

Before performance claims, prove disjoint ownership, deterministic broad key
coverage, correct partial stripes, and mixed reads of previously updated keys.
Run storage pressure/checkpoint and overlay-read correctness separately. A
longer run and actual fold counters are both required to expose deferred
compression costs. The existing five-workload promotion guardrails remain in
force; new wide-key cells add coverage rather than replace unfavorable cells.

The client and validator are implemented and independently reviewed. Protocol
tests exercise the actual trial loop with both engines' result formats,
nondivisible row/operation counts, warmup updates, reads after prior updates and
full-table tail-row corruption. Review also closed two validator gaps: uniform
workloads require schema 2 with operation labels, and current-schema trials
must execute enough operations to exercise every configured client. The Go
1.27 SIMD client race suite and 40 Python tests pass; the small test logs and
pre-fix failures are retained under `docs/qualification/wide-update-client-2026-09-04`.

No wide-key timing result is available yet. These noncontended autocommit
workloads do not establish serializable interactive transactions or failure
recovery.
