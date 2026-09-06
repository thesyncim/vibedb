# Third Astra-max pass: remove discarded writer work; exception design caveats

Inspected HEAD: `32ed66fdbde1d1769f2089a652f527bd32fa87c9`, repository `/Users/thesyncim/GolandProjects/vibedb-space-savings-rf3`, after the current-main merge. This pass made no repository edit, ran no Go tests/benchmarks/compression or performance load, and spawned no agents. The conclusions below are source proofs and design analysis, not latency measurements. Work stopped when the user requested wrap-up and merge.

## 1. Immediate patch cleanup: do not construct a rank map that cannot be used

At `internal/storeio/compact_primary_stripe.go:864-950`, patch groups are now sorted by shape/hole, fixing the previously identified A/B/A repeated-shape scan. Nevertheless the code still scans all N physical rows and materializes n_s uint16 ranks for each changed shape, even when neither the source decoder nor the new encoder needs them.

The current emitter requires n_s>=64 and N>n_s: `internal/storeio/compact_stream_rank_affine.go:280-293`. Therefore an ordinary source stream needs no physical map when:

- `entry.rows == v.rows`: rank and ordinal are identical; the rank emitter cannot be selected.
- `entry.rows < 64`: the rank emitter cannot be selected under the current threshold.

Decode those ordinary streams using their local ordinals and call the ordinary `encode(values)`/nil-rank planner. Existing RankAffine source streams must retain physical coordinates even if a small stream was admitted through another legitimate source. This removes the full N-row membership scan, n_s uint16 writes, and rank-array loads without changing selection policy or encoded output. The parent accepted this cleanup and owns its implementation/verification. No latency claim is made here.

## 2. Proven writer optimization: delay three numeric encoders until after affine planning

`internal/storeio/compact_stream_codec.go:147-227` currently measures dictionary/front/alphabet, parses canonical integers, **materializes FOR, Delta and DeltaPack**, then finally calls `encodePrefixIntShape`. The last call may return a 34-byte bare-number RankAffine descriptor. In that case all three already-written numeric candidates are discarded.

Relevant implementations: `measureDictionary:230`, `measureCompactFront:379`, `measureAlphabet:420`, `encodeFOR:575`, `encodeDelta:596`, `encodeDeltaPack:618`, `encodePrefixIntShape:845` (line numbers from this inspected HEAD). FOR scans the integer array for extrema and again to pack; Delta scans it to emit; DeltaPack scans deltas twice for widths and once to pack, as well as clearing its output buffer. Reordering can remove approximately six complete numeric-array loop visits per accepted `/id` column, plus packed writes and buffer clearing. This is an operation-count observation, not elapsed-time evidence.

### Exact dominance proof

Apply this shortcut only when `allIntegers` is true and the resulting candidate is the existing **bare numeric RankAffine** kind, whose complete wire size is 34 bytes. Its nonzero slope, strictly increasing physical ranks and checked nonnegative domain prove that all n_s>=64 scalar values are distinct, monotone signed integers. The existing local-affine check has already failed before this kind can be emitted.

Let B=ceil(n_s/64).

| Discarded candidate | Lower bound on complete encoded bytes | Why it exceeds 34 |
|---|---|---|
| FOR | `20 + ceil(n_s * bitlen(n_s-1) / 8)` | Distinct integers span at least n_s−1; at n_s=64 this is 68 bytes. |
| Delta | `12 + 12*B + (n_s-B)` | Four-byte restart directories, eight-byte bases, at least one byte per delta; at n_s=64 this is 87 bytes. |
| DeltaPack, n_s>=65 | At least `12 + 13*B + ceil((n_s-B)/8)` | Even one-bit deltas need at least 46 bytes at n_s=65; further blocks increase this. |
| DeltaPack, n_s=64 | At least `12+4+9+ceil(63*2/8)=41` | One-bit zigzag deltas would all be −1: zero is excluded by distinctness. That is locally affine and would have been handled before the rank candidate. A rank candidate therefore needs at least two bits in its sole block. |

The inequalities are strict, so the shortcut cannot change a tie against these three encoders. This proof does **not** justify skipping arbitrary dictionary/front/alphabet work or every PrefixInt candidate. Keep their existing measurement and dictionary scan-preference policy unchanged. Constants, small streams, ordinary local-affine PrefixInt, negative-number fields and rejected rank candidates should follow the original selection work unless separately proven.

### Implementation and buffer/selection caveats

Keep the integer parsing pass, but postpone FOR/Delta/DeltaPack construction until prefix planning reveals whether they can win. Prefix parsing still happens once, as it does now. Failed/late-rejected rank candidates perform the same original parsing/encoding work, merely reordered; this change does not add a speculative full-value pass.

A reserved prefix backing slot such as 7 avoids aliasing when deferred numeric candidates are subsequently built in slots 3–5. **Do not merely put the candidate in slot 7 and continue iterating the old contiguous candidate count**: that can ignore it or compare stale/uninitialized slots. Hold it separately and compare it after the existing valid numeric/date candidates, or maintain an explicit valid-slot list. Preserve the current tie order (FOR, Delta, DeltaPack, Date, then Prefix/Rank), and the dictionary tie/preference behavior. The result must be byte-identical before and after the scheduling change on every fixture.

The parent may use this optimization if necessary to clear performance qualification; it is not implemented by this researcher.

### Performance assumptions still needing falsification

A near-affine stream with the final scalar changed still pays the existing full prefix parse, then up to n_s rank checks before falling back to the ordinary prefix/delta path. This is an additional full numeric validation pass relative to the old format; successful-candidate benchmarks do not cover it. Random positive numeric fields normally fail the fit early, but that is workload-dependent. Single-shape data, fewer-than-64 shapes, unquoted negative numbers and unrelated strings take early gates and cannot earn the proposed rank-specific savings. Include those cases in the paired qualification. Existing dictionary/alphabet work can remain dominant even after the three discarded encoders are removed.

## 3. One numeric exception: promising format idea, not an approved implementation

An existing bare RankAffine descriptor has a 12-byte stream header, four bytes of dictionary ends and an 18-byte body: 34 bytes. A separate one-exception kind could retain that base/step body and add a physical-rank u16 plus a signed replacement int64: **44 bytes total**. It can replace a column that otherwise falls back to hundreds or thousands of bytes after one numeric update.

For the same immutable leaf row order, the untouched values are already certified by the old descriptor. A direct patch could update those ten bytes, replace an existing exception, or remove the exception when the value is restored. A second different exception initially falls back to the full existing planner. This has the potential to eliminate O(n_s) column decoding/parsing/packing on the first mutation. It does not eliminate full-page COW assembly/checksum/I/O, and exception survival under actual update churn is unmeasured.

### Deterministic full rebuild is the central constraint

The current patch contract promises the same bytes as a complete rebuild (apart from generation). A patch-only exception representation would violate that contract unless the full writer independently chooses the same descriptor from the final canonical values. A field name, prior page, mutation history or the first two values cannot define the full-writer model.

For n_s>=5 and at most one exception, the intended affine line is unique: two different lines can agree at at most one physical rank, while two lines each fitting n_s−1 rows would have at least n_s−2 common inliers.

A bounded deterministic search can handle an exception in the first, second or last position:

1. Try the three pairs among the first three physical-rank/value pairs: (0,1), (0,2), (1,2). At least one pair contains two inliers.
2. Use checked subtraction, divisibility, base derivation and the complete leaf-domain endpoint proof. A negative candidate value cannot be an inlier of the current nonnegative base family, so pairs containing it can be rejected before unsafe subtraction. The one signed exception itself may be negative.
3. Require a candidate to agree with at least four of the first five points. With at most one exception this identifies the unique line; a wrong line can hit at most one true inlier plus the exceptional point.
4. Validate every remaining value. Zero exceptions uses the existing exact-kind policy; one records its actual rank and exact signed value; two or more decline. Do not sample the suffix.

This algorithm must share the existing parse pass or it can regress unrelated and nearly affine writes. The three-pair prefix search avoids three full scans but does not by itself establish a cost win on rejected data.

### A concrete canonical-selection trap

Even a 44-byte exception descriptor is **not always the full writer's preferred encoding**, regardless of a large row count. Suppose one shape has physical ranks `0,1,...,126,128` and values `1000+rank`. Its old global rank model is exact; local ordinal values are not affine. Change only the final value from 1128 to 1127. The new values are now locally affine `1000..1127`, and ordinary local PrefixInt costs 34 bytes. A patch that blindly preserves the old model plus one exception emits 44 bytes and violates the full-rebuild identity.

Consequently the fast patch needs a proof that no earlier/smaller canonical candidate can win, or must fall back when that cannot be established cheaply. One option is to prove from several unchanged shape ranks that local-affine repair is impossible, falling back for ambiguous cases; any affirmative claim still requires a complete invariant. Another is a stored/admitted shape property, which has its own format and validation cost. Do not weaken the current byte-identical contract silently. Candidate-size bounds alone do not solve this local-affine case.

### Exact query and read semantics

Use a separate exception kind so ordinary RankAffine arithmetic does not gain an exception lookup inside its value helper. This preserves its logical path; enum dispatch/code generation can still change, so zero latency impact requires measurement.

For the exceptional kind, a point read compares physical rank with the exception rank and otherwise evaluates the existing formula. The owning shape must contain the exception rank and the descriptor count must remain the full leaf count. Native projection and integer groups consume the signed replacement directly.

For any equality/order/interval predicate P, the owning-shape result is:

`base_shape_count(P) - P(base_value_at_exception) + P(replacement)`.

This handles duplicates correctly: if the replacement equals another inlier's value, equality can return two; uniqueness of the original line is not uniqueness of the mutated values. Negative exceptions are also valid signed numbers although the base sequence is nonnegative. Spelling and numeric equality must preserve their existing separate semantics.

For extrema, find the first/last owning-shape ranks **excluding the replaced rank**, then compare their base values with the replacement. If the exception replaces a shape endpoint, use the next/previous shape ordinal. An interior exception can still become a new minimum or maximum. Do not retain the original endpoint value after replacing it.

This candidate needs reviewed byte-identity rules, first/second/last exception tests, duplicate-value predicates, signed extrema, malformed shape membership, break/restore and second-exception fallback, and paired read/update measurements before production use. The parent explicitly retained it as research while finishing the current primary and sealed-route qualification.

## 4. Other structural directions

The current free-space format already records generation fences and independently addressed segments; replacing it with an unversioned bitmap would lose essential ownership information rather than remove redundancy (`internal/storeio/free_extent.go:8-27`, `store/durable/store_file_free.go:10-41,86-118`). No additional allocator-format saving was established in this bounded pass. The earlier overflow allocation cliff remains real, but neither overflow sharing nor large redo-removal proposals became qualified by this work.

The strongest immediate action is the accepted no-map patch path; the strongest precisely proven writer follow-up is deferred numeric materialization. One-exception storage remains a potentially useful next format experiment whose canonical full-rebuild selection problem is substantive, not a minor implementation detail.
