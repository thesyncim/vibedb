# VibeDB replicated format research — Astra, maximum reasoning

Research date: 2026-09-05. Source tree: `/Users/thesyncim/GolandProjects/vibedb-space-savings-rf3`. Main baseline: `f05df25e8bebc13d9bfe11a2038bab43805f6c3d`; task HEAD: `809f96dc4581ded1852de755dc963de3effd76ef`. The earlier body-retirement experiment is absent from this source tree and is not assumed here. This is source-based design research, not a performance result. No build or performance load was run.

## Recommendation and ranking

The strongest architecture change is **a replicated-only recovery format in which the existing Raft log is the sole redo source, and a group checkpoint contains complete, coherent primary-root images for every collection**. Current primary-graph storage already uses COW pages. It does not need to be replaced by an LSM or by document reads against Raft. The change is where recovery authority lives, when a checkpoint becomes authoritative, and which generation fences physical reuse. This can remove the entire local afterimage-journal layer and transaction-marker layer from replicated apply.

The strongest small node-log-format change is **implicit wave geometry**: encode one extent length per authenticated extent, one first index per group batch, and term runs, instead of repeating extent identity/offset/length and absolute indexes per entry. The sealed point-lookup index can stay identical. This is smaller in impact but easier to isolate and qualify.

| Rank | Mechanism | Physical-space effect | Foreground effect established by construction | Main qualification risk |
|---|---|---|---|---|
| 1 | Complete group-root checkpoint + Raft redo | Removes member recovery-journal files and `txn.vtm`; replaces the small group certificate with roughly 8 KiB per member across two banks | Removes K journal appends and one decision append per physical apply; primary lookup/decoding unchanged | Exact-root cold opening, persistent allocator fences, membership/snapshot activation, and partial checkpoint recovery |
| 2 | Implicit node-wave extent/index geometry | Roughly 11 B/entry in a concrete 64-entry example; no saving to the fixed active/spare reserve | Fewer encoded/decoded integers and fewer bytes copied, MACed, and written; sealed point lookup unchanged | Versioned frame decoding and retry identity across old/new physical encodings |
| 3 | Raft afterimage references in existing collection redo | Removes most *written redo bytes* for eligible puts; does not itself remove today's sealed capacities | Existing read/primary/update publication path; smaller redo encoding/checksum/write | Provenance propagation and lifetime floor; generated afterimages still need inline fallback |
| 4 | Self-contained command-header factoring before encryption | Up to about 200 B/command for homogeneous multi-entry extents; little for single-entry extents | Fewer bytes encrypted/written | Adds decode/expansion work to Raft reads; no blanket latency claim |

For a meaningful architecture implementation, begin rank 1 as a new, explicitly selected replicated recovery mode with fixed catalog membership, retaining the existing primary graph and batching. Rank 2 is a separable, lower-risk format patch. Rank 3 is a useful stepping stone if rank 1 is deferred, but should not be sold as physical reserve removal. None of these designs has yet qualified the user's “reads/inserts/updates must not become slower” requirement in latency measurements.

## 1. What is physically repeated today

Let `D` be canonical document bytes, `R` the original mutation-value bytes carried by the replicated command, `k` its key bytes, `I` the combined tenant/distribution/shard identity-string bytes, `N` entries per node wave, `E` encrypted extents, `K` dirty local collections in a physical apply transaction, and `M` all fixed members in the checkpoint group. `round512(x)` rounds to the journal damage granule.

### Command, node wave, sealed route, and primary value

An ordinary single-relation mutation command is:

```
C = 256-byte command header + 8-byte envelope checksum + I
    + sum(8-byte mutation header + key bytes + value bytes)
    + 40 bytes for each digest-conditional mutation.
```

Multiple relations add 8 bytes per relation batch; session Open/Renew/Revoke add their 16-byte lease body. Normal mutation commands do not have that lease body. Source: `internal/replication/types.go:57-79`, `command.go:565-667`, `command.go:775-789`, `command.go:1825`.

The fixed command header contains substantial repeated authority: bytes 32..167 alone carry 136 bytes of cluster/incarnation/group/allocation/policy/ownership/schema/routing coordinates (`command.go:578-590`). That is separate from the group identity in the physical node wave. These are deliberately part of the portable deterministic command, so removing them from the logical wire requires more than deleting fields from the local WAL.

`NodeStore.persistWave` copies all entry data into `plainArena`, packs entry-aligned extents, encrypts them into `cipherArena`, and passes those bytes to the segment engine (`internal/raftstore/node_store.go:1261-1313`, `1358-1423`). Extents target 32 KiB; a larger entry gets one dedicated extent. Each pays one 16-byte AEAD tag. Extent AAD binds log ID, wave ID, and extent ordinal. `seglog.prepareWave` then copies the ciphertext into its frame and computes the wave MAC (`internal/raftstore/seglog/engine.go:1207-1221`).

The exact normal data-wave charge is:

```
72 + U(number_of_group_batches)
   + sum(group delta + flags + optional Ready/control fields + U(entry_count))
   + sum(U(index) + U(term) + optional U(type)
         + U(extent_id) + U(extent_offset) + U(extent_bytes)
         + U(data_offset) + U(data_bytes))
   + U(cipher_blob_bytes) + sum(C_i) + 16*E.
```

Here `U(x)` is canonical uvarint length. The 72 bytes are the 40-byte frame prefix and 32-byte wave digest. This is directly implemented by `seglog/engine.go:1038-1228` and `waveSize:1235-1311`. It is not an estimate of protobuf network framing.

Sealing adds a 64-byte index header, group-run directory/summaries, 40-byte route descriptors for blocks of up to 256 entries, and route payloads. The route payload already uses implicit contiguous indexes, term deltas, and once-per-extent identity; it is more compact than the active-wave entry metadata (`seglog/sealed_index.go:11-25`, `536-580`). Segment framing is 256 bytes before data and a 160-byte footer (`seglog/format.go:32-35`). Thus it would be misleading to price a 40-byte descriptor *per entry*.

The current document read path is independent of Raft and local redo. Inline values participate in compact primary stripes. Values above the default 512-byte inline limit use a raw forward overflow chain (`store/durable/store_file_options.go:1021`; `store_file_overflow.go:10-14`). Each overflow extent has 132 bytes of framing: 64-byte common header + 60-byte overflow header + 8-byte trailer. Each piece is rounded to 4 KiB and capped at `MaxPageSize` (`store_file_overflow.go:24-69`; `internal/storeio/overflow_page.go:10`; `internal/storeio/page.go:12-16`). The leaf stores a 32-byte head reference. Large values are not VCS1-compressed. The primary count is approximately:

```
P(D) = 32-byte head reference
       + sum(round4096(132 + piece_bytes))
       + primary key/router/index share.
```

Current checkpointing resolves a volatile overflow chain and remints its durable chain (`store_file_primary_mutation.go:2262-2286`, `2363-2389`). That is real extra memory/encoding work, but the volatile chain itself is memory-only; counting both as permanently allocated on-disk copies would be wrong.

### Local afterimage redo and decisions

Apply canonicalizes JSON before validation, hashing, and durable staging. Canonical source slices already borrow the command bytes when possible (`internal/replicatedstate/mutation_canonical.go:14-24`, `108-151`). It nevertheless loses source provenance when net overlays call plain `WriteBatch.Put` (`apply_batch_overlay.go:439-455`). The durable planner then adds the full final value to `batchJournalEntries` (`store_file_primary_batch.go:942-979`).

One dirty member's conditional record is:

```
J_j = round512(32 record prefix + 32 conditional header
                + sum(12 entry header + key bytes + canonical value bytes)
                + 8 trailer).
```

Deletes have no value. A 65,536-byte afterimage with a 16-byte key costs `round512(65,636) = 66,048` bytes. A small reference could make that user record 512 bytes; omitting the entire layer removes all 66,048. Source: `internal/storeio/recovery_journal.go:62-84`, `535-598`; `recovery_journal_conditional.go:8-20`, `369-417`.

`txn.vtm` separately records `(StoreID, JournalID, PreparedGeneration)` at 40 bytes per dirty member, within another 32+8-byte, sector-padded frame. Its decision charge is `T(K)=round512(40+40*K)`. Two dirty collections cost one 512-byte decision. It contains no document body (`internal/storeio/txn_marker.go:66-76`, `434-456`).

The exact per-apply redo output is therefore `sum(J_j) + T(K)`. It is **K journal appends and one marker append, not K+1 fsyncs**: `CheckpointGroup.commitTransitionLocked` explicitly passes `forceSync=false` (`checkpoint_group.go:1924-1940`). Every member has staged successfully before prepares are appended, then all collection snapshot gates are acquired and all roots plus visible transaction/applied cut publish before any gate is released (`1896-1918`, `1964-1978`).

The default group checkpoint happens after 128 physical transactions; a physical transaction can represent up to 128 consecutive Raft entries. At checkpoint the group syncs *every fixed member journal*, writes/syncs a certificate, then physically folds/recycles each collection (`checkpoint_group.go:57-61`, `1728-1738`, `2391-2473`). The certificate is durable before the member folds because certified conditional records can redo any missing fold. This is the protocol a replacement must actually replace.

### Replication and retained auxiliary payloads

For one direct logical version whose command and journal are still retained, there are three roles per replica: command value, canonical local redo value, and primary value. RF3 repeats these three roles across three independent nodes. This is not a universal `9*D` steady-state formula: primary compression differs from wire JSON, only the live/reader-retained primary versions remain, and WAL/journal windows differ. A correct capacity equation is:

```
RF3 physical space = sum over three nodes(
    live + reader/recovery-retained primary extents
    + allocated node segments/spares
    + allocated collection journals/marker
    + catalogs/certificates/auxiliary collections).
```

Distributed transaction staging may retain mutation/coordinator payloads as hidden collection values, then journal those values too. See `internal/replicatedstate/transaction_apply.go:448-480` and target-stage code. The single-target apply kernel explicitly does not install retained mutation payloads (`transaction_apply.go:1001-1004`). Range-split capture adds a generated capture record to a separate collection and therefore another journal afterimage when active (`apply.go:2692-2707`, `2827-2835`). Session slots/control envelopes are further rows, usually metadata rather than another whole user document (`state_codec.go:18`; `session_codec.go:20-28`). A proposal that only handles user `Put` does not remove these other redo bytes.

### Latest-main allocation correction

The sealed logical geometries remain:

* User journal record region: `16,777,216 + 34*512 = 16,794,624` bytes, plus 1,024 header bytes.
* Basic hidden-system journal: 16,777,216 bytes, plus 1,024 headers. Optional transaction/ledger profiles are larger and validated against actual frozen limits.
* Marker: 1,048,576-byte record region, plus 1,024 headers.

Sources: `sql/driver/replicated_sidecars.go:12-29`, `72-95`, `127-175`.

**Latest main now selects portable sealed allocation for replicated sidecars.** The exact logical size remains but is no longer a universal guarantee of private physical backing. Linux still calls `fallocate`; unsupported Linux filesystems fall back to growing the file. Darwin requests native preallocation when supported. Reopen no longer re-proves portable physical backing (`replicated_apply.go:437`, `replicated_store.go:505`, `catalog.go:1101-1103`; `internal/storeio/sealed_allocation_linux.go:12-20`; `sealed_allocation_darwin.go:12-27`; `recovery_journal.go:1583-1594`). Measure `st_blocks`, file length, and bytes written separately. Historical “33 MiB/node” is a useful fully-backed basic-profile calculation, not a universal actual-allocation floor.

The shared node log's 32 MiB active plus two spare segments are per physical node, not per group. Shrinking logical wave bytes does not remove that reserve; it reduces subsequent sealed-segment growth and retention.

## 2. Rank 1: complete group-root checkpoints, with Raft as redo

### Why this is supported by the present architecture

`CreateFromPrimary` graphs explicitly avoid in-place materialization (`store_file_verify.go:24-26`). Primary checkpoint code allocates new leaf/overflow pages and records retirements (`store_file_primary_mutation.go:2192-2211`, `2286`, `2310-2331`, `2416-2422`). The manual committer already lets publication perform no device operation and waits for explicit Flush to persist bounded staged COW generations (`internal/storeio/committer.go:82-88`). This removes a major obstacle: ordinary user reads and the primary page representation can remain identical.

The existing member root is a self-contained 4,096-byte `InlineSuperblock`: StateRoot, bounds, identities, and the cumulative inline free delta (`internal/storeio/inline_superblock.go:12-37`). Page size is fixed at 4 KiB (`store_file_options.go:954-960`). A group checkpoint can retain exact copies of those roots rather than retaining full mutation afterimages.

### New recovery authority

Introduce a replicated-only, versioned group mode. A checkpoint bank contains:

1. The complete fixed member set and schema/membership lineage, each exact store identity and root image.
2. The coherent **applied Raft index C**, its term/digest/bound group identity, and the relevant state-chain/checkpoint binding. C comes from the state machine's coherent applied cut, never from last appended index.
3. A monotonic checkpoint sequence and an integrity binding over every root image and member coordinate.
4. The retention authorization needed by the existing WAL checkpoint/snapshot protocol.

Use **complete inline-root images**, not merely the generation numbers or two mutable root-slot addresses. A metadata-only materialization/split can advance individual physical roots more than once at the same logical applied cut; its two slots are not a persistent archive of the last group cut. The full image must include the inline free delta, `FileEnd`, `NextLogicalID`, exact catalog/primary/index roots, geometry and ownership identities.

An intentionally simple first encoding allocates each bank as:

```
V(M) = round4096(256 fixed bytes + M*(64 identity bytes + 4096 root bytes)).
```

Two basic members give 24,576 bytes for both banks; 60 members give 499,712 bytes for both. These are proposed format sizes, not existing constants. It is acceptable to optimize zero-filled root tails later; correctness is clearer with exact images first.

### Foreground publication

Keep all current command planning, canonicalization, deterministic validation, whole-transaction admission, pre-staging, rollback, exact-index staging, and snapshot-gate ordering. Replace the journal-dependent staging contract with an explicitly owner-authorized “already backed by retained Raft” contract. Only a runtime with authenticated local Raft persistence and the fixed group binding can invoke it. Generic SQL/local collection users keep their current journal protocol.

After all members stage successfully, acquire all current snapshot gates, publish every staged root and visible applied cut, then release gates exactly as today. There is no durable local marker per apply: the committed Raft entry and old coherent checkpoint are the recovery source. A failure before publication exposes no partial logical update. A failure after an unknown durable checkpoint publication poisons the owner; recovery chooses authority instead of guessing.

This must not be implemented by merely setting existing `forceSync=false` or disabling a journal option: that is already the current apply behavior, and current `preparePrimaryBatchConditionalLocked` requires the journal (`store_file_primary_batch.go:794-811`). The new owner contract is explicit.

### Checkpoint and crash protocol

Start with both group banks independently recovering coherent cut C0. Pin all C0 roots' graphs before the allocator can reuse anything they reach. Hold the group publication lock for the coherent capture, preserving the same periodic and pre-admission pressure policy initially.

1. Select visible, applied C1 and the complete member set at that cut. No partially planned next transaction participates.
2. Permit the owner to materialize/flush each member's C1 graph while retaining the C0 graph. These are the existing COW physical folds; change the physical fence so only this exact owner/cut can perform them. Arbitrary `Flush` remains rejected.
3. Wait for all member graph/root device durability fences. Capture the exact durable root image of each member. A member that was not logically changed carries its coherent root unchanged.
4. Write and sync bank A for C1. Bank B still recovers C0. Both are safe: C1's entire graph was flushed first; C0's entire graph remains pinned.
5. Write and sync bank B with the same C1 member roots, using the next sequence. Only after successful publication of both independent banks may the group release C0's recovery floor and authorize WAL prefix retirement through C1.
6. If A-only or a sync reports unknown outcome, do not allow further destructive retirement. Reopen validates both banks and selects a complete candidate; repair/converge the banks before releasing any floor. A new cut must never accidentally strand a fallback to C0.

A crash during any member fold chooses C0; partially written C1 roots are unreachable from group authority. A crash after A chooses C1 if A is valid, or C0 through B if A is torn. A crash after B can recover C1 from either bank. Successful checkpoint authority certifies a fully applied cut even if a commit-only Raft HardState hint had not separately reached disk; retain the existing applied-proof binding rather than equating applied with appended or with an unverified volatile hint. The existing distinction is documented in `cmd/vibedb-shard/rf3_recovery_log.go:55-60` and `NodeStore.LogBounds:2023-2027`.

### Reclamation is part of the format

Current free-space reuse is fenced by readers and the other individual root's generation (`store_file_free.go:86-122`, `244-247`; `internal/storeio/committer.go:946-972`). Add a persistent group recovery floor for each member:

```
safe reuse floor = min(active reader floor,
                       individual recovery-root floor,
                       every still-selectable group checkpoint's member generation).
```

Apply that same floor to cold `restoreFencedExtents`, runtime reclamation, physical hole punching, and any recovery/retirement bypass. It must be installed **before opening a collection exposes its allocator**, not after ordinary Open has already restored its free set. Current cold Open independently chooses the newest member root (`store_file_open.go:190-235`); that is not valid in this mode.

The checkpoint root image's free log is its allocator truth. A newer independently flushed root must not donate its free set to an older selected group cut. Conversely, physical file length and allocation high water can exceed the selected root's logical `FileEnd`; that does not make the orphan tail part of the graph.

Provide `OpenAtAuthenticatedGroupRoot` or an equivalent sealed internal capability. Validate the root image and all existing root/identity/reference grammar, seed cache/allocator from that selected image, and keep actual physical backing separate from logical FileEnd. For the first implementation, perform a non-serving repair that durably replaces any ahead-of-group mutable root slots with the selected group root before enabling allocation. This prevents a later ordinary-root selector or equal-generation mismatch from resurrecting a discarded future root. Reuse the established fallback-root rollback semantics for logical bounds; do not independently select the maximum `NextLogicalID` from one root and the free log from another. If a monotonic allocation-ID reservation is required by an additional format, persist that as a separately defined fence rather than silently combining roots. Existing current primary pages use checksums rather than nonce derivation, but the in-place/encrypted alternatives should not be admitted to the first mode.

Complete copied roots also solve loss of the root descriptor during structural checkpoint churn: an individual superblock overwrite cannot erase the group's saved root descriptor. The external floor keeps that descriptor's reachable pages and free-log chain alive. This is the indispensable difference from simply reversing today's certificate/fold order. It does not by itself prove a constant bound on retained intermediate pages.

### Pressure constraint: a generation floor can retain more than one old graph

`FreeExtent` stores offset, length and retirement generation, not allocation/birth generation. The free set coalesces retirement metadata. A minimum generation fence therefore conservatively retains all later retired extents, including intermediate physical generations never reachable from the saved group root. Calling this “only one old graph of extra retention” would be unproven. It can exhaust the bounded retired-extent table or free-space admission and force extra checkpoints even though the removed journal I/O is real.

For the initial implementation, retain the existing rule at `checkpoint_group.go:2896-2907`: ordinary member physical materialization remains forbidden while visible transaction exceeds the group-certified cut. Grant an exception only to the checkpoint owner's captured cut while the whole group publication protocol runs. Keep periodic and pressure admission behavior initially. This limits the opportunities for arbitrary intermediate physical folds; it does not eliminate the need to measure/prove the number and size of intermediate folds inside a checkpoint.

There is a second case even when `visibleTxn == certTxn`: topology/metadata-equivalent commits can change root generations at the same logical applied cut. The new checkpoint must track **root-image dirtiness**, not just transaction dirtiness. Today's `checkpointLocked` early return (`2392-2393`) compares transaction counters alone. In the new mode it must also compare each member's actual durable root to the saved vector, and perform a same-applied-cut vector refresh when releasing those older roots is required. A metadata-only stream that triggers repeated refreshes adds group-bank writes/syncs; count those, do not hide them as “unchanged checkpoint scheduling.”

Two implementation policies are defensible, with different costs:

1. Conservative first mode: bound the intermediate physical-generation/retirement budget and refresh the same-cut root vector before it runs out. If this produces more foreground checkpoint work than current main under legal structural churn, it fails the intended no-regression requirement.
2. Finer later representation: persist birth/retirement intervals for extents, or an explicit reachability protection set for pinned group roots. An extent born after all pinned roots need not wait for their minimum generation. Coalescing must preserve interval distinctions, so this is a free-log/reclaimer format change, not an extra comparison against today's `RetiredGeneration` alone.

Before enabling journal-free production roots, qualification must establish peak retained bytes/extents and checkpoint counts for repeated splits, metadata-only materialization, snapshot pressure and maximum-width batched overflow updates. Byte savings from removed files must be reported net of this retained graph space. This is the largest remaining performance/space uncertainty in rank 1.

### Raft retention and cold replay

The Raft source floor must be at most the **minimum applied cut recoverable by every selectable group checkpoint bank**, including any older checkpoint fallback reachable through catalog/generation activation. An applied index alone is not permission to delete log bytes. Existing `CaptureWALBase` seals both authenticated checkpoint slots and creates a coherent snapshot under the SQL publication lock (`sql/driver/wal_base.go:90-166`); preserve its proof sequence and the later NodeStore checkpoint publication. A node snapshot/truncation may advance to C1 only after the new group's durable roots and both recovery banks cover C1. Failures must leave the old source floor pinned.

On restart, first open the authenticated node log, then authenticate the group/root checkpoint, open **all** collections at that same cut C, and only then replay committed Raft entries C+1 onward. This ordering has no inherent startup cycle: `serve_rf3.go:556-561` already opens NodeOwner before SQL group preparation; the selected-log bridge only permits sealed backends (`rf3_recovery_log.go:10-43`). Replay never runs against a mixture of user C1 and system C0. That coherent base is why full deterministic command replay is safe here, while it is unsafe as a drop-in replacement for one member's afterimage journal recovery.

Replaying the same suffix after another crash is safe because the selected base is coherent and pinned. It need not skip writes independently by per-collection applied watermarks. Bound replay by actual committed knowledge; an appended but uncommitted suffix is not an application input. A resumed RF3 quorum can establish commit for previously acknowledged entries whose commit hints were volatile.

Schema/membership replacement, split capture, snapshot installation, and activation must name the new complete root-vector generation with the exact member set. No individual relation can silently remain outside the vector. Existing staged-source/target membership proofs remain meaningful, but need a new mode-specific publication grammar. Start with fixed-membership groups and fail closed on unsupported activation transitions until their protocols are implemented; this is a rollout constraint, not evidence the general design works already.

### Byte and work accounting

For a fully allocated basic two-member profile, current journal/marker lengths total:

```
16,795,648 + 16,778,240 + 1,049,600 = 34,623,488 bytes/node.
```

Current `checkpoint.vgc` is 8,192 bytes. Replacing those sidecars with the proposed 24,576-byte root-vector pair saves **34,607,104 bytes/node**, or **103,821,312 bytes for RF3 (about 99.01 MiB)**, before any primary/log changes. Actual physical saving is the removed files' allocated blocks minus new checkpoint blocks, so sparse/portable files require measurement. Each additional fully backed user/global-index journal removed contributes about another 16.017 MiB per replica, at roughly 8 KiB/member extra checkpoint capacity.

For each physical apply, eliminated bytes are exactly `sum(J_j)+T(K)`. One 64 KiB user afterimage alone removes 66,048 bytes/replica, or 198,144 bytes for RF3, before system/marker output. Those are *write bytes removed*, not additional physical capacity savings to add to the deleted fixed journal files.

Per checkpoint, current redo-side barriers are M member-journal syncs plus one certificate sync, followed by the primary folds. The proposed straightforward protocol has two group-bank syncs, plus the same member graph/root folds. Thus the redo-side sync count falls by **M−1 per checkpoint**, not K+1 per apply. It also removes journal recycling and marker rollover work. It adds root-image copying/checksumming and roughly `2*V(M)` checkpoint bytes written per full two-bank publication. For small M and 128-transaction intervals, those bytes are much smaller than the removed per-apply metadata/afterimage output; still measure the checkpoint tail.

Primary read decoding, live document layout, key lookup and ordinary index access remain the same. That is a stronger “reads preserved” case than putting document values in a log. Journal encoding, CRC passes, append syscalls and buffer copying are removed by construction. Latency can still regress if the new recovery floor retains too many pages, consumes allocator headroom, creates earlier checkpoints, or adds locking to reads; these are falsifiable risks, not reasons to claim unmeasured speedup.

### Concrete first patch surface and falsification

First implement format/ownership and cold-open support, with a test-only mode on fresh fixed-membership RF3 roots. Relevant surfaces:

* `store/durable/checkpoint_group.go`: new mode, root-bank codec, no-redo staged group publication, exact owner checkpoint fence.
* `checkpoint_group_recovery.go`, `store_file_open.go`, `internal/storeio/inline_superblock.go`: externally authenticated full-root validation/selection; no mixed member roots.
* `store_file_free.go`, `store_file_hole_punch.go`, committer recovery/fallback integration: group floor before allocator exposure and through every destructive path.
* `store_file_primary_batch.go`: retain staging/admission and indexes, separate journal-specific record capacity from the owner-backed primary staging contract.
* `sql/driver/replicated_store.go`, `replicated_apply.go`, `replicated_sidecars.go`, `wal_base.go`: authenticated format selection, sidecar ownership, applied/root proof and retention.
* Existing node SQL binders: the real authenticated log capability, not an arbitrary recovery reader callback.

Do not migrate by deleting journals from an old root. The safe conversion first folds the old mode to an exact coherent cut, publishes both new-mode banks and an explicit catalog/primary ownership capability that old readers reject, then retires old sidecars after every fallback/activation path requires the new mode. The primary graph data need not be copied. Reverse migration needs an explicit conversion, not an old binary opening the files.

Correctness tests that could reject the design:

1. Crash after each member data/root fence, each bank write/sync, each floor release, each allocator reuse and each node checkpoint/truncation publication. On every restart assert all members at one coherent base and deterministic replay to the committed result.
2. Corrupt/torn newest bank with the older bank intact; advance individual roots repeatedly at the same logical cut; force reuse of the old base's leaf, overflow, free-index and free-delta extents. Any missing older graph disproves the floor implementation.
3. Crash with the latest physical member root ahead, with a reused free extent below selected FileEnd, and with an orphan appended tail above it. Reopen, repair, allocate, close, crash again, and verify no future root is resurrected and no logical identity/bounds are mixed.
4. Same-key changes across a 128-entry physical batch, uniqueness/index changes, state-only no-ops, session retries, conflicts, distributed staged payloads and capture enabled. These expose improper per-member replay shortcuts.
5. Three real RF3 processes: acknowledge, lose leader, restart followers with lagging durable commit hints, recover quorum, then read every acknowledged value. Refuse appended-only replay. Exercise snapshot install, schema/member transition and log prefix loss as separate gated features.

Performance falsifiers: compare identical schedules and admission limits; instrument journal append/syscall counts, primary physical bytes, cache misses, group-lock wait, allocator pressure, checkpoint count, and retained root bytes. The design fails the intended objective if removed journal calls are replaced by foreground data writes, if an extra root floor causes additional pressure checkpoints in steady state, or if primary reads gain log reads/decodes. Then use randomized isolated paired RF3 trials on tiny documents, near-overflow-threshold documents, 64 KiB/4 MiB random documents, mixed relations, and small-field versus full replacements. Operation-count reductions are evidence of a mechanism, not proof of p99 non-regression.

## 3. Rank 2: derive wave geometry instead of storing it repeatedly

Current extent validation already demands an ordinal sequence of adjacent extents and contiguous non-overlapping entry slices (`seglog/engine.go:1314-1356`). These invariants make most per-entry coordinates derivable. The sealed route format independently reconstructs coordinates for random lookup.

Add a new physical wave grammar with:

* A once-per-wave extent-length vector, extent IDs equal to ordinal+1, and offsets as prefix sums of ciphertext lengths.
* Per group, first index plus entry count; successive indexes are implicit. Encode term runs/changes rather than repeating a constant term.
* Per entry, logical type when needed and payload length; entry offsets follow a prefix sum within each extent. Zero-data entries have an explicit canonical no-payload rule. Extent coverage must still be exact.

The reconstructed `EntryLocation`, events and sealed route payload are unchanged. No primary document path is involved and sealed term/data point reads keep the same index operations. Active recovery uses the new decoder; wave MACs still cover every serialized byte. This is not an invitation to remove the sealed index or its independent authentication.

Concrete example: 64 contiguous normal entries at index 1,000,000, term 5, each with 512 payload bytes, in one 32,768-byte plaintext extent. Current seven-coordinate entry metadata is 863 bytes: fixed part `64*(3+1+1+1+3+2)=704` plus 159 bytes of DataOffset varints. A simple new grammar uses 128 bytes of entry lengths plus approximately 9 bytes for first index, constant term/mode and extent count/length: about 137 bytes. Saving: **726 bytes/wave, 11.34 bytes/entry**, plus any further avoided redundant flags. RF3 repeats that benefit three times. At one 64 KiB entry/wave the saving is only a few bytes; this will not explain large-document benchmark wins.

The encoder/decoder executes fewer varint operations in that common case. Use the old grammar if the complete new encoding is not smaller, but calculate this in the existing bounded preflight rather than running two full encoders. No compression search, history dictionary, additional disk lookup or new fsync is needed.

Version the physical frame explicitly; old readers must fail closed. Preserve retry behavior: the engine's existing remembered wave digest is over physical encoded bytes, so retrying an old wave ID must use its original authenticated grammar or verify an explicitly defined canonical logical identity. Simply re-encoding an existing wave with v2 and comparing its MAC to the old one would falsely reject an exact retry. Test empty entries, cross-group shared extents, nonconstant terms, suffix replacement, old/v2 mixed segments, frame tears, cold reopen and exact latest-wave retry. Compare encoded byte counts and integer-decode counts before testing latency.

## 4. Rank 3: authenticated afterimage references, without changing group decisions

A narrow alternative retains the current prepare/decision/certificate protocol but introduces a journal entry kind whose value is an authenticated reference to a final afterimage in an already durable Raft command. Bind source group/log/member lineage, Raft index+term, exact command digest, relation+mutation coordinate, canonical rendering version, length and afterimage digest. A 512-byte record can hold this metadata for one user value instead of the 66,048-byte 64 KiB example.

Propagate the source descriptor through `finalMutation`, net overlays and `WriteBatch`. The final surviving mutation is the source, not the first mutation of that key in a physical batch. Reuse `describeFinalMutation`'s existing length/hash where available (`apply.go:2366-2395`). Canonical source values already borrow command memory; rewritten values need a deterministic, versioned canonicalization recipe. Generated conflict results, retained transaction-derived values, capture and hidden rows initially stay inline unless their exact afterimages can be extracted without current state.

On journal recovery, resolve the immutable afterimage and invoke existing final-key Put/Delete replay. **Do not re-run the complete state machine or CAS against individually recovered collections.** Some collections may already be ahead of others; the journal is afterimage recovery, not command replay. This is the crucial contrast with the coherent root-vector base.

Pin each source index until every reference depending on it is folded past in a root and its journal recovery window is recycled. The retention protocol must discover these floors before allowing NodeStore truncation during startup. `CaptureWALBase` currently folds/recycles before releasing its retention witness; preserve that and make the source dependency an enforced bound, not a caller convention. Missing source or mismatched identity/digest is corruption, not an inline fallback invented at recovery.

This removes duplicate redo I/O, but **the current sealed 16 MiB per-member size cannot be reduced just because common puts use references**. Legal inline/generated records remain large, and retained windows span 128 physical transactions. A one-command 16 MiB aggregate admission bound does not bound all those retained transactions. Pooling all fallbacks into 16 MiB can cause earlier checkpoints or serialization than today's per-member windows. A variable, shared group redo arena is a separate design that must account for resident fallback bytes, concurrent admission and physical reserve replenishment; no free capacity reduction follows automatically.

## 5. Other format challenges and rejected shortcuts

**Command-header factoring:** A self-contained extent can store one 256-byte header template and a 32-bit bitmap of changed 8-byte words for each command. If six words change, the header charge is 52 bytes/command plus one 256-byte template, rather than 256 bytes/command. For 64 commands that saves 12,800 bytes before page metadata. It can reconstruct the original command bytes exactly, so portable wire commands, fingerprints and `normalEntryDigest` remain unchanged (`apply.go:1067-1077`). A dictionary shared across segments would create recovery/retention dependencies; keep it local to one authenticated extent. The costs are template comparisons and command reconstruction on Raft reads. There must be a bounded expansion limit and explicit codec; simply guessing a magic prefix after decryption is not enough. This is promising for tiny homogeneous commands, but less compelling than removing redo and less naturally compatible with the no-slower-read constraint than implicit wave geometry.

**Point the primary directly into Raft payloads:** Rejected as the first change. The node log is encrypted and grouped; a point read currently authenticates/decrypts the containing extent under NodeStore mutex (`node_store.go:1976-2016`), potentially shared with other entries. Primary PageRef is collection-owned and generation/allocator-bound. Direct Raft references add a new read/decrypt/cache path and pin entire log extents/segments for long-lived primary rows; stable large values could prevent reclamation indefinitely. A shared immutable value arena would need independent value ownership/refcounts or tracing, canonical representation and encryption identity, plus decoupling from Raft segment lifetime. That is a larger value-store architecture, not a safe small aliasing optimization.

**Remove the local journals and replay from latest roots:** Incorrect. Latest roots can contain different applied cuts, and system state/session decisions can be ahead of a missing user fold. Replaying commands against mixed roots can take different CAS/conflict branches. The root vector and its allocator floor are mandatory.

**A per-collection applied watermark is sufficient:** Incorrect for arbitrary cross-collection deterministic planning. Skipping writes to an ahead member does not recreate the historical reads/branch decisions needed by another member. It would require afterimage provenance or a coherent historical base.

**Shrink all journal reserves to one maximum command:** Incorrect as a general no-regression claim. Maximum single-command bytes and maximum retained checkpoint-window bytes are different quantities. Latest-main portable allocation also does not imply the files are sparse on Linux.

**Compress large overflow JSON and assume reads improve:** Not established. Raw overflow is a clear representation gap, but a codec adds CPU on every full read. The prior 64 KiB RF3 fixture fills payload with one repeated character and changes that character between cycles (`cmd/vibedb-shard/wal_retention_process_qualification_test.go:234`); it is highly compressible and is not evidence for random large documents or small-field updates. A compression proposal needs an explicit raw fallback and a workload/read-cost argument, not a ratio from this fixture.

**Replicate small update programs instead of full afterimages:** Potentially substantial for a 64 KiB document changing one scalar. The gateway already reads the old document and sends full replacement plus a 40-byte old-length/digest condition (`gateway/replicated_sql_transaction.go:578-592`). A bounded deterministic scalar-patch command can retain that condition and materialize exactly once at apply. It saves wire and Raft bytes, but followers still materialize and validate the full afterimage; it does not remove primary raw overflow or local full redo by itself. General SQL-expression replay is not the bounded kernel. This merits a separate command-format design, especially paired with root-vector recovery, but it does not help whole-payload replacements and is not the first all-workload change.

**Lower RF3 replication of authoritative bytes:** Not appropriate. These designs remove duplication inside each replica. They preserve each node's independent durable log and recoverable state. Cross-node deduplication/erasure schemes alter availability, recovery work and quorum semantics.

**Certified body retirement:** Useful for dead history behind a slow group, but it is lifecycle reclamation rather than a more compact live representation. It is orthogonal to these formats and remains an incomplete experiment. It cannot substitute for this analysis.

## Decision

The format is improvable. The high-impact target is the layering that stores a logical command as Raft redo and then stores its afterimages again for local recovery. The current COW graph gives a concrete route to remove that duplication while leaving document reads alone: make a coherent complete-root checkpoint the application recovery base, enforce its floor in the existing allocator, and replay the retained committed Raft suffix. The small accompanying physical-format target is metadata already implied by wave contiguity and extent geometry. Both have explicit byte/work accounting and tests capable of disproving the design; neither should inherit an unqualified performance claim from the earlier GC experiment.
