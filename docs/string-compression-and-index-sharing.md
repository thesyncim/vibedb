# String compression and shared index fields

This work evaluates two sources of storage cost: long strings in primary
dictionary streams, and fields repeated across different secondary indexes.
The per-string compression experiment was rejected for production because
its physical savings were inconsistent and it slowed resident reads. Its
measurements are retained below. The chosen next implementation is bounded
physical packs of canonical exact-index leaves, compressed once on disk and
decoded at Open. See [the implementation plan](exact-index-packed-storage-plan.md).
Durable pack integration and its performance qualification are still pending.

## Primary dictionary strings

Primary stripes already select dictionary, prefix, numeric and alphabet
encodings. The rejected candidate applied LZ4 to individual unique JSON string spellings
only after dictionary encoding wins. Packed row IDs remain directly readable;
a point read decompresses its selected value into caller-owned output.

Keys are excluded. Decoded entries are bounded to 4 KiB before allocation or
decompression. Incompressible entries retained raw bytes. A compressed stream
had to save at least 64 bytes and 12.5 percent including its directory
and row IDs. That admission rule did not reliably reduce physical page
allocation. Scratch memory was reused; preparation could allocate, while warm
codec encode, decode and validation have allocation tests.

The experiment used LZ4 v4.1.28. Codec experiments favored its decode cost
over Snappy and S2 for this representation. Whole-page compression would need
separate physical and decoded page sizes in the cache and write path; it is a
different storage change.

### Preliminary physical-file screen

One baseline/candidate pair on Apple M4 Max, Go 1.27, portable backend, 20,000
rows, with 204-byte string values containing a repeated 96-byte token. These
are deliberately eligible synthetic dictionaries. They are not the 10M-row
CockroachDB comparison, a production workload distribution, or statistically
qualified performance results.

| Distinct strings | Baseline file B/row | Candidate file B/row | Space reduction | Resident read, baseline/candidate ns | Full scan, baseline/candidate ns/row |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8 | 6.758 | 6.758 | 0% | 386.4 / 408.0 | 182.9 / 212.8 |
| 32 | 8.806 | 7.782 | 11.6% | 388.7 / 408.0 | 184.6 / 214.3 |
| 64 | 10.85 | 9.626 | 11.3% | 388.9 / 409.2 | 196.7 / 212.2 |
| 128 | 13.93 | 11.88 | 14.7% | 398.2 / 410.2 | 196.7 / 210.0 |

Resident reads and full scans reported zero allocations in both builds.
The eight-value case demonstrates why stream-byte savings alone are
insufficient. The production per-string codec was removed in `7bb512749`.
Historical write timings excluded final Flush because `testing.B.Loop`
stops its timer when it returns false. Those timings must not be used as
end-to-end durable write results. The corrected harness explicitly restarts
the timer for final Flush before reporting the combined cost.

The original 10M varied-v1 payload uses deterministic pseudo-random ASCII
already handled well by the alphabet codec. Sample probes did not justify
this dictionary compressor for that payload. No new 10M space or performance
result is claimed here, and earlier log-reclamation opportunities are not
counted as compression savings.

## Fields shared by different indexes

Examples include `(tenant, address, status)` and `(tenant, address, category)`.
Existing storage already aliases identical index definitions, uses compact
row postings rather than full primary keys, prefix-compresses adjacent terms,
and locally shares repeated posting representations. Slot-stable updates
skip index records when the indexed term does not change.

Different compound indexes still own their canonical term bytes. Repeated
components are not stored in a shared cross-index field dictionary. A useful
measurement must count remaining encoded and allocated index bytes after
prefix and posting compression, varying shared-field position, cardinality,
length and number of indexes. Summing raw JSON field lengths overstates the
opportunity.

A global dictionary is not a free extension: current leaves are ordered by
canonical tuple bytes. Replacing the resident key representation with
arbitrary IDs would require lookups for ordering; ordered IDs are difficult
to keep stable through insertion. However, exact leaves already have
GC-owned canonical resident bytes populated at Open. Sharing confined to the
durable representation can resolve references during admission and preserve
the existing resident comparator and query path. Ownership, recovery and
reclamation still need explicit rules.

### Physical overlap census

The candidate's 20,000-row census uses independently assigned tenants and
shared values. Two different indexes contain the same field; the field is
either before or after another varying component. Values are deterministic
high-entropy strings, so this measures existing term prefix compression and
layout, rather than the primary LZ4 candidate's repeated-token fixture.

| Shared field | Position | Total file B/row | Encoded index key B/row | Index leaf extent B/row |
| --- | --- | ---: | ---: | ---: |
| 256 bytes, 8 values | Leading after tenant | 257.0 | 87.88 | 231.4 |
| 256 bytes, 8 values | After varying component | 789.7 | 490.4 | 764.1 |
| 256 bytes, 1,024 values | Leading after tenant | 1,014 | 491.8 | 794.4 |
| 256 bytes, 1,024 values | After varying component | 1,063 | 529.0 | 843.6 |
| 16 bytes, 64 values | Leading after tenant | 206.6 | 26.57 | 181.0 |
| 16 bytes, 64 values | After varying component | 229.0 | 48.82 | 203.4 |

These are baseline representation costs, not savings from an implemented
shared dictionary. Both repeated non-leading components and physical extent
slack are material. For the short leading-field case, keys, metadata and
postings together occupy about 68.3 B/row, while their leaf extents occupy
181.0 B/row. Packing immutable logical leaves could address that slack
without increasing the logical dirty-run rewrite unit.

Long strings repeated across several indexes are the initial measurement
target. A production design needs a demonstrated net file-size benefit and
paired point, range, insert and update measurements, including fields that
change and fields that remain unchanged. Recovery and retained-snapshot
correctness are required before enabling any shared representation.

## References

- [LZ4 Go implementation](https://github.com/pierrec/lz4)
- [RocksDB compression](https://github.com/facebook/rocksdb/wiki/Compression)
- [RocksDB dictionary compression](https://github.com/facebook/rocksdb/wiki/Dictionary-Compression)
- [FSST](https://github.com/cwida/fsst)
- [OnPair](https://github.com/axiomhq/onpair)

The references informed candidate selection; their published results do not
establish VibeDB performance.
