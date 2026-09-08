# String compression and shared index fields

This work evaluates two sources of storage cost: long strings in primary
dictionary streams, and fields repeated across different secondary indexes.
The compression implementation is a candidate under qualification. Shared
storage across distinct indexes is not implemented.

## Primary dictionary strings

Primary stripes already select dictionary, prefix, numeric and alphabet
encodings. The candidate applies LZ4 to individual unique JSON string spellings
only after dictionary encoding wins. Packed row IDs remain directly readable;
a point read decompresses its selected value into caller-owned output.

Keys are excluded. Decoded entries are bounded to 4 KiB before allocation or
decompression. Incompressible entries retain raw bytes. A compressed stream
must currently save at least 64 bytes and 12.5 percent including its directory
and row IDs. This admission rule is still being qualified against physical
page rounding. Scratch memory is reused; preparation can allocate, while warm
codec encode, decode and validation have allocation tests.

The implementation uses LZ4 v4.1.28. Codec experiments favored its decode cost
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
insufficient. Scan preparation and physical-space admission remain open.
Write benchmarks include final Flush in their timing; their first short
screen is insufficient to claim a write improvement or regression.

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
canonical tuple bytes. Arbitrary IDs require value lookups for ordering;
ordered IDs are difficult to keep stable through insertion. Reassigning IDs
at checkpoints would rewrite otherwise unchanged leaves. Durable shared IDs
also require snapshot ownership, recovery and reclamation rules.

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
