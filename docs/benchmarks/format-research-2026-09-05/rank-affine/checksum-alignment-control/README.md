# Checksum and alignment diagnostics

These frozen research inputs are copied into a temporary directory on a
diagnostic workflow runner. They are never added to a production package and
they do not change the benchmark source used by the ordinary comparison.

The control assembly contains two noescape functions with the same 16-byte
byte-XOR loop. `PCALIGN $64` followed by explicit padding puts one loop at
offset 24 and the other at offset 56 from a 64-byte boundary. The test file
checks both functions against an independent scalar oracle for lengths and
slice offsets that cross alignment and tail boundaries. The workflow verifies
the emitted bytes and addresses before it runs six alternating control pairs.

The stabilized durable replacement keeps the same byte-XOR result while
folding `uint64` chunks with `encoding/binary`. It is applied only in two
temporary detached worktrees. The packed runner retains the original and
stabilized logs in separate ABBA blocks, so a result cannot be mistaken for a
production benchmark or pooled with the primary comparison.

Its seven packed cases are the high-cardinality point read plus workers 1 and
4 for the 100,000-row low-cardinality, one-million-row low-cardinality, and
100,000-row high-cardinality partitioned scans. Replacement and batch-write
cases remain separate post-timing profiles.

Files:

- [control Go declarations](checksum_alignment_control.go.txt)
- [control oracle and benchmark](checksum_alignment_control_test.go.txt)
- [control amd64 assembly](checksum_alignment_control_amd64.s.txt)
- [stabilized durable helper](stabilized_touch_unified_scan_all_bytes.go.txt)
- [stabilized durable oracle](stabilized_touch_unified_scan_all_bytes_test.go.txt)
- [control runner](run_control_pairs.py.txt)
- [packed ABBA runner](run_packed_abba.py.txt)
- [control verifier](verify_control.py.txt)
