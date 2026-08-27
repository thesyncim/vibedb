# Competitive results

No current competitive result is published for this documentation revision.

This file intentionally contains no copied historical number. A new result
must come from the current harness and must include the metadata and raw rows
that [the performance guide](../../docs/performance.md) requires.

## Publication template

Record:

- Commit and dirty state
- Toolchain and dependency versions
- Machine, operating system, architecture, and filesystem
- Storage device and durability configuration
- Corpus, seed, index, cache, and client settings
- Complete command line
- Raw result artifact
- Repetition count and summary method
- Any diagnostic or non-publishable flag

Then add a table that links each summary row to its raw artifact. Keep latency,
throughput, storage, allocation, and residency units explicit.

A full embedded/RF3 publication must also link the immutable `VALIDATED.tsv`
receipt produced by `cmd/publishcheck`. Do not hand-create that receipt or copy
selected rows into this file when the complete bundle does not validate.
