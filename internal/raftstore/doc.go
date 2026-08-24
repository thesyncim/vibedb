// Package raftstore provides the bounded, disk-backed Raft StableStore used by
// the current non-serving Raft kernel.
//
// Every open handle owns one append-only WAL with an immutable snapshot base.
// A healthy handle can capture an exact current-slot cut and build a fully
// synced, strictly reopened compacted sibling around a newer certified base.
// That sibling is deliberately non-authoritative: this package does not yet
// publish a family manifest, activate a generation, reclaim an older one,
// serve traffic, or provide an HA transport.
// Offline generation replay authenticates every captured source record but
// writes only the changing suffix above the future checkpoint into one
// deterministic preallocated stage. Heap use is bounded by one fixed scan
// buffer, one source-record ciphertext/plaintext pair, one retained chunk and
// its bounded encoding, plus fixed-count entry descriptors. Disk admission
// requires source and target to coexist until authenticated activation makes
// reclamation safe.
// Runtime durability is qualified only on Linux and macOS. Other targets may
// compile, but Create and Open fail before touching the WAL namespace.
package raftstore
