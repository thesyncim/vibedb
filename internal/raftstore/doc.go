// Package raftstore provides the bounded, disk-backed Raft StableStore used by
// the current non-serving Raft kernel.
//
// Every WAL is created with one mandatory authenticated family manifest. A
// healthy source can capture an exact current-slot cut and build a fully
// synced, strictly reopened compacted sibling around a newer certified base.
// Selection fences both the WAL and its bound SQL apply cut. Activation first
// settles that exact SQL snapshot, atomically replaces the logical WAL leaf,
// syncs the parent, and only then publishes the family generation active.
// Repeated generations form one authenticated parent-binding chain.
// Offline generation replay authenticates every captured source record but
// writes only the changing suffix above the future checkpoint into one
// deterministic preallocated stage. Heap use is bounded by one fixed scan
// buffer, one source-record ciphertext/plaintext pair, one retained chunk and
// its bounded encoding, plus fixed-count entry descriptors. Disk admission
// requires source and target to coexist only until authenticated activation;
// deterministic stages and transient source descriptors prevent abandoned
// work from pinning unbounded full-size generations.
// Runtime durability is qualified only on Linux and macOS. Other targets may
// compile, but Create and Open fail before touching the WAL namespace.
package raftstore
