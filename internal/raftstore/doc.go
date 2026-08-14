// Package raftstore provides the bounded, disk-backed Raft StableStore used by
// the current non-serving Raft kernel.
//
// The package deliberately implements one append-only WAL and a static
// bootstrap snapshot. It does not compact the log, replace the bootstrap
// snapshot, serve traffic, or provide an HA transport.
// Runtime durability is qualified only on Linux and macOS. Other targets may
// compile, but Create and Open fail before touching the WAL namespace.
package raftstore
