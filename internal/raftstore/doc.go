// Package raftstore provides the bounded, disk-backed Raft StableStore used by
// the first durable-storage slice.
//
// The package deliberately implements one append-only WAL and a static
// bootstrap snapshot. It does not compact the log, replace the bootstrap
// snapshot, serve traffic, or provide an HA transport. Those later concerns
// must not weaken the recovery and durability boundary implemented here.
// Runtime durability is qualified only on Linux and macOS. Other targets may
// compile, but Create and Open fail before touching the WAL namespace.
package raftstore
