// Package replication defines the strict, deterministic state-machine command
// and completion envelopes used at the replication boundary.
//
// The package deliberately contains no consensus, transport, persistence, or
// apply implementation. A command is opaque data to the consensus core;
// its term and log index belong to that core's entry envelope. A completion is
// likewise only a bounded value format, not a completion table or blob store.
//
// Decoders return borrowed views into their input. Callers must keep the input
// immutable and live while using a view or iterator. A copied view or iterator
// retains the complete bounded envelope backing, so callers must not cache,
// queue, or otherwise retain one beyond its immediate apply/read boundary.
// Copy the bytes of only those individual fields that require independent
// ownership into fresh storage. Borrowed slices are capacity-clamped to prevent
// append from overwriting adjacent validated fields, but their existing
// elements remain read-only by contract.
//
// Mutation order is semantic. Encoders preserve every caller-provided ordinal,
// including repeated keys, without sorting, collapsing, or deduplicating it.
package replication
