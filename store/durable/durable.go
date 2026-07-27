// Package durable provides the bounded-residency, automatically persisted
// collection.
//
// A collection owns a single mutable generation stream but not the caller's
// *os.File lifetime. Create and Open acquire an exclusive writer lease before
// inspecting mutable state. Readers use explicit immutable Snapshot leases;
// Close releases both I/O resources and the writer lease.
package durable
