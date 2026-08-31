// Package shardservice defines the bounded gateway-to-shard protocols and
// their server implementations.
//
// [Server] is the static, single-owner SQL endpoint. [ReplicatedServer] serves
// the shard-native and control grammar over replicated state. Authenticated
// serving wrappers add TLS identity and capability checks for remote command
// paths; raw loopback and test entry points also exist, so neither server type
// is inherently an authentication boundary. Their request grammars, admission
// rules, read semantics, and failure outcomes are intentionally separate;
// callers must not infer RF3 guarantees from the static server or vice versa.
//
// The package is under active development. See
// docs/operations/distributed.md and docs/reference/protocols.md for the
// currently qualified topology and protocol boundaries.
package shardservice
