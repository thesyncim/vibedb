// Package vitessroute is an optional, strictly bounded Vitess-compatible
// mapping profile for the dependency-free distribution router. It reproduces
// the deterministic keyspace-id behavior of the Vitess xxhash and multicol
// Vindexes over the fixed 8-byte keyspace and presents them through the
// distribution.Mapper interface, so they interchange with distribution's
// native mapper.
//
// The package deliberately exposes only types from the distribution package
// and the Go standard library; no upstream Vitess type ever crosses its public
// API. It lives in a nested module so the root module carries no Vitess
// dependency graph.
//
// The reproduced mapping algorithms are derived from Vitess
// (github.com/vitessio/vitess, go/vt/vtgate/vindexes), which is licensed under
// the Apache License, Version 2.0. The shipped adapter reimplements the small
// keyspace-id algorithms directly rather than importing the Vitess dependency
// graph; the standalone hash primitive uses github.com/cespare/xxhash/v2, the
// same library Vitess uses. See LICENSE-VITESS for the upstream license and
// attribution. Behavioral equivalence with a single pinned upstream release is
// established by differential golden vectors, which are authoritative: a
// mismatch is a stop condition, never an edit target.
package vitessroute
