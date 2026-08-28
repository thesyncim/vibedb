// Package resultformat is the dependency-free registry for fixed replicated
// completion grammars. A value identifies semantics, not a codec version.
package resultformat

const (
	Mutation      uint16 = 1
	Transaction   uint16 = 2
	RouteGate     uint16 = 3
	RequestLedger uint16 = 4
	ExecutionPin  uint16 = 5
)

// Paired array bounds make collisions compile failures even when tests are
// skipped. The registry is intentionally dense while the product is
// unreleased; adding a grammar requires the next value, never a version suffix.
var (
	_ [int(Transaction) - int(Mutation) - 1]struct{}
	_ [int(Mutation) + 1 - int(Transaction)]struct{}
	_ [int(RouteGate) - int(Transaction) - 1]struct{}
	_ [int(Transaction) + 1 - int(RouteGate)]struct{}
	_ [int(RequestLedger) - int(RouteGate) - 1]struct{}
	_ [int(RouteGate) + 1 - int(RequestLedger)]struct{}
	_ [int(ExecutionPin) - int(RequestLedger) - 1]struct{}
	_ [int(RequestLedger) + 1 - int(ExecutionPin)]struct{}
)
