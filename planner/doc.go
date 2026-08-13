// Package planner provides the bounded, deterministic search core used to
// turn relational expressions into physical query plans.
//
// The package deliberately knows nothing about SQL syntax, catalogs, storage,
// or RPC. Callers supply rules and a Model. The core owns the parts that every
// optimizer otherwise gets subtly wrong in its own way: equivalence groups,
// duplicate suppression, rule scheduling, required and provided physical
// properties, enforcers, multidimensional costs, cancellation, and hard search
// budgets.
//
// A Memo group represents one relational result. Logical transformation rules
// and physical implementation rules add equivalent expressions to that group.
// Optimize performs top-down dynamic programming over (group, required
// properties), so one group may have different winners for singleton,
// partitioned, or ordered consumers without copying the logical tree.
package planner
