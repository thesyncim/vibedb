package planner

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

// ErrInvalidPhysicalProperties reports a malformed required or provided
// distribution/order contract.
var ErrInvalidPhysicalProperties = errors.New("planner: invalid physical properties")

// GroupID identifies an equivalence class in a Memo.
type GroupID uint32

// ExprID identifies one expression in a Memo.
type ExprID uint32

// PrivateID is a caller-owned reference to immutable operator metadata. The
// optimizer compares it as part of expression identity but never interprets it.
type PrivateID uint32

const (
	NoGroup GroupID = ^GroupID(0)
	NoExpr  ExprID  = ^ExprID(0)
)

// Operator identifies a logical or physical relational operator. The built-in
// vocabulary is intentionally small; FirstUserOperator leaves most of the
// domain available to a caller without changing the memo representation.
type Operator uint16

const (
	OpInvalid Operator = iota

	// Logical operators.
	OpLogicalScan
	OpLogicalFilter
	OpLogicalProject
	OpLogicalJoin
	OpLogicalAggregate
	OpLogicalSort
	OpLogicalLimit
	OpLogicalRemoteQuery

	// Physical operators.
	OpTableScan
	OpIndexScan
	OpFilter
	OpProject
	OpHashJoin
	OpMergeJoin
	OpNestedLoopJoin
	OpPartialAggregate
	OpFinalAggregate
	OpSort
	OpTopK
	OpRemoteQuery
	OpGather
	OpMergeGather
	OpRepartition
	OpBroadcast

	FirstUserOperator Operator = 1024
)

func (o Operator) String() string {
	switch o {
	case OpLogicalScan:
		return "logical-scan"
	case OpLogicalFilter:
		return "logical-filter"
	case OpLogicalProject:
		return "logical-project"
	case OpLogicalJoin:
		return "logical-join"
	case OpLogicalAggregate:
		return "logical-aggregate"
	case OpLogicalSort:
		return "logical-sort"
	case OpLogicalLimit:
		return "logical-limit"
	case OpLogicalRemoteQuery:
		return "logical-remote-query"
	case OpTableScan:
		return "table-scan"
	case OpIndexScan:
		return "index-scan"
	case OpFilter:
		return "filter"
	case OpProject:
		return "project"
	case OpHashJoin:
		return "hash-join"
	case OpMergeJoin:
		return "merge-join"
	case OpNestedLoopJoin:
		return "nested-loop-join"
	case OpPartialAggregate:
		return "partial-aggregate"
	case OpFinalAggregate:
		return "final-aggregate"
	case OpSort:
		return "sort"
	case OpTopK:
		return "top-k"
	case OpRemoteQuery:
		return "remote-query"
	case OpGather:
		return "gather"
	case OpMergeGather:
		return "merge-gather"
	case OpRepartition:
		return "repartition"
	case OpBroadcast:
		return "broadcast"
	default:
		return "operator(" + strconv.FormatUint(uint64(o), 10) + ")"
	}
}

// Expression is one memo alternative. Children name equivalence groups rather
// than concrete expressions; physical optimization chooses each child under
// the properties required by its parent.
type Expression struct {
	Op       Operator
	Children []GroupID
	Private  PrivateID
}

func cloneExpression(e Expression) Expression {
	e.Children = slices.Clone(e.Children)
	return e
}

// ColumnID is a stable column identity assigned by the caller's binder.
type ColumnID uint32

// Estimate carries a central estimate and an uncertainty interval. Cost models
// can use Upper for risk-aware planning instead of pretending guessed
// cardinalities are exact.
type Estimate struct {
	Value      float64 `json:"value"`
	Lower      float64 `json:"lower"`
	Upper      float64 `json:"upper"`
	Confidence float64 `json:"confidence"`
}

// ExactEstimate returns a certain non-negative estimate.
func ExactEstimate(value float64) Estimate {
	if value < 0 || math.IsNaN(value) {
		value = 0
	}
	return Estimate{Value: value, Lower: value, Upper: value, Confidence: 1}
}

// Normalize returns a finite, ordered uncertainty interval. Unknown or invalid
// inputs become a conservative non-negative estimate rather than NaN poison in
// cost comparisons.
func (e Estimate) Normalize(fallback float64) Estimate {
	if fallback < 0 || math.IsNaN(fallback) || math.IsInf(fallback, 0) {
		fallback = 0
	}
	if e.Value < 0 || math.IsNaN(e.Value) || math.IsInf(e.Value, 0) {
		e.Value = fallback
	}
	if e.Lower < 0 || math.IsNaN(e.Lower) || math.IsInf(e.Lower, 0) {
		e.Lower = 0
	}
	if e.Upper < e.Value || math.IsNaN(e.Upper) || math.IsInf(e.Upper, 0) {
		e.Upper = max(e.Value, fallback)
	}
	if e.Lower > e.Value {
		e.Lower = e.Value
	}
	if e.Confidence < 0 || math.IsNaN(e.Confidence) {
		e.Confidence = 0
	} else if e.Confidence > 1 {
		e.Confidence = 1
	}
	return e
}

// LogicalProperties are facts shared by every expression in one equivalence
// group. Columns and UniqueKeys use binder identities, not display names.
type LogicalProperties struct {
	Rows       Estimate
	RowBytes   Estimate
	Columns    []ColumnID
	UniqueKeys [][]ColumnID
}

func cloneLogicalProperties(p LogicalProperties) LogicalProperties {
	p.Columns = slices.Clone(p.Columns)
	p.UniqueKeys = slices.Clone(p.UniqueKeys)
	for i := range p.UniqueKeys {
		p.UniqueKeys[i] = slices.Clone(p.UniqueKeys[i])
	}
	return p
}

// DistributionKind describes how rows are placed across execution workers.
type DistributionKind uint8

const (
	DistributionAny DistributionKind = iota
	DistributionSingleton
	DistributionRandom
	DistributionHash
	DistributionRange
	DistributionReplicated
)

func (k DistributionKind) String() string {
	switch k {
	case DistributionAny:
		return "any"
	case DistributionSingleton:
		return "singleton"
	case DistributionRandom:
		return "random"
	case DistributionHash:
		return "hash"
	case DistributionRange:
		return "range"
	case DistributionReplicated:
		return "replicated"
	default:
		return "invalid"
	}
}

// Distribution is a physical partitioning property. Partitions zero means the
// consumer does not require an exact degree of parallelism.
type Distribution struct {
	Kind       DistributionKind
	Keys       []ColumnID
	Partitions uint32
}

// Direction is one ordering direction.
type Direction uint8

const (
	Ascending Direction = iota
	Descending
)

// OrderingColumn is one term in a physical ordering. NullsFirst is explicit so
// an enforcer never treats two observably different SQL orders as equivalent.
type OrderingColumn struct {
	Column     ColumnID
	Direction  Direction
	NullsFirst bool
}

// PhysicalProperties are required from or provided by a physical expression.
// Ordering is local within each partition unless Distribution is Singleton;
// MergeGather is the usual enforcer that turns locally ordered partitions into
// one globally ordered stream.
type PhysicalProperties struct {
	Distribution Distribution
	Ordering     []OrderingColumn
}

// Validate rejects property shapes whose meaning would be ambiguous to search
// or satisfaction checks.
func (p PhysicalProperties) Validate() error {
	switch p.Distribution.Kind {
	case DistributionAny:
		if len(p.Distribution.Keys) != 0 || p.Distribution.Partitions != 0 {
			return fmt.Errorf("%w: any distribution cannot carry keys or a partition count", ErrInvalidPhysicalProperties)
		}
	case DistributionSingleton:
		if len(p.Distribution.Keys) != 0 || p.Distribution.Partitions > 1 {
			return fmt.Errorf("%w: singleton distribution has keys or more than one partition", ErrInvalidPhysicalProperties)
		}
	case DistributionRandom, DistributionReplicated:
		if len(p.Distribution.Keys) != 0 {
			return fmt.Errorf("%w: %s distribution cannot carry keys", ErrInvalidPhysicalProperties, p.Distribution.Kind)
		}
	case DistributionHash, DistributionRange:
		if len(p.Distribution.Keys) == 0 {
			return fmt.Errorf("%w: %s distribution requires at least one key", ErrInvalidPhysicalProperties, p.Distribution.Kind)
		}
		for i, key := range p.Distribution.Keys {
			if slices.Contains(p.Distribution.Keys[:i], key) {
				return fmt.Errorf("%w: %s distribution repeats key c%d", ErrInvalidPhysicalProperties, p.Distribution.Kind, key)
			}
		}
	default:
		return fmt.Errorf("%w: unknown distribution kind %d", ErrInvalidPhysicalProperties, p.Distribution.Kind)
	}
	for _, term := range p.Ordering {
		if term.Direction != Ascending && term.Direction != Descending {
			return fmt.Errorf("%w: ordering for c%d has direction %d", ErrInvalidPhysicalProperties, term.Column, term.Direction)
		}
	}
	return nil
}

func (p PhysicalProperties) validateProvided() error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.Distribution.Kind == DistributionAny {
		return fmt.Errorf("%w: a physical expression must provide a concrete distribution", ErrInvalidPhysicalProperties)
	}
	return nil
}

func clonePhysicalProperties(p PhysicalProperties) PhysicalProperties {
	p.Distribution.Keys = slices.Clone(p.Distribution.Keys)
	p.Ordering = slices.Clone(p.Ordering)
	return p
}

// Equal reports exact physical-property identity.
func (p PhysicalProperties) Equal(other PhysicalProperties) bool {
	return p.Distribution.Kind == other.Distribution.Kind &&
		p.Distribution.Partitions == other.Distribution.Partitions &&
		slices.Equal(p.Distribution.Keys, other.Distribution.Keys) &&
		slices.Equal(p.Ordering, other.Ordering)
}

// Satisfies reports whether provided is strong enough for required.
func (provided PhysicalProperties) Satisfies(required PhysicalProperties) bool {
	if !distributionSatisfies(provided.Distribution, required.Distribution) {
		return false
	}
	if len(required.Ordering) > len(provided.Ordering) {
		return false
	}
	return slices.Equal(provided.Ordering[:len(required.Ordering)], required.Ordering)
}

func distributionSatisfies(provided, required Distribution) bool {
	if required.Kind == DistributionAny {
		return true
	}
	if provided.Kind != required.Kind {
		return false
	}
	if required.Partitions != 0 && provided.Partitions != required.Partitions {
		return false
	}
	return slices.Equal(provided.Keys, required.Keys)
}

func (p PhysicalProperties) hash(salt uint64) uint64 {
	h := indexHashAdd(salt^3, salt, uint64(p.Distribution.Kind))
	h = indexHashAdd(h, salt, uint64(p.Distribution.Partitions))
	h = indexHashAdd(h, salt, uint64(len(p.Distribution.Keys)))
	for _, key := range p.Distribution.Keys {
		h = indexHashAdd(h, salt, uint64(key))
	}
	h = indexHashAdd(h, salt, uint64(len(p.Ordering)))
	for _, term := range p.Ordering {
		h = indexHashAdd(h, salt, uint64(term.Column))
		h = indexHashAdd(h, salt, uint64(term.Direction))
		h = indexHashAdd(h, salt, uint64(boolByte(term.NullsFirst)))
	}
	return indexHashFinish(h)
}

const (
	fnv64Offset = uint64(14695981039346656037)
	fnv64Prime  = uint64(1099511628211)
)

func hashByte(hash uint64, value byte) uint64 {
	return (hash ^ uint64(value)) * fnv64Prime
}

func boolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}

func (p PhysicalProperties) String() string {
	var b strings.Builder
	b.WriteString(p.Distribution.Kind.String())
	if len(p.Distribution.Keys) != 0 {
		b.WriteByte('(')
		for i, key := range p.Distribution.Keys {
			if i != 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "c%d", key)
		}
		b.WriteByte(')')
	}
	if p.Distribution.Partitions != 0 {
		fmt.Fprintf(&b, "/%d", p.Distribution.Partitions)
	}
	if len(p.Ordering) != 0 {
		b.WriteString(" order(")
		for i, term := range p.Ordering {
			if i != 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "c%d", term.Column)
			if term.Direction == Descending {
				b.WriteString(" desc")
			} else {
				b.WriteString(" asc")
			}
			if term.NullsFirst {
				b.WriteString(" nulls-first")
			} else {
				b.WriteString(" nulls-last")
			}
		}
		b.WriteByte(')')
	}
	return b.String()
}

// Cost is a multidimensional, non-negative physical cost. Memory is peak live
// bytes and composes with max; the other dimensions compose additively.
type Cost struct {
	Startup float64
	CPU     float64
	IO      float64
	Network float64
	Memory  float64
}

func (c Cost) valid() bool {
	return finiteNonNegative(c.Startup) && finiteNonNegative(c.CPU) &&
		finiteNonNegative(c.IO) && finiteNonNegative(c.Network) &&
		finiteNonNegative(c.Memory)
}

func finiteNonNegative(v float64) bool {
	return v >= 0 && !math.IsNaN(v) && !math.IsInf(v, 0)
}

// Plus composes sequential/operator costs without double-counting peak memory.
// A physical model uses CostComposer when children are concurrently live or
// other dimensions follow an operator-specific critical path.
func (c Cost) Plus(other Cost) Cost {
	return Cost{
		Startup: c.Startup + other.Startup,
		CPU:     c.CPU + other.CPU,
		IO:      c.IO + other.IO,
		Network: c.Network + other.Network,
		Memory:  max(c.Memory, other.Memory),
	}
}

// Objective turns the cost vector into a workload-specific score while
// retaining a hard memory feasibility bound.
type Objective struct {
	StartupWeight float64
	CPUWeight     float64
	IOWeight      float64
	NetworkWeight float64
	MemoryWeight  float64
	MaxMemory     float64
}

// DefaultObjective favors avoiding network movement and IO, then CPU. Memory
// remains visible but is primarily controlled by MaxMemory when configured.
func DefaultObjective() Objective {
	return Objective{
		StartupWeight: 2,
		CPUWeight:     1,
		IOWeight:      4,
		NetworkWeight: 8,
		MemoryWeight:  1.0 / (64 << 20),
	}
}

func (o Objective) withDefaults() Objective {
	if o.StartupWeight == 0 && o.CPUWeight == 0 && o.IOWeight == 0 &&
		o.NetworkWeight == 0 && o.MemoryWeight == 0 {
		defaults := DefaultObjective()
		defaults.MaxMemory = o.MaxMemory
		return defaults
	}
	return o
}

func (o Objective) valid() bool {
	return finiteNonNegative(o.StartupWeight) && finiteNonNegative(o.CPUWeight) &&
		finiteNonNegative(o.IOWeight) && finiteNonNegative(o.NetworkWeight) &&
		finiteNonNegative(o.MemoryWeight) && finiteNonNegative(o.MaxMemory)
}

func (o Objective) feasible(cost Cost) bool {
	return cost.valid() && (o.MaxMemory <= 0 || cost.Memory <= o.MaxMemory)
}

// Score returns the scalar objective value used to compare feasible plans.
func (o Objective) Score(cost Cost) float64 {
	o = o.withDefaults()
	return cost.Startup*o.StartupWeight + cost.CPU*o.CPUWeight +
		cost.IO*o.IOWeight + cost.Network*o.NetworkWeight +
		cost.Memory*o.MemoryWeight
}
