package query

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"
)

const (
	// DefaultSetTreeRows bounds rows materialized across every leaf and binary
	// node in one execution.
	DefaultSetTreeRows int64 = 1_000_000
	// DefaultSetTreeBytes bounds the peak statement-wide relation, binary
	// workspace, and tree-control footprint.
	DefaultSetTreeBytes int64 = 64 << 20
	// DefaultSetTreeDepth bounds the validated expression-tree depth.
	DefaultSetTreeDepth = 256
	// DefaultSetTreeNodes bounds leaves plus binary set nodes.
	DefaultSetTreeNodes = 4_096
)

var (
	// ErrSetTreeConfig classifies invalid physical limits.
	ErrSetTreeConfig = errors.New("query: invalid set-expression configuration")
	// ErrSetTreePlan classifies malformed, cyclic, shared, or disconnected trees.
	ErrSetTreePlan = errors.New("query: invalid set-expression tree")
	// ErrSetTreeProgram reports a missing leaf provider.
	ErrSetTreeProgram = errors.New("query: set-expression program is nil")
	// ErrSetTreeSource classifies a missing or unstable leaf relation.
	ErrSetTreeSource = errors.New("query: invalid set-expression leaf relation")
	// ErrSetTreeInUse reports concurrent or re-entrant use of one executor.
	ErrSetTreeInUse = errors.New("query: set-expression executor is already running")
	// ErrSetTreeArity classifies incompatible ordinal widths.
	ErrSetTreeArity = errors.New("query: set-expression arity mismatch")
	// ErrSetTreeRows classifies exhaustion of the cumulative materialized-row limit.
	ErrSetTreeRows = errors.New("query: set-expression total row limit exceeded")
	// ErrSetTreeBytes classifies exhaustion of the peak physical byte limit.
	ErrSetTreeBytes = errors.New("query: set-expression byte limit exceeded")
	// ErrSetTreeDepth classifies a tree deeper than its configured bound.
	ErrSetTreeDepth = errors.New("query: set-expression depth limit exceeded")
	// ErrSetTreeNodes classifies a tree larger than its configured node bound.
	ErrSetTreeNodes = errors.New("query: set-expression node limit exceeded")
	// ErrSetTreeSize classifies integer or address-space overflow.
	ErrSetTreeSize = errors.New("query: set-expression size overflow")
)

// SetTreeOperation is one SQL binary set-operation mode.
type SetTreeOperation uint8

const (
	SetTreeUnionAll SetTreeOperation = iota
	SetTreeUnionDistinct
	SetTreeIntersectAll
	SetTreeIntersectDistinct
	SetTreeExceptAll
	SetTreeExceptDistinct
)

func (op SetTreeOperation) valid() bool { return op <= SetTreeExceptDistinct }

func (op SetTreeOperation) binary() setOperation { return setOperation(op) }

func (op SetTreeOperation) precedence() uint8 {
	if op == SetTreeIntersectAll || op == SetTreeIntersectDistinct {
		return 2
	}
	return 1
}

// SetTreeNodeKind distinguishes relation leaves from binary operators.
type SetTreeNodeKind uint8

const (
	SetTreeLeafNode SetTreeNodeKind = iota
	SetTreeBinaryNode
)

// SetTreeNode is one postorder physical expression node. A leaf names a
// lowering-owned source and declares its ordinal width. A binary node refers
// to two earlier nodes and propagates their common width.
type SetTreeNode struct {
	Kind      SetTreeNodeKind
	Source    int
	Columns   int
	Left      int
	Right     int
	Operation SetTreeOperation
}

// NewSetTreeLeaf constructs one leaf node.
func NewSetTreeLeaf(source, columns int) SetTreeNode {
	return SetTreeNode{Kind: SetTreeLeafNode, Source: source, Columns: columns}
}

// NewSetTreeBinary constructs one binary node over earlier postorder nodes.
func NewSetTreeBinary(operation SetTreeOperation, left, right int) SetTreeNode {
	return SetTreeNode{
		Kind: SetTreeBinaryNode, Operation: operation, Left: left, Right: right,
	}
}

// SetTreePlan is a lowering-supplied parenthesized tree in postorder. Root must
// be the final node. Each earlier node has exactly one parent, preventing DAG
// aliasing and making immediate intermediate release safe.
type SetTreePlan struct {
	Nodes []SetTreeNode
	Root  int
}

// SetTreeSource is a synchronous, stable, read-only leaf relation. The source
// may be reused as soon as its Leaf callback returns because Run immediately
// materializes an owned ordinal spool.
type SetTreeSource interface {
	Rows() int
	Columns() int
	Cell(row, column int) Cell
}

// SetTreeProgram resolves lowering-owned leaf identifiers. The cancellation
// flag is the same cooperative signal used by the tree and binary kernels.
type SetTreeProgram interface {
	Leaf(source int, cancel *CancelFlag) (SetTreeSource, error)
}

// SetTreeOptions are total physical limits for one execution. Zero selects a
// finite default and minus one disables one limit. Values below minus one are
// invalid, and at least one limit must remain finite.
type SetTreeOptions struct {
	MaxRows  int64
	MaxBytes int64
	MaxDepth int
	MaxNodes int
	Cancel   *CancelFlag
}

type setTreeOptions struct {
	maxRows  int64
	maxBytes int64
	maxDepth int
	maxNodes int
	cancel   *CancelFlag
}

func normalizeSetTreeOptions(options SetTreeOptions) (setTreeOptions, error) {
	rows := options.MaxRows
	if rows == 0 {
		rows = DefaultSetTreeRows
	} else if rows < -1 {
		return setTreeOptions{}, fmt.Errorf(
			"query: set-expression MaxRows must be -1, zero, or positive, got %d: %w",
			rows, ErrSetTreeConfig,
		)
	}
	bytes := options.MaxBytes
	if bytes == 0 {
		bytes = DefaultSetTreeBytes
	} else if bytes < -1 {
		return setTreeOptions{}, fmt.Errorf(
			"query: set-expression MaxBytes must be -1, zero, or positive, got %d: %w",
			bytes, ErrSetTreeConfig,
		)
	}
	depth := options.MaxDepth
	if depth == 0 {
		depth = DefaultSetTreeDepth
	} else if depth < -1 {
		return setTreeOptions{}, fmt.Errorf(
			"query: set-expression MaxDepth must be -1, zero, or positive, got %d: %w",
			depth, ErrSetTreeConfig,
		)
	}
	nodes := options.MaxNodes
	if nodes == 0 {
		nodes = DefaultSetTreeNodes
	} else if nodes < -1 {
		return setTreeOptions{}, fmt.Errorf(
			"query: set-expression MaxNodes must be -1, zero, or positive, got %d: %w",
			nodes, ErrSetTreeConfig,
		)
	}
	if rows < 0 && bytes < 0 && depth < 0 && nodes < 0 {
		return setTreeOptions{}, fmt.Errorf(
			"query: set-expression execution must retain a finite limit: %w",
			ErrSetTreeConfig,
		)
	}
	return setTreeOptions{
		maxRows: rows, maxBytes: bytes, maxDepth: depth, maxNodes: nodes,
		cancel: options.Cancel,
	}, nil
}

// SetTreeArityError identifies the node whose positional widths disagree.
type SetTreeArityError struct {
	Node  int
	Left  int
	Right int
}

func (e *SetTreeArityError) Error() string {
	return fmt.Sprintf(
		"query: set-expression node %d has ordinal widths %d and %d: %v",
		e.Node, e.Left, e.Right, ErrSetTreeArity,
	)
}

func (e *SetTreeArityError) Unwrap() error { return ErrSetTreeArity }

// SetTreeRowBudgetError reports cumulative rows through the rejecting node.
type SetTreeRowBudgetError struct {
	Node  int
	Rows  int64
	Limit int64
}

func (e *SetTreeRowBudgetError) Error() string {
	return fmt.Sprintf(
		"query: set-expression needs %d total rows through node %d, exceeding %d: %v",
		e.Rows, e.Node, e.Limit, ErrSetTreeRows,
	)
}

func (e *SetTreeRowBudgetError) Unwrap() error { return ErrSetTreeRows }

// SetTreeByteBudgetError reports the peak resource reservation that failed.
type SetTreeByteBudgetError struct {
	Resource string
	Bytes    int64
	Limit    int64
	cause    error
}

func (e *SetTreeByteBudgetError) Error() string {
	return fmt.Sprintf(
		"query: set-expression %s needs %d bytes, exceeding %d: %v",
		e.Resource, e.Bytes, e.Limit, ErrSetTreeBytes,
	)
}

func (e *SetTreeByteBudgetError) Unwrap() []error {
	return []error{ErrSetTreeBytes, e.cause}
}

// SetTreeDepthError reports the first node exceeding the configured depth.
type SetTreeDepthError struct {
	Node  int
	Depth int
	Limit int
}

func (e *SetTreeDepthError) Error() string {
	return fmt.Sprintf(
		"query: set-expression node %d reaches depth %d, exceeding %d: %v",
		e.Node, e.Depth, e.Limit, ErrSetTreeDepth,
	)
}

func (e *SetTreeDepthError) Unwrap() error { return ErrSetTreeDepth }

// SetTreeNodeBudgetError reports the complete supplied node count.
type SetTreeNodeBudgetError struct {
	Nodes int
	Limit int
}

func (e *SetTreeNodeBudgetError) Error() string {
	return fmt.Sprintf(
		"query: set-expression has %d nodes, exceeding %d: %v",
		e.Nodes, e.Limit, ErrSetTreeNodes,
	)
}

func (e *SetTreeNodeBudgetError) Unwrap() error { return ErrSetTreeNodes }

type setTreeSlotKind uint8

const (
	setTreeSlotLeaf setTreeSlotKind = iota
	setTreeSlotBinary
)

type setTreeSlot struct {
	leaf   relationSpool
	binary setExecutor
	charge int64
	kind   setTreeSlotKind
	active bool
}

func (s *setTreeSlot) relation() *relationSpool {
	if s == nil {
		return nil
	}
	if s.kind == setTreeSlotBinary {
		return s.binary.relation()
	}
	return &s.leaf
}

func (s *setTreeSlot) resetLogical() {
	if s == nil {
		return
	}
	if s.kind == setTreeSlotBinary {
		s.binary.result.reset()
		s.binary.workspace.reset()
	} else {
		s.leaf.reset()
	}
	s.charge = 0
	s.active = false
}

func (s *setTreeSlot) release() {
	if s == nil {
		return
	}
	s.leaf.release()
	s.binary.release()
	*s = setTreeSlot{}
}

// SetTreeExecutor evaluates validated postorder plans with a liveness-reused
// slot pool. Its zero value is ready. One executor is single-owner; independent
// executors share no mutable statement state and may run concurrently.
type SetTreeExecutor struct {
	running atomic.Bool

	options setTreeOptions
	frame   statementFrame
	slots   []setTreeSlot
	free    []int

	nodeSlots []int
	arities   []int
	depths    []int
	parents   []uint8

	totalRows    int64
	controlBytes int64
	rootSlot     int
}

// Run evaluates plan and returns an owned stable-order result. The result is
// valid until the next Run or Release on e.
func (e *SetTreeExecutor) Run(
	plan SetTreePlan,
	program SetTreeProgram,
	options SetTreeOptions,
) (result SetTreeResult, err error) {
	if e == nil {
		return SetTreeResult{}, fmt.Errorf("query: nil set-expression executor: %w", ErrSetTreeConfig)
	}
	if !e.running.CompareAndSwap(false, true) {
		return SetTreeResult{}, ErrSetTreeInUse
	}
	defer e.running.Store(false)

	e.resetRun()
	if program == nil {
		return SetTreeResult{}, ErrSetTreeProgram
	}
	e.options, err = normalizeSetTreeOptions(options)
	if err != nil {
		return SetTreeResult{}, err
	}
	if err = e.frame.begin(ExecOptions{IntermediateBytes: e.options.maxBytes}); err != nil {
		return SetTreeResult{}, err
	}
	maxSlots, err := e.preflightPlan(plan)
	if err != nil {
		return SetTreeResult{}, err
	}
	e.controlBytes = setTreeControlRetainedBytes(len(plan.Nodes), maxSlots)
	if e.controlBytes == math.MaxInt64 {
		return SetTreeResult{}, ErrSetTreeSize
	}
	if err = e.frame.intermediate.reserve(
		"set-expression control", e.controlBytes,
	); err != nil {
		e.controlBytes = 0
		return SetTreeResult{}, e.translateError(err)
	}
	defer func() {
		if err != nil {
			err = e.translateError(err)
			e.abortRun()
			result = SetTreeResult{}
		}
	}()
	if err = e.preparePlanStorage(len(plan.Nodes), maxSlots); err != nil {
		return SetTreeResult{}, err
	}
	if err = e.validatePlan(plan); err != nil {
		return SetTreeResult{}, err
	}

	for nodeIndex, node := range plan.Nodes {
		if err = cancellationError(e.options.cancel); err != nil {
			return SetTreeResult{}, err
		}
		if node.Kind == SetTreeLeafNode {
			if err = e.executeLeaf(nodeIndex, node, program); err != nil {
				return SetTreeResult{}, err
			}
			continue
		}
		if err = e.executeBinary(nodeIndex, node); err != nil {
			return SetTreeResult{}, err
		}
	}
	if err = cancellationError(e.options.cancel); err != nil {
		return SetTreeResult{}, err
	}
	e.rootSlot = e.nodeSlots[plan.Root]
	root := e.slots[e.rootSlot].relation()
	e.frame.intermediate.release(e.controlBytes)
	e.controlBytes = 0
	return SetTreeResult{relation: root}, nil
}

func (e *SetTreeExecutor) preflightPlan(plan SetTreePlan) (int, error) {
	nodes := len(plan.Nodes)
	if nodes == 0 || plan.Root != nodes-1 {
		return 0, fmt.Errorf(
			"query: set-expression root %d does not name final node %d: %w",
			plan.Root, nodes-1, ErrSetTreePlan,
		)
	}
	if e.options.maxNodes >= 0 && nodes > e.options.maxNodes {
		return 0, &SetTreeNodeBudgetError{Nodes: nodes, Limit: e.options.maxNodes}
	}
	stack, maxSlots := 0, 0
	for index, node := range plan.Nodes {
		switch node.Kind {
		case SetTreeLeafNode:
			stack++
			maxSlots = max(maxSlots, stack)
		case SetTreeBinaryNode:
			if !node.Operation.valid() || node.Left < 0 || node.Right < 0 ||
				node.Left >= index || node.Right >= index || node.Left == node.Right || stack < 2 {
				return 0, fmt.Errorf(
					"query: malformed binary set-expression node %d: %w", index, ErrSetTreePlan,
				)
			}
			// The output slot is acquired before two child slots are released.
			maxSlots = max(maxSlots, stack+1)
			stack--
		default:
			return 0, fmt.Errorf(
				"query: set-expression node %d has kind %d: %w",
				index, node.Kind, ErrSetTreePlan,
			)
		}
	}
	if stack != 1 {
		return 0, fmt.Errorf(
			"query: set-expression postorder leaves %d live values: %w", stack, ErrSetTreePlan,
		)
	}
	return maxSlots, nil
}

func (e *SetTreeExecutor) preparePlanStorage(nodes, slots int) error {
	var err error
	e.nodeSlots, err = reserveSetTreeSlice(e.nodeSlots, nodes)
	if err != nil {
		return err
	}
	e.arities, err = reserveSetTreeSlice(e.arities, nodes)
	if err != nil {
		return err
	}
	e.depths, err = reserveSetTreeSlice(e.depths, nodes)
	if err != nil {
		return err
	}
	e.parents, err = reserveSetTreeSlice(e.parents, nodes)
	if err != nil {
		return err
	}
	clear(e.parents)
	if len(e.slots) < slots {
		e.slots, err = reserveSetTreeSlots(e.slots, slots)
		if err != nil {
			return err
		}
	}
	e.free, err = reserveSetTreeSlice(e.free[:0], slots)
	if err != nil {
		return err
	}
	e.free = e.free[:0]
	for slot := slots - 1; slot >= 0; slot-- {
		e.slots[slot].resetLogical()
		e.free = append(e.free, slot)
	}
	return nil
}

func reserveSetTreeSlots(slots []setTreeSlot, need int) ([]setTreeSlot, error) {
	if need < 0 {
		return slots, ErrSetTreeSize
	}
	var err error
	slots, err = reserveRecursiveSlice(slots, need)
	if err != nil {
		return slots, ErrSetTreeSize
	}
	if len(slots) < need {
		slots = slots[:need]
	}
	return slots, nil
}

func reserveSetTreeSlice[T any](slice []T, need int) ([]T, error) {
	if need < 0 {
		return slice, ErrSetTreeSize
	}
	var err error
	slice, err = reserveRecursiveSlice(slice[:0], need)
	if err != nil {
		return slice, ErrSetTreeSize
	}
	return slice[:need], nil
}

func (e *SetTreeExecutor) validatePlan(plan SetTreePlan) error {
	for index, node := range plan.Nodes {
		switch node.Kind {
		case SetTreeLeafNode:
			if node.Source < 0 || node.Columns < 0 {
				return fmt.Errorf(
					"query: set-expression leaf %d has source/columns %d/%d: %w",
					index, node.Source, node.Columns, ErrSetTreePlan,
				)
			}
			e.arities[index] = node.Columns
			e.depths[index] = 1
		case SetTreeBinaryNode:
			if e.parents[node.Left] == math.MaxUint8 || e.parents[node.Right] == math.MaxUint8 {
				return ErrSetTreeSize
			}
			e.parents[node.Left]++
			e.parents[node.Right]++
			leftColumns, rightColumns := e.arities[node.Left], e.arities[node.Right]
			if leftColumns != rightColumns {
				return &SetTreeArityError{
					Node: index, Left: leftColumns, Right: rightColumns,
				}
			}
			e.arities[index] = leftColumns
			e.depths[index] = max(e.depths[node.Left], e.depths[node.Right]) + 1
		default:
			return ErrSetTreePlan
		}
		if e.options.maxDepth >= 0 && e.depths[index] > e.options.maxDepth {
			return &SetTreeDepthError{
				Node: index, Depth: e.depths[index], Limit: e.options.maxDepth,
			}
		}
	}
	for node := range plan.Nodes {
		want := uint8(1)
		if node == plan.Root {
			want = 0
		}
		if e.parents[node] != want {
			return fmt.Errorf(
				"query: set-expression node %d has %d parents, want %d: %w",
				node, e.parents[node], want, ErrSetTreePlan,
			)
		}
	}
	return nil
}

func (e *SetTreeExecutor) executeLeaf(
	nodeIndex int,
	node SetTreeNode,
	program SetTreeProgram,
) error {
	source, err := program.Leaf(node.Source, e.options.cancel)
	if err != nil {
		return err
	}
	if source == nil {
		return fmt.Errorf(
			"query: set-expression leaf %d source %d is nil: %w",
			nodeIndex, node.Source, ErrSetTreeSource,
		)
	}
	rows, columns := source.Rows(), source.Columns()
	if rows < 0 || columns < 0 {
		return fmt.Errorf(
			"query: set-expression leaf %d has shape %dx%d: %w",
			nodeIndex, rows, columns, ErrSetTreeSource,
		)
	}
	if columns != node.Columns {
		return &SetTreeArityError{Node: nodeIndex, Left: node.Columns, Right: columns}
	}
	if err := e.admitRows(nodeIndex, rows); err != nil {
		return err
	}
	slot, err := e.acquireSlot()
	if err != nil {
		return err
	}
	charge, err := materializeSetTreeLeaf(
		&e.slots[slot].leaf, source, rows, columns, &e.frame,
		e.options.cancel, "set-expression leaf",
	)
	if err != nil {
		return err
	}
	e.slots[slot].kind = setTreeSlotLeaf
	e.slots[slot].charge = charge
	e.slots[slot].active = true
	e.nodeSlots[nodeIndex] = slot
	e.totalRows += int64(rows)
	return nil
}

func (e *SetTreeExecutor) executeBinary(nodeIndex int, node SetTreeNode) error {
	leftSlot, rightSlot := e.nodeSlots[node.Left], e.nodeSlots[node.Right]
	if leftSlot == rightSlot || leftSlot < 0 || rightSlot < 0 ||
		leftSlot >= len(e.slots) || rightSlot >= len(e.slots) ||
		!e.slots[leftSlot].active || !e.slots[rightSlot].active {
		return ErrSetTreePlan
	}
	outputSlot, err := e.acquireSlot()
	if err != nil {
		return err
	}
	left := e.slots[leftSlot].relation()
	right := e.slots[rightSlot].relation()
	charge, err := e.slots[outputSlot].binary.execute(
		node.Operation.binary(), left, right, &e.frame, e.options.cancel,
	)
	if err != nil {
		return err
	}
	e.slots[outputSlot].kind = setTreeSlotBinary
	e.slots[outputSlot].charge = charge
	e.slots[outputSlot].active = true
	rows := e.slots[outputSlot].binary.result.rows
	if err := e.admitRows(nodeIndex, rows); err != nil {
		return err
	}
	e.totalRows += int64(rows)
	e.nodeSlots[nodeIndex] = outputSlot
	e.releaseSlot(leftSlot)
	e.releaseSlot(rightSlot)
	return nil
}

func (e *SetTreeExecutor) admitRows(node, rows int) error {
	if rows < 0 || e.totalRows > math.MaxInt64-int64(rows) {
		return ErrSetTreeSize
	}
	required := e.totalRows + int64(rows)
	if e.options.maxRows >= 0 && required > e.options.maxRows {
		return &SetTreeRowBudgetError{
			Node: node, Rows: required, Limit: e.options.maxRows,
		}
	}
	return nil
}

func materializeSetTreeLeaf(
	dst *relationSpool,
	source SetTreeSource,
	rows, columns int,
	frame *statementFrame,
	cancel *CancelFlag,
	resource string,
) (charge int64, err error) {
	dst.reset()
	payload := int64(0)
	for row := 0; row < rows; row++ {
		if err := cancellationCheckpoint(cancel, row); err != nil {
			return 0, err
		}
		for column := 0; column < columns; column++ {
			cell := source.Cell(row, column)
			if cell.kind < TypeNull || cell.kind > TypeJSON {
				return 0, fmt.Errorf(
					"query: set-expression leaf cell %d/%d has type %d: %w",
					row, column, cell.kind, ErrSetTreeSource,
				)
			}
			bytes, err := relationCellOwnedBytesCancelable(cell, cancel)
			if err != nil {
				return 0, err
			}
			payload = saturatedBytes(payload, int64(bytes))
			if payload == math.MaxInt64 {
				return 0, ErrSetTreeSize
			}
		}
	}
	charge = relationSpoolRetainedBytes(rows, columns, payload)
	if charge == math.MaxInt64 {
		return 0, ErrSetTreeSize
	}
	if err := frame.intermediate.reserve(resource, charge); err != nil {
		return 0, err
	}
	reserved := true
	defer func() {
		if err != nil {
			dst.reset()
		}
		if reserved {
			frame.intermediate.release(charge)
		}
	}()
	if err = dst.begin(rows, columns, payload); err != nil {
		return 0, err
	}
	for row := 0; row < rows; row++ {
		if err = cancellationCheckpoint(cancel, row); err != nil {
			return 0, err
		}
		for column := 0; column < columns; column++ {
			owned, ownErr := dst.ownCell(source.Cell(row, column), cancel)
			if ownErr != nil {
				return 0, ownErr
			}
			dst.columns[column][row] = owned
		}
	}
	if len(dst.data) != dst.plannedData {
		return 0, fmt.Errorf(
			"query: set-expression leaf changed during materialization: %w",
			ErrSetTreeSource,
		)
	}
	if err = cancellationError(cancel); err != nil {
		return 0, err
	}
	reserved = false
	return charge, nil
}

func (e *SetTreeExecutor) acquireSlot() (int, error) {
	if len(e.free) == 0 {
		return 0, ErrSetTreeSize
	}
	last := len(e.free) - 1
	slot := e.free[last]
	e.free = e.free[:last]
	e.slots[slot].resetLogical()
	return slot, nil
}

func (e *SetTreeExecutor) releaseSlot(slot int) {
	if slot < 0 || slot >= len(e.slots) || !e.slots[slot].active {
		return
	}
	e.frame.intermediate.release(e.slots[slot].charge)
	e.slots[slot].resetLogical()
	e.free = append(e.free, slot)
}

func (e *SetTreeExecutor) resetRun() {
	for slot := range e.slots {
		if e.slots[slot].active {
			e.frame.intermediate.release(e.slots[slot].charge)
		}
		e.slots[slot].resetLogical()
	}
	if e.controlBytes != 0 {
		e.frame.intermediate.release(e.controlBytes)
	}
	e.free = e.free[:0]
	e.totalRows = 0
	e.controlBytes = 0
	e.rootSlot = -1
}

func (e *SetTreeExecutor) abortRun() {
	for slot := range e.slots {
		if e.slots[slot].active {
			e.frame.intermediate.release(e.slots[slot].charge)
		}
		e.slots[slot].resetLogical()
	}
	if e.controlBytes != 0 {
		e.frame.intermediate.release(e.controlBytes)
	}
	e.controlBytes = 0
	e.totalRows = 0
	e.rootSlot = -1
}

func (e *SetTreeExecutor) translateError(err error) error {
	if err == nil {
		return nil
	}
	var budget *IntermediateBudgetError
	if errors.As(err, &budget) {
		return &SetTreeByteBudgetError{
			Resource: budget.Resource,
			Bytes:    budget.Bytes,
			Limit:    budget.Limit,
			cause:    err,
		}
	}
	if errors.Is(err, errSetSize) || errors.Is(err, errRelationSpoolSizing) {
		return errors.Join(ErrSetTreeSize, err)
	}
	return err
}

func setTreeControlRetainedBytes(nodes, slots int) int64 {
	if nodes < 0 || slots < 0 {
		return math.MaxInt64
	}
	perNode := int64(3*unsafe.Sizeof(int(0)) + unsafe.Sizeof(uint8(0)))
	perSlot := int64(unsafe.Sizeof(setTreeSlot{}) + unsafe.Sizeof(int(0)))
	return saturatedBytes(
		saturatedProduct(int64(nodes), perNode),
		saturatedProduct(int64(slots), perSlot),
	)
}

// Release drops all retained tree, leaf, and binary-node high-water storage.
// It must not race Run.
func (e *SetTreeExecutor) Release() {
	if e == nil {
		return
	}
	e.resetRun()
	for slot := range e.slots {
		e.slots[slot].release()
	}
	e.slots = nil
	e.free = nil
	e.nodeSlots = nil
	e.arities = nil
	e.depths = nil
	e.parents = nil
	e.options = setTreeOptions{}
	e.frame = statementFrame{}
}

// SetTreeResult is the root ordinal relation. It borrows executor-owned storage
// until the next Run or Release.
type SetTreeResult struct{ relation *relationSpool }

// Rows returns root cardinality.
func (r SetTreeResult) Rows() int {
	if r.relation == nil {
		return 0
	}
	return r.relation.rows
}

// Columns returns root ordinal width.
func (r SetTreeResult) Columns() int {
	if r.relation == nil {
		return 0
	}
	return len(r.relation.columns)
}

// Cell returns a root cell or SQL NULL for an invalid ordinal.
func (r SetTreeResult) Cell(row, column int) Cell {
	if r.relation == nil || row < 0 || row >= r.relation.rows ||
		column < 0 || column >= len(r.relation.columns) {
		return nullCell()
	}
	return cellFromScalar(r.relation.columns[column][row])
}

// Missing reports whether an output NULL retains the missing-path marker.
func (r SetTreeResult) Missing(row, column int) bool {
	if r.relation == nil || row < 0 || row >= r.relation.rows ||
		column < 0 || column >= len(r.relation.columns) {
		return false
	}
	value := r.relation.columns[column][row]
	return value.kind == kindNull && value.raw == nil
}

var _ SetTreeSource = SetTreeResult{}
