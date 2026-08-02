package query

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
)

var errStatementRecursiveDefinition = errors.New(
	"query: invalid owning Statement recursive CTE definition",
)

// statementRecursiveDefinition is the cold, lowering-neutral publication
// sidecar for one future WITH RECURSIVE definition. The ordinary statementCTE
// keeps only a nil pointer when this feature is absent.
//
// A successful installation transfers ownership of both Statement-term
// adapters and their separately prepared Statements to the owning Statement.
// Its CTE catalog releases all four objects through this sidecar.
type statementRecursiveDefinition struct {
	definition     *statementCTE
	descriptor     *RecursiveCTEDescriptor
	anchor         *RecursiveCTEStatementTerm
	recursive      *RecursiveCTEStatementTerm
	anchorStmt     *Statement
	recursiveStmt  *Statement
	baseCollection string
	params         int
	references     int
	arguments      []any

	// execution is feature-owned cold state. In particular, runtime and the
	// Statement-term adapters may retain &execution during their synchronous
	// run, but they never retain the caller's statementFrame. That boundary is
	// what keeps the ordinary statementCTE.materializeInto frame parameter
	// non-escaping when no recursive definition is installed.
	active    atomic.Bool
	execution statementFrame
	runtime   RecursiveCTERuntime
}

// installStatementRecursiveDefinition is the hook a future WITH RECURSIVE
// lowerer calls after it has prepared the owning Statement and separately
// prepared both physical terms. It does not parse or mutate SQL AST metadata.
func installStatementRecursiveDefinition(
	owner *Statement,
	definition *statementCTE,
	descriptor *RecursiveCTEDescriptor,
	baseCollection string,
) (*statementRecursiveDefinition, error) {
	if owner == nil || definition == nil || descriptor == nil ||
		definition.definition == nil || definition.recursiveDefinition != nil {
		return nil, fmt.Errorf(
			"query: recursive definition installation has nil or duplicate state: %w",
			errStatementRecursiveDefinition,
		)
	}
	catalog := owner.cteCatalog()
	if catalog == nil || catalog.find(definition.tree) != definition {
		return nil, fmt.Errorf(
			"query: recursive definition %q is not owned by the supplied Statement: %w",
			descriptor.name, errStatementRecursiveDefinition,
		)
	}
	if descriptor.name != definition.definition.Name {
		return nil, fmt.Errorf(
			"query: recursive descriptor %q cannot publish definition %q: %w",
			descriptor.name, definition.definition.Name,
			errStatementRecursiveDefinition,
		)
	}
	if len(descriptor.columns) != len(definition.names) {
		return nil, &RecursiveCTEArityError{
			Name: descriptor.name, Term: "owning definition",
			Expected: len(definition.names), Actual: len(descriptor.columns),
		}
	}
	for ordinal := range descriptor.columns {
		if descriptor.columns[ordinal] != definition.names[ordinal] {
			return nil, fmt.Errorf(
				"query: recursive definition %q column %d is %q, descriptor has %q: %w",
				descriptor.name, ordinal, definition.names[ordinal],
				descriptor.columns[ordinal], errStatementRecursiveDefinition,
			)
		}
	}
	anchor, anchorOK := descriptor.anchor.(*RecursiveCTEStatementTerm)
	recursive, recursiveOK := descriptor.recursive.(*RecursiveCTEStatementTerm)
	if !anchorOK || !recursiveOK || anchor == nil || recursive == nil ||
		anchor == recursive || anchor.statement == nil || recursive.statement == nil ||
		anchor.statement == recursive.statement || anchor.statement == owner ||
		recursive.statement == owner || anchor.target != nil || recursive.target == nil {
		return nil, fmt.Errorf(
			"query: recursive definition %q requires distinct prepared anchor and recursive Statements: %w",
			descriptor.name, errStatementRecursiveDefinition,
		)
	}
	params := owner.NumParams()
	if !recursiveStatementRangeFits(anchor, params) ||
		!recursiveStatementRangeFits(recursive, params) ||
		recursiveStatementRangesOverlap(anchor, recursive) {
		return nil, fmt.Errorf(
			"query: recursive definition %q has invalid or overlapping term placeholder ranges inside %d owning parameters: %w",
			descriptor.name, params, errStatementRecursiveDefinition,
		)
	}
	if baseCollection == "" {
		baseCollection = anchor.statement.Collection()
	}
	if baseCollection == "" {
		return nil, fmt.Errorf(
			"query: recursive definition %q has no base collection: %w",
			descriptor.name, errStatementRecursiveDefinition,
		)
	}
	prepared := &statementRecursiveDefinition{
		definition: definition,
		descriptor: descriptor,
		anchor:     anchor, recursive: recursive,
		anchorStmt: anchor.statement, recursiveStmt: recursive.statement,
		baseCollection: strings.Clone(baseCollection),
		params:         params, references: definition.references,
		arguments: make([]any, params),
	}
	fusionStatement := definition.firstReference.owner
	needsRelower := fusionStatement != nil && fusionStatement.canFuseCTE()
	definition.recursiveDefinition = prepared
	if needsRelower {
		previousPrepareMode := fusionStatement.prepareMode
		fusionStatement.prepareMode = true
		if err := fusionStatement.lower(fusionStatement.args); err != nil {
			fusionStatement.prepareMode = previousPrepareMode
			definition.recursiveDefinition = nil
			return nil, err
		}
		fusionStatement.prepareMode = previousPrepareMode
	}
	return prepared, nil
}

func recursiveStatementRangeFits(term *RecursiveCTEStatementTerm, bound int) bool {
	if term == nil || term.statement == nil || term.paramBase < 0 || bound < 0 ||
		term.paramBase > bound {
		return false
	}
	return term.statement.NumParams() <= bound-term.paramBase
}

func recursiveStatementRangesOverlap(
	left, right *RecursiveCTEStatementTerm,
) bool {
	leftParams, rightParams := left.statement.NumParams(), right.statement.NumParams()
	if leftParams == 0 || rightParams == 0 {
		return false
	}
	leftEnd := left.paramBase + leftParams
	rightEnd := right.paramBase + rightParams
	return left.paramBase < rightEnd && right.paramBase < leftEnd
}

func (d *statementRecursiveDefinition) materializeInto(
	parent *Exec,
	source Source,
	consumer *Statement,
	frame *statementFrame,
	destination *relationSpool,
	resource string,
) (charge int64, err error) {
	if d == nil || d.definition == nil || d.descriptor == nil ||
		d.definition.recursiveDefinition != d ||
		d.definition.references != d.references || parent == nil || consumer == nil ||
		frame == nil || destination == nil || len(frame.args) != d.params {
		if destination != nil {
			destination.reset()
		}
		return 0, fmt.Errorf(
			"query: recursive definition execution does not match its owning frame: %w",
			errStatementRecursiveDefinition,
		)
	}
	if !d.active.CompareAndSwap(false, true) {
		destination.reset()
		return 0, &RecursiveCTEReentryError{Name: d.descriptor.name}
	}
	defer d.active.Store(false)
	if err := cancellationError(parent.Options.Cancel); err != nil {
		destination.reset()
		return 0, err
	}
	base, err := source.subquerySource(
		consumer.Collection(), d.baseCollection,
	)
	if err != nil {
		destination.reset()
		return 0, err
	}
	// A prior interrupted owner must never leave its retained runtime charges
	// attached to this sidecar. Normal executions take this path with frame nil.
	if d.runtime.frame != nil {
		if d.runtime.frame != &d.execution {
			destination.reset()
			return 0, fmt.Errorf(
				"query: recursive definition %q retained a foreign execution frame: %w",
				d.descriptor.name, errStatementRecursiveDefinition,
			)
		}
		d.runtime.releaseExecution(&d.execution)
	}

	// The owned frame receives exactly the caller's currently available
	// statement-wide allowance. Once the fixpoint is ready, its live charge is
	// mirrored into the outer account while the durable CTE spool is copied, so
	// the two simultaneously-live relations can never exceed the outer limit.
	remaining := frame.intermediate.remaining()
	if remaining == 0 {
		destination.reset()
		return 0, &IntermediateBudgetError{
			Resource: "recursive CTE execution",
			Bytes:    saturatedBytes(frame.intermediate.used, 1),
			Limit:    frame.intermediate.limit,
		}
	}
	childOptions := parent.Options
	childOptions.IntermediateBytes = remaining
	if err := d.execution.begin(childOptions); err != nil {
		destination.reset()
		return 0, err
	}
	copy(d.arguments, frame.args)
	d.execution.args = d.arguments
	defer func() {
		d.runtime.releaseExecution(&d.execution)
		d.execution.args = nil
		clear(d.arguments)
	}()
	result, err := d.runtime.executeStatementTerms(
		d.descriptor, base, &d.execution, childOptions,
	)
	if err != nil {
		destination.reset()
		return 0, rebaseRecursiveDefinitionBudgetError(
			err, frame.intermediate.used, frame.intermediate.limit,
		)
	}
	if result.relation == nil || len(result.names) != len(d.descriptor.columns) {
		destination.reset()
		return 0, fmt.Errorf(
			"query: recursive definition %q produced no publishable snapshot: %w",
			d.descriptor.name, errStatementRecursiveDefinition,
		)
	}
	liveExecutionBytes := d.execution.intermediate.used
	if err := frame.intermediate.reserve(
		"recursive CTE execution", liveExecutionBytes,
	); err != nil {
		destination.reset()
		return 0, err
	}
	defer frame.intermediate.release(liveExecutionBytes)
	return materializeRecursiveDefinitionResult(
		destination, result.relation, frame, parent.Options.Cancel, resource,
	)
}

func rebaseRecursiveDefinitionBudgetError(
	err error,
	outerBytes int64,
	outerLimit int64,
) error {
	var recursive *RecursiveCTEIntermediateError
	if errors.As(err, &recursive) {
		return &RecursiveCTEIntermediateError{
			Name:  recursive.Name,
			Bytes: saturatedBytes(outerBytes, recursive.Bytes),
			Limit: outerLimit,
		}
	}
	var intermediate *IntermediateBudgetError
	if errors.As(err, &intermediate) {
		return &IntermediateBudgetError{
			Resource: intermediate.Resource,
			Bytes:    saturatedBytes(outerBytes, intermediate.Bytes),
			Limit:    outerLimit,
		}
	}
	return err
}

// materializeRecursiveDefinitionResult copies the runtime snapshot into the
// ordinary CTE spool. Full ownership is required even for shared definitions:
// reference-local evaluation may immediately reuse the same runtime, and no
// reference may borrow bytes that the next evaluation can overwrite.
func materializeRecursiveDefinitionResult(
	destination *relationSpool,
	source *relationSpool,
	frame *statementFrame,
	cancel *CancelFlag,
	resource string,
) (charge int64, err error) {
	if destination == nil || source == nil || destination == source ||
		source.rows < 0 || frame == nil {
		return 0, fmt.Errorf(
			"query: recursive definition has an invalid result relation: %w",
			errStatementRecursiveDefinition,
		)
	}
	destination.reset()
	rows, columns := source.rows, len(source.columns)
	payload := int64(0)
	for column := 0; column < columns; column++ {
		if len(source.columns[column]) != rows {
			return 0, &RecursiveCTEArityError{
				Name: "", Term: "published column", Expected: rows,
				Actual: len(source.columns[column]),
			}
		}
		for row := 0; row < rows; row++ {
			if err := cancellationCheckpoint(cancel, row); err != nil {
				return 0, err
			}
			bytes, err := relationCellOwnedBytesCancelable(
				cellFromScalar(source.columns[column][row]), cancel,
			)
			if err != nil {
				return 0, err
			}
			payload = saturatedBytes(payload, int64(bytes))
			if payload == math.MaxInt64 {
				return 0, ErrRecursiveSize
			}
		}
	}
	charge = relationSpoolRetainedBytes(rows, columns, payload)
	if charge == math.MaxInt64 {
		return 0, ErrRecursiveSize
	}
	if err = frame.intermediate.reserve(resource, charge); err != nil {
		return 0, err
	}
	reserved := charge
	defer func() {
		if err != nil {
			destination.reset()
			frame.intermediate.release(reserved)
			charge = 0
		}
	}()
	if err = cancellationError(cancel); err != nil {
		return 0, err
	}
	if err = destination.begin(rows, columns, payload); err != nil {
		return 0, err
	}
	for column := 0; column < columns; column++ {
		for row := 0; row < rows; row++ {
			if err = cancellationCheckpoint(cancel, row); err != nil {
				return 0, err
			}
			destination.columns[column][row], err = destination.ownCell(
				cellFromScalar(source.columns[column][row]), cancel,
			)
			if err != nil {
				return 0, err
			}
		}
	}
	if len(destination.data) != destination.plannedData {
		return 0, destination.sizingError(0)
	}
	return charge, cancellationError(cancel)
}

func (d *statementRecursiveDefinition) releaseExecution(frame *statementFrame) {
	if d == nil {
		return
	}
	// materializeInto tears down the feature-owned frame before publication
	// returns. Keep the catalog hook idempotent for failed/legacy executions.
	d.runtime.releaseExecution(&d.execution)
	d.execution.args = nil
	clear(d.arguments)
}

func (d *statementRecursiveDefinition) discardExecution() {
	if d == nil {
		return
	}
	d.runtime.releaseExecution(&d.execution)
	d.execution.args = nil
	clear(d.arguments)
}

func (d *statementRecursiveDefinition) release() {
	if d == nil {
		return
	}
	d.runtime.Release()
	if d.definition != nil && d.definition.recursiveDefinition == d {
		d.definition.recursiveDefinition = nil
	}
	anchor, recursive := d.anchor, d.recursive
	anchorStmt, recursiveStmt := d.anchorStmt, d.recursiveStmt
	if anchor != nil {
		anchor.Release()
	}
	if recursive != nil && recursive != anchor {
		recursive.Release()
	}
	if anchorStmt != nil {
		anchorStmt.Release()
	}
	if recursiveStmt != nil && recursiveStmt != anchorStmt {
		recursiveStmt.Release()
	}
	*d = statementRecursiveDefinition{}
}
