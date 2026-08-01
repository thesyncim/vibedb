package query

import (
	"fmt"
	"strconv"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

// CTEColumnAliasArityError is the runtime-schema counterpart of the parser's
// known-arity diagnostic. Wildcard expansion is deliberately resolved by the
// prepared child statement, then checked before any source is opened.
type CTEColumnAliasArityError struct {
	Name    string
	Aliases int
	Outputs int
	Pos     int
}

func (e *CTEColumnAliasArityError) Error() string {
	return fmt.Sprintf(
		"query: common table expression %q has %d column aliases but its query has %d outputs",
		e.Name, e.Aliases, e.Outputs,
	)
}

func (e *CTEColumnAliasArityError) Position() int {
	if e == nil {
		return 0
	}
	return e.Pos
}

type cteExecutionMode uint8

const (
	cteReferenceLocal cteExecutionMode = iota
	cteSharedMaterialized
	cteIndependent
	cteFused
)

func (m cteExecutionMode) String() string {
	switch m {
	case cteSharedMaterialized:
		return "materialized"
	case cteIndependent:
		return "not-materialized"
	case cteFused:
		return "fused"
	default:
		return "reference-local"
	}
}

type cteMaterializationState uint8

const (
	cteIdle cteMaterializationState = iota
	cteRunning
	cteReady
)

// statementCTEs is allocated only for a SELECT tree that declares or inherits
// a WITH scope. Definitions are keyed by the parser's stable Query identity;
// no execution-time string lookup, map, or per-reference preparation exists.
type statementCTEs struct {
	defs []*statementCTE
}

type statementCTE struct {
	definition *sqlast.CommonTableExpr
	tree       *sqlast.SelectStmt
	stmt       *Statement
	exec       Exec
	argBase    int

	spool       relationSpool
	names       []string
	ordinalSpec []string
	specData    []byte
	activeBytes int64
	state       cteMaterializationState

	references     int
	firstReference *statementCTEReference
	runEvaluations uint64

	// recursiveBinding is installed only for one synchronous prepared
	// recursive-Statement term invocation. Ordinary CTEs keep it nil and pay
	// no branch until they cross the existing materialization boundary.
	recursiveBinding *statementRecursiveBinding
	// recursiveOwner disables CTE fusion while one prepared Statement adapter
	// designates this definition as its delta relation. The cached plan must
	// never bypass the materialization hook merely because no invocation is
	// active at the instant lowering inspects the definition. Identity makes
	// adapter teardown exact when the borrowed Statement is reused.
	recursiveOwner *RecursiveCTEStatementTerm
}

// statementCTEReference owns storage only for reference-local and explicitly
// NOT MATERIALIZED evaluation. Shared materializations live on the definition;
// a fused reference has no relation storage at all.
type statementCTEReference struct {
	def         *statementCTE
	owner       *Statement
	spool       relationSpool
	activeBytes int64
}

func (s *Statement) cteCatalog() *statementCTEs {
	if s == nil || s.nested == nil {
		return nil
	}
	return s.nested.ctes
}

func (s *Statement) cteReference() *statementCTEReference {
	if s == nil || s.nested == nil {
		return nil
	}
	return s.nested.cte
}

func (c *statementCTEs) find(tree *sqlast.SelectStmt) *statementCTE {
	if c == nil || tree == nil {
		return nil
	}
	for _, def := range c.defs {
		if def != nil && def.tree == tree {
			return def
		}
	}
	return nil
}

func (s *Statement) prepareCTEDefinitions(argBase int) error {
	catalog := s.cteCatalog()
	if catalog == nil || s.tree.With == nil {
		return nil
	}
	for i := range s.tree.With.CTEs {
		definition := &s.tree.With.CTEs[i]
		if catalog.find(definition.Query) != nil {
			continue
		}
		def := &statementCTE{
			definition: definition,
			tree:       definition.Query,
			argBase:    argBase + definition.Query.ParamBase,
		}
		// Publish the stable identity before preparing the body. Non-recursive
		// binding means the body cannot reference itself, while nested local WITH
		// clauses may legitimately resolve earlier definitions in this catalog.
		catalog.defs = append(catalog.defs, def)
		child, err := prepareTreeInContext(
			s.text, definition.Query, 0, catalog, def.argBase,
		)
		if err != nil {
			return err
		}
		def.stmt = child
		def.names = append(def.names, child.Columns()...)
		if len(definition.Columns) > len(def.names) {
			position := definition.Pos
			if len(definition.ColumnPos) > len(def.names) {
				position = definition.ColumnPos[len(def.names)]
			}
			return &CTEColumnAliasArityError{
				Name: definition.Name, Aliases: len(definition.Columns),
				Outputs: len(def.names), Pos: position,
			}
		}
		copy(def.names, definition.Columns)
		def.ordinalSpec = make([]string, len(def.names))
		for ordinal := range def.ordinalSpec {
			start := len(def.specData)
			def.specData = append(def.specData, '/')
			def.specData = strconv.AppendInt(def.specData, int64(ordinal), 10)
			def.ordinalSpec[ordinal] = byteview.String(
				def.specData[start:len(def.specData):len(def.specData)],
			)
		}
	}
	return nil
}

func (s *Statement) prepareCTEReference() error {
	ref := &s.tree.From[0]
	def := s.cteCatalog().find(ref.Query)
	if def == nil {
		return fmt.Errorf(
			"query: CTE reference %q does not resolve to a prepared definition",
			ref.Name,
		)
	}
	def.references++
	prepared := &statementCTEReference{def: def, owner: s}
	if def.firstReference == nil {
		def.firstReference = prepared
	}
	s.ensureNested().cte = prepared
	return s.validateRelationReferences()
}

func (r *statementCTEReference) mode() cteExecutionMode {
	if r == nil || r.def == nil || r.def.definition == nil {
		return cteReferenceLocal
	}
	switch r.def.definition.Materialization {
	case sqlast.CTEMaterialized:
		return cteSharedMaterialized
	case sqlast.CTENotMaterialized:
		return cteIndependent
	default:
		if r.def.references > 1 {
			return cteSharedMaterialized
		}
		if r.owner != nil && r.owner.safeCTEFusionShape(r.def) {
			return cteFused
		}
		return cteReferenceLocal
	}
}

// safeCTEFusionShape proves the narrow identity projection for which removing
// the relation boundary changes no observable clause order. The defining plan
// is still allowed path renames and a conjunction of ordinary predicates, but
// no semantic barrier or nested source. Broader substitution belongs in the
// generalized relation planner; reporting a boundary is preferable to calling
// a spool "inline".
func (s *Statement) safeCTEFusionShape(def *statementCTE) bool {
	if s == nil || def == nil || def.stmt == nil || def.recursiveOwner != nil ||
		def.recursiveBinding != nil ||
		len(s.tree.Columns) != 1 {
		return false
	}
	outer := s.tree
	column := &outer.Columns[0]
	if column.Agg != sqlast.AggNone || column.Alias != "" || column.Path == nil ||
		len(column.Path.Segments) != 0 || outer.Distinct || outer.Where != nil ||
		len(outer.GroupBy) != 0 || outer.Having != nil || len(outer.OrderBy) != 0 ||
		outer.Limit != nil || outer.Offset != nil || len(outer.From) != 1 {
		return false
	}
	// The child Result carries the defining SELECT headers. Returning it
	// directly is schema-safe only when the CTE did not override those headers.
	if len(def.definition.Columns) != 0 {
		return false
	}
	body := def.tree
	if body == nil || body.With != nil || body.Distinct || len(body.From) != 1 ||
		body.From[0].Kind != sqlast.RelationCollection || len(body.GroupBy) != 0 ||
		body.Having != nil || len(body.OrderBy) != 0 || body.Limit != nil ||
		body.Offset != nil || !cteConjunctivePredicate(body.Where) {
		return false
	}
	for i := range body.Columns {
		if body.Columns[i].Agg != sqlast.AggNone || body.Columns[i].Path == nil {
			return false
		}
	}
	return true
}

func cteConjunctivePredicate(expr *sqlast.Expr) bool {
	if expr == nil {
		return true
	}
	if expr.Subquery != nil {
		return false
	}
	if len(expr.Kids) == 0 {
		return true
	}
	if expr.Kind != sqlast.ExprAnd {
		return false
	}
	for _, kid := range expr.Kids {
		if !cteConjunctivePredicate(kid) {
			return false
		}
	}
	return true
}

func (s *Statement) canFuseCTE() bool {
	ref := s.cteReference()
	return ref != nil && ref.mode() == cteFused
}

// DrivingPredicate returns the predicate executed directly against the
// physical source. Adapters use it only for immutable point-read selection. A
// safely fused CTE exposes its defining predicate; every other statement keeps
// the outer predicate, so a materialization boundary can never be bypassed by
// source planning.
//
// The returned AST is owned by s and follows Statement's lifetime.
func (s *Statement) DrivingPredicate() *sqlast.Expr {
	if s == nil {
		return nil
	}
	return s.drivingPredicate
}

func (s *Statement) resolveDrivingPredicate() *sqlast.Expr {
	if s == nil || s.tree == nil {
		return nil
	}
	if window := s.window(); window != nil {
		return window.input.DrivingPredicate()
	}
	if s.relationJoin() != nil {
		return nil
	}
	if s.canFuseCTE() {
		return s.cteReference().def.tree.Where
	}
	return s.tree.Where
}

func (s *Statement) runFusedCTE(
	parent *Exec,
	src Source,
	frame *statementFrame,
	intermediateResource string,
) (Cursor, error) {
	ref := s.cteReference()
	def := ref.def
	if err := cancellationError(parent.Options.Cancel); err != nil {
		return Cursor{}, err
	}
	args, err := def.boundArgs(frame)
	if err != nil {
		return Cursor{}, err
	}
	nestedSource, err := src.subquerySource(s.Collection(), def.stmt.Collection())
	if err != nil {
		return Cursor{}, err
	}
	def.runEvaluations++
	_, err = def.stmt.runIntoFrame(
		parent, nestedSource, args, frame, intermediateResource,
	)
	if err != nil {
		return Cursor{}, err
	}
	return s.cursor(&parent.Result), nil
}

func (d *statementCTE) boundArgs(frame *statementFrame) ([]any, error) {
	if d == nil || d.stmt == nil || frame == nil {
		return nil, fmt.Errorf("query: invalid CTE runtime")
	}
	n := d.stmt.NumParams()
	if d.argBase < 0 || d.argBase+n > len(frame.args) {
		return nil, fmt.Errorf("query: invalid CTE placeholder range")
	}
	return frame.args[d.argBase : d.argBase+n], nil
}

func (s *Statement) runRelations(
	parent *Exec,
	src Source,
	args []any,
	frame *statementFrame,
) (Source, error) {
	if join := s.relationJoin(); join != nil {
		return join.run(s, parent, src, args, frame)
	}
	if s.cteReference() != nil {
		return s.runCTE(parent, src, frame)
	}
	return s.runDerived(parent, src, args, frame)
}

func (s *Statement) runCTE(
	parent *Exec,
	src Source,
	frame *statementFrame,
) (Source, error) {
	ref := s.cteReference()
	if ref == nil {
		return src, nil
	}
	if err := cancellationError(parent.Options.Cancel); err != nil {
		return Source{}, err
	}
	if ref.mode() == cteSharedMaterialized {
		if err := ref.def.ensureMaterialized(parent, src, s, frame); err != nil {
			return Source{}, err
		}
		return fromRelationSpool(&ref.def.spool), nil
	}
	ref.spool.reset()
	ref.activeBytes = 0
	charge, err := ref.def.materializeInto(
		parent, src, s, frame, &ref.spool, "CTE reference-local spool",
	)
	if err != nil {
		return Source{}, err
	}
	ref.activeBytes = charge
	return fromRelationSpool(&ref.spool), nil
}

func (d *statementCTE) ensureMaterialized(
	parent *Exec,
	src Source,
	consumer *Statement,
	frame *statementFrame,
) error {
	if d.state == cteReady {
		return nil
	}
	if d.state == cteRunning {
		return fmt.Errorf("query: recursive CTE execution is not supported")
	}
	d.state = cteRunning
	d.spool.reset()
	d.activeBytes = 0
	charge, err := d.materializeInto(
		parent, src, consumer, frame, &d.spool, "materialized CTE spool",
	)
	if err != nil {
		d.state = cteIdle
		return err
	}
	d.activeBytes = charge
	d.state = cteReady
	return nil
}

func (d *statementCTE) materializeInto(
	parent *Exec,
	src Source,
	consumer *Statement,
	frame *statementFrame,
	spool *relationSpool,
	resource string,
) (int64, error) {
	if d != nil && d.recursiveBinding != nil {
		d.runEvaluations++
		return d.recursiveBinding.materializeInto(
			spool, frame, parent.Options.Cancel, resource,
		)
	}
	args, err := d.boundArgs(frame)
	if err != nil {
		return 0, err
	}
	nestedSource, err := src.subquerySource(
		consumer.Collection(), d.stmt.Collection(),
	)
	if err != nil {
		return 0, err
	}
	d.exec.Options = parent.Options
	d.runEvaluations++
	cursor, err := d.stmt.runIntoFrame(
		&d.exec, nestedSource, args, frame, "CTE query result",
	)
	if err != nil {
		d.cleanupChild(frame)
		return 0, err
	}
	resultBytes := d.exec.Result.resultBytesUsed
	if err := frame.intermediate.reserve("CTE query result", resultBytes); err != nil {
		d.cleanupChild(frame)
		return 0, err
	}
	charge, materializeErr := spool.materialize(
		cursor, len(d.names), frame, parent.Options.Cancel, resource,
	)
	frame.intermediate.release(resultBytes)
	d.cleanupChild(frame)
	if materializeErr != nil {
		return 0, materializeErr
	}
	return charge, nil
}

func (d *statementCTE) cleanupChild(frame *statementFrame) {
	if d == nil || d.stmt == nil {
		return
	}
	clearExecBorrowedViews(&d.exec)
	d.stmt.releaseRelations(frame)
}

func (s *Statement) releaseRelations(frame *statementFrame) {
	if window := s.window(); window != nil {
		window.releaseExecution(frame)
	}
	if join := s.relationJoin(); join != nil {
		join.releaseExecution(frame)
	}
	s.releaseDerived(frame)
	s.releaseCTEReference(frame)
	if s.nested != nil && s.nested.ownsCTEs && s.nested.ctes != nil {
		s.nested.ctes.releaseExecution(frame)
	}
}

func (s *Statement) releaseCTEReference(frame *statementFrame) {
	ref := s.cteReference()
	if ref == nil || ref.mode() == cteSharedMaterialized || ref.mode() == cteFused {
		return
	}
	ref.spool.reset()
	frame.intermediate.release(ref.activeBytes)
	ref.activeBytes = 0
}

func (s *Statement) discardRelations() {
	if window := s.window(); window != nil {
		window.discardExecution()
	}
	if join := s.relationJoin(); join != nil {
		join.discardExecution()
	}
	s.discardDerived()
	ref := s.cteReference()
	if ref != nil {
		ref.spool.reset()
		ref.activeBytes = 0
	}
	if s.nested != nil && s.nested.ownsCTEs && s.nested.ctes != nil {
		s.nested.ctes.discardExecution()
	}
}

func (c *statementCTEs) releaseExecution(frame *statementFrame) {
	if c == nil {
		return
	}
	for _, def := range c.defs {
		if def == nil {
			continue
		}
		def.cleanupChild(frame)
		def.spool.reset()
		frame.intermediate.release(def.activeBytes)
		def.activeBytes = 0
		def.state = cteIdle
	}
}

func (c *statementCTEs) discardExecution() {
	if c == nil {
		return
	}
	for _, def := range c.defs {
		if def == nil {
			continue
		}
		clearExecBorrowedViews(&def.exec)
		if def.stmt != nil {
			def.stmt.discardRelations()
		}
		def.spool.reset()
		def.activeBytes = 0
		def.state = cteIdle
		def.runEvaluations = 0
	}
}

func (c *statementCTEs) release() {
	if c == nil {
		return
	}
	for _, def := range c.defs {
		if def == nil {
			continue
		}
		if def.stmt != nil {
			def.stmt.Release()
		}
		def.exec.Release()
		def.spool.release()
		*def = statementCTE{}
	}
	*c = statementCTEs{}
}

// relationBinding presents derived and CTE roots through one ordinal namespace
// without installing an interface or relation branch in any row loop.
type relationBinding struct {
	names       []string
	ordinalSpec []string
}

func (s *Statement) relationBinding() relationBinding {
	if join := s.relationJoin(); join != nil {
		return join.sourceBinding(0)
	}
	if ref := s.cteReference(); ref != nil && ref.def != nil {
		return relationBinding{names: ref.def.names, ordinalSpec: ref.def.ordinalSpec}
	}
	if derived := s.derived(); derived != nil {
		return relationBinding{names: derived.names, ordinalSpec: derived.ordinalSpec}
	}
	return relationBinding{}
}

func (s *Statement) hasRelationBinding() bool {
	return s.relationJoin() != nil || s.derived() != nil || s.cteReference() != nil
}

func (s *Statement) relationBindingForSource(source int) relationBinding {
	if join := s.relationJoin(); join != nil {
		return join.sourceBinding(source)
	}
	if source == 0 {
		return s.relationBinding()
	}
	return relationBinding{}
}

func (s *Statement) resolveRelationColumn(name string) (int, error) {
	return s.resolveRelationColumnAt(0, name)
}

func (s *Statement) resolveRelationColumnAt(source int, name string) (int, error) {
	if join := s.relationJoin(); join != nil {
		relation := ""
		if source >= 0 && source < len(s.tree.From) {
			relation = s.tree.From[source].Alias
		}
		return join.resolve(source, name, relation)
	}
	binding := s.relationBinding()
	found, matches := -1, 0
	for i := range binding.names {
		if binding.names[i] == name {
			found = i
			matches++
		}
	}
	if matches != 1 {
		return -1, &RelationColumnError{
			Relation: s.tree.From[source].Alias,
			Column:   name,
			Matches:  matches,
		}
	}
	return found, nil
}

func selectRequiresCatalog(tree *sqlast.SelectStmt) bool {
	var first string
	return scanSelectCatalog(tree, &first)
}

func scanSelectCatalog(tree *sqlast.SelectStmt, first *string) bool {
	if tree == nil {
		return false
	}
	if len(tree.From) > 1 {
		return true
	}
	if tree.With != nil {
		for i := range tree.With.CTEs {
			if scanSelectCatalog(tree.With.CTEs[i].Query, first) {
				return true
			}
		}
	}
	for i := range tree.From {
		ref := &tree.From[i]
		switch ref.Kind {
		case sqlast.RelationCollection:
			if *first == "" {
				*first = ref.Name
			} else if *first != ref.Name {
				return true
			}
		case sqlast.RelationDerived:
			if scanSelectCatalog(ref.Query, first) {
				return true
			}
		}
	}
	return scanExprCatalog(tree.Where, first) || scanExprCatalog(tree.Having, first)
}

func scanExprCatalog(expr *sqlast.Expr, first *string) bool {
	if expr == nil {
		return false
	}
	if scanSelectCatalog(expr.Subquery, first) {
		return true
	}
	for _, kid := range expr.Kids {
		if scanExprCatalog(kid, first) {
			return true
		}
	}
	return false
}
