package query

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// recursiveSQLPreparedDefinition owns cold preparation state until install
// atomically transfers it to the owning Statement catalog.
type recursiveSQLPreparedDefinition struct {
	plan          *recursiveSQLDefinitionPlan
	anchorStmt    *Statement
	recursiveStmt *Statement
	anchor        *RecursiveCTEStatementTerm
	recursive     *RecursiveCTEStatementTerm
	descriptor    *RecursiveCTEDescriptor
	base          string
	transferred   bool
}

func prepareRecursiveSQLDefinition(
	src string,
	owner *Statement,
	catalog *statementCTEs,
	plan *recursiveSQLDefinitionPlan,
	options RecursiveSQLStatementOptions,
) (prepared recursiveSQLPreparedDefinition, err error) {
	prepared.plan = plan
	if owner == nil || catalog == nil || plan == nil {
		return prepared, fmt.Errorf(
			"query: recursive SQL preparation has no owning catalog: %w",
			errStatementRecursiveDefinition,
		)
	}
	definition := catalog.find(plan.anchor)
	if definition == nil || definition.definition != plan.definition {
		return prepared, fmt.Errorf(
			"query: recursive SQL definition %q has no prepared owner identity: %w",
			plan.definition.Name, errStatementRecursiveDefinition,
		)
	}
	ownerParams := owner.NumParams()
	anchorTree, err := cloneRecursiveSQLFullFrameTree(
		plan.anchor, plan.absoluteAnchorBase, ownerParams,
	)
	if err != nil {
		return prepared, positionedRecursiveSQLCloneError(src, plan.anchor, err)
	}
	prepared.anchorStmt, err = prepareTreeInContext(
		src, anchorTree, 0, catalog, 0,
	)
	if err != nil {
		return prepared, err
	}
	defer func() {
		if err != nil {
			prepared.release()
		}
	}()

	recursiveTree, recursiveName, err := recursiveSQLFullFrameTermTree(
		plan, definition.names, ownerParams,
	)
	if err != nil {
		return prepared, positionedRecursiveSQLCloneError(src, plan.term, err)
	}
	catalogBase := len(catalog.defs)
	prepared.recursiveStmt, err = prepareTreeInContext(
		src, recursiveTree, 0, catalog, 0,
	)
	added := append([]*statementCTE(nil), catalog.defs[catalogBase:]...)
	clear(catalog.defs[catalogBase:])
	catalog.defs = catalog.defs[:catalogBase]
	if err != nil {
		releaseRecursiveSQLDetachedCatalog(added)
		return prepared, err
	}
	if len(added) != 1 || added[0] == nil ||
		added[0].definition == nil || added[0].definition.Name != recursiveName {
		releaseRecursiveSQLDetachedCatalog(added)
		return prepared, fmt.Errorf(
			"query: recursive SQL term published %d private delta definitions: %w",
			len(added), errStatementRecursiveDefinition,
		)
	}
	// Every prepared reference already owns an exact statementCTE pointer. The
	// term needs the owner catalog only during preparation. Give it ownership of
	// exactly the detached synthetic delta afterwards, so ordinary dependencies
	// remain owner-managed and the private target is released exactly once.
	privateCatalog := &statementCTEs{defs: added}
	prepared.recursiveStmt.ensureNested().ctes = privateCatalog
	prepared.recursiveStmt.nested.ownsCTEs = true

	prepared.anchor, err = PrepareRecursiveCTEStatementTerm(
		prepared.anchorStmt,
		RecursiveCTEStatementTermOptions{ParamBase: 0},
	)
	if err != nil {
		return prepared, err
	}
	prepared.recursive, err = PrepareRecursiveCTEStatementTerm(
		prepared.recursiveStmt,
		RecursiveCTEStatementTermOptions{
			ParamBase: 0, RecursiveRelation: recursiveName,
		},
	)
	if err != nil {
		return prepared, err
	}
	union := RecursiveUnionDistinct
	if plan.operation == sqlast.SetUnionAll {
		union = RecursiveUnionAll
	}
	materialization := RecursiveCTEShared
	if plan.definition.Materialization == sqlast.CTENotMaterialized {
		materialization = RecursiveCTEReferenceLocal
	}
	prepared.descriptor, err = PrepareRecursiveCTEDescriptor(
		plan.definition.Name, definition.names,
		prepared.anchor, prepared.recursive,
		union, materialization, options.Limits,
	)
	if err != nil {
		return prepared, err
	}
	prepared.base = prepared.anchorStmt.Collection()
	if prepared.base == "" {
		if source, ok := recursiveSQLPhysicalSource(plan.anchor, plan.anchor, 0); ok {
			prepared.base = source.Name
		}
	}
	return prepared, nil
}

func positionedRecursiveSQLCloneError(
	src string,
	tree *sqlast.SelectStmt,
	err error,
) error {
	return sqlast.NewFeatureNotSupportedError(
		src, recursiveSQLSelectPos(tree),
		"recursive SQL placeholder ranges cannot be represented safely: "+err.Error(),
	)
}

func releaseRecursiveSQLDetachedCatalog(definitions []*statementCTE) {
	if len(definitions) == 0 {
		return
	}
	catalog := statementCTEs{defs: definitions}
	catalog.release()
}

func (p *recursiveSQLPreparedDefinition) release() {
	if p == nil || p.transferred {
		return
	}
	anchor, recursive := p.anchor, p.recursive
	anchorStmt, recursiveStmt := p.anchorStmt, p.recursiveStmt
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
	*p = recursiveSQLPreparedDefinition{}
}

func (p *recursiveSQLPreparedDefinition) install(
	owner *Statement,
	catalog *statementCTEs,
) error {
	if p == nil || p.plan == nil || p.transferred || owner == nil ||
		catalog == nil || p.descriptor == nil || p.anchor == nil ||
		p.recursive == nil || p.anchorStmt == nil || p.recursiveStmt == nil {
		return fmt.Errorf(
			"query: recursive SQL installation has incomplete prepared state: %w",
			errStatementRecursiveDefinition,
		)
	}
	definition := catalog.find(p.plan.anchor)
	if definition == nil || definition.definition != p.plan.definition {
		return fmt.Errorf(
			"query: recursive SQL definition %q lost its owner identity: %w",
			p.plan.definition.Name, errStatementRecursiveDefinition,
		)
	}
	if err := installFullFrameRecursiveSQLDefinition(
		owner, definition, p.descriptor, p.anchor, p.recursive,
		p.anchorStmt, p.recursiveStmt, p.base,
	); err != nil {
		return err
	}
	p.transferred = true
	return nil
}

// installFullFrameRecursiveSQLDefinition is the SQL-specific ownership hook.
// Both terms intentionally view the complete owner argument frame so earlier
// CTE parameter ranges retain their original absolute ordinals. The generic
// callback installer rejects overlapping term slices; this hook instead proves
// exact full-frame identity before publishing the same runtime sidecar.
func installFullFrameRecursiveSQLDefinition(
	owner *Statement,
	definition *statementCTE,
	descriptor *RecursiveCTEDescriptor,
	anchor, recursive *RecursiveCTEStatementTerm,
	anchorStmt, recursiveStmt *Statement,
	baseCollection string,
) error {
	if owner == nil || definition == nil || descriptor == nil ||
		definition.definition == nil || definition.recursiveDefinition != nil ||
		anchor == nil || recursive == nil || anchor == recursive ||
		anchorStmt == nil || recursiveStmt == nil || anchorStmt == recursiveStmt ||
		anchor.statement != anchorStmt || recursive.statement != recursiveStmt ||
		anchor.target != nil || recursive.target == nil {
		return fmt.Errorf(
			"query: recursive SQL definition has invalid term ownership: %w",
			errStatementRecursiveDefinition,
		)
	}
	catalog := owner.cteCatalog()
	if catalog == nil || catalog.find(definition.tree) != definition ||
		descriptor.name != definition.definition.Name {
		return fmt.Errorf(
			"query: recursive SQL definition %q is not owned by its Statement: %w",
			descriptor.name, errStatementRecursiveDefinition,
		)
	}
	if len(descriptor.columns) != len(definition.names) {
		return &RecursiveCTEArityError{
			Name: descriptor.name, Term: "owning definition",
			Expected: len(definition.names), Actual: len(descriptor.columns),
		}
	}
	for ordinal := range descriptor.columns {
		if descriptor.columns[ordinal] != definition.names[ordinal] {
			return fmt.Errorf(
				"query: recursive SQL definition %q column %d is %q, descriptor has %q: %w",
				descriptor.name, ordinal, definition.names[ordinal],
				descriptor.columns[ordinal], errStatementRecursiveDefinition,
			)
		}
	}
	params := owner.NumParams()
	if anchor.paramBase != 0 || recursive.paramBase != 0 ||
		anchorStmt.NumParams() != params || recursiveStmt.NumParams() != params {
		return fmt.Errorf(
			"query: recursive SQL definition %q does not use the exact %d-value owner frame: %w",
			descriptor.name, params, errStatementRecursiveDefinition,
		)
	}
	if baseCollection == "" {
		baseCollection = anchorStmt.Collection()
	}
	if baseCollection == "" {
		return fmt.Errorf(
			"query: recursive SQL definition %q has no coherent base collection: %w",
			descriptor.name, errStatementRecursiveDefinition,
		)
	}
	prepared := &statementRecursiveDefinition{
		definition: definition, descriptor: descriptor,
		anchor: anchor, recursive: recursive,
		anchorStmt: anchorStmt, recursiveStmt: recursiveStmt,
		baseCollection: strings.Clone(baseCollection),
		params:         params, references: definition.references,
		arguments: make([]any, params),
	}
	var fusionStatement *Statement
	if definition.firstReference != nil {
		fusionStatement = definition.firstReference.owner
	}
	needsRelower := fusionStatement != nil && fusionStatement.canFuseCTE()
	definition.recursiveDefinition = prepared
	if needsRelower {
		previousPrepareMode := fusionStatement.prepareMode
		fusionStatement.prepareMode = true
		err := fusionStatement.lower(fusionStatement.args)
		fusionStatement.prepareMode = previousPrepareMode
		if err != nil {
			definition.recursiveDefinition = nil
			return err
		}
	}
	return nil
}

func cloneRecursiveSQLFullFrameTree(
	source *sqlast.SelectStmt,
	absoluteBase int,
	ownerParams int,
) (*sqlast.SelectStmt, error) {
	if source == nil || source.Set != nil || source.With != nil ||
		absoluteBase < 0 || ownerParams < 0 || source.Params < 0 ||
		absoluteBase > ownerParams || source.Params > ownerParams-absoluteBase {
		return nil, errStatementRecursiveDefinition
	}
	tree := new(sqlast.SelectStmt)
	*tree = *source
	tree.ParamBase = 0
	tree.Params = ownerParams

	if len(source.Columns) != 0 {
		tree.Columns = append([]sqlast.ResultColumn(nil), source.Columns...)
		for i := range tree.Columns {
			if source.Columns[i].Scalar != nil {
				var err error
				tree.Columns[i].Scalar, err = cloneRecursiveSQLScalar(
					source.Columns[i].Scalar, absoluteBase, ownerParams,
				)
				if err != nil {
					return nil, err
				}
			}
			if source.Columns[i].Window == nil {
				continue
			}
			window := new(sqlast.WindowExpr)
			*window = *source.Columns[i].Window
			if err := rebaseRecursiveSQLWindow(window, absoluteBase, ownerParams); err != nil {
				return nil, err
			}
			tree.Columns[i].Window = window
		}
	}
	if len(source.Windows) != 0 {
		tree.Windows = append([]sqlast.NamedWindow(nil), source.Windows...)
		for i := range tree.Windows {
			if err := rebaseRecursiveSQLWindowSpec(
				&tree.Windows[i].Spec, absoluteBase, ownerParams,
			); err != nil {
				return nil, err
			}
		}
	}
	if len(source.From) != 0 {
		tree.From = append([]sqlast.TableRef(nil), source.From...)
		for i := range tree.From {
			ref := &tree.From[i]
			if ref.Kind == sqlast.RelationDerived && ref.Query != nil {
				query, err := cloneRecursiveSQLNestedRoot(
					ref.Query, absoluteBase, ownerParams,
				)
				if err != nil {
					return nil, err
				}
				ref.Query = query
			}
			if ref.On != nil {
				condition := new(sqlast.JoinCond)
				*condition = *ref.On
				var err error
				condition.Expr, err = cloneRecursiveSQLExpr(
					ref.On.Expr, absoluteBase, ownerParams,
				)
				if err != nil {
					return nil, err
				}
				ref.On = condition
			}
		}
	}
	var err error
	tree.Where, err = cloneRecursiveSQLExpr(source.Where, absoluteBase, ownerParams)
	if err != nil {
		return nil, err
	}
	tree.Having, err = cloneRecursiveSQLExpr(source.Having, absoluteBase, ownerParams)
	if err != nil {
		return nil, err
	}
	tree.Limit, err = cloneRecursiveSQLOperand(source.Limit, absoluteBase, ownerParams)
	if err != nil {
		return nil, err
	}
	tree.Offset, err = cloneRecursiveSQLOperand(source.Offset, absoluteBase, ownerParams)
	if err != nil {
		return nil, err
	}
	return tree, nil
}

func cloneRecursiveSQLNestedRoot(
	source *sqlast.SelectStmt,
	absoluteBase int,
	ownerParams int,
) (*sqlast.SelectStmt, error) {
	if source == nil || source.ParamBase < 0 || source.Params < 0 ||
		absoluteBase > math.MaxInt-source.ParamBase {
		return nil, errStatementRecursiveDefinition
	}
	base := absoluteBase + source.ParamBase
	if base > ownerParams || source.Params > ownerParams-base {
		return nil, errStatementRecursiveDefinition
	}
	tree := new(sqlast.SelectStmt)
	*tree = *source
	tree.ParamBase = base
	return tree, nil
}

func cloneRecursiveSQLExpr(
	source *sqlast.Expr,
	absoluteBase int,
	ownerParams int,
) (*sqlast.Expr, error) {
	return cloneRecursiveSQLExprTree(source, absoluteBase, ownerParams, false)
}

func cloneRecursiveSQLExprTree(
	source *sqlast.Expr,
	absoluteBase int,
	ownerParams int,
	ownPaths bool,
) (*sqlast.Expr, error) {
	if source == nil {
		return nil, nil
	}
	expr := new(sqlast.Expr)
	*expr = *source
	if ownPaths {
		expr.Path = cloneRecursiveSQLPath(source.Path)
		expr.RightPath = cloneRecursiveSQLPath(source.RightPath)
	}
	if err := rebaseRecursiveSQLOperand(&expr.Value, absoluteBase, ownerParams); err != nil {
		return nil, err
	}
	if len(source.List) != 0 {
		expr.List = append([]sqlast.Operand(nil), source.List...)
		for i := range expr.List {
			if err := rebaseRecursiveSQLOperand(
				&expr.List[i], absoluteBase, ownerParams,
			); err != nil {
				return nil, err
			}
		}
	}
	if source.Subquery != nil {
		query, err := cloneRecursiveSQLNestedRoot(
			source.Subquery, absoluteBase, ownerParams,
		)
		if err != nil {
			return nil, err
		}
		expr.Subquery = query
	}
	if source.ScalarLeft != nil || source.ScalarRight != nil {
		var err error
		expr.ScalarLeft, err = cloneRecursiveSQLScalar(
			source.ScalarLeft, absoluteBase, ownerParams,
		)
		if err != nil {
			return nil, err
		}
		expr.ScalarRight, err = cloneRecursiveSQLScalar(
			source.ScalarRight, absoluteBase, ownerParams,
		)
		if err != nil {
			return nil, err
		}
	}
	if len(source.Kids) != 0 {
		expr.Kids = make([]*sqlast.Expr, len(source.Kids))
		for i := range source.Kids {
			kid, err := cloneRecursiveSQLExprTree(
				source.Kids[i], absoluteBase, ownerParams, ownPaths,
			)
			if err != nil {
				return nil, err
			}
			expr.Kids[i] = kid
		}
	}
	return expr, nil
}

// cloneRecursiveSQLScalar gives a full-frame recursive term exclusive
// ownership of every cold scalar node before rebasing its placeholders. CASE
// is the only scalar node with parser-arena slices and predicate edges, but the
// same walk handles CAST and arithmetic children so parameters cannot retain a
// leaf-local ordinal through either form.
func cloneRecursiveSQLScalar(
	source *sqlast.ScalarExpr,
	absoluteBase int,
	ownerParams int,
) (*sqlast.ScalarExpr, error) {
	if source == nil {
		return nil, nil
	}
	scalar := new(sqlast.ScalarExpr)
	*scalar = *source
	scalar.Path = cloneRecursiveSQLPath(source.Path)
	if err := rebaseRecursiveSQLOperand(
		&scalar.Value, absoluteBase, ownerParams,
	); err != nil {
		return nil, err
	}
	var err error
	scalar.Left, err = cloneRecursiveSQLScalar(
		source.Left, absoluteBase, ownerParams,
	)
	if err != nil {
		return nil, err
	}
	scalar.Right, err = cloneRecursiveSQLScalar(
		source.Right, absoluteBase, ownerParams,
	)
	if err != nil {
		return nil, err
	}
	if len(source.Whens) != 0 {
		scalar.Whens = append([]sqlast.ScalarWhen(nil), source.Whens...)
		for i := range scalar.Whens {
			arm := &scalar.Whens[i]
			arm.Predicate, err = cloneRecursiveSQLExprTree(
				source.Whens[i].Predicate, absoluteBase, ownerParams, true,
			)
			if err != nil {
				return nil, err
			}
			arm.Match, err = cloneRecursiveSQLScalar(
				source.Whens[i].Match, absoluteBase, ownerParams,
			)
			if err != nil {
				return nil, err
			}
			arm.Result, err = cloneRecursiveSQLScalar(
				source.Whens[i].Result, absoluteBase, ownerParams,
			)
			if err != nil {
				return nil, err
			}
		}
	}
	scalar.Else, err = cloneRecursiveSQLScalar(
		source.Else, absoluteBase, ownerParams,
	)
	if err != nil {
		return nil, err
	}
	return scalar, nil
}

func cloneRecursiveSQLPath(source *sqlast.PathExpr) *sqlast.PathExpr {
	if source == nil {
		return nil
	}
	path := new(sqlast.PathExpr)
	*path = *source
	if len(source.Segments) != 0 {
		path.Segments = append([]sqlast.Segment(nil), source.Segments...)
	}
	return path
}

func cloneRecursiveSQLOperand(
	source *sqlast.Operand,
	absoluteBase int,
	ownerParams int,
) (*sqlast.Operand, error) {
	if source == nil {
		return nil, nil
	}
	operand := new(sqlast.Operand)
	*operand = *source
	if err := rebaseRecursiveSQLOperand(operand, absoluteBase, ownerParams); err != nil {
		return nil, err
	}
	return operand, nil
}

func rebaseRecursiveSQLOperand(
	operand *sqlast.Operand,
	absoluteBase int,
	ownerParams int,
) error {
	if operand == nil || operand.Kind != sqlast.OperandParam {
		return nil
	}
	if operand.Ordinal < 0 || absoluteBase < 0 ||
		absoluteBase > math.MaxInt-operand.Ordinal {
		return errStatementRecursiveDefinition
	}
	ordinal := absoluteBase + operand.Ordinal
	if ordinal < 0 || ordinal >= ownerParams {
		return errStatementRecursiveDefinition
	}
	operand.Ordinal = ordinal
	return nil
}

func rebaseRecursiveSQLWindow(
	window *sqlast.WindowExpr,
	absoluteBase int,
	ownerParams int,
) error {
	if window == nil {
		return nil
	}
	for _, operand := range []*sqlast.Operand{
		&window.Offset, &window.Buckets, &window.Nth, &window.Default,
	} {
		if err := rebaseRecursiveSQLOperand(operand, absoluteBase, ownerParams); err != nil {
			return err
		}
	}
	return rebaseRecursiveSQLWindowSpec(&window.Spec, absoluteBase, ownerParams)
}

func rebaseRecursiveSQLWindowSpec(
	spec *sqlast.WindowSpec,
	absoluteBase int,
	ownerParams int,
) error {
	if spec == nil || !spec.Frame.Explicit {
		return nil
	}
	if err := rebaseRecursiveSQLOperand(
		&spec.Frame.Start.Offset, absoluteBase, ownerParams,
	); err != nil {
		return err
	}
	return rebaseRecursiveSQLOperand(
		&spec.Frame.End.Offset, absoluteBase, ownerParams,
	)
}

func recursiveSQLFullFrameTermTree(
	plan *recursiveSQLDefinitionPlan,
	names []string,
	ownerParams int,
) (*sqlast.SelectStmt, string, error) {
	if plan == nil || plan.term == nil || plan.anchor == nil {
		return nil, "", errStatementRecursiveDefinition
	}
	tree, err := cloneRecursiveSQLFullFrameTree(
		plan.term, plan.absoluteTermBase, ownerParams,
	)
	if err != nil {
		return nil, "", err
	}
	self := -1
	for i := range tree.From {
		ref := &tree.From[i]
		if ref.Kind == sqlast.RelationCTE && ref.Name == plan.definition.Name &&
			ref.Query == plan.anchor {
			if self >= 0 {
				return nil, "", errStatementRecursiveDefinition
			}
			self = i
		}
	}
	if self < 0 {
		return nil, "", errStatementRecursiveDefinition
	}
	driving, ok := recursiveSQLPhysicalSource(plan.term, plan.anchor, 0)
	if !ok {
		driving, ok = recursiveSQLPhysicalSource(plan.anchor, plan.anchor, 0)
	}
	if !ok {
		return nil, "", fmt.Errorf(
			"recursive definition %q has no physical source",
			plan.definition.Name,
		)
	}
	driving.Join = sqlast.JoinNone
	driving.On = nil
	driving.Lateral = nil
	path := &sqlast.PathExpr{Source: 0, Pos: driving.Pos}
	columns := make([]sqlast.ResultColumn, len(names))
	for i := range columns {
		columns[i] = sqlast.ResultColumn{Path: path, Alias: names[i], Pos: driving.Pos}
	}
	placeholder := &sqlast.SelectStmt{
		Columns: columns, From: []sqlast.TableRef{driving},
	}
	name := "\x00recursive-delta:" + strconv.Itoa(plan.index)
	definition := []sqlast.CommonTableExpr{{
		Name: name, Columns: append([]string(nil), names...), Query: placeholder,
		Pos: plan.definition.Pos, HintPos: -1,
	}}
	tree.With = &sqlast.WithClause{CTEs: definition, Pos: recursiveSQLSelectPos(tree)}
	tree.From[self].Name = name
	tree.From[self].Query = placeholder
	return tree, name, nil
}

func recursiveSQLPhysicalSource(
	tree *sqlast.SelectStmt,
	self *sqlast.SelectStmt,
	depth int,
) (sqlast.TableRef, bool) {
	if tree == nil || depth > 2*1024 {
		return sqlast.TableRef{}, false
	}
	if tree.Set != nil {
		return recursiveSQLPhysicalSetSource(tree.Set.Root, self, depth+1)
	}
	for i := range tree.From {
		ref := &tree.From[i]
		switch ref.Kind {
		case sqlast.RelationCollection:
			if ref.UnresolvedCTE.Kind == sqlast.CTEReferenceNone {
				return *ref, true
			}
		case sqlast.RelationDerived:
			if physical, ok := recursiveSQLPhysicalSource(ref.Query, self, depth+1); ok {
				return physical, true
			}
		case sqlast.RelationCTE:
			if ref.Query != self {
				if physical, ok := recursiveSQLPhysicalSource(ref.Query, self, depth+1); ok {
					return physical, true
				}
			}
		}
	}
	if physical, ok := recursiveSQLPhysicalExprSource(tree.Where, self, depth+1); ok {
		return physical, true
	}
	return recursiveSQLPhysicalExprSource(tree.Having, self, depth+1)
}

func recursiveSQLPhysicalSetSource(
	expr *sqlast.SetExpr,
	self *sqlast.SelectStmt,
	depth int,
) (sqlast.TableRef, bool) {
	if expr == nil {
		return sqlast.TableRef{}, false
	}
	switch expr.Kind {
	case sqlast.SetSelectExpr:
		return recursiveSQLPhysicalSource(expr.Select, self, depth+1)
	case sqlast.SetBinaryExpr:
		if physical, ok := recursiveSQLPhysicalSetSource(expr.Left, self, depth+1); ok {
			return physical, true
		}
		return recursiveSQLPhysicalSetSource(expr.Right, self, depth+1)
	case sqlast.SetGroupExpr:
		return recursiveSQLPhysicalSetSource(expr.Child, self, depth+1)
	default:
		return sqlast.TableRef{}, false
	}
}

func recursiveSQLPhysicalExprSource(
	expr *sqlast.Expr,
	self *sqlast.SelectStmt,
	depth int,
) (sqlast.TableRef, bool) {
	if expr == nil {
		return sqlast.TableRef{}, false
	}
	if expr.Subquery != nil {
		if physical, ok := recursiveSQLPhysicalSource(expr.Subquery, self, depth+1); ok {
			return physical, true
		}
	}
	for _, child := range expr.Kids {
		if physical, ok := recursiveSQLPhysicalExprSource(child, self, depth+1); ok {
			return physical, true
		}
	}
	return sqlast.TableRef{}, false
}
