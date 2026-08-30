package query

import (
	"fmt"
	"math"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// RecursiveSQLStatementOptions supplies physical limits for every recursive
// definition in one SQL statement. Zero values select RecursiveFixpoint's
// finite defaults; -1 disables the corresponding limit.
type RecursiveSQLStatementOptions struct {
	Limits RecursiveCTELimits
	// preserveDocumentUnknown is set only by INSERT ... SELECT preparation. The
	// recursive bridge prepares detached anchor/term statements before document
	// lineage can be marked, so they must inherit that cold analysis context.
	preserveDocumentUnknown bool
	parameterTypes          []ParameterType
}

// RecursiveSQLStatementRequired is the zero-allocation detector shared prepare
// dispatchers call before ordinary CTE/set lowering. pos is the first recursive
// definition's byte offset when required is true. Keeping detection here lets
// the set lowerer connect this bridge without duplicating recursive AST rules
// or importing publication internals.
func RecursiveSQLStatementRequired(
	tree *sqlast.SelectStmt,
) (required bool, pos int) {
	definition := findNestedRecursiveSQLDefinition(tree, nil)
	if definition == nil {
		return false, 0
	}
	return true, definition.Pos
}

// PrepareRecursiveSQLStatement is the isolated WITH RECURSIVE prepare bridge.
// The ordinary prepare dispatcher calls RecursiveSQLStatementRequired before
// set lowering and enters this hook for a supported top-level recursive SELECT.
// Keeping construction here leaves set and recursive ownership independently
// testable and gives nested/compound positions one typed refusal boundary.
func PrepareRecursiveSQLStatement(
	src string,
	options RecursiveSQLStatementOptions,
) (*Statement, error) {
	tree, err := sqlast.Parse(src)
	if err != nil {
		return nil, err
	}
	return PrepareParsedRecursiveSQLStatement(src, tree, options)
}

// PrepareParsedRecursiveSQLStatement prepares a validated parser tree through
// the owning recursive-definition publication bridge. It mutates no caller AST
// state after return, including on failure. The tree and its Parser storage must
// obey the same lifetime contract as PrepareParsedStatement.
func PrepareParsedRecursiveSQLStatement(
	src string,
	tree *sqlast.SelectStmt,
	options RecursiveSQLStatementOptions,
) (_ *Statement, err error) {
	if tree == nil {
		return nil, fmt.Errorf("query: recursive SQL prepare received a nil SELECT")
	}
	if tree.Set != nil {
		return nil, sqlast.NewFeatureNotSupportedError(
			src, tree.Set.Pos,
			"a top-level compound query containing WITH RECURSIVE awaits the general set-statement lowerer",
		)
	}
	plans, err := planRecursiveSQLDefinitions(src, tree)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return prepareTreeInContext(
			src, tree, 0, nil, 0, options.parameterTypes,
			unknownOutputPrepareMode{preserveDocument: options.preserveDocumentUnknown},
		)
	}
	// The ordinary owner prepare below temporarily substitutes recursive bodies
	// with their anchors. Capture the complete authored reachability first so a
	// physical relation used only by a recursive term is not lost from the
	// source contract.
	requiresCatalog := selectRequiresCatalog(tree)

	var references []recursiveSQLReferenceRewrite
	for i := range plans {
		plan := &plans[i]
		rewriteRecursiveSQLReferences(
			tree, plan.authored, plan.anchor, &references,
		)
		plan.definition.Query = plan.anchor
		plan.anchor.ParamBase = plan.absoluteAnchorBase
	}
	defer func() {
		for i := len(references) - 1; i >= 0; i-- {
			references[i].ref.Query = references[i].query
		}
		for i := range plans {
			plans[i].definition.Query = plans[i].authored
			plans[i].anchor.ParamBase = plans[i].relativeAnchorBase
		}
	}()

	owner, err := prepareTreeInContext(
		src, tree, 0, nil, 0, options.parameterTypes,
		unknownOutputPrepareMode{preserveDocument: options.preserveDocumentUnknown},
	)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			owner.Release()
		}
	}()

	catalog := owner.cteCatalog()
	prepared := make([]recursiveSQLPreparedDefinition, 0, len(plans))
	defer func() {
		for i := range prepared {
			if !prepared[i].transferred {
				prepared[i].release()
			}
		}
	}()
	for i := range plans {
		definition, prepareErr := prepareRecursiveSQLDefinition(
			src, owner, catalog, &plans[i], options,
		)
		if prepareErr != nil {
			return nil, prepareErr
		}
		prepared = append(prepared, definition)
		// Detached recursive terms use the owner's complete parameter frame.
		// Merge their analyzed domains back before publication so protocol
		// metadata sees parameters used only by the recursive arm and the usual
		// Statement conflict path retains the later authored position. Both
		// child vectors stay nil for an untyped recursive query, so this adds no
		// sidecar allocation to that path.
		current := &prepared[len(prepared)-1]
		if err := owner.mergeChildParameterTypes(current.anchorStmt); err != nil {
			return nil, err
		}
		if err := owner.mergeChildParameterTypes(current.recursiveStmt); err != nil {
			return nil, err
		}
	}
	// Preparing every term first makes reference counts final before any
	// recursive publication snapshots them. This is essential when a later
	// legal definition reads an earlier recursive definition.
	for i := range prepared {
		if err := prepared[i].install(owner, catalog); err != nil {
			return nil, err
		}
	}
	owner.requiresCatalog = owner.requiresCatalog || requiresCatalog
	committed = true
	return owner, nil
}

type recursiveSQLDefinitionPlan struct {
	definition *sqlast.CommonTableExpr
	index      int
	authored   *sqlast.SelectStmt
	anchor     *sqlast.SelectStmt
	term       *sqlast.SelectStmt
	operation  sqlast.SetOperation

	relativeAnchorBase int
	absoluteAnchorBase int
	absoluteTermBase   int
}

func planRecursiveSQLDefinitions(
	src string,
	tree *sqlast.SelectStmt,
) ([]recursiveSQLDefinitionPlan, error) {
	if nested := findNestedRecursiveSQLDefinition(tree, tree.With); nested != nil {
		return nil, sqlast.NewFeatureNotSupportedError(
			src, nested.Pos,
			"nested WITH RECURSIVE preparation is not connected to the isolated owning-Statement hook yet",
		)
	}
	if tree.With == nil {
		return nil, nil
	}
	plans := make([]recursiveSQLDefinitionPlan, 0, len(tree.With.CTEs))
	for i := range tree.With.CTEs {
		definition := &tree.With.CTEs[i]
		recursive := definition.Recursive
		if recursive.Anchor == nil {
			continue
		}
		if recursive.Term == nil || definition.Query == nil ||
			(recursive.Operation != sqlast.SetUnionAll &&
				recursive.Operation != sqlast.SetUnionDistinct) {
			return nil, sqlast.NewFeatureNotSupportedError(
				src, definition.Pos,
				"recursive CTE metadata is incomplete or was not produced by the SQL parser",
			)
		}
		if len(recursive.Anchor.From) == 0 {
			return nil, sqlast.NewFeatureNotSupportedError(
				src, recursiveSQLSelectPos(recursive.Anchor),
				"a recursive anchor must have a row source",
			)
		}
		if ref := unsupportedRecursiveSQLTermReference(
			recursive.Anchor, definition.Name, recursive.Anchor,
			tree.With.CTEs, i,
		); ref != nil {
			return nil, sqlast.NewFeatureNotSupportedError(
				src, ref.Pos,
				"a recursive anchor may read only preceding definitions from its lexical WITH scope",
			)
		}
		if ref := unsupportedRecursiveSQLTermReference(
			recursive.Term, definition.Name, recursive.Anchor,
			tree.With.CTEs, i,
		); ref != nil {
			return nil, sqlast.NewFeatureNotSupportedError(
				src, ref.Pos,
				"a recursive term may read only its direct delta and preceding definitions from its lexical WITH scope",
			)
		}
		anchorBase, err := recursiveSQLParamBase(
			definition.Query.ParamBase, recursive.Anchor.ParamBase,
			recursive.Anchor.Params, tree.Params,
		)
		if err != nil {
			return nil, sqlast.NewFeatureNotSupportedError(
				src, recursiveSQLSelectPos(recursive.Anchor),
				"recursive anchor placeholder range overflows its owning statement",
			)
		}
		termBase, err := recursiveSQLParamBase(
			definition.Query.ParamBase, recursive.Term.ParamBase,
			recursive.Term.Params, tree.Params,
		)
		if err != nil {
			return nil, sqlast.NewFeatureNotSupportedError(
				src, recursiveSQLSelectPos(recursive.Term),
				"recursive term placeholder range overflows its owning statement",
			)
		}
		plans = append(plans, recursiveSQLDefinitionPlan{
			definition: definition, index: i, authored: definition.Query,
			anchor: recursive.Anchor, term: recursive.Term,
			operation:          recursive.Operation,
			relativeAnchorBase: recursive.Anchor.ParamBase,
			absoluteAnchorBase: anchorBase,
			absoluteTermBase:   termBase,
		})
	}
	return plans, nil
}

// Pos returns the first stable source position available for a SELECT. Keeping
// this helper local avoids adding behavior to the plain SQL AST structs.
func recursiveSQLSelectPos(tree *sqlast.SelectStmt) int {
	if tree == nil {
		return 0
	}
	if tree.Set != nil {
		return tree.Set.Pos
	}
	if tree.With != nil {
		return tree.With.Pos
	}
	if len(tree.Columns) != 0 {
		return tree.Columns[0].Pos
	}
	if len(tree.From) != 0 {
		return tree.From[0].Pos
	}
	return 0
}

func recursiveSQLParamBase(
	bodyBase int,
	leafBase int,
	leafParams int,
	ownerParams int,
) (int, error) {
	if bodyBase < 0 || leafBase < 0 || leafParams < 0 || ownerParams < 0 ||
		bodyBase > math.MaxInt-leafBase {
		return 0, errStatementRecursiveDefinition
	}
	base := bodyBase + leafBase
	if base > ownerParams || leafParams > ownerParams-base {
		return 0, errStatementRecursiveDefinition
	}
	return base, nil
}

func unsupportedRecursiveSQLTermReference(
	tree *sqlast.SelectStmt,
	selfName string,
	selfQuery *sqlast.SelectStmt,
	definitions []sqlast.CommonTableExpr,
	definitionIndex int,
) *sqlast.TableRef {
	if tree == nil {
		return nil
	}
	if tree.With != nil {
		return &sqlast.TableRef{Pos: tree.With.Pos}
	}
	for i := range tree.From {
		ref := &tree.From[i]
		if ref.Kind == sqlast.RelationCTE {
			if ref.Name == selfName && ref.Query == selfQuery {
				continue
			}
			dependency := recursiveSQLDefinitionIndex(definitions, ref.Query)
			if dependency < 0 || dependency >= definitionIndex {
				return ref
			}
		}
		if ref.Kind == sqlast.RelationDerived {
			if nested := unsupportedRecursiveSQLTermReference(
				ref.Query, selfName, selfQuery, definitions, definitionIndex,
			); nested != nil {
				return nested
			}
		}
	}
	if ref := unsupportedRecursiveSQLExprReference(
		tree.Where, selfName, selfQuery, definitions, definitionIndex,
	); ref != nil {
		return ref
	}
	return unsupportedRecursiveSQLExprReference(
		tree.Having, selfName, selfQuery, definitions, definitionIndex,
	)
}

func recursiveSQLDefinitionIndex(
	definitions []sqlast.CommonTableExpr,
	query *sqlast.SelectStmt,
) int {
	for i := range definitions {
		definition := &definitions[i]
		if definition.Query == query || definition.Recursive.Anchor == query {
			return i
		}
	}
	return -1
}

func unsupportedRecursiveSQLExprReference(
	expr *sqlast.Expr,
	selfName string,
	selfQuery *sqlast.SelectStmt,
	definitions []sqlast.CommonTableExpr,
	definitionIndex int,
) *sqlast.TableRef {
	if expr == nil {
		return nil
	}
	if expr.Subquery != nil {
		if ref := unsupportedRecursiveSQLTermReference(
			expr.Subquery, selfName, selfQuery, definitions, definitionIndex,
		); ref != nil {
			return ref
		}
	}
	for _, child := range expr.Kids {
		if ref := unsupportedRecursiveSQLExprReference(
			child, selfName, selfQuery, definitions, definitionIndex,
		); ref != nil {
			return ref
		}
	}
	return nil
}

type recursiveSQLReferenceRewrite struct {
	ref   *sqlast.TableRef
	query *sqlast.SelectStmt
}

func rewriteRecursiveSQLReferences(
	tree *sqlast.SelectStmt,
	from *sqlast.SelectStmt,
	to *sqlast.SelectStmt,
	journal *[]recursiveSQLReferenceRewrite,
) {
	if tree == nil {
		return
	}
	if tree.Set != nil {
		rewriteRecursiveSQLSet(tree.Set.Root, from, to, journal)
		return
	}
	if tree.With != nil {
		for i := range tree.With.CTEs {
			rewriteRecursiveSQLReferences(
				tree.With.CTEs[i].Query, from, to, journal,
			)
		}
	}
	for i := range tree.From {
		ref := &tree.From[i]
		if ref.Kind == sqlast.RelationCTE && ref.Query == from {
			*journal = append(*journal, recursiveSQLReferenceRewrite{
				ref: ref, query: from,
			})
			ref.Query = to
		}
		if ref.Kind == sqlast.RelationDerived {
			rewriteRecursiveSQLReferences(ref.Query, from, to, journal)
		}
	}
	rewriteRecursiveSQLExpr(tree.Where, from, to, journal)
	rewriteRecursiveSQLExpr(tree.Having, from, to, journal)
}

func rewriteRecursiveSQLSet(
	expr *sqlast.SetExpr,
	from *sqlast.SelectStmt,
	to *sqlast.SelectStmt,
	journal *[]recursiveSQLReferenceRewrite,
) {
	if expr == nil {
		return
	}
	switch expr.Kind {
	case sqlast.SetSelectExpr:
		rewriteRecursiveSQLReferences(expr.Select, from, to, journal)
	case sqlast.SetBinaryExpr:
		rewriteRecursiveSQLSet(expr.Left, from, to, journal)
		rewriteRecursiveSQLSet(expr.Right, from, to, journal)
	case sqlast.SetGroupExpr:
		rewriteRecursiveSQLSet(expr.Child, from, to, journal)
	}
}

func rewriteRecursiveSQLExpr(
	expr *sqlast.Expr,
	from *sqlast.SelectStmt,
	to *sqlast.SelectStmt,
	journal *[]recursiveSQLReferenceRewrite,
) {
	if expr == nil {
		return
	}
	if expr.Subquery != nil {
		rewriteRecursiveSQLReferences(expr.Subquery, from, to, journal)
	}
	for _, child := range expr.Kids {
		rewriteRecursiveSQLExpr(child, from, to, journal)
	}
}

func findNestedRecursiveSQLDefinition(
	tree *sqlast.SelectStmt,
	top *sqlast.WithClause,
) *sqlast.CommonTableExpr {
	if tree == nil {
		return nil
	}
	if tree.Set != nil {
		return findNestedRecursiveSQLSet(tree.Set.Root, top)
	}
	if tree.With != nil {
		for i := range tree.With.CTEs {
			definition := &tree.With.CTEs[i]
			if definition.Recursive.Anchor != nil && tree.With != top {
				return definition
			}
			if nested := findNestedRecursiveSQLDefinition(
				definition.Query, top,
			); nested != nil {
				return nested
			}
		}
	}
	for i := range tree.From {
		if tree.From[i].Kind == sqlast.RelationDerived {
			if nested := findNestedRecursiveSQLDefinition(
				tree.From[i].Query, top,
			); nested != nil {
				return nested
			}
		}
	}
	if nested := findNestedRecursiveSQLExpr(tree.Where, top); nested != nil {
		return nested
	}
	return findNestedRecursiveSQLExpr(tree.Having, top)
}

func findNestedRecursiveSQLSet(
	expr *sqlast.SetExpr,
	top *sqlast.WithClause,
) *sqlast.CommonTableExpr {
	if expr == nil {
		return nil
	}
	switch expr.Kind {
	case sqlast.SetSelectExpr:
		return findNestedRecursiveSQLDefinition(expr.Select, top)
	case sqlast.SetBinaryExpr:
		if nested := findNestedRecursiveSQLSet(expr.Left, top); nested != nil {
			return nested
		}
		return findNestedRecursiveSQLSet(expr.Right, top)
	case sqlast.SetGroupExpr:
		return findNestedRecursiveSQLSet(expr.Child, top)
	default:
		return nil
	}
}

func findNestedRecursiveSQLExpr(
	expr *sqlast.Expr,
	top *sqlast.WithClause,
) *sqlast.CommonTableExpr {
	if expr == nil {
		return nil
	}
	if expr.Subquery != nil {
		if nested := findNestedRecursiveSQLDefinition(
			expr.Subquery, top,
		); nested != nil {
			return nested
		}
	}
	for _, child := range expr.Kids {
		if nested := findNestedRecursiveSQLExpr(child, top); nested != nil {
			return nested
		}
	}
	return nil
}
