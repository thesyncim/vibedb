package sql

import (
	"fmt"
	"strings"
)

// recursiveCTEClauseExtension recognizes the standard SEARCH/CYCLE clauses
// without making either word globally reserved. They remain usable as JSON
// field and collection identifiers everywhere the grammar is unambiguous.
func recursiveCTEClauseExtension(tok token) string {
	if tok.kind != tokIdent {
		return ""
	}
	switch {
	case strings.EqualFold(tok.text, "search"):
		return "SEARCH"
	case strings.EqualFold(tok.text, "cycle"):
		return "CYCLE"
	default:
		return ""
	}
}

func validateRecursiveCTEDefinitions(
	src string,
	definitions []CommonTableExpr,
) error {
	// Forward references are mutual-recursion candidates in this grammar. The
	// physical bridge deliberately supports only one self-recursive relation at
	// a time, so reject them before classifying any definition as ordinary.
	for i := range definitions {
		if ref := findRecursiveCTEReference(
			definitions[i].Query, CTEReferenceForward,
		); ref != nil {
			return newFeatureNotSupportedError(
				src, ref.Pos,
				fmt.Sprintf(
					"recursive common table expression %q references later definition %q; mutual or forward recursion is not supported",
					definitions[i].Name, ref.Name,
				),
			)
		}
	}

	for i := range definitions {
		definition := &definitions[i]
		self := findRecursiveCTEReference(
			definition.Query, CTEReferenceSelf,
		)
		if self == nil {
			continue
		}
		if err := classifyRecursiveCTEDefinition(src, definition, self); err != nil {
			return err
		}
	}
	return nil
}

func classifyRecursiveCTEDefinition(
	src string,
	definition *CommonTableExpr,
	firstSelf *TableRef,
) error {
	query := definition.Query
	if query == nil || query.Set == nil || query.Set.Root == nil {
		return unsupportedRecursiveCTEShape(
			src, firstSelf.Pos, definition.Name,
			"requires an anchor SELECT followed by UNION or UNION ALL and one recursive SELECT",
		)
	}
	if query.Set.Tail != nil {
		return unsupportedRecursiveCTEShape(
			src, query.Set.Tail.Pos, definition.Name,
			"cannot apply ORDER BY, LIMIT, or OFFSET to the complete recursive UNION",
		)
	}
	root := query.Set.Root
	if root.Kind != SetBinaryExpr ||
		(root.Operation != SetUnionAll && root.Operation != SetUnionDistinct) {
		return unsupportedRecursiveCTEShape(
			src, root.Pos, definition.Name,
			"must use UNION or UNION ALL between its anchor and recursive term",
		)
	}
	anchor, anchorPos, ok := recursiveCTESetLeaf(root.Left)
	if !ok {
		return unsupportedRecursiveCTEShape(
			src, anchorPos, definition.Name,
			"anchor must be exactly one SELECT without an operand-local set tail",
		)
	}
	term, termPos, ok := recursiveCTESetLeaf(root.Right)
	if !ok {
		return unsupportedRecursiveCTEShape(
			src, termPos, definition.Name,
			"recursive term must be exactly one SELECT without an operand-local set tail",
		)
	}

	anchorRefs := scanRecursiveCTESelf(anchor)
	if anchorRefs.count != 0 {
		return unsupportedRecursiveCTEShape(
			src, anchorRefs.first.Pos, definition.Name,
			"anchor term must not reference itself",
		)
	}
	termRefs := scanRecursiveCTESelf(term)
	if termRefs.count == 0 {
		return unsupportedRecursiveCTEShape(
			src, firstSelf.Pos, definition.Name,
			"recursive term must contain exactly one direct self-reference",
		)
	}
	if termRefs.count > 1 {
		return unsupportedRecursiveCTEShape(
			src, termRefs.second.Pos, definition.Name,
			"recursive term contains more than one self-reference",
		)
	}
	if termRefs.direct == nil {
		return unsupportedRecursiveCTEShape(
			src, termRefs.first.Pos, definition.Name,
			"self-reference must be a direct FROM/JOIN item of the recursive SELECT",
		)
	}
	if pos, reason := validateRecursiveCTETermShape(term, termRefs.direct); reason != "" {
		return unsupportedRecursiveCTEShape(
			src, pos, definition.Name, reason,
		)
	}

	// Publish stable recursive-relation identity only after every refusal above
	// has passed. A failed parse therefore never exposes a partially rebound AST.
	termRefs.direct.Kind = RelationCTE
	termRefs.direct.Query = anchor
	termRefs.direct.UnresolvedCTE = CTEReferenceMetadata{}
	definition.Recursive = RecursiveCTE{
		Anchor: anchor, Term: term, Operation: root.Operation,
	}
	return nil
}

func validateRecursiveCTETermShape(
	term *SelectStmt,
	self *TableRef,
) (int, string) {
	if term == nil || self == nil {
		return 0, "has an invalid recursive term"
	}
	if term.Distinct {
		return recursiveCTESelectPos(term),
			"cannot use SELECT DISTINCT inside the recursive term; use UNION distinct for fixpoint duplicate elimination"
	}
	for i := range term.Columns {
		if term.Columns[i].Agg != AggNone {
			return term.Columns[i].Pos,
				"cannot use aggregate functions inside the recursive term"
		}
		if term.Columns[i].Window != nil {
			return term.Columns[i].Pos,
				"cannot use window functions inside the recursive term"
		}
	}
	if len(term.GroupBy) != 0 {
		return term.GroupBy[0].Pos,
			"cannot use GROUP BY inside the recursive term"
	}
	if term.Having != nil {
		return term.Having.Pos,
			"cannot use HAVING inside the recursive term"
	}
	if len(term.Windows) != 0 {
		return term.Windows[0].Pos,
			"cannot declare windows inside the recursive term"
	}
	if len(term.OrderBy) != 0 {
		return term.OrderBy[0].Pos,
			"cannot use operand-local ORDER BY inside the recursive term"
	}
	if term.Limit != nil {
		return term.Limit.Pos,
			"cannot use operand-local LIMIT inside the recursive term"
	}
	if term.Offset != nil {
		return term.Offset.Pos,
			"cannot use operand-local OFFSET inside the recursive term"
	}

	selfIndex := -1
	for i := range term.From {
		if &term.From[i] == self {
			selfIndex = i
			break
		}
	}
	if selfIndex < 0 {
		return self.Pos,
			"must bind its self-reference directly in the recursive SELECT"
	}
	// In the left-deep join tree, LEFT makes only the new right operand
	// nullable, while RIGHT makes the accumulated left input nullable. FULL
	// always makes the recursive relation nullable. Preserved-side outer joins
	// remain monotone over the previous breadth-first delta.
	if selfIndex > 0 {
		switch term.From[selfIndex].Join {
		case JoinLeft, JoinFull:
			return term.From[selfIndex].Pos,
				"cannot place its self-reference on the nullable side of an outer join"
		}
	}
	for i := selfIndex + 1; i < len(term.From); i++ {
		switch term.From[i].Join {
		case JoinRight, JoinFull:
			return term.From[i].Pos,
				"cannot make its self-reference nullable through a later outer join"
		}
	}
	return 0, ""
}

func recursiveCTESelectPos(tree *SelectStmt) int {
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

func unsupportedRecursiveCTEShape(
	src string,
	pos int,
	name string,
	reason string,
) error {
	return newFeatureNotSupportedError(
		src, pos,
		fmt.Sprintf("recursive common table expression %q %s", name, reason),
	)
}

func recursiveCTESetLeaf(expr *SetExpr) (*SelectStmt, int, bool) {
	if expr == nil {
		return nil, 0, false
	}
	pos := expr.Pos
	for expr.Kind == SetGroupExpr {
		if expr.Tail != nil || expr.Child == nil {
			return nil, pos, false
		}
		expr = expr.Child
	}
	if expr.Kind != SetSelectExpr || expr.Select == nil {
		return nil, expr.Pos, false
	}
	return expr.Select, expr.Pos, true
}

type recursiveCTESelfScan struct {
	count  int
	first  *TableRef
	second *TableRef
	direct *TableRef
}

func scanRecursiveCTESelf(query *SelectStmt) recursiveCTESelfScan {
	var scan recursiveCTESelfScan
	scanRecursiveCTESelect(query, true, CTEReferenceSelf, &scan)
	return scan
}

func findRecursiveCTEReference(
	query *SelectStmt,
	kind CTEReferenceKind,
) *TableRef {
	var scan recursiveCTESelfScan
	if query != nil && query.Set != nil {
		scanRecursiveCTESet(query.Set.Root, kind, &scan)
	} else {
		scanRecursiveCTESelect(query, true, kind, &scan)
	}
	return scan.first
}

func scanRecursiveCTESet(
	expr *SetExpr,
	kind CTEReferenceKind,
	scan *recursiveCTESelfScan,
) {
	if expr == nil {
		return
	}
	switch expr.Kind {
	case SetSelectExpr:
		scanRecursiveCTESelect(expr.Select, true, kind, scan)
	case SetBinaryExpr:
		scanRecursiveCTESet(expr.Left, kind, scan)
		scanRecursiveCTESet(expr.Right, kind, scan)
	case SetGroupExpr:
		scanRecursiveCTESet(expr.Child, kind, scan)
	}
}

func scanRecursiveCTESelect(
	query *SelectStmt,
	direct bool,
	kind CTEReferenceKind,
	scan *recursiveCTESelfScan,
) {
	if query == nil || scan == nil {
		return
	}
	if query.Set != nil {
		scanRecursiveCTESet(query.Set.Root, kind, scan)
		return
	}
	if query.With != nil {
		for i := range query.With.CTEs {
			scanRecursiveCTESelect(
				query.With.CTEs[i].Query, false, kind, scan,
			)
		}
	}
	for i := range query.From {
		ref := &query.From[i]
		if ref.Kind == RelationCollection && ref.UnresolvedCTE.Kind == kind {
			scan.count++
			if scan.first == nil {
				scan.first = ref
			} else if scan.second == nil {
				scan.second = ref
			}
			if direct && scan.direct == nil {
				scan.direct = ref
			}
		}
		if ref.Kind == RelationDerived {
			scanRecursiveCTESelect(ref.Query, false, kind, scan)
		}
	}
	scanRecursiveCTEExpr(query.Where, kind, scan)
	scanRecursiveCTEExpr(query.Having, kind, scan)
}

func scanRecursiveCTEExpr(
	expr *Expr,
	kind CTEReferenceKind,
	scan *recursiveCTESelfScan,
) {
	if expr == nil {
		return
	}
	if expr.Subquery != nil {
		scanRecursiveCTESelect(expr.Subquery, false, kind, scan)
	}
	for _, child := range expr.Kids {
		scanRecursiveCTEExpr(child, kind, scan)
	}
}
