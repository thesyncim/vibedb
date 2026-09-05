package query

import (
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

// scalarCaseDomain is the SQL type-domain CASE resolves at prepare. Dynamic is
// a schemaless dependency or placeholder; Null is neutral during unification.
// JSON is distinct from Dynamic even though both expose TypeAny metadata.
type scalarCaseDomain uint8

const (
	caseDomainDynamic scalarCaseDomain = iota
	caseDomainNull
	caseDomainBoolean
	caseDomainNumeric
	caseDomainText
	caseDomainJSON
)

type statementScalarRange struct {
	start int32
	end   int32
	root  int32
}

type statementScalarCaseArm struct {
	condition int32
	match     statementScalarRange
	result    statementScalarRange
	matchDom  scalarCaseDomain
	resultDom scalarCaseDomain
	matchPos  int
	resultPos int
}

type statementScalarCase struct {
	simple      bool
	selector    statementScalarRange
	selectorDom scalarCaseDomain
	selectorPos int
	simpleDom   scalarCaseDomain
	armStart    int32
	armCount    int32
	fallback    statementScalarRange
	fallbackDom scalarCaseDomain
	fallbackPos int
	domain      scalarCaseDomain
}

func (r *statementScalar) compileCase(s *Statement, expr *sqlast.ScalarExpr) (int32, error) {
	if len(expr.Whens) == 0 {
		return 0, fmt.Errorf("query: scalar CASE has no WHEN arms")
	}
	if err := validateTypedCaseResultDomains(s, expr); err != nil {
		return 0, err
	}
	root := int32(len(r.nodes))
	caseIndex := int32(len(r.cases))
	r.cases = append(r.cases, statementScalarCase{})
	armStart := int32(len(r.caseArms))
	r.caseArms = append(r.caseArms, make([]statementScalarCaseArm, len(expr.Whens))...)
	r.nodes = append(r.nodes, statementScalarNode{
		kind: statementScalarCaseNode, left: -1, right: -1,
		caseIndex: caseIndex, skip: -1, pos: expr.Pos,
	})
	program := statementScalarCase{
		simple: expr.Left != nil, armStart: armStart,
		fallback: statementScalarRange{root: -1},
	}
	if program.simple {
		var err error
		program.selector, err = r.compileCaseRange(s, expr.Left)
		if err != nil {
			return 0, err
		}
		program.selectorDom = r.nodeDomain(program.selector.root)
		program.selectorPos = expr.Left.Pos
	}
	simpleSelector, simpleSelectorStatic := staticScalarCaseValue(expr.Left)

	domain := caseDomainNull
	simpleDomain := program.selectorDom
	searchedTerminated := false
	// A NULL selector cannot equal any WHEN value, including NULL. Preserve
	// every arm's validation and semantic dependencies below, but retain only
	// the fallback in the executable program.
	simpleTerminated := program.simple && simpleSelectorStatic && simpleSelector.kind == kindNull
	simpleFallbackDead := false
	runtimeArms := 0
	for i := range expr.Whens {
		authored := &expr.Whens[i]
		arm := statementScalarCaseArm{
			condition: -1, matchPos: authored.Pos, resultPos: authored.Result.Pos,
		}
		var err error
		if program.simple {
			if authored.Match == nil || authored.Predicate != nil {
				return 0, fmt.Errorf("query: malformed simple CASE arm")
			}
			match, matchStatic := staticScalarCaseValue(authored.Match)
			reachable := !simpleTerminated
			staticMatch := simpleSelectorStatic && matchStatic &&
				simpleSelector.kind != kindNull && match.kind != kindNull &&
				compareScalar(simpleSelector, match) == 0
			if simpleSelectorStatic && matchStatic && !staticMatch {
				reachable = false
			}
			if reachable {
				arm.match, err = r.compileCaseRange(s, authored.Match)
				if err == nil {
					arm.matchDom = r.nodeDomain(arm.match.root)
				}
			} else {
				arm.matchDom, err = r.probeCaseRangeDomain(s, authored.Match)
			}
			if err != nil {
				return 0, err
			}
			arm.matchPos = authored.Match.Pos
			if merged, ok := unifyCaseDomain(simpleDomain, arm.matchDom); ok {
				simpleDomain = merged
			} else {
				return 0, sqlast.NewFeatureNotSupportedError(
					s.text, authored.Match.Pos,
					"simple CASE selector and WHEN value have irreconcilable static SQL types",
				)
			}
			if !reachable {
				arm.resultDom, err = r.probeCaseRangeDomain(s, authored.Result)
				if err != nil {
					return 0, err
				}
				arm.resultPos = authored.Result.Pos
				if merged, ok := unifyCaseDomain(domain, arm.resultDom); ok {
					domain = merged
				} else {
					return 0, sqlast.NewFeatureNotSupportedError(
						s.text, authored.Result.Pos,
						"CASE result arms have irreconcilable static SQL types; cast them to one exact type",
					)
				}
				continue
			}
			if staticMatch {
				simpleTerminated = true
				simpleFallbackDead = true
			}
		} else {
			if authored.Predicate == nil || authored.Match != nil {
				return 0, fmt.Errorf("query: malformed searched CASE arm")
			}
			truth, constant := staticScalarCasePredicate(authored.Predicate)
			reachable := !searchedTerminated && (!constant || truth == triTrue)
			if !reachable || constant {
				if err := r.probeCasePredicate(s, authored.Predicate); err != nil {
					return 0, err
				}
			} else {
				arm.condition, err = r.compileCasePredicate(s, authored.Predicate)
				if err != nil {
					return 0, err
				}
			}
			if reachable && constant {
				arm.condition = int32(len(r.predicates))
				r.predicates = append(r.predicates, statementScalarPredicate{
					kind: sqlast.ExprConstant, left: 1, right: -1,
					pos: authored.Predicate.Pos,
				})
			}
			if !reachable {
				arm.resultDom, err = r.probeCaseRangeDomain(s, authored.Result)
				if err != nil {
					return 0, err
				}
				arm.resultPos = authored.Result.Pos
				if merged, ok := unifyCaseDomain(domain, arm.resultDom); ok {
					domain = merged
				} else {
					return 0, sqlast.NewFeatureNotSupportedError(
						s.text, authored.Result.Pos,
						"CASE result arms have irreconcilable static SQL types; cast them to one exact type",
					)
				}
				continue
			}
			if constant && truth == triTrue {
				searchedTerminated = true
			}
		}
		arm.result, err = r.compileCaseRange(s, authored.Result)
		if err != nil {
			return 0, err
		}
		arm.resultDom = r.nodeDomain(arm.result.root)
		if merged, ok := unifyCaseDomain(domain, arm.resultDom); ok {
			domain = merged
		} else {
			return 0, sqlast.NewFeatureNotSupportedError(
				s.text, authored.Result.Pos,
				"CASE result arms have irreconcilable static SQL types; cast them to one exact type",
			)
		}
		r.caseArms[int(armStart)+runtimeArms] = arm
		runtimeArms++
	}
	program.armCount = int32(runtimeArms)
	program.simpleDom = simpleDomain

	if expr.Else != nil {
		var err error
		if (searchedTerminated && !program.simple) || simpleFallbackDead {
			program.fallbackDom, err = r.probeCaseRangeDomain(s, expr.Else)
		} else {
			program.fallback, err = r.compileCaseRange(s, expr.Else)
			if err == nil {
				program.fallbackDom = r.nodeDomain(program.fallback.root)
			}
		}
		if err != nil {
			return 0, err
		}
		program.fallbackPos = expr.Else.Pos
		if merged, ok := unifyCaseDomain(domain, program.fallbackDom); ok {
			domain = merged
		} else {
			return 0, sqlast.NewFeatureNotSupportedError(
				s.text, expr.Else.Pos,
				"CASE ELSE has an irreconcilable static SQL type; cast all results to one exact type",
			)
		}
	} else {
		program.fallbackDom = caseDomainNull
	}
	program.domain = domain
	if err := inferTypedCaseParameterTypes(s, expr, &program); err != nil {
		return 0, err
	}
	r.cases[caseIndex] = program
	r.nodes[root].skip = int32(len(r.nodes))
	return root, nil
}

// inferTypedCaseParameterTypes records only the PostgreSQL type-resolution
// contexts introduced by a type 'string' expression. Direct unknown
// placeholders in a CASE comparison or result inherit the selected BOOL/TEXT
// domain; ordinary schemaless CASE expressions keep unspecified metadata.
// pgwire can then run boolin once while binding, leaving row execution as a
// native boolean comparison with no conversion or allocation in the hot path.
func inferTypedCaseParameterTypes(
	s *Statement,
	expr *sqlast.ScalarExpr,
	program *statementScalarCase,
) error {
	if s == nil || expr == nil || program == nil {
		return nil
	}
	if program.simple && typedSimpleCaseComparison(expr) {
		paramType := caseDomainParameterType(program.simpleDom)
		if paramType != ParameterTypeUnspecified {
			if err := mergeDirectCaseComparisonParameterType(
				s, expr.Left, paramType, true,
			); err != nil {
				return err
			}
			for i := range expr.Whens {
				if err := mergeDirectCaseComparisonParameterType(
					s, expr.Whens[i].Match, paramType, false,
				); err != nil {
					return err
				}
			}
		}
	}
	if !typedCaseResults(expr) {
		return nil
	}
	paramType := caseDomainParameterType(program.domain)
	if paramType == ParameterTypeUnspecified {
		return nil
	}
	for i := range expr.Whens {
		if err := mergeDirectCaseParameterType(
			s, expr.Whens[i].Result, paramType,
		); err != nil {
			return err
		}
	}
	return mergeDirectCaseParameterType(s, expr.Else, paramType)
}

func mergeDirectCaseComparisonParameterType(
	s *Statement,
	expr *sqlast.ScalarExpr,
	paramType ParameterType,
	parameterOnLeft bool,
) error {
	if s == nil || expr == nil || expr.Kind != sqlast.ScalarLiteral ||
		expr.Value.Kind != sqlast.OperandParam {
		return nil
	}
	existing := s.ParameterType(expr.Value.Ordinal)
	if existing != ParameterTypeUnspecified && existing != paramType &&
		!parameterTypesShareStringCategory(existing, paramType) {
		left, right := paramType.String(), existing.String()
		if parameterOnLeft {
			left, right = existing.String(), paramType.String()
		}
		return sqlast.NewUndefinedOperatorError(s.text, expr.Value.Pos, left, right)
	}
	return mergeDirectCaseParameterType(s, expr, paramType)
}

func mergeDirectCaseParameterType(
	s *Statement,
	expr *sqlast.ScalarExpr,
	paramType ParameterType,
) error {
	if expr == nil || expr.Kind != sqlast.ScalarLiteral ||
		expr.Value.Kind != sqlast.OperandParam {
		return nil
	}
	ordinal := expr.Value.Ordinal
	if ordinal < 0 || s.paramBase > int(^uint(0)>>1)-ordinal {
		return fmt.Errorf("query: scalar CASE parameter ordinal overflows: %w", ErrParameterType)
	}
	return s.mergeParameterType(s.paramBase+ordinal, paramType, expr.Value.Pos)
}

func caseDomainParameterType(domain scalarCaseDomain) ParameterType {
	switch domain {
	case caseDomainBoolean:
		return ParameterTypeBool
	case caseDomainText:
		return ParameterTypeText
	default:
		return ParameterTypeUnspecified
	}
}

func typedSimpleCaseComparison(expr *sqlast.ScalarExpr) bool {
	if expr == nil || expr.Kind != sqlast.ScalarCase || expr.Left == nil {
		return false
	}
	if scalarExpressionHasTypedResult(expr.Left) {
		return true
	}
	for i := range expr.Whens {
		if scalarExpressionHasTypedResult(expr.Whens[i].Match) {
			return true
		}
	}
	return false
}

func typedCaseResults(expr *sqlast.ScalarExpr) bool {
	if expr == nil || expr.Kind != sqlast.ScalarCase {
		return false
	}
	if scalarExpressionHasTypedResult(expr.Else) {
		return true
	}
	for i := range expr.Whens {
		if scalarExpressionHasTypedResult(expr.Whens[i].Result) {
			return true
		}
	}
	return false
}

// scalarExpressionHasTypedResult follows output-producing children only. A
// typed value used solely as a nested simple-CASE selector constrains that
// comparison, not the nested CASE result seen by its parent.
func scalarExpressionHasTypedResult(expr *sqlast.ScalarExpr) bool {
	if expr == nil {
		return false
	}
	if expr.TypedConstant {
		return true
	}
	if expr.Kind != sqlast.ScalarCase {
		return scalarExpressionHasTypedResult(expr.Left) ||
			scalarExpressionHasTypedResult(expr.Right)
	}
	if scalarExpressionHasTypedResult(expr.Else) {
		return true
	}
	for i := range expr.Whens {
		if scalarExpressionHasTypedResult(expr.Whens[i].Result) {
			return true
		}
	}
	return false
}

// validateTypedCaseResultDomains gives the PostgreSQL typed-constant seam the
// same datatype-mismatch class and ELSE-first error position as
// select_common_type. Ordinary schemaless CASE keeps its established explicit
// 0A000 refusal; this preflight activates only when a result descends from the
// type 'string' grammar production.
func validateTypedCaseResultDomains(s *Statement, expr *sqlast.ScalarExpr) error {
	if s == nil || expr == nil || expr.Kind != sqlast.ScalarCase ||
		!typedCaseResults(expr) {
		return nil
	}
	candidate := caseDomainNull
	if expr.Else != nil {
		candidate = scalarASTCaseDomain(s, expr.Else)
	}
	for i := range expr.Whens {
		result := expr.Whens[i].Result
		domain := scalarASTCaseDomain(s, result)
		merged, ok := unifyCaseDomain(candidate, domain)
		if !ok {
			return &ScalarTypeError{
				Pos: typedCaseResultPosition(result), Operation: "CASE common type",
				Left: candidate.valueType(), Right: domain.valueType(),
			}
		}
		candidate = merged
	}
	return nil
}

func scalarASTCaseDomain(s *Statement, expr *sqlast.ScalarExpr) scalarCaseDomain {
	if expr == nil || expr.Kind == sqlast.ScalarNull {
		return caseDomainNull
	}
	switch expr.Kind {
	case sqlast.ScalarPath:
		return caseDomainDynamic
	case sqlast.ScalarLiteral:
		switch expr.Value.Kind {
		case sqlast.OperandBool:
			return caseDomainBoolean
		case sqlast.OperandNumber:
			return caseDomainNumeric
		case sqlast.OperandString:
			return caseDomainText
		case sqlast.OperandParam:
			if s == nil {
				return caseDomainDynamic
			}
			switch s.ParameterType(expr.Value.Ordinal) {
			case ParameterTypeBool:
				return caseDomainBoolean
			case ParameterTypeText, ParameterTypeVarchar,
				ParameterTypeName, ParameterTypeBPChar:
				return caseDomainText
			default:
				return caseDomainDynamic
			}
		default:
			return caseDomainDynamic
		}
	case sqlast.ScalarUnary, sqlast.ScalarAggregate:
		return caseDomainNumeric
	case sqlast.ScalarBinary:
		if expr.Op.Conditional() {
			domain, _ := unifyCaseDomain(scalarASTCaseDomain(s, expr.Left), scalarASTCaseDomain(s, expr.Right))
			return domain
		}
		if expr.Op == sqlast.ScalarConcat {
			return caseDomainText
		}
		return caseDomainNumeric
	case sqlast.ScalarCast:
		switch expr.Cast {
		case sqlast.ScalarCastBoolean:
			return caseDomainBoolean
		case sqlast.ScalarCastText:
			return caseDomainText
		case sqlast.ScalarCastNumeric:
			return caseDomainNumeric
		default:
			return caseDomainJSON
		}
	case sqlast.ScalarCase:
		candidate := caseDomainNull
		if expr.Else != nil {
			candidate = scalarASTCaseDomain(s, expr.Else)
		}
		for i := range expr.Whens {
			result := scalarASTCaseDomain(s, expr.Whens[i].Result)
			merged, ok := unifyCaseDomain(candidate, result)
			if !ok {
				return caseDomainDynamic
			}
			candidate = merged
		}
		return candidate
	default:
		return caseDomainDynamic
	}
}

func typedCaseResultPosition(expr *sqlast.ScalarExpr) int {
	if expr == nil {
		return 0
	}
	if expr.Kind == sqlast.ScalarCast && expr.TypedConstant {
		return expr.TargetPos
	}
	return expr.Pos
}

type scalarCaseCompileMark struct {
	nodes, dependencies, predicates int
	cases, arms, conditionals       int
	hasAggregate                    bool
}

func (r *statementScalar) caseCompileMark() scalarCaseCompileMark {
	return scalarCaseCompileMark{
		nodes: len(r.nodes), dependencies: len(r.deps),
		predicates: len(r.predicates), cases: len(r.cases),
		arms: len(r.caseArms), conditionals: len(r.conditionals), hasAggregate: r.hasAggregate,
	}
}

func (r *statementScalar) rewindCaseCompile(mark scalarCaseCompileMark) {
	clear(r.nodes[mark.nodes:])
	r.nodes = r.nodes[:mark.nodes]
	clear(r.deps[mark.dependencies:])
	r.deps = r.deps[:mark.dependencies]
	clear(r.predicates[mark.predicates:])
	r.predicates = r.predicates[:mark.predicates]
	clear(r.cases[mark.cases:])
	r.cases = r.cases[:mark.cases]
	clear(r.caseArms[mark.arms:])
	r.caseArms = r.caseArms[:mark.arms]
	clear(r.conditionals[mark.conditionals:])
	r.conditionals = r.conditionals[:mark.conditionals]
	r.hasAggregate = mark.hasAggregate
}

// probeCaseRangeDomain performs all prepare-time validation and type
// resolution for a statically unreachable result without retaining its path,
// aggregate, node, or nested-CASE dependencies in the executable plan.
func (r *statementScalar) probeCaseRangeDomain(
	s *Statement,
	expr *sqlast.ScalarExpr,
) (domain scalarCaseDomain, err error) {
	mark := r.caseCompileMark()
	defer r.rewindCaseCompile(mark)
	program, err := r.compileCaseRange(s, expr)
	if err != nil {
		return 0, err
	}
	r.recordCaseSemanticDependencies(mark.dependencies)
	return r.nodeDomain(program.root), nil
}

func (r *statementScalar) probeCasePredicate(
	s *Statement,
	expr *sqlast.Expr,
) (err error) {
	mark := r.caseCompileMark()
	defer r.rewindCaseCompile(mark)
	_, err = r.compileCasePredicate(s, expr)
	if err == nil {
		r.recordCaseSemanticDependencies(mark.dependencies)
	}
	return err
}

func (r *statementScalar) recordCaseSemanticDependencies(start int) {
	for i := start; i < len(r.deps); i++ {
		candidate := r.deps[i]
		seen := false
		for j := range r.semanticDeps {
			dep := &r.semanticDeps[j]
			if dep.agg == candidate.agg && dep.spec == candidate.spec {
				seen = true
				break
			}
		}
		if !seen {
			r.semanticDeps = append(r.semanticDeps, candidate)
		}
	}
}

// staticScalarCaseValue recognizes source-independent SQL literals without
// evaluating casts or arithmetic at prepare time. The returned scalar borrows
// parser-owned text, so exact decimal comparison remains allocation-free and
// uses the same equality relation as every runtime CASE comparison.
func staticScalarCaseValue(expr *sqlast.ScalarExpr) (scalar, bool) {
	if expr == nil {
		return scalar{}, false
	}
	if expr.Kind == sqlast.ScalarNull {
		return scalar{kind: kindNull}, true
	}
	if expr.Kind != sqlast.ScalarLiteral {
		return scalar{}, false
	}
	switch expr.Value.Kind {
	case sqlast.OperandString:
		return scalar{kind: kindString, sval: expr.Value.Text}, true
	case sqlast.OperandNumber:
		raw := byteview.Bytes(expr.Value.Text)
		value := scalar{kind: kindNumber, num: raw, raw: raw}
		value.ival, value.isInt = int64Spelling(expr.Value.Text)
		return value, true
	case sqlast.OperandBool:
		return scalar{kind: kindBool, bval: expr.Value.Bool}, true
	default:
		// Placeholders are constant only for one bind, not for the prepared
		// statement, and JSON operands are not scalar SQL literals.
		return scalar{}, false
	}
}

// staticScalarCasePredicate recognizes only three-valued expressions whose
// answer is independent of every input row. AND/OR still use annihilators, so
// FALSE AND dynamic and TRUE OR dynamic are compile-time constants without
// inspecting the dead side. Anything else remains in the ordinary lazy
// runtime program.
func staticScalarCasePredicate(expr *sqlast.Expr) (tri, bool) {
	if expr == nil {
		return triUnknown, false
	}
	switch expr.Kind {
	case sqlast.ExprConstant:
		if expr.Value.Kind != sqlast.OperandBool {
			return triUnknown, false
		}
		return boolTri(expr.Value.Bool), true
	case sqlast.ExprScalarTruth:
		if expr.ScalarLeft == nil {
			return triUnknown, false
		}
		switch expr.ScalarLeft.Kind {
		case sqlast.ScalarNull:
			return triUnknown, true
		case sqlast.ScalarLiteral:
			if expr.ScalarLeft.Value.Kind == sqlast.OperandBool {
				return boolTri(expr.ScalarLeft.Value.Bool), true
			}
		}
		return triUnknown, false
	case sqlast.ExprNot:
		if len(expr.Kids) != 1 {
			return triUnknown, false
		}
		value, known := staticScalarCasePredicate(expr.Kids[0])
		if !known {
			return triUnknown, false
		}
		return notTri(value), true
	case sqlast.ExprAnd:
		unknown, dynamic := false, false
		for _, child := range expr.Kids {
			value, known := staticScalarCasePredicate(child)
			if !known {
				dynamic = true
				continue
			}
			if value == triFalse {
				return triFalse, true
			}
			unknown = unknown || value == triUnknown
		}
		if dynamic {
			return triUnknown, false
		}
		if unknown {
			return triUnknown, true
		}
		return triTrue, true
	case sqlast.ExprOr:
		unknown, dynamic := false, false
		for _, child := range expr.Kids {
			value, known := staticScalarCasePredicate(child)
			if !known {
				dynamic = true
				continue
			}
			if value == triTrue {
				return triTrue, true
			}
			unknown = unknown || value == triUnknown
		}
		if dynamic {
			return triUnknown, false
		}
		if unknown {
			return triUnknown, true
		}
		return triFalse, true
	default:
		return triUnknown, false
	}
}

func (r *statementScalar) compileCaseRange(
	s *Statement,
	expr *sqlast.ScalarExpr,
) (statementScalarRange, error) {
	if expr == nil {
		return statementScalarRange{}, fmt.Errorf("query: nil scalar CASE branch")
	}
	start := int32(len(r.nodes))
	root, err := r.compileExpr(s, expr)
	if err != nil {
		return statementScalarRange{}, err
	}
	return statementScalarRange{start: start, end: int32(len(r.nodes)), root: root}, nil
}

func (r *statementScalar) compileCasePredicate(s *Statement, expr *sqlast.Expr) (int32, error) {
	if expr == nil {
		return 0, fmt.Errorf("query: nil searched CASE predicate")
	}
	node := statementScalarPredicate{
		kind: expr.Kind, op: expr.Op, left: -1, right: -1,
		negated: expr.Negated, pos: expr.Pos,
	}
	switch expr.Kind {
	case sqlast.ExprAnd, sqlast.ExprOr, sqlast.ExprNot:
		if expr.Kind == sqlast.ExprNot && len(expr.Kids) != 1 ||
			expr.Kind != sqlast.ExprNot && len(expr.Kids) < 2 {
			return 0, fmt.Errorf("query: malformed searched CASE boolean predicate")
		}
		for _, kid := range expr.Kids {
			index, err := r.compileCasePredicate(s, kid)
			if err != nil {
				return 0, err
			}
			node.kids = append(node.kids, index)
		}
	case sqlast.ExprConstant:
		if expr.Value.Kind != sqlast.OperandBool {
			return 0, fmt.Errorf("query: searched CASE constant is not boolean")
		}
		node.left = 1
		if !expr.Value.Bool {
			node.left = 0
		}
	case sqlast.ExprCompare:
		node.start = int32(len(r.nodes))
		left, err := r.compileDependency(s, expr.Path, expr.Agg, expr.Pos)
		if err != nil {
			return 0, err
		}
		node.left = left
		if expr.RightPath != nil {
			node.pathCompare = true
			node.pos = expr.Value.Pos
			node.right, err = r.compileDependency(s, expr.RightPath, sqlast.AggNone, expr.RightPath.Pos)
		} else if expr.Subquery != nil {
			return 0, sqlast.NewFeatureNotSupportedError(
				s.text, expr.Pos, "subqueries in searched CASE WHEN require a correlated scalar-subquery stage",
			)
		} else {
			node.right = r.compileCaseOperand(expr.Value)
		}
		if err != nil {
			return 0, err
		}
		node.end = int32(len(r.nodes))
		node.leftDom, node.rightDom = r.nodeDomain(node.left), r.nodeDomain(node.right)
		if caseDomainsConflict(node.leftDom, node.rightDom) {
			return 0, sqlast.NewFeatureNotSupportedError(
				s.text, expr.Pos, "searched CASE comparison has irreconcilable static SQL types",
			)
		}
	case sqlast.ExprIsNull:
		node.start = int32(len(r.nodes))
		left, err := r.compileDependency(s, expr.Path, expr.Agg, expr.Pos)
		if err != nil {
			return 0, err
		}
		node.left, node.end = left, int32(len(r.nodes))
	case sqlast.ExprScalarCompare:
		node.start = int32(len(r.nodes))
		left, err := r.compileExpr(s, expr.ScalarLeft)
		if err != nil {
			return 0, err
		}
		right, err := r.compileExpr(s, expr.ScalarRight)
		if err != nil {
			return 0, err
		}
		node.left, node.right, node.end = left, right, int32(len(r.nodes))
		node.leftDom, node.rightDom = r.nodeDomain(left), r.nodeDomain(right)
		if caseDomainsConflict(node.leftDom, node.rightDom) {
			return 0, sqlast.NewFeatureNotSupportedError(
				s.text, expr.Pos, "searched CASE comparison has irreconcilable static SQL types",
			)
		}
	case sqlast.ExprScalarIsNull, sqlast.ExprScalarTruth:
		node.start = int32(len(r.nodes))
		left, err := r.compileExpr(s, expr.ScalarLeft)
		if err != nil {
			return 0, err
		}
		node.left, node.end = left, int32(len(r.nodes))
		node.leftDom = r.nodeDomain(left)
		if expr.Kind == sqlast.ExprScalarTruth &&
			node.leftDom != caseDomainDynamic && node.leftDom != caseDomainNull &&
			node.leftDom != caseDomainBoolean {
			return 0, sqlast.NewFeatureNotSupportedError(
				s.text, expr.Pos, "searched CASE WHEN requires a boolean condition",
			)
		}
	default:
		return 0, sqlast.NewFeatureNotSupportedError(
			s.text, expr.Pos,
			"this searched CASE predicate is not executable yet; use boolean comparisons, IS NULL, AND, OR, or NOT",
		)
	}
	r.predicates = append(r.predicates, node)
	return int32(len(r.predicates) - 1), nil
}

func (r *statementScalar) compileCaseOperand(operand sqlast.Operand) int32 {
	r.nodes = append(r.nodes, statementScalarNode{
		kind: statementScalarLiteral, operand: operand, left: -1, right: -1,
		pos: operand.Pos,
	})
	return int32(len(r.nodes) - 1)
}

func (r *statementScalar) evalCase(
	result *Result,
	row int,
	node *statementScalarNode,
	arena *[]byte,
	budget *aggregateBudget,
	intermediate *intermediateBudget,
	intermediateCharge *int64,
	cancel *CancelFlag,
) (statementScalarValue, error) {
	program := &r.cases[node.caseIndex]
	var selector statementScalarValue
	if program.simple {
		if err := r.evalCaseRange(result, row, program.selector, arena, budget,
			intermediate, intermediateCharge, cancel); err != nil {
			return statementScalarValue{}, err
		}
		selector = r.values[program.selector.root]
		if err := validateScalarCaseComparisonValue(
			selector, program.selectorDom, program.simpleDom,
			program.selectorPos,
		); err != nil {
			return statementScalarValue{}, err
		}
	}
	arms := r.caseArms[program.armStart : program.armStart+program.armCount]
	for i := range arms {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return statementScalarValue{}, err
		}
		arm := &arms[i]
		matched := false
		if program.simple {
			if err := r.evalCaseRange(result, row, arm.match, arena, budget,
				intermediate, intermediateCharge, cancel); err != nil {
				return statementScalarValue{}, err
			}
			var err error
			matched, err = scalarCaseEqual(
				selector, program.selectorDom, r.values[arm.match.root],
				arm.matchDom, program.simpleDom, arm.matchPos,
			)
			if err != nil {
				return statementScalarValue{}, err
			}
		} else {
			truth, err := r.evalCasePredicate(
				result, row, arm.condition, arena, budget,
				intermediate, intermediateCharge, cancel,
			)
			if err != nil {
				return statementScalarValue{}, err
			}
			matched = truth == triTrue
		}
		if !matched {
			continue
		}
		if err := r.evalCaseRange(result, row, arm.result, arena, budget,
			intermediate, intermediateCharge, cancel); err != nil {
			return statementScalarValue{}, err
		}
		return validateScalarCaseResult(
			r.values[arm.result.root], arm.resultDom, program.domain, arm.resultPos,
		)
	}
	if program.fallback.root < 0 {
		return statementScalarValue{value: scalar{kind: kindNull}}, nil
	}
	if err := r.evalCaseRange(result, row, program.fallback, arena, budget,
		intermediate, intermediateCharge, cancel); err != nil {
		return statementScalarValue{}, err
	}
	return validateScalarCaseResult(
		r.values[program.fallback.root], program.fallbackDom, program.domain, program.fallbackPos,
	)
}

func (r *statementScalar) evalCaseRange(
	result *Result,
	row int,
	program statementScalarRange,
	arena *[]byte,
	budget *aggregateBudget,
	intermediate *intermediateBudget,
	intermediateCharge *int64,
	cancel *CancelFlag,
) error {
	if program.start < 0 || program.end < program.start ||
		program.root < program.start || program.root >= program.end {
		return fmt.Errorf("query: malformed scalar CASE branch range")
	}
	return r.evalNodes(result, row, int(program.start), int(program.end), arena,
		budget, intermediate, intermediateCharge, cancel)
}

func (r *statementScalar) evalCasePredicate(
	result *Result,
	row int,
	index int32,
	arena *[]byte,
	budget *aggregateBudget,
	intermediate *intermediateBudget,
	intermediateCharge *int64,
	cancel *CancelFlag,
) (tri, error) {
	node := &r.predicates[index]
	switch node.kind {
	case sqlast.ExprAnd:
		out := triTrue
		for i, kid := range node.kids {
			if err := cancellationCheckpoint(cancel, i); err != nil {
				return triFalse, err
			}
			value, err := r.evalCasePredicate(result, row, kid, arena, budget,
				intermediate, intermediateCharge, cancel)
			if err != nil {
				return triFalse, err
			}
			if value == triFalse {
				return triFalse, nil
			}
			if value == triUnknown {
				out = triUnknown
			}
		}
		return out, nil
	case sqlast.ExprOr:
		out := triFalse
		for i, kid := range node.kids {
			if err := cancellationCheckpoint(cancel, i); err != nil {
				return triFalse, err
			}
			value, err := r.evalCasePredicate(result, row, kid, arena, budget,
				intermediate, intermediateCharge, cancel)
			if err != nil {
				return triFalse, err
			}
			if value == triTrue {
				return triTrue, nil
			}
			if value == triUnknown {
				out = triUnknown
			}
		}
		return out, nil
	case sqlast.ExprNot:
		value, err := r.evalCasePredicate(result, row, node.kids[0], arena, budget,
			intermediate, intermediateCharge, cancel)
		return notTri(value), err
	case sqlast.ExprConstant:
		return boolTri(node.left != 0), nil
	}

	program := statementScalarRange{start: node.start, end: node.end, root: node.left}
	if err := r.evalCaseRange(result, row, program, arena, budget,
		intermediate, intermediateCharge, cancel); err != nil {
		return triFalse, err
	}
	left := r.values[node.left]
	var value tri
	switch node.kind {
	case sqlast.ExprIsNull, sqlast.ExprScalarIsNull:
		value = boolTri(left.value.kind == kindNull)
	case sqlast.ExprScalarTruth:
		if left.value.kind == kindNull {
			value = triUnknown
		} else if left.value.kind != kindBool {
			return triFalse, &ScalarTypeError{
				Pos: node.pos, Operation: "CASE WHEN condition",
				Left: valueTypeOfScalar(left.value), Right: TypeBool,
			}
		} else {
			value = boolTri(left.value.bval)
		}
	default:
		right := r.values[node.right]
		if left.value.kind == kindNull || right.value.kind == kindNull {
			value = triUnknown
		} else if node.pathCompare {
			var err error
			value, err = compareSQLPathScalars(
				node.pos, left.value, Op(node.op), right.value,
			)
			if err != nil {
				return triFalse, err
			}
		} else if err := scalarCaseComparable(
			left, node.leftDom, right, node.rightDom, node.pos,
		); err != nil {
			return triFalse, err
		} else {
			value = boolTri(acceptSign(compareScalar(left.value, right.value), Op(node.op)))
		}
	}
	if node.negated {
		value = notTri(value)
	}
	return value, nil
}

func scalarCaseEqual(
	left statementScalarValue,
	leftDomain scalarCaseDomain,
	right statementScalarValue,
	rightDomain scalarCaseDomain,
	expected scalarCaseDomain,
	pos int,
) (bool, error) {
	if left.value.kind == kindNull || right.value.kind == kindNull {
		return false, nil
	}
	if err := validateScalarCaseComparisonValue(
		right, rightDomain, expected, pos,
	); err != nil {
		return false, err
	}
	if err := scalarCaseComparable(left, leftDomain, right, rightDomain, pos); err != nil {
		return false, err
	}
	return compareScalar(left.value, right.value) == 0, nil
}

func validateScalarCaseComparisonValue(
	value statementScalarValue,
	actual scalarCaseDomain,
	expected scalarCaseDomain,
	pos int,
) error {
	if value.value.kind == kindNull || expected == caseDomainDynamic ||
		expected == caseDomainNull {
		return nil
	}
	if actual == caseDomainDynamic {
		actual = scalarValueDomain(value.value)
	}
	if actual == expected {
		return nil
	}
	return &ScalarTypeError{
		Pos: pos, Operation: "CASE comparison",
		Left: actual.valueType(), Right: expected.valueType(),
	}
}

func scalarCaseComparable(
	left statementScalarValue,
	leftDomain scalarCaseDomain,
	right statementScalarValue,
	rightDomain scalarCaseDomain,
	pos int,
) error {
	liveLeft, liveRight := leftDomain, rightDomain
	if liveLeft == caseDomainDynamic {
		liveLeft = scalarValueDomain(left.value)
	}
	if liveRight == caseDomainDynamic {
		liveRight = scalarValueDomain(right.value)
	}
	if liveLeft == liveRight {
		return nil
	}
	return &ScalarTypeError{
		Pos: pos, Operation: "CASE comparison",
		Left: liveLeft.valueType(), Right: liveRight.valueType(),
	}
}

func validateScalarCaseResult(
	value statementScalarValue,
	actual scalarCaseDomain,
	expected scalarCaseDomain,
	pos int,
) (statementScalarValue, error) {
	if value.value.kind == kindNull || expected == caseDomainDynamic || expected == caseDomainNull {
		return value, nil
	}
	if actual == caseDomainDynamic {
		actual = scalarValueDomain(value.value)
	}
	if actual == expected {
		return value, nil
	}
	return statementScalarValue{}, &ScalarTypeError{
		Pos: pos, Operation: "CASE result",
		Left: actual.valueType(), Right: expected.valueType(),
	}
}

func (r *statementScalar) nodeDomain(root int32) scalarCaseDomain {
	if root < 0 || int(root) >= len(r.nodes) {
		return caseDomainDynamic
	}
	node := &r.nodes[root]
	switch node.kind {
	case statementScalarDependency:
		if r.deps[node.dependency].agg != sqlast.AggNone {
			return caseDomainNumeric
		}
		return caseDomainDynamic
	case statementScalarLiteral:
		switch node.operand.Kind {
		case sqlast.OperandBool:
			return caseDomainBoolean
		case sqlast.OperandNumber:
			return caseDomainNumeric
		case sqlast.OperandString:
			return caseDomainText
		default:
			return caseDomainDynamic
		}
	case statementScalarNull:
		return caseDomainNull
	case statementScalarUnary:
		return caseDomainNumeric
	case statementScalarBinary:
		if node.op == sqlast.ScalarConcat {
			return caseDomainText
		}
		return caseDomainNumeric
	case statementScalarCast:
		switch node.cast {
		case sqlast.ScalarCastText:
			return caseDomainText
		case sqlast.ScalarCastBoolean:
			return caseDomainBoolean
		case sqlast.ScalarCastNumeric:
			return caseDomainNumeric
		default:
			return caseDomainJSON
		}
	case statementScalarCaseNode:
		return r.cases[node.caseIndex].domain
	case statementScalarConditionalNode:
		return r.conditionals[node.conditionalIndex].domain
	default:
		return caseDomainDynamic
	}
}

func unifyCaseDomain(left, right scalarCaseDomain) (scalarCaseDomain, bool) {
	if left == caseDomainNull || left == caseDomainDynamic {
		if right == caseDomainNull {
			return left, true
		}
		return right, true
	}
	if right == caseDomainNull || right == caseDomainDynamic || left == right {
		return left, true
	}
	return caseDomainDynamic, false
}

func caseDomainsConflict(left, right scalarCaseDomain) bool {
	return left != caseDomainDynamic && right != caseDomainDynamic &&
		left != caseDomainNull && right != caseDomainNull && left != right
}

func scalarValueDomain(value scalar) scalarCaseDomain {
	switch value.kind {
	case kindNull:
		return caseDomainNull
	case kindBool:
		return caseDomainBoolean
	case kindNumber:
		return caseDomainNumeric
	case kindString:
		return caseDomainText
	default:
		return caseDomainJSON
	}
}

func (d scalarCaseDomain) valueType() ValueType {
	switch d {
	case caseDomainNull:
		return TypeNull
	case caseDomainBoolean:
		return TypeBool
	case caseDomainNumeric:
		return TypeNumber
	case caseDomainText:
		return TypeString
	case caseDomainJSON:
		return TypeJSON
	default:
		return TypeAny
	}
}

func (d scalarCaseDomain) schemaType() ValueType {
	if d == caseDomainJSON {
		return TypeAny
	}
	return d.valueType()
}

func (d scalarCaseDomain) representation() OutputRepresentation {
	switch d {
	case caseDomainBoolean:
		return OutputSQLBool
	case caseDomainNumeric:
		return OutputSQLNumber
	case caseDomainText:
		return OutputSQLText
	default:
		return OutputJSON
	}
}
