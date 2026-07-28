package query

import (
	"strconv"
	"strings"
)

type nativePredicateKind uint8

const (
	nativePredicateInvalid nativePredicateKind = iota
	nativePredicateAnd
	nativePredicateOr
	nativePredicateNot
	nativePredicateField
)

type nativeFieldOperator uint8

const (
	nativeFieldInvalid nativeFieldOperator = iota
	nativeFieldEq
	nativeFieldNe
	nativeFieldLt
	nativeFieldLe
	nativeFieldGt
	nativeFieldGe
	nativeFieldIn
	nativeFieldNotIn
	nativeFieldExists
	nativeFieldNull
	nativeFieldMissing
	nativeFieldNullish
)

type nativePredicateSyntax struct {
	kind     nativePredicateKind
	path     string
	operator nativeFieldOperator
	operand  nativeOperandSyntax
	children []nativePredicateSyntax
}

func (p *nativeParser) parsePredicate(
	value qvalue, pointer string, depth int,
) (nativePredicateSyntax, error) {
	if depth > nativeMaxDepth {
		return nativePredicateSyntax{}, nativeSyntaxErr(
			"limit_exceeded", pointer, "predicate nesting exceeds %d", nativeMaxDepth,
		)
	}
	members, err := p.members(value, pointer)
	if err != nil {
		return nativePredicateSyntax{}, err
	}
	if len(members) == 0 {
		return nativePredicateSyntax{}, nativeSyntaxErr(
			"invalid_predicate", pointer, "predicate object must not be empty",
		)
	}

	hasBooleanOperator := false
	for _, member := range members {
		if strings.HasPrefix(member.name, "$") {
			hasBooleanOperator = true
			break
		}
	}
	if hasBooleanOperator {
		if len(members) != 1 {
			return nativePredicateSyntax{}, nativeSyntaxErr(
				"invalid_predicate", pointer,
				"boolean operator cannot be mixed with field predicates",
			)
		}
		member := members[0]
		at := nativePointerMember(pointer, member.name)
		switch member.name {
		case "$and", "$or":
			if member.value.kind() != qArray {
				return nativePredicateSyntax{}, nativeSyntaxErr(
					"invalid_operand", at, "%s requires an array of predicates",
					member.name,
				)
			}
			if member.value.length() > nativeMaxBooleanFanIn {
				return nativePredicateSyntax{}, nativeSyntaxErr(
					"limit_exceeded", at, "%s has %d children; maximum is %d",
					member.name, member.value.length(), nativeMaxBooleanFanIn,
				)
			}
			kind := nativePredicateAnd
			if member.name == "$or" {
				kind = nativePredicateOr
			}
			out := nativePredicateSyntax{
				kind:     kind,
				children: make([]nativePredicateSyntax, 0, member.value.length()),
			}
			if err := p.notePredicate(pointer); err != nil {
				return nativePredicateSyntax{}, err
			}
			err := member.value.elements(func(index int, element qvalue) error {
				child, err := p.parsePredicate(
					element, nativePointerElement(at, index), depth+1,
				)
				if err != nil {
					return err
				}
				out.children = append(out.children, child)
				return nil
			})
			return out, err
		case "$not":
			if err := p.notePredicate(pointer); err != nil {
				return nativePredicateSyntax{}, err
			}
			child, err := p.parsePredicate(member.value, at, depth+1)
			if err != nil {
				return nativePredicateSyntax{}, err
			}
			return nativePredicateSyntax{
				kind: nativePredicateNot, children: []nativePredicateSyntax{child},
			}, nil
		default:
			return nativePredicateSyntax{}, nativeSyntaxErr(
				"invalid_operator", at,
				"unknown predicate operator %q; expected $and, $or, or $not",
				member.name,
			)
		}
	}

	children := make([]nativePredicateSyntax, 0, len(members))
	for _, member := range members {
		at := nativePointerMember(pointer, member.name)
		if err := nativeValidatePointer(member.name); err != nil {
			return nativePredicateSyntax{}, nativeSyntaxErr(
				"invalid_path", at, "%q is not an RFC 6901 pointer: %v",
				member.name, err,
			)
		}
		field, err := p.parseFieldPredicate(member.name, member.value, at)
		if err != nil {
			return nativePredicateSyntax{}, err
		}
		children = append(children, field...)
	}
	if len(children) == 1 {
		return children[0], nil
	}
	if err := p.notePredicate(pointer); err != nil {
		return nativePredicateSyntax{}, err
	}
	return nativePredicateSyntax{kind: nativePredicateAnd, children: children}, nil
}

func (p *nativeParser) parseFieldPredicate(
	path string, value qvalue, pointer string,
) ([]nativePredicateSyntax, error) {
	if value.kind() != qObject {
		operand, err := p.parseScalarOperand(value, pointer)
		if err != nil {
			return nil, err
		}
		if err := p.notePredicate(pointer); err != nil {
			return nil, err
		}
		return []nativePredicateSyntax{{
			kind: nativePredicateField, path: strings.Clone(path),
			operator: nativeFieldEq, operand: operand,
		}}, nil
	}

	members, err := p.members(value, pointer)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nativeSyntaxErr(
			"invalid_predicate", pointer, "field operator object must not be empty",
		)
	}
	out := make([]nativePredicateSyntax, 0, len(members))
	for _, member := range members {
		at := nativePointerMember(pointer, member.name)
		operator, operand, err := p.parseFieldOperator(member.name, member.value, at)
		if err != nil {
			return nil, err
		}
		if err := p.notePredicate(at); err != nil {
			return nil, err
		}
		out = append(out, nativePredicateSyntax{
			kind: nativePredicateField, path: strings.Clone(path),
			operator: operator, operand: operand,
		})
	}
	return out, nil
}

func (p *nativeParser) parseFieldOperator(
	name string, value qvalue, pointer string,
) (nativeFieldOperator, nativeOperandSyntax, error) {
	var operator nativeFieldOperator
	switch name {
	case "$eq":
		operator = nativeFieldEq
	case "$ne":
		operator = nativeFieldNe
	case "$lt":
		operator = nativeFieldLt
	case "$lte":
		operator = nativeFieldLe
	case "$gt":
		operator = nativeFieldGt
	case "$gte":
		operator = nativeFieldGe
	case "$in":
		operator = nativeFieldIn
	case "$nin":
		operator = nativeFieldNotIn
	case "$exists":
		operator = nativeFieldExists
	case "$null":
		operator = nativeFieldNull
	case "$missing":
		operator = nativeFieldMissing
	case "$nullish":
		operator = nativeFieldNullish
	default:
		return nativeFieldInvalid, nativeOperandSyntax{}, nativeSyntaxErr(
			"invalid_operator", pointer, "unknown field operator %q", name,
		)
	}

	switch operator {
	case nativeFieldEq, nativeFieldNe:
		operand, err := p.parseOperand(value, pointer)
		return operator, operand, err
	case nativeFieldLt, nativeFieldLe, nativeFieldGt, nativeFieldGe:
		operand, err := p.parseOperand(value, pointer)
		if err != nil {
			return nativeFieldInvalid, nativeOperandSyntax{}, err
		}
		if operand.kind == nativeOperandScalar &&
			operand.scalar.kind != qNumber && operand.scalar.kind != qString {
			return nativeFieldInvalid, nativeOperandSyntax{}, nativeSyntaxErr(
				"invalid_operand", pointer,
				"ordered comparison requires a number, string, or compatible parameter",
			)
		}
		return operator, operand, nil
	case nativeFieldIn, nativeFieldNotIn:
		operand, err := p.parseListOperand(value, pointer)
		return operator, operand, err
	default:
		enabled, ok := value.boolean()
		if !ok || !enabled {
			return nativeFieldInvalid, nativeOperandSyntax{}, nativeSyntaxErr(
				"invalid_operand", pointer, "%s requires the literal true", name,
			)
		}
		return operator, nativeOperandSyntax{}, nil
	}
}

func (p *nativeParser) parseOperand(
	value qvalue, pointer string,
) (nativeOperandSyntax, error) {
	if value.kind() == qObject {
		return p.parseParameter(value, pointer)
	}
	return p.parseScalarOperand(value, pointer)
}

func (p *nativeParser) parseScalarOperand(
	value qvalue, pointer string,
) (nativeOperandSyntax, error) {
	scalar, err := p.parseScalar(value, pointer)
	if err != nil {
		return nativeOperandSyntax{}, err
	}
	return nativeOperandSyntax{kind: nativeOperandScalar, scalar: scalar}, nil
}

func (p *nativeParser) parseListOperand(
	value qvalue, pointer string,
) (nativeOperandSyntax, error) {
	if value.kind() == qObject {
		return p.parseParameter(value, pointer)
	}
	if value.kind() != qArray {
		return nativeOperandSyntax{}, nativeSyntaxErr(
			"invalid_operand", pointer,
			"membership requires an array of scalar literals or a parameter",
		)
	}
	if value.length() > nativeMaxMembershipItems {
		return nativeOperandSyntax{}, nativeSyntaxErr(
			"limit_exceeded", pointer,
			"membership has %d items; maximum is %d",
			value.length(), nativeMaxMembershipItems,
		)
	}
	out := nativeOperandSyntax{
		kind: nativeOperandList,
		list: make([]nativeScalarSyntax, 0, value.length()),
	}
	err := value.elements(func(index int, element qvalue) error {
		scalar, err := p.parseScalar(
			element, nativePointerElement(pointer, index),
		)
		if err != nil {
			return err
		}
		out.list = append(out.list, scalar)
		return nil
	})
	return out, err
}

func (p *nativeParser) parseParameter(
	value qvalue, pointer string,
) (nativeOperandSyntax, error) {
	members, err := p.members(value, pointer)
	if err != nil {
		return nativeOperandSyntax{}, err
	}
	if len(members) != 1 || members[0].name != "$param" {
		return nativeOperandSyntax{}, nativeSyntaxErr(
			"invalid_operand", pointer,
			"operand object must contain exactly one $param member",
		)
	}
	namePointer := nativePointerMember(pointer, "$param")
	name, err := p.requiredText(members[0].value, namePointer, "parameter name")
	if err != nil {
		return nativeOperandSyntax{}, err
	}
	if !nativeValidName(name) {
		return nativeOperandSyntax{}, nativeSyntaxErr(
			"invalid_parameter", namePointer, "%q is not a valid parameter name", name,
		)
	}
	if p.parameters == nil {
		p.parameters = make(map[string]struct{})
	}
	if _, exists := p.parameters[name]; !exists {
		if len(p.parameters) == nativeMaxParameters {
			return nativeOperandSyntax{}, nativeSyntaxErr(
				"limit_exceeded", namePointer, "query references more than %d parameters",
				nativeMaxParameters,
			)
		}
		p.parameters[name] = struct{}{}
	}
	return nativeOperandSyntax{
		kind: nativeOperandParameter, param: strings.Clone(name),
	}, nil
}

func (p *nativeParser) parseScalar(
	value qvalue, pointer string,
) (nativeScalarSyntax, error) {
	var scalar nativeScalarSyntax
	switch value.kind() {
	case qNull:
		scalar.kind = qNull
		p.literalBytes += len("null")
	case qBool:
		boolean, ok := value.boolean()
		if !ok {
			return nativeScalarSyntax{}, nativeSyntaxErr(
				"invalid_operand", pointer, "unreadable boolean literal",
			)
		}
		scalar.kind, scalar.boolean = qBool, boolean
		if boolean {
			p.literalBytes += len("true")
		} else {
			p.literalBytes += len("false")
		}
	case qNumber:
		text, ok := p.numberText(value)
		if !ok {
			return nativeScalarSyntax{}, nativeSyntaxErr(
				"invalid_operand", pointer, "unreadable number literal",
			)
		}
		scalar.kind, scalar.text = qNumber, text
		p.literalBytes += len(text)
	case qString:
		text, ok := value.text(&p.compiler)
		if !ok {
			return nativeScalarSyntax{}, nativeSyntaxErr(
				"invalid_operand", pointer, "unreadable string literal",
			)
		}
		scalar.kind, scalar.text = qString, strings.Clone(text)
		p.literalBytes += len(text)
	default:
		return nativeScalarSyntax{}, nativeSyntaxErr(
			"invalid_operand", pointer,
			"operand must be null, boolean, exact number, or string, not %s",
			value.describeKind(),
		)
	}
	if p.literalBytes > nativeMaxLiteralBytes {
		return nativeScalarSyntax{}, nativeSyntaxErr(
			"limit_exceeded", pointer,
			"inline literals exceed %d bytes", nativeMaxLiteralBytes,
		)
	}
	return scalar, nil
}

func (p *nativeParser) notePredicate(pointer string) error {
	p.predicateNodes++
	if p.predicateNodes > nativeMaxPredicateNodes {
		return nativeSyntaxErr(
			"limit_exceeded", pointer,
			"query has more than %d predicate nodes", nativeMaxPredicateNodes,
		)
	}
	return nil
}

func (p nativePredicateSyntax) String() string {
	switch p.kind {
	case nativePredicateAnd:
		return "$and"
	case nativePredicateOr:
		return "$or"
	case nativePredicateNot:
		return "$not"
	case nativePredicateField:
		return p.path + ":" + strconv.Itoa(int(p.operator))
	default:
		return "<invalid>"
	}
}
