package driver

import (
	"encoding/binary"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
)

// VUC2 templates contain only closed scalar/predicate instructions. Parameters
// have dense, template-local ordinals; their values follow the template. Limits
// are checked before descending or allocating, including for untrusted replay.
const (
	replicatedConflictNodeLimit  = 16384
	replicatedConflictDepthLimit = 128
)

type conflictProgramCodec struct {
	data       []byte
	offset     int
	writing    bool
	err        error
	nodes      int
	params     int
	usedParams int
	ordinals   map[int]int
}

func (c *conflictProgramCodec) fail() { c.err = errReplicatedConflictProgram }
func (c *conflictProgramCodec) number(value, size int) int {
	if c.err != nil {
		return 0
	}
	if c.writing {
		if value < 0 || (size < 4 && value >= 1<<(size*8)) || len(c.data) > replication.MaxMutationValueBytes-size {
			c.fail()
			return 0
		}
		for i := 0; i < size; i++ {
			c.data = append(c.data, byte(value>>(i*8)))
		}
		return value
	}
	if len(c.data)-c.offset < size {
		c.fail()
		return 0
	}
	value = 0
	for i := 0; i < size; i++ {
		value |= int(c.data[c.offset+i]) << (i * 8)
	}
	c.offset += size
	if value < 0 {
		c.fail()
		return 0
	}
	return value
}
func (c *conflictProgramCodec) text(value string, size int) string {
	n := c.number(len(value), size)
	if c.err != nil {
		return ""
	}
	if c.writing {
		if len(c.data) > replication.MaxMutationValueBytes-n || !utf8.ValidString(value) {
			c.fail()
			return ""
		}
		c.data = append(c.data, value...)
		return value
	}
	if n > len(c.data)-c.offset {
		c.fail()
		return ""
	}
	value = string(c.data[c.offset : c.offset+n])
	c.offset += n
	if !utf8.ValidString(value) {
		c.fail()
	}
	return value
}
func (c *conflictProgramCodec) enter(depth int) bool {
	c.nodes++
	if depth > replicatedConflictDepthLimit || c.nodes > replicatedConflictNodeLimit {
		c.fail()
	}
	return c.err == nil
}
func (c *conflictProgramCodec) path(path *sqlast.PathExpr) *sqlast.PathExpr {
	source := 255
	if path != nil {
		source = path.Source
	}
	source = c.number(source, 1)
	if source == 255 || c.err != nil {
		return nil
	}
	if source > 1 {
		c.fail()
		return nil
	}
	name := ""
	if c.writing {
		if len(path.Segments) != 1 || path.Segments[0].IsIndex || path.MergedUsing != 0 {
			c.fail()
			return nil
		}
		name = path.Segments[0].Key
	}
	name = c.text(name, 2)
	if name == "" {
		c.fail()
	}
	if c.writing {
		return path
	}
	return &sqlast.PathExpr{Source: source, Segments: []sqlast.Segment{{Key: name}}}
}
func (c *conflictProgramCodec) operand(value sqlast.Operand, excluded bool) sqlast.Operand {
	if !c.enter(0) {
		return sqlast.Operand{}
	}
	kind := c.number(int(value.Kind), 1)
	switch sqlast.OperandKind(kind) {
	case sqlast.OperandParam:
		ordinal := value.Ordinal
		if c.writing {
			if ordinal < 0 {
				c.fail()
				return sqlast.Operand{}
			}
			if c.ordinals == nil {
				c.ordinals = make(map[int]int)
			}
			dense, ok := c.ordinals[ordinal]
			if !ok {
				dense = len(c.ordinals)
				c.ordinals[ordinal] = dense
			}
			ordinal = dense
		}
		ordinal = c.number(ordinal, 2)
		if !c.writing && ordinal >= c.usedParams {
			if ordinal != c.usedParams {
				c.fail()
			} else {
				c.usedParams++
			}
		}
		if ordinal >= replicatedConflictAssignmentLimit || (!c.writing && ordinal >= c.params) {
			c.fail()
		}
		return sqlast.Operand{Kind: sqlast.OperandParam, Ordinal: ordinal}
	case sqlast.OperandExcluded:
		if !excluded {
			c.fail()
		}
		name := c.text(value.Text, 2)
		if name == "" {
			c.fail()
		}
		return sqlast.Operand{Kind: sqlast.OperandExcluded, Text: name}
	case sqlast.OperandNull:
		return sqlast.Operand{Kind: sqlast.OperandNull}
	case sqlast.OperandBool:
		b := 0
		if value.Bool {
			b = 1
		}
		b = c.number(b, 1)
		if b > 1 {
			c.fail()
		}
		return sqlast.Operand{Kind: sqlast.OperandBool, Bool: b == 1}
	case sqlast.OperandString, sqlast.OperandNumber, sqlast.OperandJSON:
		value.Text = c.text(value.Text, 4)
		value.Kind = sqlast.OperandKind(kind)
		if value.Kind == sqlast.OperandNumber && !validJSONNumber(value.Text) {
			c.fail()
		}
		if value.Kind == sqlast.OperandJSON && vibejson.Validate([]byte(value.Text)) != nil {
			c.fail()
		}
		return value
	default:
		c.fail()
		return sqlast.Operand{}
	}
}
func (c *conflictProgramCodec) scalar(input *sqlast.ScalarExpr, depth int) *sqlast.ScalarExpr {
	kind := 255
	if input != nil {
		kind = int(input.Kind)
	}
	kind = c.number(kind, 1)
	if kind == 255 || !c.enter(depth) {
		return nil
	}
	var zero sqlast.ScalarExpr
	if input == nil {
		input = &zero
	}
	out := input
	if !c.writing {
		out = &sqlast.ScalarExpr{Kind: sqlast.ScalarExprKind(kind)}
	}
	switch sqlast.ScalarExprKind(kind) {
	case sqlast.ScalarPath:
		path := c.path(input.Path)
		if path == nil {
			c.fail()
		}
		if !c.writing {
			out.Path = path
		}
	case sqlast.ScalarLiteral:
		value := c.operand(input.Value, false)
		if !c.writing {
			out.Value = value
		}
	case sqlast.ScalarNull:
	case sqlast.ScalarUnary, sqlast.ScalarBinary:
		op := c.number(int(input.Op), 1)
		if (kind == int(sqlast.ScalarUnary) && op != int(sqlast.ScalarPositive) && op != int(sqlast.ScalarNegative)) ||
			(kind == int(sqlast.ScalarBinary) && (op > int(sqlast.ScalarNotDistinct) || op == int(sqlast.ScalarPositive) || op == int(sqlast.ScalarNegative))) {
			c.fail()
		}
		left := c.scalar(input.Left, depth+1)
		if left == nil {
			c.fail()
		}
		var right *sqlast.ScalarExpr
		if kind == int(sqlast.ScalarBinary) {
			right = c.scalar(input.Right, depth+1)
			if right == nil {
				c.fail()
			}
		}
		if !c.writing {
			out.Op = sqlast.ScalarOp(op)
			out.Left = left
			out.Right = right
		}
	case sqlast.ScalarCast:
		target := c.number(int(input.Cast), 1)
		typed := 0
		if input.TypedConstant {
			typed = 1
		}
		typed = c.number(typed, 1)
		if target > int(sqlast.ScalarCastJSON) || typed > 1 {
			c.fail()
		}
		left := c.scalar(input.Left, depth+1)
		if left == nil {
			c.fail()
		}
		if !c.writing {
			out.Cast = sqlast.ScalarCastTarget(target)
			out.TypedConstant = typed == 1
			out.Left = left
		}
	case sqlast.ScalarCase:
		left := c.scalar(input.Left, depth+1)
		n := c.number(len(input.Whens), 2)
		if n == 0 || n > replicatedConflictAssignmentLimit || n > replicatedConflictNodeLimit-c.nodes {
			c.fail()
			return nil
		}
		if !c.writing {
			out.Left = left
			out.Whens = make([]sqlast.ScalarWhen, n)
		}
		for i := 0; i < n && c.err == nil; i++ {
			var arm sqlast.ScalarWhen
			if c.writing {
				arm = input.Whens[i]
			}
			if left == nil {
				arm.Predicate = c.predicate(arm.Predicate, depth+1)
				if arm.Predicate == nil {
					c.fail()
				}
			} else {
				arm.Match = c.scalar(arm.Match, depth+1)
				if arm.Match == nil {
					c.fail()
				}
			}
			arm.Result = c.scalar(arm.Result, depth+1)
			if arm.Result == nil {
				c.fail()
			}
			if !c.writing {
				out.Whens[i] = arm
			}
		}
		other := c.scalar(input.Else, depth+1)
		if !c.writing {
			out.Else = other
		}
	default:
		// Aggregates, subqueries, windows and new unversioned operators cannot be
		// smuggled into the apply engine by constructing an AST directly.
		c.fail()
	}
	return out
}
func (c *conflictProgramCodec) predicate(input *sqlast.Expr, depth int) *sqlast.Expr {
	kind := 255
	if input != nil {
		kind = int(input.Kind)
	}
	kind = c.number(kind, 1)
	if kind == 255 || !c.enter(depth) {
		return nil
	}
	var zero sqlast.Expr
	if input == nil {
		input = &zero
	}
	if input.Subquery != nil || input.Agg != sqlast.AggNone {
		c.fail()
	}
	out := input
	if !c.writing {
		out = &sqlast.Expr{Kind: sqlast.ExprKind(kind), Column: -1}
	}
	flags := 0
	if input.Negated {
		flags |= 1
	}
	if input.Insensitive {
		flags |= 2
	}
	flags = c.number(flags, 1)
	if flags > 3 {
		c.fail()
	}
	if !c.writing {
		out.Negated = flags&1 != 0
		out.Insensitive = flags&2 != 0
	}
	switch sqlast.ExprKind(kind) {
	case sqlast.ExprAnd, sqlast.ExprOr, sqlast.ExprNot:
		n := c.number(len(input.Kids), 2)
		if n == 0 || n > replicatedConflictNodeLimit-c.nodes || (kind == int(sqlast.ExprNot) && n != 1) {
			c.fail()
			return nil
		}
		if !c.writing {
			out.Kids = make([]*sqlast.Expr, n)
		}
		for i := 0; i < n && c.err == nil; i++ {
			var child *sqlast.Expr
			if c.writing {
				child = input.Kids[i]
			}
			child = c.predicate(child, depth+1)
			if child == nil {
				c.fail()
			}
			if !c.writing {
				out.Kids[i] = child
			}
		}
	case sqlast.ExprConstant:
		value := c.operand(input.Value, false)
		if value.Kind != sqlast.OperandBool {
			c.fail()
		}
		if !c.writing {
			out.Value = value
		}
	case sqlast.ExprScalarCompare, sqlast.ExprScalarIsNull, sqlast.ExprScalarTruth:
		left := c.scalar(input.ScalarLeft, depth+1)
		if left == nil {
			c.fail()
		}
		if !c.writing {
			out.ScalarLeft = left
		}
		if kind == int(sqlast.ExprScalarCompare) {
			op := c.number(int(input.Op), 1)
			if op > int(sqlast.OpGe) {
				c.fail()
			}
			right := c.scalar(input.ScalarRight, depth+1)
			if right == nil {
				c.fail()
			}
			if !c.writing {
				out.Op = sqlast.CmpOp(op)
				out.ScalarRight = right
			}
		}
	case sqlast.ExprCompare, sqlast.ExprIn, sqlast.ExprBetween, sqlast.ExprIsNull, sqlast.ExprIsMissing, sqlast.ExprContains, sqlast.ExprLike:
		path := c.path(input.Path)
		if path == nil {
			c.fail()
		}
		if !c.writing {
			out.Path = path
		}
		switch sqlast.ExprKind(kind) {
		case sqlast.ExprCompare:
			op := c.number(int(input.Op), 1)
			if op > int(sqlast.OpGe) {
				c.fail()
			}
			right := c.path(input.RightPath)
			if !c.writing {
				out.Op = sqlast.CmpOp(op)
				out.RightPath = right
			}
			if right == nil {
				value := c.operand(input.Value, false)
				if !c.writing {
					out.Value = value
				}
			}
		case sqlast.ExprContains, sqlast.ExprLike:
			value := c.operand(input.Value, false)
			if !c.writing {
				out.Value = value
			}
		case sqlast.ExprIn, sqlast.ExprBetween:
			n := c.number(len(input.List), 2)
			if n == 0 || n > replicatedConflictAssignmentLimit || (kind == int(sqlast.ExprBetween) && n != 2) {
				c.fail()
				return nil
			}
			if !c.writing {
				out.List = make([]sqlast.Operand, n)
			}
			for i := 0; i < n && c.err == nil; i++ {
				var value sqlast.Operand
				if c.writing {
					value = input.List[i]
				}
				value = c.operand(value, false)
				if !c.writing {
					out.List[i] = value
				}
			}
		}
	default:
		c.fail()
	}
	return out
}

func encodeConflictTemplate(action *sqlast.InsertConflictUpdate, parameterTypes []query.ParameterType) ([]byte, map[int]int, error) {
	c := conflictProgramCodec{writing: true}
	seen := make(map[string]bool, len(action.Assignments))
	c.number(len(action.Assignments), 2)
	c.number(0, 2)
	for _, assignment := range action.Assignments {
		if assignment.Column == "" || seen[assignment.Column] {
			c.fail()
		}
		seen[assignment.Column] = true
		c.text(assignment.Column, 2)
		kind := 0
		if assignment.Expr != nil {
			kind = 1
		}
		c.number(kind, 1)
		if kind == 1 {
			c.scalar(assignment.Expr, 0)
		} else {
			c.operand(assignment.Value, true)
		}
	}
	if c.err != nil {
		return nil, nil, c.err
	}
	binary.LittleEndian.PutUint16(c.data[2:4], uint16(len(c.ordinals)))
	types := make([]byte, len(c.ordinals))
	for ordinal, dense := range c.ordinals {
		if ordinal < len(parameterTypes) {
			if parameterTypes[ordinal] >= query.ParameterTypeInvalid {
				return nil, nil, errReplicatedConflictProgram
			}
			types[dense] = byte(parameterTypes[ordinal])
		}
	}
	if len(c.data) > replication.MaxMutationValueBytes-len(types) {
		return nil, nil, errReplicatedConflictProgram
	}
	c.data = append(c.data, types...)
	return c.data, c.ordinals, nil
}

func decodeConflictTemplate(template []byte) ([]sqlast.UpdateAssignment, []query.ParameterType, error) {
	c := conflictProgramCodec{data: template}
	count := c.number(0, 2)
	c.params = c.number(0, 2)
	if count == 0 || count > replicatedConflictAssignmentLimit || c.params > replicatedConflictAssignmentLimit {
		return nil, nil, errReplicatedConflictProgram
	}
	assignments := make([]sqlast.UpdateAssignment, count)
	seen := make(map[string]bool, count)
	for i := range assignments {
		column := c.text("", 2)
		if column == "" || seen[column] {
			c.fail()
		}
		seen[column] = true
		assignments[i].Column = column
		switch c.number(0, 1) {
		case 0:
			assignments[i].Value = c.operand(sqlast.Operand{}, true)
		case 1:
			assignments[i].Value.Kind = sqlast.OperandExpression
			assignments[i].Expr = c.scalar(nil, 0)
			if assignments[i].Expr == nil {
				c.fail()
			}
		default:
			c.fail()
		}
		if c.err != nil {
			break
		}
	}
	types := make([]query.ParameterType, c.params)
	for i := range types {
		types[i] = query.ParameterType(c.number(0, 1))
		if types[i] >= query.ParameterTypeInvalid {
			c.fail()
		}
	}
	if c.err != nil || c.offset != len(template) || c.usedParams != c.params {
		return nil, nil, errReplicatedConflictProgram
	}
	return assignments, types, nil
}
