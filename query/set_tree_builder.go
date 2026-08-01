package query

import "fmt"

// SetTreeLeafSpec is one operand in an unparenthesized SQL set chain.
type SetTreeLeafSpec struct {
	Source  int
	Columns int
}

type setTreeBuildToken struct {
	leaf      SetTreeLeafSpec
	operation SetTreeOperation
	isLeaf    bool
}

// SetTreeBuilder is reusable lowering-time storage for SQL precedence and
// associativity resolution. BuildChain applies INTERSECT before UNION/EXCEPT;
// equal-precedence operators reduce left-to-right. Parenthesized SQL is
// represented directly with NewSetTreeLeaf/NewSetTreeBinary nodes.
type SetTreeBuilder struct {
	nodes      []SetTreeNode
	tokens     []setTreeBuildToken
	operators  []SetTreeOperation
	valueStack []int
}

// BuildChain resolves one unparenthesized sequence into postorder tree shape.
// The returned plan borrows b until its next BuildChain or Release.
func (b *SetTreeBuilder) BuildChain(
	leaves []SetTreeLeafSpec,
	operations []SetTreeOperation,
) (SetTreePlan, error) {
	if b == nil {
		return SetTreePlan{}, fmt.Errorf("query: nil set-expression builder: %w", ErrSetTreePlan)
	}
	b.nodes = b.nodes[:0]
	b.tokens = b.tokens[:0]
	b.operators = b.operators[:0]
	b.valueStack = b.valueStack[:0]
	if len(leaves) == 0 || len(operations) != len(leaves)-1 {
		return SetTreePlan{}, fmt.Errorf(
			"query: set chain has %d leaves and %d operators: %w",
			len(leaves), len(operations), ErrSetTreePlan,
		)
	}
	for index, leaf := range leaves {
		if leaf.Source < 0 || leaf.Columns < 0 {
			return SetTreePlan{}, fmt.Errorf(
				"query: set chain leaf %d has source/columns %d/%d: %w",
				index, leaf.Source, leaf.Columns, ErrSetTreePlan,
			)
		}
		b.tokens = append(b.tokens, setTreeBuildToken{leaf: leaf, isLeaf: true})
		if index == len(operations) {
			continue
		}
		operation := operations[index]
		if !operation.valid() {
			return SetTreePlan{}, fmt.Errorf(
				"query: set chain operator %d has mode %d: %w",
				index, operation, ErrSetTreePlan,
			)
		}
		for len(b.operators) != 0 &&
			b.operators[len(b.operators)-1].precedence() >= operation.precedence() {
			b.appendOperatorToken()
		}
		b.operators = append(b.operators, operation)
	}
	for len(b.operators) != 0 {
		b.appendOperatorToken()
	}
	for _, token := range b.tokens {
		if token.isLeaf {
			node := len(b.nodes)
			b.nodes = append(b.nodes, NewSetTreeLeaf(token.leaf.Source, token.leaf.Columns))
			b.valueStack = append(b.valueStack, node)
			continue
		}
		if len(b.valueStack) < 2 {
			return SetTreePlan{}, ErrSetTreePlan
		}
		rightAt := len(b.valueStack) - 1
		right := b.valueStack[rightAt]
		left := b.valueStack[rightAt-1]
		b.valueStack = b.valueStack[:rightAt-1]
		node := len(b.nodes)
		b.nodes = append(b.nodes, NewSetTreeBinary(token.operation, left, right))
		b.valueStack = append(b.valueStack, node)
	}
	if len(b.valueStack) != 1 {
		return SetTreePlan{}, ErrSetTreePlan
	}
	return SetTreePlan{Nodes: b.nodes, Root: b.valueStack[0]}, nil
}

func (b *SetTreeBuilder) appendOperatorToken() {
	last := len(b.operators) - 1
	operation := b.operators[last]
	b.operators = b.operators[:last]
	b.tokens = append(b.tokens, setTreeBuildToken{operation: operation})
}

// Release drops all lowering-time high-water storage.
func (b *SetTreeBuilder) Release() {
	if b != nil {
		*b = SetTreeBuilder{}
	}
}
