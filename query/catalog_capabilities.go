package query

// statementCatalogCapabilities is cold prepared-plan metadata used by storage
// adapters before execution. It is derived from the already-owned statement
// graph instead of copied into every wrapper, keeping ordinary Statements free
// of another feature sidecar and making wrapper propagation exact by identity.
type statementCatalogCapabilities struct {
	requires bool
	direct   bool
	blocked  bool
	joins    int
}

const maxStatementCatalogCapabilityDepth = 256

func (s *Statement) catalogCapabilities(depth int) statementCatalogCapabilities {
	if s == nil {
		return statementCatalogCapabilities{}
	}
	if set := s.setSQL(); set != nil {
		return set.catalogCapabilities(depth)
	}
	if window := s.window(); window != nil {
		if depth >= maxStatementCatalogCapabilityDepth {
			return statementCatalogCapabilities{requires: s.requiresCatalog}
		}
		return window.input.catalogCapabilities(depth + 1)
	}

	capabilities := statementCatalogCapabilities{
		requires: s.requiresCatalog,
		joins:    s.numDecorrelatedExists(),
		direct:   s.hasDecorrelatedExists(),
	}
	if s.tree != nil && len(s.tree.From) > 1 {
		capabilities.joins += len(s.tree.From) - 1
		if s.relationJoin() == nil {
			// The legacy physical join may fan out. Its durable executor
			// intentionally supports semi-joins only, so any wrapper containing
			// this operator must retain the driver's coherent heap adapter.
			capabilities.blocked = true
		}
	}
	if s.relationJoin() != nil {
		// The generalized relation pipeline owns and routes every operand from
		// the coherent durable catalog supplied by its caller.
		capabilities.direct = true
	}
	if depth >= maxStatementCatalogCapabilityDepth || s.nested == nil {
		return capabilities
	}

	merge := func(child *Statement) {
		childCapabilities := child.catalogCapabilities(depth + 1)
		capabilities.requires = capabilities.requires || childCapabilities.requires
		capabilities.joins += childCapabilities.joins
		capabilities.direct = capabilities.direct || childCapabilities.direct
		capabilities.blocked = capabilities.blocked || childCapabilities.blocked
	}

	for i := range s.nested.subqueries {
		merge(s.nested.subqueries[i].stmt)
	}
	if derived := s.derived(); derived != nil {
		merge(derived.stmt)
	}
	if reference := s.cteReference(); reference != nil && reference.def != nil {
		merge(reference.def.stmt)
	}
	if join := s.relationJoin(); join != nil {
		for i := range join.operands {
			operand := &join.operands[i]
			merge(operand.stmt)
			if operand.cte != nil && operand.cte.def != nil {
				merge(operand.cte.def.stmt)
			}
		}
	}
	if capabilities.blocked {
		capabilities.direct = false
	}
	return capabilities
}

func (set *statementSetSQL) catalogCapabilities(
	_ int,
) statementCatalogCapabilities {
	if set == nil {
		return statementCatalogCapabilities{}
	}
	return statementCatalogCapabilities{
		requires: set.requiresCatalog,
		direct:   set.directCatalog,
		blocked:  set.directBlocked,
		joins:    set.joins,
	}
}
