package distribution

// UnionDomains retains every value required by either consumer. Unknown is
// the complete domain; empty is the identity. Values stay byte-canonical.
func UnionDomains(a, b ValueDomain) (ValueDomain, error) {
	if a.Kind == DomainUnknown || b.Kind == DomainUnknown {
		return UnknownDomain(), nil
	}
	if a.Kind == DomainEmpty {
		return b, nil
	}
	if b.Kind == DomainEmpty {
		return a, nil
	}
	return FiniteDomain(append(append([]Scalar(nil), a.Values...), b.Values...)...)
}

// IntersectDomains combines necessary predicates on one placement ordinal.
// Unknown contributes no restriction and a contradiction remains empty.
func IntersectDomains(a, b ValueDomain) (ValueDomain, error) {
	if a.Kind == DomainEmpty || b.Kind == DomainEmpty {
		return EmptyDomain(), nil
	}
	if a.Kind == DomainUnknown {
		return b, nil
	}
	if b.Kind == DomainUnknown {
		return a, nil
	}
	builder := NewConstraintBuilder()
	if err := builder.AddMembership(a.Values); err != nil {
		return ValueDomain{}, err
	}
	if err := builder.AddMembership(b.Values); err != nil {
		return ValueDomain{}, err
	}
	return builder.Domain(), nil
}
