package distribution

import "fmt"

// InvalidNumberError reports a spelling that is not a well-formed JSON
// number and therefore cannot become a Number scalar.
type InvalidNumberError struct {
	Spelling string
}

func (e *InvalidNumberError) Error() string {
	return fmt.Sprintf("distribution: invalid number spelling %q", e.Spelling)
}

// UnsupportedScalarError reports a Scalar outside the closed placement
// scalar set, which can only occur for an unconstructed (zero-value) Scalar
// since every exported constructor produces KindString or KindNumber.
type UnsupportedScalarError struct {
	Kind ScalarKind
}

func (e *UnsupportedScalarError) Error() string {
	return fmt.Sprintf("distribution: unsupported scalar kind %q", e.Kind)
}
