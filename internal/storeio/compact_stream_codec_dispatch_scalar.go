//go:build !go1.27 || go1.28 || !goexperiment.simd || !arm64

package storeio

// The packed equality scan stays on the scalar implementation unless the
// Go 1.27 SIMD arm64 implementation is selected by its build constraints.
// Keeping the binding in a target-specific file makes the production call
// site identical in portable and SIMD builds and gives tests a concrete
// function value to inspect.
var (
	countCompactPacked7EqualImpl  = countCompactPacked7EqualScalar
	countCompactPacked10EqualImpl = countCompactPacked10EqualScalar
)
