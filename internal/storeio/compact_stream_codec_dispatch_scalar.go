//go:build !go1.27 || go1.28 || !goexperiment.simd || (!arm64 && !amd64)

package storeio

// The packed equality scan stays on the scalar implementation unless the
// Go 1.27 SIMD arm64 or amd64 implementation is built for the target.
// Keeping the binding in a target-specific file makes the production call
// site identical in portable and SIMD builds and gives tests a concrete
// function value to inspect.
var (
	countCompactPacked7EqualImpl    = countCompactPacked7EqualScalar
	countCompactPacked8EqualImpl    = countCompactPacked8EqualScalar
	countCompactPacked10EqualImpl   = countCompactPacked10EqualScalar
	countCompactPacked16EqualImpl   = countCompactPacked16EqualScalar
	countCompactPacked7LessImpl     = countCompactPacked7LessScalar
	countCompactPacked8LessImpl     = countCompactPacked8LessScalar
	countCompactPacked10LessImpl    = countCompactPacked10LessScalar
	countCompactPacked16LessImpl    = countCompactPacked16LessScalar
	countCompactPacked7BetweenImpl  = countCompactPacked7BetweenScalar
	countCompactPacked8BetweenImpl  = countCompactPacked8BetweenScalar
	countCompactPacked10BetweenImpl = countCompactPacked10BetweenScalar
	countCompactPacked16BetweenImpl = countCompactPacked16BetweenScalar
	countCompactPacked7ExtremaImpl  = countCompactPacked7ExtremaScalar
	countCompactPacked8ExtremaImpl  = countCompactPacked8ExtremaScalar
	countCompactPacked10ExtremaImpl = countCompactPacked10ExtremaScalar
	countCompactPacked16ExtremaImpl = countCompactPacked16ExtremaScalar
)
