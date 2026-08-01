package distribution

import (
	"errors"
	"fmt"
)

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

// ErrInvalidManifest is the sentinel every manifest validation failure matches
// under errors.Is.
var ErrInvalidManifest = errors.New("distribution: invalid shard manifest")

// ManifestError reports why NewManifest rejected its input. It wraps
// ErrInvalidManifest.
type ManifestError struct {
	Reason string
}

func (e *ManifestError) Error() string {
	return "distribution: invalid shard manifest: " + e.Reason
}

func (e *ManifestError) Unwrap() error { return ErrInvalidManifest }

// ErrInvalidDestination is the sentinel every DestinationSet malformation
// matches under errors.Is.
var ErrInvalidDestination = errors.New("distribution: invalid destination set")

// DestinationError reports why a DestinationSet was rejected. It wraps
// ErrInvalidDestination.
type DestinationError struct {
	Reason string
}

func (e *DestinationError) Error() string {
	return "distribution: invalid destination set: " + e.Reason
}

func (e *DestinationError) Unwrap() error { return ErrInvalidDestination }
