package replicatedstate

import "bytes"

// ProjectionRow is an ordered cold-path replacement image. A digest of these
// bytes is not authorization: the importer must validate the target schema and
// the caller must authenticate its enclosing target plan.
type ProjectionRow struct{ Key, Value []byte }

// ProjectionImageDigest uses the existing singleton image grammar, allowing a
// sealed restore receipt to be checked without reopening a live database.
func ProjectionImageDigest(name string, validationDigest [32]byte, rows []ProjectionRow) ([32]byte, error) {
	h, err := newCanonicalImageHasher(name, ValidationDeterministicMutation, validationDigest, projectionDigestValidator{})
	if err != nil {
		return [32]byte{}, err
	}
	for i, row := range rows {
		if len(row.Key) == 0 || i > 0 && bytes.Compare(rows[i-1].Key, row.Key) >= 0 {
			return [32]byte{}, ErrInvalidCollection
		}
		if err := h.add(row.Key, row.Value); err != nil {
			return [32]byte{}, err
		}
	}
	return h.sum(), nil
}

type projectionDigestValidator struct{}

func (projectionDigestValidator) ValidatePut(_, _ []byte) MutationValidation {
	return MutationValidationAccept
}
func (projectionDigestValidator) ValidateDelete(_, _ []byte, _ bool) MutationValidation {
	return MutationValidationAccept
}
