package replicatedstate

import "github.com/thesyncim/vibedb/distribution"

// validateGlobalRetainedPrune refuses to infer global-index cleanup authority
// from a base row. The exact schema-bound physical key must belong outside the
// sealed retained range, even when the conditional delete finds no current row.
func validateGlobalRetainedPrune(profile GlobalIndexProfile, key []byte, retained distribution.KeyRange) MutationValidation {
	point, ok := profile.GlobalIndexStorageKeyPoint(key)
	if !ok {
		return MutationValidationInvalid
	}
	if retained.Contains(point) {
		return MutationValidationWrongShard
	}
	return MutationValidationAccept
}
