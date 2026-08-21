package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/store/durable"
)

var logicalDigestDomain = []byte("vibedb/replicated-state/logical-image\x00")

type finalMutation struct {
	key         []byte
	value       []byte
	before      []byte
	delete      bool
	beforeFound bool
}

func logicalDigest(
	name string,
	validation ValidationProfile,
	validationDigest [32]byte,
	validator MutationValidator,
	snapshot *durable.Snapshot,
	overlay []finalMutation,
) ([32]byte, error) {
	if validation != ValidationDeterministicMutation ||
		validationDigest == ([32]byte{}) || validator == nil {
		return [32]byte{}, ErrInvalidCollection
	}
	h := sha256.New()
	_, _ = h.Write(logicalDigestDomain)
	_, _ = h.Write([]byte{byte(validation)})
	_, _ = h.Write(validationDigest[:])
	writeHashFrame(h, []byte(name))

	ordered := slices.Clone(overlay)
	slices.SortFunc(ordered, func(a, b finalMutation) int {
		return bytes.Compare(a.key, b.key)
	})
	next := 0
	hashMutation := func(key, value []byte) error {
		switch result := validator.ValidatePut(key, value); result {
		case MutationValidationAccept:
		case MutationValidationInvalid:
			return fmt.Errorf("%w: logical image contains an invalid row", ErrSchemaProfile)
		case MutationValidationTargetBound:
			return fmt.Errorf("%w: logical image row exceeds the validator target", ErrSchemaProfile)
		case MutationValidationWrongShard:
			return fmt.Errorf("%w: logical image row belongs to another shard", ErrSchemaProfile)
		default:
			return fmt.Errorf("%w: mutation validator returned %d", ErrInvalidCollection, result)
		}
		_, _ = h.Write([]byte{1})
		writeHashFrame(h, key)
		writeHashFrame(h, value)
		return nil
	}
	if snapshot != nil {
		err := snapshot.RangeRaw(func(key, value []byte) error {
			for next < len(ordered) && bytes.Compare(ordered[next].key, key) < 0 {
				if !ordered[next].delete {
					if err := hashMutation(ordered[next].key, ordered[next].value); err != nil {
						return err
					}
				}
				next++
			}
			if next < len(ordered) && bytes.Equal(ordered[next].key, key) {
				if !ordered[next].delete {
					if err := hashMutation(ordered[next].key, ordered[next].value); err != nil {
						return err
					}
				}
				next++
				return nil
			}
			return hashMutation(key, value)
		})
		if err != nil {
			return [32]byte{}, err
		}
	}
	for next < len(ordered) {
		if !ordered[next].delete {
			if err := hashMutation(ordered[next].key, ordered[next].value); err != nil {
				return [32]byte{}, err
			}
		}
		next++
	}
	_, _ = h.Write([]byte{0})
	var result [32]byte
	_ = h.Sum(result[:0])
	return result, nil
}
