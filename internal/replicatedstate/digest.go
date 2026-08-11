package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"slices"

	"github.com/thesyncim/vibedb/store/durable"
)

var (
	logicalDigestDomain          = []byte("vibedb/replicated-state/logical-image/v1\x00")
	logicalDigestValidatedDomain = []byte(
		"vibedb/replicated-state/logical-image/validated/v1\x00",
	)
)

type finalMutation struct {
	key    []byte
	value  []byte
	delete bool
}

func logicalDigestV1(
	name string,
	validation ValidationProfile,
	validationDigest [32]byte,
	snapshot *durable.Snapshot,
	overlay []finalMutation,
) ([32]byte, error) {
	h := sha256.New()
	if validation == ValidationSchemaFreeJSONV1 && validationDigest == ([32]byte{}) {
		// This branch is byte-for-byte compatible with the original v1 logical
		// digest. Existing schema-free roots retain their exact publication.
		_, _ = h.Write(logicalDigestDomain)
	} else {
		_, _ = h.Write(logicalDigestValidatedDomain)
		_, _ = h.Write([]byte{byte(validation)})
		_, _ = h.Write(validationDigest[:])
	}
	writeHashFrame(h, []byte(name))

	ordered := slices.Clone(overlay)
	slices.SortFunc(ordered, func(a, b finalMutation) int {
		return bytes.Compare(a.key, b.key)
	})
	next := 0
	hashMutation := func(key, value []byte) {
		_, _ = h.Write([]byte{1})
		writeHashFrame(h, key)
		writeHashFrame(h, value)
	}
	if snapshot != nil {
		err := snapshot.RangeRaw(func(key, value []byte) error {
			for next < len(ordered) && bytes.Compare(ordered[next].key, key) < 0 {
				if !ordered[next].delete {
					hashMutation(ordered[next].key, ordered[next].value)
				}
				next++
			}
			if next < len(ordered) && bytes.Equal(ordered[next].key, key) {
				if !ordered[next].delete {
					hashMutation(ordered[next].key, ordered[next].value)
				}
				next++
				return nil
			}
			hashMutation(key, value)
			return nil
		})
		if err != nil {
			return [32]byte{}, err
		}
	}
	for next < len(ordered) {
		if !ordered[next].delete {
			hashMutation(ordered[next].key, ordered[next].value)
		}
		next++
	}
	_, _ = h.Write([]byte{0})
	var result [32]byte
	_ = h.Sum(result[:0])
	return result, nil
}
