package replicatedstate

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/replication"
)

// MutationCompletionResultBytes is the fixed width of an applied ordinary
// mutation result. Refusals and session-lifecycle results carry no result
// bytes. The signed representation matches transaction affected-row results
// while rejecting negative or noncanonical values at every boundary.
const MutationCompletionResultBytes = 8

// MaxMutationCompletionEnvelopeBytes is the exact largest inline envelope for
// an ordinary mutation or session-lifecycle completion. Applied mutations add
// the fixed affected-row result; refusals and lifecycle results remain empty.
const MaxMutationCompletionEnvelopeBytes = replication.MaxEmptyResultCompletionEnvelopeBytes + MutationCompletionResultBytes

// MaxMutationAffectedRows is the largest ordinary command result admitted by
// the dense bundle grammar. Every JSON relation may contribute its complete
// distinct-mutation bound; global-index relations contribute no user-visible
// rows. The product is a compile-time bound well below int64 overflow.
const MaxMutationAffectedRows = int64(MaxDistinctMutations) *
	int64(replication.MaxRelationsPerBundle)

// AppendMutationCompletionResult appends the sole byte-native ordinary
// mutation result grammar. On error dst is unchanged. With enough capacity it
// allocates zero.
func AppendMutationCompletionResult(dst []byte, resultCode uint32, affectedRows int64) ([]byte, error) {
	if !isMutationResultCode(resultCode) || affectedRows < 0 ||
		affectedRows > MaxMutationAffectedRows ||
		resultCode != ResultApplied && affectedRows != 0 {
		return dst, fmt.Errorf("%w: invalid mutation result", ErrCompletionCorrupt)
	}
	if resultCode != ResultApplied {
		return dst, nil
	}
	return binary.LittleEndian.AppendUint64(dst, uint64(affectedRows)), nil
}

// OpenMutationCompletionResult validates and decodes one exact ordinary
// mutation result. An applied result is always the fixed eight-byte affected
// row count; every refusal is canonically empty.
func OpenMutationCompletionResult(resultCode uint32, src []byte) (int64, error) {
	if !isMutationResultCode(resultCode) {
		return 0, fmt.Errorf("%w: invalid mutation result code", ErrCompletionCorrupt)
	}
	if resultCode != ResultApplied {
		if len(src) != 0 {
			return 0, fmt.Errorf("%w: refusal carries mutation result", ErrCompletionCorrupt)
		}
		return 0, nil
	}
	if len(src) != MutationCompletionResultBytes {
		return 0, fmt.Errorf("%w: mutation result length", ErrCompletionCorrupt)
	}
	rows := binary.LittleEndian.Uint64(src)
	if rows > math.MaxInt64 || rows > uint64(MaxMutationAffectedRows) {
		return 0, fmt.Errorf("%w: negative mutation result", ErrCompletionCorrupt)
	}
	return int64(rows), nil
}

func addMutationAffectedRows(total, rows int64) (int64, error) {
	if total < 0 || rows < 0 || total > MaxMutationAffectedRows ||
		rows > MaxMutationAffectedRows-total {
		return 0, fmt.Errorf("%w: mutation affected-row bound", ErrInvalidCollection)
	}
	return total + rows, nil
}
