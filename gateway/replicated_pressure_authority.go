package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

const MaxReplicatedPressureRecordBytes = 4 << 20

var ErrReplicatedPressureMissing = errors.New("gateway: replicated pressure cut is missing")

var replicatedPressureDocumentID = [...]byte{
	'p', 'r', 'e', 's', 's', 'u', 'r', 'e', '/', 'c', 'u', 'r', 'r', 'e', 'n', 't',
}

var replicatedPressureKey = fixedControlPlaneKey(replicatedPressureDocumentID[:])

// ReplicatedPressureRecord is the opaque catalog-Raft envelope for one
// controller-partition pressure cut. Payload has one canonical vibejson
// representation owned by the hot-shard package; the catalog authority binds
// its digest, catalog generation, and monotonic authority revision atomically.
type ReplicatedPressureRecord struct {
	CatalogGeneration uint64
	AuthorityRevision uint64
	PayloadDigest     [sha256.Size]byte
	Payload           []byte
}

type replicatedPressurePayload struct {
	AuthorityRevision uint64 `json:"authority_revision"`
	CatalogGeneration uint64 `json:"catalog_generation"`
	Payload           []byte `json:"payload"`
	PayloadDigest     []byte `json:"payload_digest"`
}

func validReplicatedPressureRecord(record ReplicatedPressureRecord) bool {
	return record.CatalogGeneration != 0 && record.AuthorityRevision != 0 &&
		record.PayloadDigest != ([sha256.Size]byte{}) && len(record.Payload) != 0 &&
		len(record.Payload) <= MaxReplicatedPressureRecordBytes/2 &&
		sha256.Sum256(record.Payload) == record.PayloadDigest && vibejson.Valid(record.Payload)
}

func appendReplicatedPressureRecord(dst []byte, record ReplicatedPressureRecord) ([]byte, error) {
	start := len(dst)
	if !validReplicatedPressureRecord(record) {
		return dst, ErrReplicatedCatalog
	}
	canonical, err := vibejson.AppendCanonicalize(nil, record.Payload)
	if err != nil || !bytes.Equal(canonical, record.Payload) {
		return dst, errors.Join(err, ErrReplicatedCatalog)
	}
	payload, err := vibejson.Marshal(&replicatedPressurePayload{
		AuthorityRevision: record.AuthorityRevision,
		CatalogGeneration: record.CatalogGeneration,
		Payload:           record.Payload, PayloadDigest: record.PayloadDigest[:],
	})
	if err != nil {
		return dst, err
	}
	dst, err = appendControlPlaneDocument(
		dst, replicatedPressureDocumentID[:], payload, MaxReplicatedPressureRecordBytes,
	)
	if err != nil {
		return dst[:start], err
	}
	return dst, nil
}

func openReplicatedPressureRecord(raw []byte) (ReplicatedPressureRecord, error) {
	if len(raw) == 0 || len(raw) > MaxReplicatedPressureRecordBytes {
		return ReplicatedPressureRecord{}, ErrReplicatedCatalog
	}
	payload, err := openTypedControlPlaneDocument(
		raw, replicatedPressureDocumentID[:], MaxReplicatedPressureRecordBytes,
	)
	if err != nil {
		return ReplicatedPressureRecord{}, err
	}
	var decoded replicatedPressurePayload
	if err = vibejson.Unmarshal(payload, &decoded); err != nil ||
		len(decoded.PayloadDigest) != sha256.Size {
		return ReplicatedPressureRecord{}, errors.Join(err, ErrReplicatedCatalog)
	}
	record := ReplicatedPressureRecord{CatalogGeneration: decoded.CatalogGeneration,
		AuthorityRevision: decoded.AuthorityRevision, Payload: decoded.Payload}
	copy(record.PayloadDigest[:], decoded.PayloadDigest)
	if !validReplicatedPressureRecord(record) {
		return ReplicatedPressureRecord{}, ErrReplicatedCatalog
	}
	canonical, err := appendReplicatedPressureRecord(nil, record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ReplicatedPressureRecord{}, errors.Join(err, ErrReplicatedCatalog)
	}
	return record, nil
}

// ReadPressureRecord reads the latest catalog-Raft pressure cut. Absence is
// distinct from an empty cut so a controller cannot invent revision zero.
func (authority *ReplicatedCatalogAuthority) ReadPressureRecord(
	ctx context.Context,
) (ReplicatedPressureRecord, error) {
	result, err := authority.readRaw(ctx, replicatedPressureKey, MaxReplicatedPressureRecordBytes)
	if err != nil {
		return ReplicatedPressureRecord{}, err
	}
	if !result.Found {
		return ReplicatedPressureRecord{}, ErrReplicatedPressureMissing
	}
	return openReplicatedPressureRecord(result.Value)
}

// PublishPressureRecord creates revision one or CAS-replaces exactly the prior
// replicated revision. Exact retries settle as success; gaps, regression, and
// a concurrent publisher fail closed. No wall-clock value participates.
func (authority *ReplicatedCatalogAuthority) PublishPressureRecord(
	ctx context.Context, expectedRevision uint64, record ReplicatedPressureRecord,
) error {
	if authority == nil || authority.session == nil || ctx == nil ||
		!validReplicatedPressureRecord(record) ||
		record.AuthorityRevision != expectedRevision+1 {
		return ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	current, err := authority.readRaw(ctx, replicatedPressureKey, MaxReplicatedPressureRecordBytes)
	if err != nil {
		return err
	}
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedPressureRecord(authority.scratch, record)
	if err != nil {
		return err
	}
	var result NativeResult
	if !current.Found {
		if expectedRevision != 0 {
			return ErrReplicatedCatalogConflict
		}
		result, err = authority.session.PutIfAbsentOrEqual(
			ctx, replicatedPressureKey, authority.scratch,
		)
	} else {
		prior, openErr := openReplicatedPressureRecord(current.Value)
		if openErr != nil {
			return openErr
		}
		if prior.AuthorityRevision == record.AuthorityRevision {
			if bytes.Equal(current.Value, authority.scratch) {
				return nil
			}
			return ErrReplicatedCatalogConflict
		}
		if prior.AuthorityRevision != expectedRevision {
			return ErrReplicatedCatalogConflict
		}
		digest := sha256.Sum256(current.Value)
		result, err = authority.session.ComparePut(ctx, replicatedPressureKey,
			authority.scratch, uint64(len(current.Value)), replication.Digest(digest))
	}
	if err != nil {
		if authority.session.Status().Pending {
			return errors.Join(ErrReplicatedCatalogPending, err)
		}
		return err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedCatalog
	}
	return nil
}
