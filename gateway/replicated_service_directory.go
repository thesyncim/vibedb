package gateway

import (
	"bytes"
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

// The service directory is a separate control-plane row because a node
// lifecycle CAS and a captured frontend token set have different retry and
// size bounds.  The row is still committed by the catalog RF3 session, so a
// receiver never consumes a process-local continuation grant.
const maxReplicatedServiceDirectoryBytes = replication.MaxMutationValueBytes

type replicatedServiceDirectory struct {
	Revision     uint64                               `json:"revision"`
	Drains       []replicatedFrontendDrainEntry       `json:"drains,omitempty"`
	Reservations []replicatedFrontendDrainReservation `json:"reservations,omitempty"`
}

type replicatedFrontendDrainEntry struct {
	DrainID  []byte `json:"drain_id"`
	Revision uint64 `json:"revision"`
	Digest   []byte `json:"digest"`
}

// A reservation occupies an index slot before a frontend listener is closed.
// It is keyed by the immutable drain identity and is consumed atomically when
// the serialized Prepared child is written. This prevents two coordinators
// from both passing a stale capacity check and stranding one Active gateway.
type replicatedFrontendDrainReservation struct {
	DrainID []byte `json:"drain_id"`
}

const (
	maxReplicatedFrontendDrainEntries = serviceauthz.AbsoluteMaxServiceBindings
	frontendDrainDocumentIDBytes      = len("drain/") + 64
)

var replicatedFrontendDrainDocumentPrefix = [...]byte{'d', 'r', 'a', 'i', 'n', '/'}

// ServiceDirectoryContinuationGrantReader exposes the durable continuation
// set to the runtime service-directory projection.  The revision is the CAS
// revision of this row, independent of the node-directory revision.
type ServiceDirectoryContinuationGrantReader interface {
	ReadServiceDirectoryContinuationGrantCut(context.Context) (uint64, []serviceauthz.CommittedFrontendContinuationGrant, error)
}

// ServiceDirectoryDrainFenceReader exposes durable empty-admission fences to
// the runtime projection. A fence is separate from a continuation grant: it
// proves that a draining gateway had no accepted frontend token to carry.
type ServiceDirectoryDrainFenceReader interface {
	ReadServiceDirectoryDrainFenceCut(context.Context) (uint64, []serviceauthz.CommittedFrontendDrainFence, error)
}

func appendReplicatedServiceDirectory(
	dst []byte, directory replicatedServiceDirectory,
) ([]byte, error) {
	if directory.Revision == 0 || len(directory.Drains)+len(directory.Reservations) > serviceauthz.AbsoluteMaxServiceBindings {
		return dst, ErrReplicatedCatalog
	}
	for index, entry := range directory.Drains {
		if len(entry.DrainID) != 32 || entry.Revision == 0 || len(entry.Digest) != 32 ||
			bytes.Equal(entry.DrainID, make([]byte, 32)) || bytes.Equal(entry.Digest, make([]byte, 32)) ||
			index > 0 && bytes.Compare(directory.Drains[index-1].DrainID, entry.DrainID) >= 0 {
			return dst, ErrReplicatedCatalog
		}
	}
	for index, reservation := range directory.Reservations {
		if len(reservation.DrainID) != 32 || bytes.Equal(reservation.DrainID, make([]byte, 32)) ||
			index > 0 && bytes.Compare(directory.Reservations[index-1].DrainID, reservation.DrainID) >= 0 {
			return dst, ErrReplicatedCatalog
		}
		for _, entry := range directory.Drains {
			if bytes.Equal(entry.DrainID, reservation.DrainID) {
				return dst, ErrReplicatedCatalog
			}
		}
	}
	payload, err := vibejson.Marshal(&directory)
	if err != nil {
		return dst, err
	}
	return appendControlPlaneDocument(dst, replicatedServiceDirectoryDocumentID[:], payload,
		maxReplicatedServiceDirectoryBytes)
}

func openReplicatedServiceDirectory(raw []byte) (replicatedServiceDirectory, error) {
	var directory replicatedServiceDirectory
	payload, err := openTypedControlPlaneDocument(raw, replicatedServiceDirectoryDocumentID[:],
		maxReplicatedServiceDirectoryBytes)
	if err != nil {
		return directory, err
	}
	if err = vibejson.Unmarshal(payload, &directory); err != nil {
		return replicatedServiceDirectory{}, errors.Join(err, ErrReplicatedCatalog)
	}
	canonical, err := appendReplicatedServiceDirectory(nil, directory)
	if err != nil || !bytes.Equal(canonical, raw) {
		return replicatedServiceDirectory{}, errors.Join(err, ErrReplicatedCatalog)
	}
	return directory, nil
}

func appendReplicatedFrontendDrainDocument(
	dst []byte, record FrontendDrainRecord,
) ([]byte, error) {
	record = record.normalized()
	if !record.Valid() {
		return dst, ErrReplicatedCatalog
	}
	payload, err := vibejson.Marshal(&record)
	if err != nil {
		return dst, errors.Join(err, ErrReplicatedCatalog)
	}
	identifier := make([]byte, 0, frontendDrainDocumentIDBytes)
	identifier = append(identifier, replicatedFrontendDrainDocumentPrefix[:]...)
	for _, value := range record.DrainID {
		identifier = append(identifier, lowerHex[value>>4], lowerHex[value&0x0f])
	}
	return appendControlPlaneDocument(dst, identifier, payload, maxReplicatedServiceDirectoryBytes)
}

func openReplicatedFrontendDrainDocument(raw []byte) (FrontendDrainRecord, error) {
	var record FrontendDrainRecord
	identifier, payload, ok := openFixedControlPlaneDocument(raw, frontendDrainDocumentIDBytes)
	if !ok || !bytes.Equal(identifier[:len(replicatedFrontendDrainDocumentPrefix)], replicatedFrontendDrainDocumentPrefix[:]) {
		return record, ErrReplicatedCatalog
	}
	var drainID [32]byte
	encoded := identifier[len(replicatedFrontendDrainDocumentPrefix):]
	for index := range drainID {
		high, highOK := lowerHexNibble(encoded[index*2])
		low, lowOK := lowerHexNibble(encoded[index*2+1])
		if !highOK || !lowOK {
			return FrontendDrainRecord{}, ErrReplicatedCatalog
		}
		drainID[index] = high<<4 | low
	}
	if drainID == ([32]byte{}) {
		return FrontendDrainRecord{}, ErrReplicatedCatalog
	}
	if err := vibejson.Unmarshal(payload, &record); err != nil {
		return FrontendDrainRecord{}, errors.Join(err, ErrReplicatedCatalog)
	}
	record = record.normalized()
	if record.DrainID != drainID || !record.Valid() {
		return FrontendDrainRecord{}, ErrReplicatedCatalog
	}
	canonical, err := appendReplicatedFrontendDrainDocument(nil, record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return FrontendDrainRecord{}, errors.Join(err, ErrReplicatedCatalog)
	}
	return record, nil
}

func frontendDrainDocumentKey(drainID [32]byte) []byte {
	identifier := make([]byte, 0, frontendDrainDocumentIDBytes)
	identifier = append(identifier, replicatedFrontendDrainDocumentPrefix[:]...)
	for _, value := range drainID {
		identifier = append(identifier, lowerHex[value>>4], lowerHex[value&0x0f])
	}
	return fixedControlPlaneKey(identifier)
}

func frontendDrainEntry(record FrontendDrainRecord, raw []byte) replicatedFrontendDrainEntry {
	digest := scalingDigest(raw)
	return replicatedFrontendDrainEntry{DrainID: append([]byte(nil), record.DrainID[:]...),
		Revision: record.Revision, Digest: append([]byte(nil), digest[:]...)}
}

func frontendDrainEntryID(entry replicatedFrontendDrainEntry) ([32]byte, bool) {
	var id [32]byte
	if len(entry.DrainID) != len(id) || len(entry.Digest) != len(id) || entry.Revision == 0 {
		return id, false
	}
	copy(id[:], entry.DrainID)
	return id, id != ([32]byte{})
}

func (authority *ReplicatedCatalogAuthority) ReadServiceDirectoryContinuationGrantCut(
	ctx context.Context,
) (uint64, []serviceauthz.CommittedFrontendContinuationGrant, error) {
	revision, grants, _, _, err := authority.ReadFrontendDrainServiceCut(ctx)
	return revision, grants, err
}

// ReadServiceDirectoryContinuationGrants is the small reader seam used by
// older DirectoryReader adapters.  Callers that need CAS use the cut reader
// above so two concurrent drain coordinators cannot overwrite one another.
func (authority *ReplicatedCatalogAuthority) ReadServiceDirectoryContinuationGrants(
	ctx context.Context,
) ([]serviceauthz.CommittedFrontendContinuationGrant, error) {
	_, grants, err := authority.ReadServiceDirectoryContinuationGrantCut(ctx)
	return grants, err
}

func (authority *ReplicatedCatalogAuthority) ReadServiceDirectoryDrainFenceCut(
	ctx context.Context,
) (uint64, []serviceauthz.CommittedFrontendDrainFence, error) {
	revision, _, fences, _, err := authority.ReadFrontendDrainServiceCut(ctx)
	return revision, fences, err
}

func sameServiceDrainFenceImmutable(
	left, right serviceauthz.CommittedFrontendDrainFence,
) bool {
	return left.TrustDomain == right.TrustDomain && left.PhysicalNode == right.PhysicalNode &&
		left.PhysicalIncarnation == right.PhysicalIncarnation && left.PeerKeyDigest == right.PeerKeyDigest &&
		left.GatewayServiceID == right.GatewayServiceID && left.GatewaySessionID == right.GatewaySessionID &&
		left.GatewaySessionRevision == right.GatewaySessionRevision && left.DrainID == right.DrainID &&
		left.Revision == right.Revision && left.Fence == right.Fence
}

// sameServiceDrainFenceProof compares the immutable identity and internal
// fence while allowing the node-bound revision to advance with the atomic
// Active -> Draining -> Decommissioned lifecycle. The revision is checked
// separately by each transition and is never allowed to move backwards.

var _ ServiceDirectoryContinuationGrantReader = (*ReplicatedCatalogAuthority)(nil)
var _ ServiceDirectoryDrainFenceReader = (*ReplicatedCatalogAuthority)(nil)
