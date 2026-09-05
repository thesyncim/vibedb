package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	maxScalingNodeRecordBytes      = 32 << 10
	maxScalingNodeDirectoryBytes   = 256 << 10
	maxScalingIntentRecordBytes    = 128 << 10
	maxScalingIntentDirectoryBytes = 64 << 10
	maxEnrollmentRecordBytes       = 128 << 10
	maxEnrollmentDirectoryBytes    = 256 << 10
	maxScalingTerminalHistoryBytes = 128 << 10
	maxEnrollmentHistoryBytes      = 512 << 10
	maxScalingTerminalHistory      = MaxScalingIntents
	maxEnrollmentHistory           = 1024
	scalingNodeIdentifierBytes     = len("node/") + 32 + 1 + 16
	scalingIntentIdentifierBytes   = len("scale/") + 64
	enrollmentIdentifierBytes      = len("enroll/") + 64
)

var (
	scalingNodeDirectoryDocumentID   = [...]byte{'n', 'o', 'd', 'e', '/', 'd', 'i', 'r', 'e', 'c', 't', 'o', 'r', 'y'}
	scalingIntentDirectoryDocumentID = [...]byte{'s', 'c', 'a', 'l', 'e', '/', 'd', 'i', 'r', 'e', 'c', 't', 'o', 'r', 'y'}
	enrollmentDirectoryDocumentID    = [...]byte{'e', 'n', 'r', 'o', 'l', 'l', '/', 'd', 'i', 'r', 'e', 'c', 't', 'o', 'r', 'y'}
	scalingHistoryDocumentID         = [...]byte{'s', 'c', 'a', 'l', 'e', '/', 'h', 'i', 's', 't', 'o', 'r', 'y'}
	enrollmentHistoryDocumentID      = [...]byte{'e', 'n', 'r', 'o', 'l', 'l', '/', 'h', 'i', 's', 't', 'o', 'r', 'y'}
	scalingNodeDirectoryKey          = fixedControlPlaneKey(scalingNodeDirectoryDocumentID[:])
	scalingIntentDirectoryKey        = fixedControlPlaneKey(scalingIntentDirectoryDocumentID[:])
	enrollmentDirectoryKey           = fixedControlPlaneKey(enrollmentDirectoryDocumentID[:])
	scalingHistoryKey                = fixedControlPlaneKey(scalingHistoryDocumentID[:])
	enrollmentHistoryKey             = fixedControlPlaneKey(enrollmentHistoryDocumentID[:])
)

var (
	ErrScalingNodeMissing      = errors.New("gateway: scaling node record is missing")
	ErrScalingIntentMissing    = errors.New("gateway: scaling intent is missing")
	ErrEnrollmentIntentMissing = errors.New("gateway: enrollment intent is missing")
)

type scalingNodeDirectoryEntry struct {
	NodeID      []byte `json:"node_id"`
	Incarnation uint64 `json:"incarnation"`
	Revision    uint64 `json:"revision"`
	Digest      []byte `json:"digest"`
}

type scalingNodeDirectory struct {
	Revision uint64                      `json:"revision"`
	Nodes    []scalingNodeDirectoryEntry `json:"nodes"`
}

type scalingIDDirectoryEntry struct {
	ID       []byte `json:"id"`
	Revision uint64 `json:"revision"`
	Digest   []byte `json:"digest"`
}

type scalingIDDirectory struct {
	Entries []scalingIDDirectoryEntry `json:"entries"`
}

func appendScalingNodeIdentifier(dst []byte, record NodeRecord) []byte {
	start := len(dst)
	dst = append(dst, "node/"...)
	for _, value := range record.NodeID {
		dst = append(dst, lowerHex[value>>4], lowerHex[value&0xf])
	}
	dst = append(dst, '/')
	// A node incarnation is rendered as fixed-width hexadecimal so the
	// ordered key has one spelling and preserves numeric order.
	for shift := uint(60); ; shift -= 4 {
		nibble := byte(record.Incarnation >> shift)
		dst = append(dst, lowerHex[nibble&0xf])
		if shift == 0 {
			break
		}
	}
	if len(dst)-start != scalingNodeIdentifierBytes {
		panic("gateway: invalid scaling node identifier")
	}
	return dst
}

func scalingNodeKey(node rafttransport.NodeID, incarnation uint64) []byte {
	var identifier [scalingNodeIdentifierBytes]byte
	record := NodeRecord{NodeID: node, Incarnation: incarnation}
	return fixedControlPlaneKey(appendScalingNodeIdentifier(identifier[:0], record))
}

func appendScalingIDIdentifier(dst []byte, prefix []byte, id [32]byte) []byte {
	dst = append(dst, prefix...)
	for _, value := range id {
		dst = append(dst, lowerHex[value>>4], lowerHex[value&0xf])
	}
	return dst
}

func scalingIntentKey(id [32]byte) []byte {
	var identifier [scalingIntentIdentifierBytes]byte
	return fixedControlPlaneKey(appendScalingIDIdentifier(identifier[:0], []byte("scale/"), id))
}

func enrollmentIntentKey(id [32]byte) []byte {
	var identifier [enrollmentIdentifierBytes]byte
	return fixedControlPlaneKey(appendScalingIDIdentifier(identifier[:0], []byte("enroll/"), id))
}

func appendScalingNodeRecord(dst []byte, record NodeRecord) ([]byte, error) {
	if !record.Valid() {
		return dst, ErrInvalidScalingMetadata
	}
	payload, err := vibejson.Marshal(&record)
	if err != nil {
		return dst, errors.Join(err, ErrInvalidScalingMetadata)
	}
	var identifier [scalingNodeIdentifierBytes]byte
	return appendControlPlaneDocument(dst, appendScalingNodeIdentifier(identifier[:0], record), payload, maxScalingNodeRecordBytes)
}

func openScalingNodeRecord(raw []byte, node rafttransport.NodeID, incarnation uint64) (NodeRecord, error) {
	if len(raw) == 0 || len(raw) > maxScalingNodeRecordBytes {
		return NodeRecord{}, ErrInvalidScalingMetadata
	}
	var expected [scalingNodeIdentifierBytes]byte
	identifier := appendScalingNodeIdentifier(expected[:0], NodeRecord{NodeID: node, Incarnation: incarnation})
	payload, err := openTypedControlPlaneDocument(raw, identifier, maxScalingNodeRecordBytes)
	if err != nil {
		return NodeRecord{}, errors.Join(err, ErrInvalidScalingMetadata)
	}
	var record NodeRecord
	if err = vibejson.Unmarshal(payload, &record); err != nil || record.NodeID != node ||
		record.Incarnation != incarnation || !record.Valid() {
		return NodeRecord{}, errors.Join(err, ErrInvalidScalingMetadata)
	}
	canonical, err := appendScalingNodeRecord(nil, record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return NodeRecord{}, errors.Join(err, ErrInvalidScalingMetadata)
	}
	return record, nil
}

func appendScalingNodeDirectoryAt(dst []byte, entries []scalingNodeDirectoryEntry, revision uint64) ([]byte, error) {
	if len(entries) > MaxScalingNodes {
		return dst, ErrScalingMetadataBound
	}
	if revision == 0 {
		return dst, ErrInvalidScalingMetadata
	}
	directory := scalingNodeDirectory{Revision: revision, Nodes: make([]scalingNodeDirectoryEntry, len(entries))}
	for index, entry := range entries {
		if len(entry.NodeID) != len(rafttransport.NodeID{}) || entry.Incarnation == 0 ||
			bytes.Equal(entry.NodeID, make([]byte, len(entry.NodeID))) || entry.Revision == 0 ||
			len(entry.Digest) != sha256.Size || bytes.Equal(entry.Digest, make([]byte, sha256.Size)) ||
			index != 0 && compareNodeDirectoryEntry(entries[index-1], entry) >= 0 {
			return dst, ErrInvalidScalingMetadata
		}
		directory.Nodes[index] = scalingNodeDirectoryEntry{NodeID: bytes.Clone(entry.NodeID), Incarnation: entry.Incarnation,
			Revision: entry.Revision, Digest: bytes.Clone(entry.Digest)}
	}
	payload, err := vibejson.Marshal(&directory)
	if err != nil {
		return dst, err
	}
	return appendControlPlaneDocument(dst, scalingNodeDirectoryDocumentID[:], payload, maxScalingNodeDirectoryBytes)
}

func appendScalingNodeDirectory(dst []byte, entries []scalingNodeDirectoryEntry) ([]byte, error) {
	var revision uint64
	for _, entry := range entries {
		if entry.Revision > revision {
			revision = entry.Revision
		}
	}
	if revision == 0 {
		revision = 1
	}
	return appendScalingNodeDirectoryAt(dst, entries, revision)
}

func compareNodeDirectoryEntry(left, right scalingNodeDirectoryEntry) int {
	if result := bytes.Compare(left.NodeID, right.NodeID); result != 0 {
		return result
	}
	if left.Incarnation < right.Incarnation {
		return -1
	}
	if left.Incarnation > right.Incarnation {
		return 1
	}
	return 0
}

func openScalingNodeDirectory(raw []byte) ([]scalingNodeDirectoryEntry, error) {
	if len(raw) == 0 || len(raw) > maxScalingNodeDirectoryBytes {
		return nil, ErrInvalidScalingMetadata
	}
	payload, err := openTypedControlPlaneDocument(raw, scalingNodeDirectoryDocumentID[:], maxScalingNodeDirectoryBytes)
	if err != nil {
		return nil, err
	}
	var directory scalingNodeDirectory
	if err = vibejson.Unmarshal(payload, &directory); err != nil || directory.Revision == 0 || len(directory.Nodes) > MaxScalingNodes {
		return nil, errors.Join(err, ErrInvalidScalingMetadata)
	}
	entries := make([]scalingNodeDirectoryEntry, len(directory.Nodes))
	for index, entry := range directory.Nodes {
		if len(entry.NodeID) != len(rafttransport.NodeID{}) || entry.Incarnation == 0 || entry.Revision == 0 ||
			len(entry.Digest) != sha256.Size || bytes.Equal(entry.Digest, make([]byte, sha256.Size)) ||
			index != 0 && compareNodeDirectoryEntry(directory.Nodes[index-1], entry) >= 0 {
			return nil, ErrInvalidScalingMetadata
		}
		entries[index] = scalingNodeDirectoryEntry{NodeID: bytes.Clone(entry.NodeID), Incarnation: entry.Incarnation,
			Revision: entry.Revision, Digest: bytes.Clone(entry.Digest)}
	}
	canonical, err := appendScalingNodeDirectoryAt(nil, entries, directory.Revision)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, errors.Join(err, ErrInvalidScalingMetadata)
	}
	return entries, nil
}

func appendScalingIDDirectory(dst []byte, documentID []byte, entries []scalingIDDirectoryEntry, maximum int) ([]byte, error) {
	if len(entries) > MaxScalingIntents && bytes.Equal(documentID, scalingIntentDirectoryDocumentID[:]) ||
		len(entries) > MaxEnrollmentIntents && bytes.Equal(documentID, enrollmentDirectoryDocumentID[:]) {
		return dst, ErrScalingMetadataBound
	}
	directory := scalingIDDirectory{Entries: make([]scalingIDDirectoryEntry, len(entries))}
	for index, entry := range entries {
		if len(entry.ID) != sha256.Size || entry.Revision == 0 || len(entry.Digest) != sha256.Size ||
			bytes.Equal(entry.ID, make([]byte, sha256.Size)) || bytes.Equal(entry.Digest, make([]byte, sha256.Size)) ||
			index != 0 && bytes.Compare(entries[index-1].ID, entry.ID) >= 0 {
			return dst, ErrInvalidScalingMetadata
		}
		directory.Entries[index] = scalingIDDirectoryEntry{ID: bytes.Clone(entry.ID), Revision: entry.Revision, Digest: bytes.Clone(entry.Digest)}
	}
	payload, err := vibejson.Marshal(&directory)
	if err != nil {
		return dst, err
	}
	return appendControlPlaneDocument(dst, documentID, payload, maximum)
}

func openScalingIDDirectory(raw []byte, documentID []byte, maximum, maximumEntries int) ([]scalingIDDirectoryEntry, error) {
	if len(raw) == 0 || len(raw) > maximum {
		return nil, ErrInvalidScalingMetadata
	}
	payload, err := openTypedControlPlaneDocument(raw, documentID, maximum)
	if err != nil {
		return nil, err
	}
	var directory scalingIDDirectory
	if err = vibejson.Unmarshal(payload, &directory); err != nil || len(directory.Entries) > maximumEntries {
		return nil, errors.Join(err, ErrInvalidScalingMetadata)
	}
	entries := make([]scalingIDDirectoryEntry, len(directory.Entries))
	for index, entry := range directory.Entries {
		if len(entry.ID) != sha256.Size || entry.Revision == 0 || len(entry.Digest) != sha256.Size ||
			bytes.Equal(entry.ID, make([]byte, sha256.Size)) || bytes.Equal(entry.Digest, make([]byte, sha256.Size)) ||
			index != 0 && bytes.Compare(directory.Entries[index-1].ID, entry.ID) >= 0 {
			return nil, ErrInvalidScalingMetadata
		}
		entries[index] = scalingIDDirectoryEntry{ID: bytes.Clone(entry.ID), Revision: entry.Revision, Digest: bytes.Clone(entry.Digest)}
	}
	canonical, err := appendScalingIDDirectory(nil, documentID, entries, maximum)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, errors.Join(err, ErrInvalidScalingMetadata)
	}
	return entries, nil
}

func appendScalingIntentRecord(dst []byte, intent ScalingIntent) ([]byte, error) {
	if !intent.Valid() {
		return dst, ErrInvalidScalingMetadata
	}
	payload, err := vibejson.Marshal(&intent)
	if err != nil {
		return dst, errors.Join(err, ErrInvalidScalingMetadata)
	}
	return appendControlPlaneDocument(dst, appendScalingIDIdentifier(nil, []byte("scale/"), intent.ID), payload, maxScalingIntentRecordBytes)
}

func openScalingIntentRecord(raw []byte, id [32]byte) (ScalingIntent, error) {
	if len(raw) == 0 || len(raw) > maxScalingIntentRecordBytes {
		return ScalingIntent{}, ErrInvalidScalingMetadata
	}
	identifier := appendScalingIDIdentifier(nil, []byte("scale/"), id)
	payload, err := openTypedControlPlaneDocument(raw, identifier, maxScalingIntentRecordBytes)
	if err != nil {
		return ScalingIntent{}, err
	}
	var intent ScalingIntent
	if err = vibejson.Unmarshal(payload, &intent); err != nil || intent.ID != id || !intent.Valid() {
		return ScalingIntent{}, errors.Join(err, ErrInvalidScalingMetadata)
	}
	canonical, err := appendScalingIntentRecord(nil, intent)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ScalingIntent{}, errors.Join(err, ErrInvalidScalingMetadata)
	}
	return intent, nil
}

func appendEnrollmentIntentRecord(dst []byte, intent GroupEnrollmentIntent) ([]byte, error) {
	if !intent.Valid() {
		return dst, ErrInvalidScalingMetadata
	}
	payload, err := vibejson.Marshal(&intent)
	if err != nil {
		return dst, errors.Join(err, ErrInvalidScalingMetadata)
	}
	return appendControlPlaneDocument(dst, appendScalingIDIdentifier(nil, []byte("enroll/"), intent.IntentID), payload, maxEnrollmentRecordBytes)
}

func openEnrollmentIntentRecord(raw []byte, id [32]byte) (GroupEnrollmentIntent, error) {
	if len(raw) == 0 || len(raw) > maxEnrollmentRecordBytes {
		return GroupEnrollmentIntent{}, ErrInvalidScalingMetadata
	}
	identifier := appendScalingIDIdentifier(nil, []byte("enroll/"), id)
	payload, err := openTypedControlPlaneDocument(raw, identifier, maxEnrollmentRecordBytes)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	var intent GroupEnrollmentIntent
	if err = vibejson.Unmarshal(payload, &intent); err != nil || intent.IntentID != id || !intent.Valid() {
		return GroupEnrollmentIntent{}, errors.Join(err, ErrInvalidScalingMetadata)
	}
	canonical, err := appendEnrollmentIntentRecord(nil, intent)
	if err != nil || !bytes.Equal(canonical, raw) {
		return GroupEnrollmentIntent{}, errors.Join(err, ErrInvalidScalingMetadata)
	}
	return intent, nil
}

func scalingDigest(raw []byte) replication.Digest { return replication.Digest(sha256.Sum256(raw)) }

func scalingMutationError(result NativeResult, err error, session *NativeSession) error {
	if err != nil {
		if session != nil && session.Status().Pending {
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

func scalingDirectoryMutation(current ReplicatedPointResult, key, value []byte) NativeMutation {
	mutation := NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: key, Value: value}
	if current.Found {
		mutation.Kind = replication.MutationPutDigestEqual
		mutation.ExpectedValueLength = uint64(len(current.Value))
		mutation.ExpectedValueDigest = scalingDigest(current.Value)
	}
	return mutation
}

// scalingPresenceFenceMutation keeps an absent directory absent-or-empty in
// the same terminal batch. Inserting the canonical empty directory is safe
// and, unlike a local absence check, makes a concurrent first writer conflict
// instead of letting a stale retirement proof pass.
func scalingPresenceFenceMutation(current ReplicatedPointResult, key, empty []byte) NativeMutation {
	if current.Found {
		return NativeMutation{Kind: replication.MutationPutDigestEqual, Key: key, Value: current.Value,
			ExpectedValueLength: uint64(len(current.Value)), ExpectedValueDigest: scalingDigest(current.Value)}
	}
	return NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: key, Value: empty}
}

func scalingRecordMutation(current ReplicatedPointResult, key, value []byte) NativeMutation {
	return scalingDirectoryMutation(current, key, value)
}

func (authority *ReplicatedCatalogAuthority) ReadNode(ctx context.Context, node rafttransport.NodeID, incarnation uint64) (NodeRecord, error) {
	if authority == nil || node == (rafttransport.NodeID{}) || incarnation == 0 {
		return NodeRecord{}, ErrInvalidScalingMetadata
	}
	key := scalingNodeKey(node, incarnation)
	result, err := authority.readRaw(ctx, key, maxScalingNodeRecordBytes)
	if err != nil {
		return NodeRecord{}, err
	}
	if !result.Found {
		return NodeRecord{}, ErrScalingNodeMissing
	}
	return openScalingNodeRecord(result.Value, node, incarnation)
}

func (authority *ReplicatedCatalogAuthority) ListNodes(ctx context.Context) ([]NodeRecord, error) {
	nodes, _, err := authority.ReadNodeDirectory(ctx)
	return nodes, err
}

// reduceCurrentNodeDirectory removes historical incarnations from the current
// physical-node cut. The replicated directory intentionally retains old keys so
// an exact drain/recovery lookup can still address them, but a current snapshot
// must contain one authoritative record per physical NodeID.
func reduceCurrentNodeDirectory(nodes []NodeRecord) ([]NodeRecord, error) {
	if len(nodes) > MaxScalingNodes {
		return nil, ErrScalingMetadataBound
	}
	latest := make(map[rafttransport.NodeID]NodeRecord, len(nodes))
	for _, node := range nodes {
		if !node.Valid() {
			return nil, ErrInvalidScalingMetadata
		}
		prior, found := latest[node.NodeID]
		if !found || node.Incarnation > prior.Incarnation {
			latest[node.NodeID] = node
			continue
		}
		if node.Incarnation == prior.Incarnation && prior != node {
			return nil, ErrReplicatedCatalogConflict
		}
	}
	result := make([]NodeRecord, 0, len(latest))
	for _, node := range latest {
		result = append(result, node)
	}
	slices.SortFunc(result, func(left, right NodeRecord) int {
		return bytes.Compare(left.NodeID[:], right.NodeID[:])
	})
	return result, nil
}

// ReadNodeDirectory returns the complete node cut and the replicated
// directory revision in the same linearizable read. The revision is global to
// the cut; deriving it from per-node record revisions would reject a valid
// add/remove when existing records have older revisions.
func (authority *ReplicatedCatalogAuthority) ReadNodeDirectory(
	ctx context.Context,
) ([]NodeRecord, uint64, error) {
	if authority == nil || ctx == nil {
		return nil, 0, ErrInvalidScalingMetadata
	}
	for attempt := 0; attempt < authority.executor.maxAttempts; attempt++ {
		directoryResult, err := authority.readRaw(ctx, scalingNodeDirectoryKey, maxScalingNodeDirectoryBytes)
		if err != nil {
			return nil, 0, err
		}
		if !directoryResult.Found {
			return nil, 0, nil
		}
		entries, err := openScalingNodeDirectory(directoryResult.Value)
		if err != nil {
			return nil, 0, err
		}
		payload, payloadErr := openTypedControlPlaneDocument(directoryResult.Value, scalingNodeDirectoryDocumentID[:], maxScalingNodeDirectoryBytes)
		if payloadErr != nil {
			return nil, 0, payloadErr
		}
		var directory scalingNodeDirectory
		if payloadErr = vibejson.Unmarshal(payload, &directory); payloadErr != nil || directory.Revision == 0 {
			return nil, 0, errors.Join(payloadErr, ErrInvalidScalingMetadata)
		}
		nodes := make([]NodeRecord, len(entries))
		for index, entry := range entries {
			var node rafttransport.NodeID
			copy(node[:], entry.NodeID)
			key := scalingNodeKey(node, entry.Incarnation)
			result, readErr := authority.readRaw(ctx, key, maxScalingNodeRecordBytes)
			if readErr != nil {
				return nil, 0, readErr
			}
			if !result.Found {
				// The directory child may have been replaced or removed by a
				// concurrent authority after the cut was read. Restart the
				// bounded cut assembly instead of returning a mixed-epoch error.
				continue
			}
			if len(result.Value) == 0 || scalingDigest(result.Value) != replication.Digest(entry.Digest) {
				continue
			}
			nodes[index], err = openScalingNodeRecord(result.Value, node, entry.Incarnation)
			if err != nil {
				return nil, 0, err
			}
		}
		latest, err := authority.readRaw(ctx, scalingNodeDirectoryKey, maxScalingNodeDirectoryBytes)
		if err != nil {
			return nil, 0, err
		}
		if latest.Found && bytes.Equal(latest.Value, directoryResult.Value) {
			current, reduceErr := reduceCurrentNodeDirectory(nodes)
			if reduceErr != nil {
				return nil, 0, reduceErr
			}
			return current, directory.Revision, nil
		}
	}
	return nil, 0, ErrReplicatedCatalogConflict
}

// ReadNodeDirectoryCut returns a complete, globally revisioned physical-node
// directory. Every child digest and the directory document itself are checked
// before the cut is returned, so a caller can use the digest as a retry fence.
func (authority *ReplicatedCatalogAuthority) ReadNodeDirectoryCut(ctx context.Context) (NodeDirectoryCut, error) {
	if authority == nil || ctx == nil {
		return NodeDirectoryCut{}, ErrInvalidScalingMetadata
	}
	for attempt := 0; attempt < authority.executor.maxAttempts; attempt++ {
		directoryResult, err := authority.readRaw(ctx, scalingNodeDirectoryKey, maxScalingNodeDirectoryBytes)
		if err != nil {
			return NodeDirectoryCut{}, err
		}
		if !directoryResult.Found {
			return NodeDirectoryCut{}, ErrScalingNodeMissing
		}
		entries, err := openScalingNodeDirectory(directoryResult.Value)
		if err != nil {
			return NodeDirectoryCut{}, err
		}
		payload, err := openTypedControlPlaneDocument(directoryResult.Value, scalingNodeDirectoryDocumentID[:], maxScalingNodeDirectoryBytes)
		if err != nil {
			return NodeDirectoryCut{}, err
		}
		var directory scalingNodeDirectory
		if err = vibejson.Unmarshal(payload, &directory); err != nil || directory.Revision == 0 {
			return NodeDirectoryCut{}, errors.Join(err, ErrInvalidScalingMetadata)
		}
		nodes := make([]NodeRecord, len(entries))
		for index, entry := range entries {
			var node rafttransport.NodeID
			copy(node[:], entry.NodeID)
			result, readErr := authority.readRaw(ctx, scalingNodeKey(node, entry.Incarnation), maxScalingNodeRecordBytes)
			if readErr != nil {
				return NodeDirectoryCut{}, readErr
			}
			if !result.Found || scalingDigest(result.Value) != replication.Digest(entry.Digest) {
				// A child changed after the directory preimage was read. Retry
				// the entire bounded cut, never assemble rows from mixed epochs.
				continue
			}
			nodes[index], err = openScalingNodeRecord(result.Value, node, entry.Incarnation)
			if err != nil {
				return NodeDirectoryCut{}, err
			}
		}
		latest, err := authority.readRaw(ctx, scalingNodeDirectoryKey, maxScalingNodeDirectoryBytes)
		if err != nil {
			return NodeDirectoryCut{}, err
		}
		if latest.Found && bytes.Equal(latest.Value, directoryResult.Value) {
			var generation uint64
			for _, node := range nodes {
				if node.CatalogGeneration > generation {
					generation = node.CatalogGeneration
				}
			}
			cut := NodeDirectoryCut{Revision: directory.Revision, Digest: scalingDigest(directoryResult.Value), CatalogGeneration: generation, Nodes: nodes}
			if !cut.Valid() {
				return NodeDirectoryCut{}, ErrInvalidScalingMetadata
			}
			return cut, nil
		}
	}
	return NodeDirectoryCut{}, ErrReplicatedCatalogConflict
}

func (authority *ReplicatedCatalogAuthority) ReadScalingIntent(ctx context.Context, id [32]byte) (ScalingIntent, error) {
	if authority == nil || ctx == nil || id == ([32]byte{}) {
		return ScalingIntent{}, ErrInvalidScalingMetadata
	}
	key := scalingIntentKey(id)
	result, err := authority.readRaw(ctx, key, maxScalingIntentRecordBytes)
	if err != nil {
		return ScalingIntent{}, err
	}
	if !result.Found {
		return ScalingIntent{}, ErrScalingIntentMissing
	}
	return openScalingIntentRecord(result.Value, id)
}

func (authority *ReplicatedCatalogAuthority) ReadScalingIntentAt(ctx context.Context, id [32]byte, revision uint64, digest replication.Digest) (ScalingIntent, error) {
	if authority == nil || ctx == nil || id == ([32]byte{}) || revision == 0 || digest == (replication.Digest{}) {
		return ScalingIntent{}, ErrScalingRevision
	}
	key := scalingIntentKey(id)
	raw, err := authority.readRaw(ctx, key, maxScalingIntentRecordBytes)
	if err != nil {
		return ScalingIntent{}, err
	}
	if !raw.Found || scalingDigest(raw.Value) != digest {
		return ScalingIntent{}, ErrScalingRevision
	}
	intent, err := openScalingIntentRecord(raw.Value, id)
	if err != nil || intent.Revision != revision {
		return ScalingIntent{}, errors.Join(err, ErrScalingRevision)
	}
	return intent, nil
}

func (authority *ReplicatedCatalogAuthority) readScalingIntentDirectoryEntry(ctx context.Context, id [32]byte, entry scalingIDDirectoryEntry) (ScalingIntent, error) {
	key := scalingIntentKey(id)
	result, err := authority.readRaw(ctx, key, maxScalingIntentRecordBytes)
	if err != nil || !result.Found || scalingDigest(result.Value) != replication.Digest(entry.Digest) {
		return ScalingIntent{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	intent, err := openScalingIntentRecord(result.Value, id)
	if err != nil || intent.Revision != entry.Revision {
		return ScalingIntent{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	return intent, nil
}

func (authority *ReplicatedCatalogAuthority) ListScalingIntents(ctx context.Context) ([]ScalingIntent, error) {
	if authority == nil || ctx == nil {
		return nil, ErrInvalidScalingMetadata
	}
	for attempt := 0; attempt < authority.executor.maxAttempts; attempt++ {
		directoryResult, err := authority.readRaw(ctx, scalingIntentDirectoryKey, maxScalingIntentDirectoryBytes)
		if err != nil {
			return nil, err
		}
		if !directoryResult.Found {
			return nil, nil
		}
		entries, err := openScalingIDDirectory(directoryResult.Value, scalingIntentDirectoryDocumentID[:], maxScalingIntentDirectoryBytes, MaxScalingIntents)
		if err != nil {
			return nil, err
		}
		intents := make([]ScalingIntent, len(entries))
		for index, entry := range entries {
			var id [32]byte
			copy(id[:], entry.ID)
			intents[index], err = authority.readScalingIntentDirectoryEntry(ctx, id, entry)
			if err != nil {
				return nil, err
			}
		}
		latest, err := authority.readRaw(ctx, scalingIntentDirectoryKey, maxScalingIntentDirectoryBytes)
		if err != nil {
			return nil, err
		}
		if latest.Found && bytes.Equal(latest.Value, directoryResult.Value) {
			return intents, nil
		}
	}
	return nil, ErrReplicatedCatalogConflict
}

func (authority *ReplicatedCatalogAuthority) ReadEnrollmentIntent(ctx context.Context, id [32]byte) (GroupEnrollmentIntent, error) {
	if authority == nil || ctx == nil || id == ([32]byte{}) {
		return GroupEnrollmentIntent{}, ErrInvalidScalingMetadata
	}
	key := enrollmentIntentKey(id)
	result, err := authority.readRaw(ctx, key, maxEnrollmentRecordBytes)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	if !result.Found {
		return GroupEnrollmentIntent{}, ErrEnrollmentIntentMissing
	}
	return openEnrollmentIntentRecord(result.Value, id)
}

func (authority *ReplicatedCatalogAuthority) ReadEnrollmentIntentAt(ctx context.Context, id [32]byte, revision uint64, digest replication.Digest) (GroupEnrollmentIntent, error) {
	if authority == nil || ctx == nil || id == ([32]byte{}) || revision == 0 || digest == (replication.Digest{}) {
		return GroupEnrollmentIntent{}, ErrScalingRevision
	}
	key := enrollmentIntentKey(id)
	raw, err := authority.readRaw(ctx, key, maxEnrollmentRecordBytes)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	if !raw.Found || scalingDigest(raw.Value) != digest {
		return GroupEnrollmentIntent{}, ErrScalingRevision
	}
	intent, err := openEnrollmentIntentRecord(raw.Value, id)
	if err != nil || intent.Revision != revision {
		return GroupEnrollmentIntent{}, errors.Join(err, ErrScalingRevision)
	}
	return intent, nil
}

func (authority *ReplicatedCatalogAuthority) readEnrollmentDirectoryEntry(ctx context.Context, id [32]byte, entry scalingIDDirectoryEntry) (GroupEnrollmentIntent, error) {
	key := enrollmentIntentKey(id)
	result, err := authority.readRaw(ctx, key, maxEnrollmentRecordBytes)
	if err != nil || !result.Found || scalingDigest(result.Value) != replication.Digest(entry.Digest) {
		return GroupEnrollmentIntent{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	intent, err := openEnrollmentIntentRecord(result.Value, id)
	if err != nil || intent.Revision != entry.Revision {
		return GroupEnrollmentIntent{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	return intent, nil
}

func (authority *ReplicatedCatalogAuthority) ListEnrollmentIntents(ctx context.Context, groupKey raftmember.GroupKey) ([]GroupEnrollmentIntent, error) {
	if authority == nil || ctx == nil || invalidGroupKey(groupKey) {
		return nil, ErrInvalidScalingMetadata
	}
	for attempt := 0; attempt < authority.executor.maxAttempts; attempt++ {
		directoryResult, err := authority.readRaw(ctx, enrollmentDirectoryKey, maxEnrollmentDirectoryBytes)
		if err != nil {
			return nil, err
		}
		if !directoryResult.Found {
			return nil, nil
		}
		entries, err := openScalingIDDirectory(directoryResult.Value, enrollmentDirectoryDocumentID[:], maxEnrollmentDirectoryBytes, MaxEnrollmentIntents)
		if err != nil {
			return nil, err
		}
		intents := make([]GroupEnrollmentIntent, 0, len(entries))
		for _, entry := range entries {
			var id [32]byte
			copy(id[:], entry.ID)
			intent, readErr := authority.readEnrollmentDirectoryEntry(ctx, id, entry)
			if readErr != nil {
				return nil, readErr
			}
			if intent.Group == groupKey {
				intents = append(intents, intent)
			}
		}
		latest, err := authority.readRaw(ctx, enrollmentDirectoryKey, maxEnrollmentDirectoryBytes)
		if err != nil {
			return nil, err
		}
		if latest.Found && bytes.Equal(latest.Value, directoryResult.Value) {
			return intents, nil
		}
	}
	return nil, ErrReplicatedCatalogConflict
}

func (authority *ReplicatedCatalogAuthority) ScanNodeReferences(ctx context.Context, node rafttransport.NodeID, incarnation uint64) (NodeReferenceEvidence, error) {
	if authority == nil || ctx == nil || node == (rafttransport.NodeID{}) || incarnation == 0 {
		return NodeReferenceEvidence{}, ErrInvalidScalingMetadata
	}
	directoryCut, err := authority.ReadNodeDirectoryCut(ctx)
	if err != nil {
		return NodeReferenceEvidence{}, err
	}
	nodes := directoryCut.Nodes
	var directoryRevision uint64
	for _, record := range nodes {
		if record.NodeID == node && record.Incarnation == incarnation {
			directoryRevision = record.Revision
			break
		}
	}
	if directoryRevision == 0 {
		return NodeReferenceEvidence{}, ErrScalingNodeMissing
	}
	if directoryCut.Revision == 0 {
		return NodeReferenceEvidence{}, ErrReplicatedCatalogConflict
	}
	snapshot, err := authority.Read(ctx)
	if err != nil {
		return NodeReferenceEvidence{}, err
	}
	headResult, err := authority.readRaw(ctx, replicatedCatalogHeadKey, maxReplicatedCatalogBytes)
	if err != nil || !headResult.Found {
		return NodeReferenceEvidence{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	evidence := NodeReferenceEvidence{NodeID: node, Incarnation: incarnation,
		DirectoryRevision: directoryRevision, DirectoryCutRevision: directoryCut.Revision,
		DirectoryCutDigest: directoryCut.Digest, CatalogHeadDigest: scalingDigest(headResult.Value)}
	evidence.CatalogGeneration = snapshot.Generation()
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	for index := 0; index < snapshot.ReplicatedRouteCount(); index++ {
		route, ok := snapshot.ReplicatedRouteAt(index, replicas[:0])
		if !ok {
			return NodeReferenceEvidence{}, ErrReplicatedCatalogConflict
		}
		isCatalog := route.Distribution == ReplicatedCatalogDistribution
		for _, replica := range route.Replicas {
			if replica.Node != node || replica.NodeIncarnation != incarnation {
				continue
			}
			evidence.ServingReplicas++
			if isCatalog {
				evidence.CatalogVoterReferences++
				evidence.ControlVoterReferences++
			}
		}
		membership, membershipOK := snapshot.ResolveReplicatedMembershipRoute(route.Distribution, route.Shard, replicas[:0])
		if !membershipOK {
			return NodeReferenceEvidence{}, ErrReplicatedCatalogConflict
		}
		if membership.HasEnrolledTarget && membership.EnrolledTarget.Node == node &&
			membership.EnrolledTarget.NodeIncarnation == incarnation {
			evidence.EnrolledTargets++
		}
	}
	// A prepared artifact is conservatively counted as a learner/reference
	// until its enrollment row reaches Complete. This may delay retirement but
	// can never produce a false safe-to-stop result.
	intents, err := listAllEnrollmentIntents(ctx, authority)
	if err != nil {
		return NodeReferenceEvidence{}, err
	}
	hash := sha256.New()
	hash.Write(node[:])
	var scalar [8]byte
	putScalar := func(value uint64) {
		binary.LittleEndian.PutUint64(scalar[:], value)
		hash.Write(scalar[:])
	}
	putScalar(evidence.CatalogGeneration)
	putScalar(evidence.DirectoryRevision)
	putScalar(evidence.DirectoryCutRevision)
	hash.Write(evidence.DirectoryCutDigest[:])
	hash.Write(evidence.CatalogHeadDigest[:])
	for _, intent := range intents {
		if intent.State >= EnrollmentReserved && intent.State < EnrollmentComplete {
			if intent.Target.Node == node && intent.Target.NodeIncarnation == incarnation {
				evidence.LearnerReplicas++
			}
			if intent.Source.Node == node && intent.Source.NodeIncarnation == incarnation {
				evidence.OutstandingMoves++
			}
		}
		if intent.State < EnrollmentComplete {
			hash.Write(intent.IntentID[:])
			intentDigest := intent.Digest()
			hash.Write(intentDigest[:])
		}
	}
	scalingIntents, err := authority.ListScalingIntents(ctx)
	if err != nil {
		return NodeReferenceEvidence{}, err
	}
	for _, intent := range scalingIntents {
		if intent.Request.Drain.NodeID == node && intent.Request.Drain.Incarnation == incarnation && intent.State < ScalingComplete {
			evidence.OutstandingMoves += uint32(len(intent.OutstandingMoves))
		}
		if intent.State < ScalingComplete {
			hash.Write(intent.ID[:])
		}
	}
	operationIDs, err := authority.ReadOperationIDs(ctx)
	if err != nil {
		return NodeReferenceEvidence{}, err
	}
	for _, operationID := range operationIDs {
		operation, readErr := authority.ReadOperation(ctx, operationID)
		if readErr != nil {
			return NodeReferenceEvidence{}, readErr
		}
		if operation.Kind == ReplicatedOperationMove && operation.State < ReplicatedOperationComplete {
			evidence.OutstandingMoves++
			hash.Write(operationID[:])
		}
	}
	for _, directory := range []struct {
		key []byte
		set func(replication.Digest)
	}{
		{key: enrollmentDirectoryKey, set: func(digest replication.Digest) { evidence.EnrollmentDirectoryDigest = digest }},
		{key: scalingIntentDirectoryKey, set: func(digest replication.Digest) { evidence.ScalingDirectoryDigest = digest }},
		{key: replicatedOperationDirectoryKey[:], set: func(digest replication.Digest) { evidence.OperationDirectoryDigest = digest }},
	} {
		result, readErr := authority.readRaw(ctx, directory.key, maxEnrollmentDirectoryBytes)
		if readErr != nil {
			return NodeReferenceEvidence{}, readErr
		}
		if result.Found {
			directory.set(scalingDigest(result.Value))
		}
	}
	hash.Write(evidence.ScalingDirectoryDigest[:])
	hash.Write(evidence.EnrollmentDirectoryDigest[:])
	hash.Write(evidence.OperationDirectoryDigest[:])
	for _, record := range nodes {
		if record.NodeID == node && record.Incarnation == incarnation && record.Roles&NodeRoleGateway != 0 {
			if authority.gatewayParticipants == nil {
				// A role bit is a capability only. Without an authenticated live
				// participant cut, conservatively keep the node referenced.
				evidence.GatewayParticipantRefs++
			} else {
				participant, scanErr := authority.gatewayParticipants.ScanGatewayParticipant(ctx, record)
				if scanErr != nil {
					return NodeReferenceEvidence{}, scanErr
				}
				if !participant.ValidFor(record) {
					return NodeReferenceEvidence{}, ErrReplicatedCatalogConflict
				}
				evidence.GatewayDirectoryRevision = participant.DirectoryRevision
				evidence.GatewayDirectoryDigest = participant.Digest
				if participant.Active {
					evidence.GatewayParticipantRefs++
				}
				hash.Write(participant.ServiceKeyDigest[:])
				hash.Write(participant.ServiceID[:])
				hash.Write(participant.SessionID[:])
				hash.Write(participant.ParticipantDigest[:])
				var participantRevision [8]byte
				binary.LittleEndian.PutUint64(participantRevision[:], participant.SessionRevision)
				hash.Write(participantRevision[:])
				hash.Write(participant.Digest[:])
			}
		}
	}
	putScalar(evidence.GatewayDirectoryRevision)
	hash.Write(evidence.GatewayDirectoryDigest[:])
	// A nonzero digest binds the returned summary to the complete set of rows
	// that was read. It is a witness only; callers must still CAS the node and
	// re-run this scan before a terminal transition.
	for _, record := range nodes {
		if record.NodeID == node && record.Incarnation == incarnation {
			raw, marshalErr := vibejson.Marshal(&record)
			if marshalErr != nil {
				return NodeReferenceEvidence{}, marshalErr
			}
			recordDigest := scalingDigest(raw)
			hash.Write(recordDigest[:])
		}
	}
	evidence.Digest = replication.Digest(sha256.Sum256(hash.Sum(nil)))
	return evidence, nil
}

func listAllEnrollmentIntents(ctx context.Context, authority *ReplicatedCatalogAuthority) ([]GroupEnrollmentIntent, error) {
	for attempt := 0; attempt < authority.executor.maxAttempts; attempt++ {
		directoryResult, err := authority.readRaw(ctx, enrollmentDirectoryKey, maxEnrollmentDirectoryBytes)
		if err != nil {
			return nil, err
		}
		if !directoryResult.Found {
			return nil, nil
		}
		entries, err := openScalingIDDirectory(directoryResult.Value, enrollmentDirectoryDocumentID[:], maxEnrollmentDirectoryBytes, MaxEnrollmentIntents)
		if err != nil {
			return nil, err
		}
		intents := make([]GroupEnrollmentIntent, len(entries))
		for index, entry := range entries {
			var id [32]byte
			copy(id[:], entry.ID)
			intents[index], err = authority.readEnrollmentDirectoryEntry(ctx, id, entry)
			if err != nil {
				return nil, err
			}
		}
		latest, err := authority.readRaw(ctx, enrollmentDirectoryKey, maxEnrollmentDirectoryBytes)
		if err != nil {
			return nil, err
		}
		if latest.Found && bytes.Equal(latest.Value, directoryResult.Value) {
			return intents, nil
		}
	}
	return nil, ErrReplicatedCatalogConflict
}

func sameNodeReferenceEvidence(left, right NodeReferenceEvidence) bool {
	return left.NodeID == right.NodeID && left.Incarnation == right.Incarnation &&
		left.CatalogGeneration == right.CatalogGeneration &&
		left.DirectoryRevision == right.DirectoryRevision &&
		left.DirectoryCutRevision == right.DirectoryCutRevision &&
		left.DirectoryCutDigest == right.DirectoryCutDigest &&
		left.CatalogHeadDigest == right.CatalogHeadDigest &&
		left.ScalingDirectoryDigest == right.ScalingDirectoryDigest &&
		left.EnrollmentDirectoryDigest == right.EnrollmentDirectoryDigest &&
		left.OperationDirectoryDigest == right.OperationDirectoryDigest &&
		left.GatewayDirectoryRevision == right.GatewayDirectoryRevision &&
		left.GatewayDirectoryDigest == right.GatewayDirectoryDigest &&
		left.ServingReplicas == right.ServingReplicas && left.LearnerReplicas == right.LearnerReplicas &&
		left.EnrolledTargets == right.EnrolledTargets && left.OutstandingMoves == right.OutstandingMoves &&
		left.CatalogVoterReferences == right.CatalogVoterReferences &&
		left.ControlVoterReferences == right.ControlVoterReferences &&
		left.GatewayParticipantRefs == right.GatewayParticipantRefs &&
		left.RetirementDrainGeneration == right.RetirementDrainGeneration && left.Digest == right.Digest
}

func (authority *ReplicatedCatalogAuthority) PutNode(ctx context.Context, record NodeRecord, expectedRevision uint64) error {
	return authority.putNode(ctx, record, expectedRevision, nil, nil)
}

// RetireNode is the only path that can cross Draining -> Decommissioned.  It
// requires a fresh complete reference scan and persists its digest beside the
// terminal node state so a restart cannot lose the safe-to-stop witness.
func (authority *ReplicatedCatalogAuthority) RetireNode(ctx context.Context, node rafttransport.NodeID, incarnation uint64, expectedRevision uint64, evidence NodeReferenceEvidence) error {
	if node == (rafttransport.NodeID{}) || incarnation == 0 || evidence.NodeID != node ||
		evidence.Incarnation != incarnation || evidence.DirectoryRevision != expectedRevision ||
		evidence.DirectoryCutRevision == 0 || evidence.DirectoryCutDigest == (replication.Digest{}) ||
		evidence.CatalogHeadDigest == (replication.Digest{}) ||
		!evidence.ZeroAllReferences() || evidence.Digest == (replication.Digest{}) {
		return ErrScalingState
	}
	fresh, err := authority.ScanNodeReferences(ctx, node, incarnation)
	if err != nil || !sameNodeReferenceEvidence(fresh, evidence) {
		return errors.Join(err, ErrScalingRevision)
	}
	headResult, err := authority.readRaw(ctx, replicatedCatalogHeadKey, maxReplicatedCatalogBytes)
	if err != nil || !headResult.Found {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	if scalingDigest(headResult.Value) != evidence.CatalogHeadDigest {
		return ErrReplicatedCatalogConflict
	}
	headPayload, err := openTypedControlPlaneDocument(headResult.Value, replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes)
	if err != nil {
		return err
	}
	headSnapshot, err := OpenSnapshotDocument(headPayload)
	if err != nil || headSnapshot.Generation() != evidence.CatalogGeneration {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	current, err := authority.ReadNode(ctx, node, incarnation)
	if err != nil {
		return err
	}
	if current.Lifecycle != NodeDraining || current.Revision != expectedRevision ||
		evidence.CatalogGeneration < current.CatalogGeneration {
		return ErrScalingState
	}
	next := current
	next.Lifecycle = NodeDecommissioned
	next.Revision++
	next.CatalogGeneration = evidence.CatalogGeneration
	next.RetirementScanDigest = evidence.Digest
	next.RetirementScanDirectoryRevision = evidence.DirectoryRevision
	next.RetirementScanCutRevision = evidence.DirectoryCutRevision
	return authority.putNode(ctx, next, expectedRevision, &evidence, &headResult)
}

func (authority *ReplicatedCatalogAuthority) putNode(ctx context.Context, record NodeRecord, expectedRevision uint64, retirement *NodeReferenceEvidence, catalogHead *ReplicatedPointResult) error {
	if authority == nil || authority.session == nil || ctx == nil || !record.Valid() {
		return ErrInvalidScalingMetadata
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	key := scalingNodeKey(record.NodeID, record.Incarnation)
	current, err := authority.readRaw(ctx, key, maxScalingNodeRecordBytes)
	if err != nil {
		return err
	}
	var priorRecord NodeRecord
	if current.Found {
		prior, openErr := openScalingNodeRecord(current.Value, record.NodeID, record.Incarnation)
		if openErr != nil {
			return openErr
		}
		priorRecord = prior
		if revisionRetryMatches(expectedRevision, record.Revision) && bytes.Equal(current.Value, mustAppendNode(record)) {
			return nil
		}
		if expectedRevision != prior.Revision || record.Revision != prior.Revision+1 ||
			record.CatalogGeneration < prior.CatalogGeneration {
			return ErrScalingRevision
		}
		if prior.Lifecycle == NodeDraining && record.Lifecycle == NodeDecommissioned && retirement == nil {
			return ErrScalingState
		}
		if prior.Lifecycle == NodeDecommissioned {
			return ErrScalingState
		}
		if err = validateNodeTransition(prior, record); err != nil {
			return err
		}
		if retirement != nil && (record.RetirementScanDigest != retirement.Digest ||
			record.RetirementScanDirectoryRevision != retirement.DirectoryRevision ||
			record.RetirementScanCutRevision != retirement.DirectoryCutRevision) {
			return ErrScalingState
		}
	} else if expectedRevision != 0 || record.Revision != 1 || record.Lifecycle != NodeJoining {
		return ErrScalingRevision
	}
	directoryResult, err := authority.readRaw(ctx, scalingNodeDirectoryKey, maxScalingNodeDirectoryBytes)
	if err != nil {
		return err
	}
	if retirement != nil {
		if !directoryResult.Found || scalingDigest(directoryResult.Value) != retirement.DirectoryCutDigest {
			return ErrScalingRevision
		}
		if record.Roles&NodeRoleGateway != 0 {
			if authority.gatewayParticipants == nil {
				return ErrScalingState
			}
			participant, scanErr := authority.gatewayParticipants.ScanGatewayParticipant(ctx, priorRecord)
			if scanErr != nil {
				return scanErr
			}
			if !participant.ValidFor(record) || participant.Active ||
				participant.DirectoryRevision != retirement.GatewayDirectoryRevision ||
				participant.Digest != retirement.GatewayDirectoryDigest {
				return ErrScalingRevision
			}
		}
	}
	var entries []scalingNodeDirectoryEntry
	var directoryRevision uint64
	var retiredNodeGC []NativeMutation
	if directoryResult.Found {
		entries, err = openScalingNodeDirectory(directoryResult.Value)
		if err != nil {
			return err
		}
		payload, openErr := openTypedControlPlaneDocument(directoryResult.Value, scalingNodeDirectoryDocumentID[:], maxScalingNodeDirectoryBytes)
		if openErr != nil {
			return openErr
		}
		var directory scalingNodeDirectory
		if err = vibejson.Unmarshal(payload, &directory); err != nil || directory.Revision == 0 {
			return errors.Join(err, ErrInvalidScalingMetadata)
		}
		directoryRevision = directory.Revision
	}
	// A physical NodeID may be reincarnated only after its prior incarnation
	// reached Decommissioned with a durable retirement witness. Retain the new
	// joining record and remove all older terminal incarnations in the same
	// directory CAS; otherwise repeated replacement cycles would eventually
	// exhaust the bounded historical node directory.
	if !current.Found && record.Lifecycle == NodeJoining {
		filtered := make([]scalingNodeDirectoryEntry, 0, len(entries))
		for _, oldEntry := range entries {
			if !bytes.Equal(oldEntry.NodeID, record.NodeID[:]) {
				filtered = append(filtered, oldEntry)
				continue
			}
			if oldEntry.Incarnation > record.Incarnation {
				return ErrScalingIdentity
			}
			var oldNode rafttransport.NodeID
			copy(oldNode[:], oldEntry.NodeID)
			oldRaw, readErr := authority.readRaw(ctx, scalingNodeKey(oldNode, oldEntry.Incarnation), maxScalingNodeRecordBytes)
			if readErr != nil || !oldRaw.Found || scalingDigest(oldRaw.Value) != replication.Digest(oldEntry.Digest) {
				return errors.Join(readErr, ErrReplicatedCatalogConflict)
			}
			oldRecord, openErr := openScalingNodeRecord(oldRaw.Value, oldNode, oldEntry.Incarnation)
			if openErr != nil || oldRecord.Lifecycle != NodeDecommissioned ||
				oldRecord.RetirementScanDigest == (replication.Digest{}) {
				return errors.Join(openErr, ErrScalingState)
			}
			retiredNodeGC = append(retiredNodeGC, NativeMutation{Kind: replication.MutationDeleteDigestEqual,
				Key: scalingNodeKey(oldNode, oldEntry.Incarnation), Value: nil,
				ExpectedValueLength: uint64(len(oldRaw.Value)), ExpectedValueDigest: scalingDigest(oldRaw.Value)})
		}
		entries = filtered
	}
	nextDirectoryRevision := directoryRevision + 1
	if nextDirectoryRevision == 0 {
		return ErrScalingMetadataBound
	}
	recordBytes, err := appendScalingNodeRecord(nil, record)
	if err != nil {
		return err
	}
	recordDigest := scalingDigest(recordBytes)
	position := 0
	entry := scalingNodeDirectoryEntry{NodeID: append([]byte(nil), record.NodeID[:]...), Incarnation: record.Incarnation,
		Revision: record.Revision, Digest: bytes.Clone(recordDigest[:])}
	for position < len(entries) && compareNodeDirectoryEntry(entries[position], entry) < 0 {
		position++
	}
	if position == len(entries) || compareNodeDirectoryEntry(entries[position], entry) != 0 {
		entries = append(entries, scalingNodeDirectoryEntry{})
		copy(entries[position+1:], entries[position:])
		entries[position] = entry
	} else {
		entries[position] = entry
	}
	directoryBytes, err := appendScalingNodeDirectoryAt(nil, entries, nextDirectoryRevision)
	if err != nil {
		return err
	}
	mutations := []NativeMutation{
		scalingRecordMutation(current, key, recordBytes),
		scalingDirectoryMutation(directoryResult, scalingNodeDirectoryKey, directoryBytes),
	}
	mutations = append(mutations, retiredNodeGC...)
	if retirement != nil {
		if catalogHead == nil || !catalogHead.Found {
			return ErrScalingState
		}
		if catalogHeadDigest := scalingDigest(catalogHead.Value); catalogHeadDigest != retirement.CatalogHeadDigest {
			return ErrScalingRevision
		}
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutDigestEqual,
			Key: replicatedCatalogHeadKey, Value: catalogHead.Value,
			ExpectedValueLength: uint64(len(catalogHead.Value)),
			ExpectedValueDigest: scalingDigest(catalogHead.Value)})
		emptyScaling, emptyErr := appendScalingIDDirectory(nil, scalingIntentDirectoryDocumentID[:], nil, maxScalingIntentDirectoryBytes)
		if emptyErr != nil {
			return emptyErr
		}
		emptyEnrollment, emptyErr := appendScalingIDDirectory(nil, enrollmentDirectoryDocumentID[:], nil, maxEnrollmentDirectoryBytes)
		if emptyErr != nil {
			return emptyErr
		}
		emptyOperations, emptyErr := appendReplicatedOperationDirectory(nil, nil)
		if emptyErr != nil {
			return emptyErr
		}
		for _, fence := range []struct {
			key      []byte
			expected replication.Digest
			empty    []byte
			maximum  uint32
		}{
			{key: scalingIntentDirectoryKey, expected: retirement.ScalingDirectoryDigest, empty: emptyScaling, maximum: maxScalingIntentDirectoryBytes},
			{key: enrollmentDirectoryKey, expected: retirement.EnrollmentDirectoryDigest, empty: emptyEnrollment, maximum: maxEnrollmentDirectoryBytes},
			{key: replicatedOperationDirectoryKey[:], expected: retirement.OperationDirectoryDigest, empty: emptyOperations, maximum: maxReplicatedOperationDirectoryBytes},
		} {
			result, readErr := authority.readRaw(ctx, fence.key, fence.maximum)
			if readErr != nil {
				return readErr
			}
			if (result.Found && fence.expected == (replication.Digest{})) ||
				(!result.Found && fence.expected != (replication.Digest{})) ||
				(result.Found && scalingDigest(result.Value) != fence.expected) {
				return ErrScalingRevision
			}
			mutations = append(mutations, scalingPresenceFenceMutation(result, fence.key, fence.empty))
		}
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	return scalingMutationError(result, err, authority.session)
}

func mustAppendNode(record NodeRecord) []byte {
	raw, _ := appendScalingNodeRecord(nil, record)
	return raw
}

func (authority *ReplicatedCatalogAuthority) validateScalingCompletion(ctx context.Context, intent ScalingIntent) error {
	if len(intent.Blockers) != 0 {
		return ErrScalingState
	}
	if intent.Request.Kind != ScalingScaleIn && intent.Request.Kind != ScalingDecommission {
		return nil
	}
	reference, scanErr := authority.ScanNodeReferences(ctx, intent.Request.Drain.NodeID, intent.Request.Drain.Incarnation)
	if scanErr != nil || !intent.Evidence.MatchesReference(reference) {
		return errors.Join(scanErr, ErrScalingRevision)
	}
	node, nodeErr := authority.ReadNode(ctx, intent.Request.Drain.NodeID, intent.Request.Drain.Incarnation)
	if nodeErr != nil {
		return nodeErr
	}
	if intent.Request.Kind == ScalingScaleIn {
		if !intent.Evidence.SafeForDataEvacuation() ||
			(node.Lifecycle != NodeDraining && node.Lifecycle != NodeDecommissioned) {
			return ErrScalingState
		}
		return nil
	}
	// RetireNode stores the pre-transition scan witness on the terminal node
	// record. The node-directory cut necessarily advances when that record is
	// changed to Decommissioned, so the completion proof must bind the fresh
	// post-transition zero-reference scan rather than compare its digest to the
	// pre-transition witness. RetireNode already atomically fenced that witness;
	// this scan prevents a later catalog/control publication from being hidden.
	if !intent.Evidence.SafeToStop() || node.Lifecycle != NodeDecommissioned ||
		node.RetirementScanDigest == (replication.Digest{}) ||
		node.RetirementScanDirectoryRevision == 0 || node.RetirementScanCutRevision == 0 ||
		!reference.ZeroAllReferences() {
		return ErrScalingState
	}
	return nil
}

func (authority *ReplicatedCatalogAuthority) scalingCompletionFenceMutations(
	ctx context.Context, evidence SafeToStopEvidence,
) ([]NativeMutation, error) {
	emptyEnrollment, err := appendScalingIDDirectory(nil, enrollmentDirectoryDocumentID[:], nil, maxEnrollmentDirectoryBytes)
	if err != nil {
		return nil, err
	}
	emptyOperations, err := appendReplicatedOperationDirectory(nil, nil)
	if err != nil {
		return nil, err
	}
	descriptors := []struct {
		key      []byte
		maximum  uint32
		expected replication.Digest
		empty    []byte
	}{
		{key: scalingNodeDirectoryKey, maximum: maxScalingNodeDirectoryBytes, expected: evidence.ScanDirectoryDigest},
		{key: replicatedCatalogHeadKey, maximum: maxReplicatedCatalogBytes, expected: evidence.CatalogHeadDigest},
		{key: enrollmentDirectoryKey, maximum: maxEnrollmentDirectoryBytes, expected: evidence.EnrollmentDirectoryDigest, empty: emptyEnrollment},
		{key: replicatedOperationDirectoryKey[:], maximum: maxReplicatedOperationDirectoryBytes, expected: evidence.OperationDirectoryDigest, empty: emptyOperations},
	}
	mutations := make([]NativeMutation, 0, len(descriptors))
	for _, descriptor := range descriptors {
		result, readErr := authority.readRaw(ctx, descriptor.key, descriptor.maximum)
		if readErr != nil {
			return nil, readErr
		}
		if (result.Found && descriptor.expected == (replication.Digest{})) ||
			(!result.Found && descriptor.expected != (replication.Digest{})) ||
			(result.Found && scalingDigest(result.Value) != descriptor.expected) {
			return nil, ErrScalingRevision
		}
		if !result.Found && len(descriptor.empty) == 0 {
			return nil, ErrScalingRevision
		}
		mutations = append(mutations, scalingPresenceFenceMutation(result, descriptor.key, descriptor.empty))
	}
	return mutations, nil
}

func (authority *ReplicatedCatalogAuthority) PutScalingIntent(ctx context.Context, intent ScalingIntent, expectedRevision uint64) error {
	if authority == nil || authority.session == nil || ctx == nil || !intent.Valid() {
		return ErrInvalidScalingMetadata
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	if intent.State == ScalingComplete {
		if err = authority.validateScalingCompletion(ctx, intent); err != nil {
			return err
		}
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	key := scalingIntentKey(intent.ID)
	current, err := authority.readRaw(ctx, key, maxScalingIntentRecordBytes)
	if err != nil {
		return err
	}
	var prior ScalingIntent
	if current.Found {
		prior, err = openScalingIntentRecord(current.Value, intent.ID)
		if err != nil {
			return err
		}
		if revisionRetryMatches(expectedRevision, intent.Revision) && bytes.Equal(current.Value, mustAppendScalingIntent(intent)) {
			return nil
		}
		if expectedRevision != prior.Revision || intent.Revision != prior.Revision+1 ||
			intent.CatalogGeneration != prior.CatalogGeneration || intent.Request.ID() != prior.Request.ID() ||
			intent.Request.Kind != prior.Request.Kind {
			return ErrScalingRevision
		}
		if prior.State >= ScalingComplete {
			return ErrScalingState
		}
		if intent.State != prior.State && !prior.State.Allows(intent.State) {
			return ErrScalingState
		}
		if !sameScalingIntentImmutable(prior, intent) {
			return ErrScalingIdentity
		}
		if len(intent.OutstandingMoves) != 0 && intent.State == ScalingCancelled {
			return ErrScalingState
		}
	} else if expectedRevision != 0 || intent.Revision != 1 || intent.State != ScalingReserved {
		return ErrScalingRevision
	}
	var drainNodeResult ReplicatedPointResult
	var drainNodeKey []byte
	if intent.Request.Kind == ScalingScaleIn || intent.Request.Kind == ScalingDecommission {
		if !intent.Request.Drain.Valid() {
			return ErrScalingIdentity
		}
		drainNodeKey = scalingNodeKey(intent.Request.Drain.NodeID, intent.Request.Drain.Incarnation)
		drainNodeResult, err = authority.readRaw(ctx, drainNodeKey, maxScalingNodeRecordBytes)
		if err != nil || !drainNodeResult.Found {
			return errors.Join(err, ErrScalingIdentity)
		}
		drainNode, openErr := openScalingNodeRecord(drainNodeResult.Value, intent.Request.Drain.NodeID, intent.Request.Drain.Incarnation)
		if openErr != nil {
			return openErr
		}
		if !current.Found && drainNode.Lifecycle != NodeActive {
			return ErrScalingState
		}
		if current.Found && drainNode.Lifecycle != NodeActive && drainNode.Lifecycle != NodeDraining && drainNode.Lifecycle != NodeDecommissioned {
			return ErrScalingState
		}
	}
	directoryResult, err := authority.readRaw(ctx, scalingIntentDirectoryKey, maxScalingIntentDirectoryBytes)
	if err != nil {
		return err
	}
	var completionFences []NativeMutation
	if intent.State == ScalingComplete &&
		(intent.Request.Kind == ScalingScaleIn || intent.Request.Kind == ScalingDecommission) {
		if (!directoryResult.Found && intent.Evidence.ScalingDirectoryDigest != (replication.Digest{})) ||
			(directoryResult.Found && scalingDigest(directoryResult.Value) != intent.Evidence.ScalingDirectoryDigest) {
			return ErrScalingRevision
		}
		completionFences, err = authority.scalingCompletionFenceMutations(ctx, intent.Evidence)
		if err != nil {
			return err
		}
	}
	var entries []scalingIDDirectoryEntry
	if directoryResult.Found {
		entries, err = openScalingIDDirectory(directoryResult.Value, scalingIntentDirectoryDocumentID[:], maxScalingIntentDirectoryBytes, MaxScalingIntents)
		if err != nil {
			return err
		}
	}
	recordBytes, err := appendScalingIntentRecord(nil, intent)
	if err != nil {
		return err
	}
	recordDigest := scalingDigest(recordBytes)
	var historyResult ReplicatedPointResult
	var historyEntries []scalingIDDirectoryEntry
	var historyBytes []byte
	var evictedHistory scalingIDDirectoryEntry
	var evictedHistoryFound bool
	if intent.State >= ScalingComplete {
		removeScalingDirectoryEntry(&entries, intent.ID)
		historyEntries, historyResult, err = authority.readTerminalHistory(ctx, scalingHistoryKey, scalingHistoryDocumentID[:], maxScalingTerminalHistoryBytes, maxScalingTerminalHistory)
		if err != nil {
			return err
		}
		evictedHistory, evictedHistoryFound = terminalHistoryEntry(&historyEntries, intent.ID, intent.Revision, recordDigest, maxScalingTerminalHistory)
		historyBytes, err = appendScalingIDDirectory(nil, scalingHistoryDocumentID[:], historyEntries, maxScalingTerminalHistoryBytes)
		if err != nil {
			return err
		}
	} else {
		insertScalingDirectoryEntry(&entries, intent.ID, intent.Revision, recordDigest)
	}
	directoryBytes, err := appendScalingIDDirectory(nil, scalingIntentDirectoryDocumentID[:], entries, maxScalingIntentDirectoryBytes)
	if err != nil {
		return err
	}
	mutations := make([]NativeMutation, 0, 5)
	if drainNodeResult.Found {
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutDigestEqual, Key: drainNodeKey,
			Value: drainNodeResult.Value, ExpectedValueLength: uint64(len(drainNodeResult.Value)),
			ExpectedValueDigest: scalingDigest(drainNodeResult.Value)})
	}
	mutations = append(mutations,
		scalingRecordMutation(current, key, recordBytes),
		scalingDirectoryMutation(directoryResult, scalingIntentDirectoryKey, directoryBytes))
	if intent.State >= ScalingComplete {
		mutations = append(mutations, scalingDirectoryMutation(historyResult, scalingHistoryKey, historyBytes))
		if evictedHistoryFound {
			var evictedID [32]byte
			copy(evictedID[:], evictedHistory.ID)
			evictedRaw, readErr := authority.readRaw(ctx, scalingIntentKey(evictedID), maxScalingIntentRecordBytes)
			if readErr != nil || !evictedRaw.Found || scalingDigest(evictedRaw.Value) != replication.Digest(evictedHistory.Digest) {
				return errors.Join(readErr, ErrReplicatedCatalogConflict)
			}
			mutations = append(mutations, NativeMutation{Kind: replication.MutationDeleteDigestEqual,
				Key: scalingIntentKey(evictedID), ExpectedValueLength: uint64(len(evictedRaw.Value)),
				ExpectedValueDigest: scalingDigest(evictedRaw.Value)})
		}
	}
	mutations = append(mutations, completionFences...)
	result, err := authority.session.MutateBatch(ctx, mutations)
	return scalingMutationError(result, err, authority.session)
}

func sameScalingIntentImmutable(left, right ScalingIntent) bool {
	return left.ID == right.ID && left.Request.ID() == right.Request.ID() &&
		left.Request.Kind == right.Request.Kind && left.Request.Drain == right.Request.Drain &&
		left.Request.DesiredNodeCount == right.Request.DesiredNodeCount &&
		left.Request.MaxMoves == right.Request.MaxMoves &&
		left.Request.MaxMigrationBytes == right.Request.MaxMigrationBytes &&
		left.Request.HysteresisPPM == right.Request.HysteresisPPM &&
		slices.Equal(left.Request.Targets, right.Request.Targets) &&
		left.CatalogGeneration == right.CatalogGeneration
}

func mustAppendScalingIntent(intent ScalingIntent) []byte {
	raw, _ := appendScalingIntentRecord(nil, intent)
	return raw
}

func (authority *ReplicatedCatalogAuthority) SubmitScalingIntent(ctx context.Context, intent ScalingIntent) error {
	return authority.PutScalingIntent(ctx, intent, 0)
}

// CancelScalingIntent performs the only safe operator cancellation: it can
// cancel a reserved/running intent only after all journaled moves have
// settled. The transition remains replicated and CAS-fenced, so a caller
// losing its response can reread the terminal tombstone and retry safely.
func (authority *ReplicatedCatalogAuthority) CancelScalingIntent(ctx context.Context, id [32]byte, expectedRevision uint64) (ScalingIntent, error) {
	if authority == nil || ctx == nil || id == ([32]byte{}) {
		return ScalingIntent{}, ErrInvalidScalingMetadata
	}
	current, err := authority.ReadScalingIntent(ctx, id)
	if err != nil {
		return ScalingIntent{}, err
	}
	if current.State >= ScalingComplete || len(current.OutstandingMoves) != 0 {
		return current, ErrScalingState
	}
	if expectedRevision == 0 {
		expectedRevision = current.Revision
	}
	if expectedRevision != current.Revision {
		return current, ErrScalingRevision
	}
	next := current
	next.State = ScalingCancelled
	next.Revision++
	next.DirectoryRevision = next.Revision
	if err = authority.PutScalingIntent(ctx, next, current.Revision); err != nil {
		return current, err
	}
	return next, nil
}

// PutEnrollmentIntent advances the durable enrollment state machine.  The
// Prepared -> Enrolled edge is intentionally unavailable here: only
// PublishEnrollmentReceipt may cross it after validating and atomically
// publishing the catalog's G+1 enrolled-target cut.
func (authority *ReplicatedCatalogAuthority) PutEnrollmentIntent(ctx context.Context, intent GroupEnrollmentIntent, expectedRevision uint64) error {
	return authority.putEnrollmentIntent(ctx, intent, expectedRevision, false)
}

func (authority *ReplicatedCatalogAuthority) putEnrollmentIntent(ctx context.Context, intent GroupEnrollmentIntent, expectedRevision uint64, allowReceipt bool) error {
	if authority == nil || authority.session == nil || ctx == nil || !intent.Valid() {
		return ErrInvalidScalingMetadata
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	key := enrollmentIntentKey(intent.IntentID)
	current, err := authority.readRaw(ctx, key, maxEnrollmentRecordBytes)
	if err != nil {
		return err
	}
	var prior GroupEnrollmentIntent
	if current.Found {
		prior, err = openEnrollmentIntentRecord(current.Value, intent.IntentID)
		if err != nil {
			return err
		}
		if revisionRetryMatches(expectedRevision, intent.Revision) {
			// A create retry may carry the caller's pre-admission zero head
			// witness.  The first attempt normalized that field from the
			// authoritative catalog row before committing the reservation.  Treat
			// only this exact immutable retry as idempotent; a nonzero mismatch
			// remains an identity error below.
			retryIntent := intent
			if retryIntent.ExpectedCatalogHeadDigest == (replication.Digest{}) {
				retryIntent.ExpectedCatalogHeadDigest = prior.ExpectedCatalogHeadDigest
			}
			if bytes.Equal(current.Value, mustAppendEnrollmentIntent(retryIntent)) {
				return nil
			}
		}
		if prior.ExpectedCatalogHeadDigest != intent.ExpectedCatalogHeadDigest {
			return ErrScalingIdentity
		}
		if expectedRevision != prior.Revision || intent.Revision != prior.Revision+1 {
			return ErrScalingRevision
		}
		if prior.State >= EnrollmentComplete {
			return ErrScalingState
		}
		if prior.State == EnrollmentCancelled {
			return ErrScalingState
		}
		if intent.State != prior.State && !prior.State.Allows(intent.State) {
			return ErrScalingState
		}
		if prior.PreparationClaim != ([32]byte{}) {
			if intent.State == EnrollmentReserved && intent.PreparationClaim != prior.PreparationClaim {
				return ErrScalingIdentity
			}
			if intent.State == EnrollmentPrepared && intent.PreparationClaim != ([32]byte{}) {
				return ErrScalingState
			}
		} else if intent.State == EnrollmentPrepared ||
			(intent.State >= EnrollmentEnrolled && intent.State <= EnrollmentComplete) {
			// Prepared may only be published after a durable claim.  This closes
			// the cancellation window between an external Prepare side effect and
			// its metadata CAS.
			return ErrScalingState
		}
		if prior.State == EnrollmentPrepared && intent.State == EnrollmentEnrolled && !allowReceipt {
			return ErrScalingState
		}
		if prior.Digest() != intent.Digest() {
			return ErrScalingIdentity
		}
		if prior.Proof != nil && intent.Proof != nil && *prior.Proof != *intent.Proof {
			return ErrScalingIdentity
		}
		if prior.Receipt != nil && intent.Receipt != nil && *prior.Receipt != *intent.Receipt {
			return ErrScalingIdentity
		}
		if prior.Receipt != nil && intent.Receipt == nil {
			return ErrScalingIdentity
		}
		if prior.MoveOperationID != ([32]byte{}) && prior.MoveOperationID != intent.MoveOperationID {
			return ErrScalingIdentity
		}
	} else if expectedRevision != 0 || intent.Revision != 1 || intent.State != EnrollmentReserved ||
		intent.PreparationClaim != ([32]byte{}) {
		return ErrScalingRevision
	}
	var enrollmentBaseHead, enrollmentBaseWitness ReplicatedPointResult
	if !current.Found {
		headResult, headErr := authority.readRaw(ctx, replicatedCatalogHeadKey, maxReplicatedCatalogBytes)
		if headErr != nil || !headResult.Found {
			return errors.Join(headErr, ErrReplicatedCatalogConflict)
		}
		enrollmentBaseHead = headResult
		witnessResult, witnessErr := authority.readRaw(ctx, replicatedCatalogHeadWitnessKey, maxReplicatedCatalogBytes)
		if witnessErr != nil || !witnessResult.Found {
			return errors.Join(witnessErr, ErrReplicatedCatalogConflict)
		}
		enrollmentBaseWitness = witnessResult
		headDigest := scalingDigest(headResult.Value)
		if intent.ExpectedCatalogHeadDigest == (replication.Digest{}) {
			intent.ExpectedCatalogHeadDigest = headDigest
		} else if headDigest != intent.ExpectedCatalogHeadDigest {
			return ErrReplicatedCatalogConflict
		}
		headPayload, headErr := openTypedControlPlaneDocument(headResult.Value, replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes)
		if headErr != nil {
			return headErr
		}
		headSnapshot, headErr := OpenSnapshotDocument(headPayload)
		if headErr != nil || headSnapshot.Generation() != intent.CatalogGeneration ||
			validateReplicatedCatalogHeadWitness(witnessResult.Value, headSnapshot.Generation(), headResult.Value) != nil {
			return errors.Join(headErr, ErrReplicatedCatalogConflict)
		}
	}
	// Reservation must observe an exact Active target revision. The active
	// enrollment directory is also the durable per-distribution transition
	// owner: a sibling group queues until the current owner reaches Complete.
	// Once a reservation has reached Prepared/Enrolled, the target may legitimately
	// enter Draining while the existing saga finishes; it is still fenced by a
	// compare-put of the observed target row below and can never be reused for
	// a new group transition.
	targetKey := scalingNodeKey(intent.Target.Node, intent.Target.NodeIncarnation)
	targetResult, err := authority.readRaw(ctx, targetKey, maxScalingNodeRecordBytes)
	if err != nil || !targetResult.Found {
		return errors.Join(err, ErrScalingIdentity)
	}
	target, err := openScalingNodeRecord(targetResult.Value, intent.Target.Node, intent.Target.NodeIncarnation)
	if err != nil {
		return errors.Join(err, ErrScalingIdentity)
	}
	if !current.Found {
		if target.Lifecycle != NodeActive || !target.PlacementEligible() || target.Revision != intent.TargetNodeRevision {
			return ErrScalingIdentity
		}
	} else if target.Lifecycle != NodeActive && target.Lifecycle != NodeDraining {
		return ErrScalingIdentity
	}
	directoryResult, err := authority.readRaw(ctx, enrollmentDirectoryKey, maxEnrollmentDirectoryBytes)
	if err != nil {
		return err
	}
	var entries []scalingIDDirectoryEntry
	if directoryResult.Found {
		entries, err = openScalingIDDirectory(directoryResult.Value, enrollmentDirectoryDocumentID[:], maxEnrollmentDirectoryBytes, MaxEnrollmentIntents)
		if err != nil {
			return err
		}
	}
	if !current.Found {
		for _, entry := range entries {
			var id [32]byte
			copy(id[:], entry.ID)
			existing, readErr := authority.readEnrollmentDirectoryEntry(ctx, id, entry)
			if readErr != nil {
				return readErr
			}
			if existing.Distribution == intent.Distribution && existing.State < EnrollmentComplete &&
				existing.State != EnrollmentCancelled {
				return ErrScalingState
			}
		}
	}
	recordBytes, err := appendEnrollmentIntentRecord(nil, intent)
	if err != nil {
		return err
	}
	recordDigest := scalingDigest(recordBytes)
	var historyResult ReplicatedPointResult
	var historyEntries []scalingIDDirectoryEntry
	var historyBytes []byte
	var evictedHistory scalingIDDirectoryEntry
	var evictedHistoryFound bool
	if intent.State >= EnrollmentComplete {
		removeScalingDirectoryEntry(&entries, intent.IntentID)
		historyEntries, historyResult, err = authority.readTerminalHistory(ctx, enrollmentHistoryKey, enrollmentHistoryDocumentID[:], maxEnrollmentHistoryBytes, maxEnrollmentHistory)
		if err != nil {
			return err
		}
		evictedHistory, evictedHistoryFound = terminalHistoryEntry(&historyEntries, intent.IntentID, intent.Revision, recordDigest, maxEnrollmentHistory)
		historyBytes, err = appendScalingIDDirectory(nil, enrollmentHistoryDocumentID[:], historyEntries, maxEnrollmentHistoryBytes)
		if err != nil {
			return err
		}
	} else {
		insertScalingDirectoryEntry(&entries, intent.IntentID, intent.Revision, recordDigest)
	}
	directoryBytes, err := appendScalingIDDirectory(nil, enrollmentDirectoryDocumentID[:], entries, maxEnrollmentDirectoryBytes)
	if err != nil {
		return err
	}
	targetMutation := NativeMutation{Kind: replication.MutationPutDigestEqual, Key: targetKey,
		Value: targetResult.Value, ExpectedValueLength: uint64(len(targetResult.Value)),
		ExpectedValueDigest: scalingDigest(targetResult.Value)}
	mutations := make([]NativeMutation, 0, 5)
	if !current.Found {
		// The immutable base catalog provenance is admitted only together with
		// the exact head and witness observed above.  A concurrent publication
		// therefore cannot leave a Reserved intent claiming a stale descriptor
		// cut or a head/witness pair from different catalog generations.
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutDigestEqual,
			Key: replicatedCatalogHeadKey, Value: enrollmentBaseHead.Value,
			ExpectedValueLength: uint64(len(enrollmentBaseHead.Value)),
			ExpectedValueDigest: scalingDigest(enrollmentBaseHead.Value)}, NativeMutation{
			Kind: replication.MutationPutDigestEqual, Key: replicatedCatalogHeadWitnessKey,
			Value: enrollmentBaseWitness.Value, ExpectedValueLength: uint64(len(enrollmentBaseWitness.Value)),
			ExpectedValueDigest: scalingDigest(enrollmentBaseWitness.Value)})
	}
	mutations = append(mutations,
		targetMutation,
		scalingRecordMutation(current, key, recordBytes),
		scalingDirectoryMutation(directoryResult, enrollmentDirectoryKey, directoryBytes),
	)
	if intent.State >= EnrollmentComplete {
		mutations = append(mutations, scalingDirectoryMutation(historyResult, enrollmentHistoryKey, historyBytes))
		if evictedHistoryFound {
			var evictedID [32]byte
			copy(evictedID[:], evictedHistory.ID)
			evictedRaw, readErr := authority.readRaw(ctx, enrollmentIntentKey(evictedID), maxEnrollmentRecordBytes)
			if readErr != nil || !evictedRaw.Found || scalingDigest(evictedRaw.Value) != replication.Digest(evictedHistory.Digest) {
				return errors.Join(readErr, ErrReplicatedCatalogConflict)
			}
			mutations = append(mutations, NativeMutation{Kind: replication.MutationDeleteDigestEqual,
				Key: enrollmentIntentKey(evictedID), ExpectedValueLength: uint64(len(evictedRaw.Value)),
				ExpectedValueDigest: scalingDigest(evictedRaw.Value)})
		}
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	return scalingMutationError(result, err, authority.session)
}

func (authority *ReplicatedCatalogAuthority) SubmitEnrollmentIntent(ctx context.Context, intent GroupEnrollmentIntent) error {
	return authority.PutEnrollmentIntent(ctx, intent, 0)
}

// ClaimEnrollmentPreparation durably reserves the right to perform the first
// node-control side effect.  The claim is a same-state revision so a second
// authority either observes the exact owner or loses the CAS before it can
// call PrepareReplica.
func (authority *ReplicatedCatalogAuthority) ClaimEnrollmentPreparation(ctx context.Context, id [32]byte, expectedRevision uint64) (GroupEnrollmentIntent, error) {
	if authority == nil || ctx == nil || id == ([32]byte{}) {
		return GroupEnrollmentIntent{}, ErrInvalidScalingMetadata
	}
	current, err := authority.ReadEnrollmentIntent(ctx, id)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	if current.State != EnrollmentReserved {
		return current, ErrScalingState
	}
	claim := EnrollmentPreparationClaim(current)
	if current.PreparationClaim == claim {
		if expectedRevision == 0 || expectedRevision == current.Revision ||
			expectedRevision != ^uint64(0) && expectedRevision+1 == current.Revision {
			return current, nil
		}
		return current, ErrScalingRevision
	}
	if expectedRevision == 0 {
		expectedRevision = current.Revision
	}
	if expectedRevision != current.Revision {
		return current, ErrScalingRevision
	}
	if current.PreparationClaim != ([32]byte{}) {
		return current, ErrScalingState
	}
	next := current
	next.PreparationClaim = claim
	next.Revision++
	if err = authority.PutEnrollmentIntent(ctx, next, current.Revision); err != nil {
		return current, err
	}
	return next, nil
}

// EnrollmentTransitionID derives the stable membership-transition identity
// for one immutable enrollment tuple.  The first sixteen bytes are consumed
// by the existing membership-grant grammar; the full tuple remains bound by
// GroupEnrollmentIntent.Digest and the receipt below.
func EnrollmentTransitionID(intent GroupEnrollmentIntent) [16]byte {
	digest := EnrollmentTransitionDigest(intent)
	var transition [16]byte
	copy(transition[:], digest[:16])
	return transition
}

// EnrollmentTransitionDigest is retained in the receipt so later membership
// grants can prove they belong to the same transition owner, not just the same
// target member.
func EnrollmentTransitionDigest(intent GroupEnrollmentIntent) [32]byte {
	if intent.IntentID == ([32]byte{}) {
		return [32]byte{}
	}
	raw := append([]byte("vibedb/enrollment-transition/1\x00"), intent.IntentID[:]...)
	digest := intent.Digest()
	raw = append(raw, digest[:]...)
	return sha256.Sum256(raw)
}

// EnrollmentGrantDigest is the canonical digest retained in a certified
// enrollment receipt.  It lets the later learner/membership step prove that
// it is using the grant planned by the same G+1 catalog publication.
func EnrollmentGrantDigest(grant membershipgrant.Grant) replication.Digest {
	if !grant.Valid() {
		return replication.Digest{}
	}
	raw, err := vibejson.Marshal(&grant)
	if err != nil {
		return replication.Digest{}
	}
	return sha256.Sum256(raw)
}

func enrollmentReplicaMatchesCatalog(identity ReplicaIdentity, replica ReplicatedReplicaDescriptor) bool {
	return identity.Member == replica.Member && identity.Node == replica.Node &&
		identity.NodeIncarnation == replica.NodeIncarnation && identity.StoreID == replica.StoreID &&
		identity.Endpoint == replica.Endpoint && identity.NativeEndpoint == replica.NativeEndpoint &&
		identity.ControlEndpoint == replica.ControlEndpoint
}

// enrollmentCatalogWithTarget constructs the only catalog cut admitted by
// PublishEnrollmentReceipt: it preserves every immutable field and adds the
// exact target outside the serving RF3.  Rebuilding through the persisted
// catalog image retains tables, schemas, request-ledger topology, and lineage
// metadata instead of silently dropping unrelated control-plane state.
func enrollmentCatalogWithTarget(current *Snapshot, intent GroupEnrollmentIntent) (*Snapshot, int, error) {
	if current == nil || !intent.Valid() || current.Generation() == ^uint64(0) {
		return nil, -1, ErrReplicatedCatalogConflict
	}
	descriptors := current.ReplicatedShardDescriptors()
	matched := -1
	for index := range descriptors {
		descriptor := descriptors[index]
		if descriptor.Group != intent.Group || descriptor.Distribution != intent.Distribution || descriptor.Shard != intent.Shard {
			continue
		}
		if matched >= 0 || descriptor.AllocationGeneration != intent.AllocationGeneration ||
			descriptor.Command != intent.ExpectedCommand || descriptor.EnrolledTarget != nil ||
			replicatedCatalogInitialRosterDigest(current, index) != intent.ExpectedRosterDigest ||
			replicatedCatalogInitialDescriptorDigest(current, index) != intent.ExpectedDescriptorDigest {
			return nil, -1, ErrReplicatedCatalogConflict
		}
		if int(intent.ReplicaOrdinal) >= len(descriptor.Replicas) ||
			!enrollmentReplicaMatchesCatalog(intent.Source, descriptor.Replicas[int(intent.ReplicaOrdinal)]) {
			return nil, -1, ErrReplicatedCatalogConflict
		}
		for _, replica := range descriptor.Replicas {
			if replica.Member == intent.Target.Member || replica.Node == intent.Target.Node ||
				replica.StoreID == intent.Target.StoreID {
				return nil, -1, ErrReplicatedCatalogConflict
			}
		}
		matched = index
	}
	if matched < 0 {
		return nil, -1, ErrReplicatedCatalogConflict
	}
	nextDescriptors := make([]ReplicatedShardDescriptor, len(current.ReplicatedShardDescriptors()))
	copy(nextDescriptors, current.ReplicatedShardDescriptors())
	target := ReplicatedReplicaDescriptor{Member: intent.Target.Member, Node: intent.Target.Node,
		StoreID: intent.Target.StoreID, NodeIncarnation: intent.Target.NodeIncarnation,
		Endpoint: intent.Target.Endpoint, NativeEndpoint: intent.Target.NativeEndpoint,
		ControlEndpoint: intent.Target.ControlEndpoint}
	nextDescriptors[matched].EnrolledTarget = &target
	next, err := NewSnapshotWithReplicatedTableMetadata(
		current.config, current.endpoints, current.Generation()+1,
		current.indexDescriptors(), current.statistics.Descriptors(), nextDescriptors,
		current.replicatedTableProfiles(), current.ReplicatedTableDeclarations(),
	)
	if err != nil {
		return nil, -1, err
	}
	if topology, ok := current.DurableRequestLedgerTopology(); ok {
		topology.Generation = next.Generation()
		if err = next.attachDurableRequestLedgerTopology(*topology); err != nil {
			return nil, -1, err
		}
	} else if err = next.attachDurableRequestLedgerRangesFromDescriptors(nextDescriptors); err != nil {
		return nil, -1, err
	}
	certified, err := advanceCatalogState(current, next)
	if err != nil {
		return nil, -1, err
	}
	return certified, matched, nil
}

func ptrPersistedReplica(replica ReplicatedReplicaDescriptor) *persistedReplicatedReplica {
	persisted := persistReplicatedReplica(replica)
	return &persisted
}

func enrollmentReceiptMatchesCatalog(intent GroupEnrollmentIntent, cut replicatedCatalogCut) bool {
	if !intent.Valid() || intent.Receipt == nil || !intent.Receipt.Valid() {
		return false
	}
	receipt := *intent.Receipt
	if receipt.IntentID != intent.IntentID || receipt.IntentDigest != intent.Digest() ||
		receipt.Target != intent.Target || cut.snapshot == nil ||
		cut.snapshot.Generation() < receipt.EnrolledCatalogGeneration {
		return false
	}
	descriptors := cut.snapshot.ReplicatedShardDescriptors()
	for index := range descriptors {
		descriptor := descriptors[index]
		if descriptor.Group != intent.Group || descriptor.Distribution != intent.Distribution || descriptor.Shard != intent.Shard ||
			descriptor.EnrolledTarget == nil || !enrollmentReplicaMatchesCatalog(intent.Target, *descriptor.EnrolledTarget) {
			continue
		}
		return replicatedCatalogInitialDescriptorDigest(cut.snapshot, index) == receipt.EnrolledDescriptorDigest
	}
	return false
}

// PublishEnrollmentReceipt is the sole authority crossing Prepared ->
// Enrolled.  It atomically publishes the non-serving enrolled-target catalog
// cut, its head witness, the exact target-row fence, and the durable receipt
// row.  A generic PutEnrollmentIntent cannot manufacture this transition.
func (authority *ReplicatedCatalogAuthority) PublishEnrollmentReceipt(ctx context.Context, intent GroupEnrollmentIntent) (GroupEnrollmentIntent, error) {
	if authority == nil || authority.session == nil || ctx == nil || !intent.Valid() ||
		intent.State != EnrollmentPrepared || intent.Proof == nil || authority.session.bundle.maxMutations < 5 {
		return GroupEnrollmentIntent{}, ErrInvalidScalingMetadata
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return GroupEnrollmentIntent{}, err
	}
	if authority.session.Status().Pending {
		return GroupEnrollmentIntent{}, ErrReplicatedCatalogPending
	}
	rowKey := enrollmentIntentKey(intent.IntentID)
	rowResult, err := authority.readRaw(ctx, rowKey, maxEnrollmentRecordBytes)
	if err != nil || !rowResult.Found {
		return GroupEnrollmentIntent{}, errors.Join(err, ErrEnrollmentIntentMissing)
	}
	currentIntent, err := openEnrollmentIntentRecord(rowResult.Value, intent.IntentID)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	if currentIntent.State == EnrollmentEnrolled {
		cut, cutErr := authority.readCatalogCut(ctx)
		if cutErr != nil || !enrollmentReceiptMatchesCatalog(currentIntent, cut) {
			return GroupEnrollmentIntent{}, errors.Join(cutErr, ErrReplicatedCatalogConflict)
		}
		return currentIntent, nil
	}
	if currentIntent.State != EnrollmentPrepared || currentIntent.Digest() != intent.Digest() ||
		currentIntent.Revision != intent.Revision || currentIntent.Proof == nil || *currentIntent.Proof != *intent.Proof {
		return GroupEnrollmentIntent{}, ErrScalingRevision
	}
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	if cut.snapshot == nil || cut.snapshot.Generation() < intent.CatalogGeneration ||
		intent.ExpectedCatalogHeadDigest == (replication.Digest{}) {
		return GroupEnrollmentIntent{}, ErrReplicatedCatalogConflict
	}
	certified, shardIndex, err := enrollmentCatalogWithTarget(cut.snapshot, intent)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	nextHead, err := appendReplicatedCatalogDocument(nil, certified, maxReplicatedCatalogBytes)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	nextWitness, err := appendReplicatedCatalogHeadWitness(nil, certified.Generation(), nextHead)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	nextDescriptorDigest := replication.Digest(replicatedCatalogInitialDescriptorDigest(certified, shardIndex))
	if nextDescriptorDigest == (replication.Digest{}) {
		return GroupEnrollmentIntent{}, ErrReplicatedCatalogConflict
	}
	grant, err := BuildReplicaReplacementMembershipGrant(certified, intent.Group,
		EnrollmentTransitionID(intent), intent.CatalogGeneration, intent.Source.Member, intent.Target.Member)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	receipt := CertifiedEnrollmentReceipt{
		IntentID: intent.IntentID, IntentDigest: intent.Digest(),
		BaseCatalogGeneration:            intent.CatalogGeneration,
		BaseCatalogHeadDigest:            intent.ExpectedCatalogHeadDigest,
		BaseDescriptorDigest:             intent.ExpectedDescriptorDigest,
		PublicationPredecessorGeneration: cut.snapshot.Generation(),
		PublicationPredecessorHeadDigest: replication.Digest(sha256.Sum256(cut.head)),
		EnrolledCatalogGeneration:        certified.Generation(),
		EnrolledCatalogHeadDigest:        replication.Digest(sha256.Sum256(nextHead)),
		EnrolledDescriptorDigest:         nextDescriptorDigest,
		Target:                           intent.Target, InitialReplicaSetVersion: intent.ExpectedCommand.ReplicaSetVersion,
		GrantDigest: EnrollmentGrantDigest(grant), TransitionID: EnrollmentTransitionDigest(intent),
	}
	if !receipt.Valid() || !ValidateEnrollmentReceipt(intent, receipt,
		receipt.PublicationPredecessorHeadDigest, receipt.EnrolledCatalogHeadDigest) {
		return GroupEnrollmentIntent{}, ErrReplicatedCatalogConflict
	}
	enrolled := currentIntent
	enrolled.State = EnrollmentEnrolled
	enrolled.Revision++
	enrolled.Receipt = &receipt
	recordBytes, err := appendEnrollmentIntentRecord(nil, enrolled)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	directoryResult, err := authority.readRaw(ctx, enrollmentDirectoryKey, maxEnrollmentDirectoryBytes)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	var entries []scalingIDDirectoryEntry
	if directoryResult.Found {
		entries, err = openScalingIDDirectory(directoryResult.Value, enrollmentDirectoryDocumentID[:], maxEnrollmentDirectoryBytes, MaxEnrollmentIntents)
		if err != nil {
			return GroupEnrollmentIntent{}, err
		}
	}
	entry, found := findScalingDirectoryEntry(entries, intent.IntentID)
	rowDigest := scalingDigest(rowResult.Value)
	if !found || entry.Revision != currentIntent.Revision || !bytes.Equal(entry.Digest, rowDigest[:]) {
		return GroupEnrollmentIntent{}, ErrReplicatedCatalogConflict
	}
	insertScalingDirectoryEntry(&entries, intent.IntentID, enrolled.Revision, scalingDigest(recordBytes))
	directoryBytes, err := appendScalingIDDirectory(nil, enrollmentDirectoryDocumentID[:], entries, maxEnrollmentDirectoryBytes)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	targetKey := scalingNodeKey(intent.Target.Node, intent.Target.NodeIncarnation)
	targetResult, err := authority.readRaw(ctx, targetKey, maxScalingNodeRecordBytes)
	if err != nil || !targetResult.Found {
		return GroupEnrollmentIntent{}, errors.Join(err, ErrScalingIdentity)
	}
	target, err := openScalingNodeRecord(targetResult.Value, intent.Target.Node, intent.Target.NodeIncarnation)
	if err != nil || (target.Lifecycle != NodeActive && target.Lifecycle != NodeDraining) {
		return GroupEnrollmentIntent{}, errors.Join(err, ErrScalingIdentity)
	}
	headDigest := sha256.Sum256(cut.head)
	witnessDigest := sha256.Sum256(cut.witness)
	mutations := []NativeMutation{
		{Kind: replication.MutationPutDigestEqual, Key: replicatedCatalogHeadKey, Value: nextHead,
			ExpectedValueLength: uint64(len(cut.head)), ExpectedValueDigest: replication.Digest(headDigest)},
		{Kind: replication.MutationPutDigestEqual, Key: replicatedCatalogHeadWitnessKey, Value: nextWitness,
			ExpectedValueLength: uint64(len(cut.witness)), ExpectedValueDigest: replication.Digest(witnessDigest)},
		{Kind: replication.MutationPutDigestEqual, Key: targetKey, Value: targetResult.Value,
			ExpectedValueLength: uint64(len(targetResult.Value)), ExpectedValueDigest: scalingDigest(targetResult.Value)},
		{Kind: replication.MutationPutDigestEqual, Key: rowKey, Value: recordBytes,
			ExpectedValueLength: uint64(len(rowResult.Value)), ExpectedValueDigest: scalingDigest(rowResult.Value)},
		scalingDirectoryMutation(directoryResult, enrollmentDirectoryKey, directoryBytes),
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	if err != nil {
		if errors.Is(err, ErrNativeCommandPending) || authority.session.Status().Pending {
			authority.pendingCatalog, authority.pendingExpected = certified, intent.CatalogGeneration
			authority.pendingGrant = membershipgrant.Grant{}
			return GroupEnrollmentIntent{}, errors.Join(ErrReplicatedCatalogPending, err)
		}
		return GroupEnrollmentIntent{}, err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return GroupEnrollmentIntent{}, ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return GroupEnrollmentIntent{}, ErrReplicatedCatalog
	}
	if err = authority.observePublishedCatalog(ctx, certified); err != nil {
		return GroupEnrollmentIntent{}, err
	}
	if err = authority.publishCommittedCatalogAfter(intent.CatalogGeneration, certified); err != nil {
		return GroupEnrollmentIntent{}, err
	}
	return enrolled, nil
}

// CancelEnrollmentIntent only cancels a Reserved row. At that point no
// provisioner side effect is authorized, so removing the active reservation
// cannot strand a prepared artifact or silently discard a learner grant.
// Prepared and later rows require the node-control abort/settlement workflow
// and are therefore rejected here rather than being marked falsely complete.
func (authority *ReplicatedCatalogAuthority) CancelEnrollmentIntent(ctx context.Context, id [32]byte, expectedRevision uint64) (GroupEnrollmentIntent, error) {
	if authority == nil || ctx == nil || id == ([32]byte{}) {
		return GroupEnrollmentIntent{}, ErrInvalidScalingMetadata
	}
	current, err := authority.ReadEnrollmentIntent(ctx, id)
	if err != nil {
		return GroupEnrollmentIntent{}, err
	}
	if current.State != EnrollmentReserved {
		return current, ErrScalingState
	}
	if current.PreparationClaim != ([32]byte{}) {
		return current, ErrScalingState
	}
	if expectedRevision == 0 {
		expectedRevision = current.Revision
	}
	if expectedRevision != current.Revision {
		return current, ErrScalingRevision
	}
	next := current
	next.State = EnrollmentCancelled
	next.Revision++
	if err = authority.PutEnrollmentIntent(ctx, next, current.Revision); err != nil {
		return current, err
	}
	return next, nil
}

func mustAppendEnrollmentIntent(intent GroupEnrollmentIntent) []byte {
	raw, _ := appendEnrollmentIntentRecord(nil, intent)
	return raw
}

func insertScalingDirectoryEntry(entries *[]scalingIDDirectoryEntry, id [32]byte, revision uint64, digest replication.Digest) {
	values := *entries
	position := 0
	for position < len(values) && bytes.Compare(values[position].ID, id[:]) < 0 {
		position++
	}
	if position < len(values) && bytes.Equal(values[position].ID, id[:]) {
		values[position].Revision = revision
		values[position].Digest = bytes.Clone(digest[:])
		return
	}
	values = append(values, scalingIDDirectoryEntry{})
	copy(values[position+1:], values[position:])
	values[position] = scalingIDDirectoryEntry{ID: bytes.Clone(id[:]), Revision: revision, Digest: bytes.Clone(digest[:])}
	*entries = values
}

func removeScalingDirectoryEntry(entries *[]scalingIDDirectoryEntry, id [32]byte) {
	values := *entries
	for index := range values {
		if bytes.Equal(values[index].ID, id[:]) {
			copy(values[index:], values[index+1:])
			*entries = values[:len(values)-1]
			return
		}
	}
}

// terminalHistoryEntry adds one terminal row to a bounded, lexicographically
// ordered tombstone directory. The record itself remains addressable until it
// is evicted, so status/wait can settle a lost response; once the bound is
// reached the smallest deterministic ID is removed in the same CAS batch.
func terminalHistoryEntry(entries *[]scalingIDDirectoryEntry, id [32]byte, revision uint64, digest replication.Digest, maximum int) (scalingIDDirectoryEntry, bool) {
	if entries == nil || maximum <= 0 {
		return scalingIDDirectoryEntry{}, false
	}
	values := *entries
	// A retry or a terminal update for an already retained ID must update the
	// existing entry in place.  In particular, never choose the just-finished
	// record as the eviction victim: doing so leaves the caller trying to
	// compare/delete a digest that was only written by the same terminal CAS.
	position := 0
	for position < len(values) && bytes.Compare(values[position].ID, id[:]) < 0 {
		position++
	}
	if position < len(values) && bytes.Equal(values[position].ID, id[:]) {
		values[position].Revision = revision
		values[position].Digest = bytes.Clone(digest[:])
		*entries = values
		return scalingIDDirectoryEntry{}, false
	}
	var evicted scalingIDDirectoryEntry
	var evictedFound bool
	if len(values) >= maximum {
		// Remove the opposite end from the insertion point.  This preserves the
		// new terminal record even when its ID sorts before every retained row.
		if position == 0 {
			evicted = values[len(values)-1]
			values = values[:len(values)-1]
		} else {
			evicted = values[0]
			values = values[1:]
			position--
		}
		evictedFound = true
	}
	values = append(values, scalingIDDirectoryEntry{})
	copy(values[position+1:], values[position:])
	values[position] = scalingIDDirectoryEntry{ID: bytes.Clone(id[:]), Revision: revision, Digest: bytes.Clone(digest[:])}
	*entries = values
	return evicted, evictedFound
}

func findScalingDirectoryEntry(entries []scalingIDDirectoryEntry, id [32]byte) (scalingIDDirectoryEntry, bool) {
	for _, entry := range entries {
		if bytes.Equal(entry.ID, id[:]) {
			return entry, true
		}
	}
	return scalingIDDirectoryEntry{}, false
}

func (authority *ReplicatedCatalogAuthority) readTerminalHistory(
	ctx context.Context, key, documentID []byte, maximum, maximumEntries int,
) ([]scalingIDDirectoryEntry, ReplicatedPointResult, error) {
	result, err := authority.readRaw(ctx, key, uint32(maximum))
	if err != nil || !result.Found {
		return nil, result, err
	}
	entries, err := openScalingIDDirectory(result.Value, documentID, maximum, maximumEntries)
	return entries, result, err
}

func revisionRetryMatches(expected, desired uint64) bool {
	return expected == desired || expected != ^uint64(0) && expected+1 == desired
}
