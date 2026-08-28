package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"math"
	"math/bits"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// RelationKind freezes the deterministic row semantics of one dense physical
// bundle slot. Relation names are used only while opening durable handles;
// apply resolves the compact slot directly.
type RelationKind uint8

const (
	RelationJSON RelationKind = iota + 1
	RelationGlobalIndex
)

// GlobalIndexKeyEncoding identifies the physical row-key grammar authenticated
// by a relation manifest.
type GlobalIndexKeyEncoding uint8

const GlobalIndexKeyCanonicalTuple GlobalIndexKeyEncoding = 1

// GlobalIndexProfile is the schema-generation-bound identity and value shape
// for one independently stored exact/global index relation.
type GlobalIndexProfile struct {
	IndexID       uint64
	Incarnation   uint64
	LocatorCount  uint8
	Unique        bool
	KeyEncoding   GlobalIndexKeyEncoding
	KeyArity      uint8
	TupleVersion  distribution.TupleVersion
	MapperVersion distribution.MapperVersion
	BucketBits    uint8
}

// RelationCollection binds one dense bundle-local slot to its already-opened
// durable handle and deterministic validation contract. LocalIndexes lists the
// exact indexes maintained natively inside a JSON collection. The list is
// canonicalized and compared with the live durable catalog during OpenBundle.
type RelationCollection struct {
	Relation     replication.RelationID
	Kind         RelationKind
	Name         string
	Target       CollectionTarget
	LocalIndexes []store.IndexDefinition
	GlobalIndex  GlobalIndexProfile
}

type relationCollection struct {
	id               replication.RelationID
	kind             RelationKind
	name             string
	target           CollectionTarget
	localIndexes     []store.IndexDefinition
	globalIndex      GlobalIndexProfile
	contract         [sha256.Size]byte
	openedImage      [sha256.Size]byte
	openedApplied    uint64
	openedGen        uint64
	placement        relationPlacementAccumulator
	placementApplied uint64
	placementGen     uint64
}

var relationManifestDigestDomain = []byte(
	"vibedb/replicated-state/relation-manifest\x00",
)

var relationImageDigestDomain = []byte(
	"vibedb/replicated-state/relation-image\x00",
)

func prepareRelationCollections(
	binding Binding,
	input []RelationCollection,
) ([]relationCollection, [sha256.Size]byte, error) {
	relations, err := prepareRelationSchemas(input)
	if err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	for _, spec := range input {
		if err := validateRelationTarget(spec); err != nil {
			return nil, [sha256.Size]byte{}, fmt.Errorf("relation %d: %w", spec.Relation, err)
		}
	}
	manifest := relationManifestDigest(binding.SchemaGeneration, relations)
	for ordinal := range relations {
		relations[ordinal].contract = relationContractDigest(manifest, &relations[ordinal])
	}
	return relations, manifest, nil
}

// prepareRelationSchemas is shared by cold allocation and live OpenBundle.
// It never substitutes a collection or a mutation validator for live serving.
func prepareRelationSchemas(input []RelationCollection) ([]relationCollection, error) {
	if len(input) == 0 || len(input) > replication.MaxRelationsPerBundle {
		return nil, ErrInvalidCollection
	}
	relations := make([]relationCollection, len(input))
	for ordinal := range input {
		spec := &input[ordinal]
		wantID := replication.RelationID(ordinal + 1)
		if spec.Relation != wantID || spec.Name == "" ||
			len(spec.Name) > replication.MaxIdentityBytes ||
			!utf8.ValidString(spec.Name) || strings.IndexByte(spec.Name, 0) >= 0 ||
			spec.Name == systemCollectionName {
			return nil, fmt.Errorf(
				"%w: relation slot %d identity", ErrInvalidCollection, ordinal+1,
			)
		}
		for prior := 0; prior < ordinal; prior++ {
			if spec.Name == input[prior].Name {
				return nil, fmt.Errorf(
					"%w: duplicate relation name", ErrInvalidCollection,
				)
			}
		}
		if err := validateRelationSchema(*spec); err != nil {
			return nil, fmt.Errorf(
				"relation %d: %w", spec.Relation, err,
			)
		}
		indexes, err := canonicalLocalIndexes(spec.LocalIndexes)
		if err != nil {
			return nil, fmt.Errorf(
				"relation %d: %w", spec.Relation, err,
			)
		}
		relations[ordinal] = relationCollection{
			id: spec.Relation, kind: spec.Kind, name: strings.Clone(spec.Name),
			target: spec.Target, localIndexes: indexes, globalIndex: spec.GlobalIndex,
		}
	}
	return relations, nil
}

func validateBundleTransactionProfile(
	_ CollectionTarget,
	relations []relationCollection,
	options Options,
) error {
	reservedCapture := options.TransitionCaptureTarget.Collection != nil ||
		options.TransitionCaptureTarget.Name != ""
	requiredCollections := len(relations) + 1
	if reservedCapture {
		requiredCollections++
	}
	if len(relations) == 0 || options.TxnLimits.MaxCollections < requiredCollections {
		return ErrInvalidOptions
	}
	relationDocuments := 0
	relationBytes := int64(0)
	for i := range relations {
		if relationDocuments > math.MaxInt-relations[i].target.Limits.MaxDistinctMutations {
			return ErrInvalidOptions
		}
		relationDocuments += relations[i].target.Limits.MaxDistinctMutations
		limit := int64(relations[i].target.Limits.MaxBatchBytes)
		if relationBytes > math.MaxInt64-limit {
			return ErrInvalidOptions
		}
		relationBytes += limit
	}
	// One admitted command carries at most MaxCommandBytes of relation keys and
	// values and MaxMutations total, even when the per-relation frozen
	// capacities sum higher.
	relationBytes = min(relationBytes, int64(replication.MaxCommandBytes))
	relationDocuments = min(relationDocuments, replication.MaxMutations)
	fenceRowBytes := len(sessionFenceKey(0, 0)) + sessionFenceBytes
	hotSystemBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + MaxAuthorityBindingBytes +
		sha256.Size + 1 + MaxSessionRecordBytes +
		sha256.Size + 3 + MaxSessionSlotRecordBytes + fenceRowBytes
	releaseSystemBytes := len(stateKey) + MaxStateEnvelopeBytes +
		2*(sha256.Size+1) + int(options.RetryWindow)*(sha256.Size+3+fenceRowBytes+routeGateResultKeyBytes)
	requiredDocuments, err := RequiredBundleTransactionDocuments(
		relationDocuments, options.RetryWindow, reservedCapture,
	)
	if err != nil {
		return err
	}
	requiredBytes := int64(releaseSystemBytes)
	if int64(hotSystemBytes) > math.MaxInt64-relationBytes {
		return ErrInvalidOptions
	}
	requiredBytes = max(requiredBytes, int64(hotSystemBytes)+relationBytes)
	if reservedCapture {
		capture := options.TransitionCaptureTarget.Collection
		if capture == nil || capture.MaxBatchBytes() <= 0 ||
			requiredBytes > math.MaxInt64-int64(capture.MaxBatchBytes()) {
			return ErrInvalidOptions
		}
		requiredBytes += int64(capture.MaxBatchBytes())
	}
	if options.TxnLimits.MaxDocuments < requiredDocuments ||
		options.TxnLimits.MaxBytes < requiredBytes {
		return ErrInvalidOptions
	}
	return nil
}

// RequiredBundleTransactionDocuments returns the exact mutation-slot ceiling
// for one replicated apply transaction. A data command publishes state,
// session and slot alongside all relation changes, and may update or delete
// one historical fence when overwriting an old slot. Session open publishes
// state, authority, session and slot but no relation batch. Release deletes one
// session header and every retry slot, updates or deletes at most one historical
// fence per slot, and publishes state. A reserved transition capture contributes
// one additional private row to every shape.
func RequiredBundleTransactionDocuments(
	relationDocuments int,
	retryWindow uint16,
	reservedCapture bool,
) (int, error) {
	if relationDocuments < 0 || relationDocuments > replication.MaxMutations ||
		retryWindow == 0 || retryWindow > MaxSessionRetryWindow {
		return 0, ErrInvalidOptions
	}
	required := max(3*int(retryWindow)+3, relationDocuments+4, 4)
	if reservedCapture {
		required++
	}
	return required, nil
}

func validateRelationTarget(spec RelationCollection) error {
	if err := validateRelationSchema(spec); err != nil {
		return err
	}
	t := spec.Target
	if t.Collection == nil || !t.schemaMatches() ||
		!t.Collection.HasSynchronousDurability() || !t.Collection.SupportsUpdate() ||
		t.Collection.HasOpaqueValues() || t.Validator == nil {
		return ErrInvalidCollection
	}
	l := t.Limits
	if l.MaxKeyBytes != t.Collection.MaxKeyBytes() || l.MaxDocumentBytes != t.Collection.MaxDocumentBytes() ||
		l.MaxDistinctMutations != t.Collection.MaxBatchDocuments() || l.MaxBatchBytes != t.Collection.MaxBatchBytes() ||
		spec.Kind == RelationGlobalIndex && (t.Collection.HasIndexes() || t.Collection.HasSchema()) {
		return ErrInvalidCollection
	}
	return nil
}

func validateRelationSchema(spec RelationCollection) error {
	t := spec.Target
	if t.Validation != ValidationDeterministicMutation || t.ValidationDigest == ([sha256.Size]byte{}) {
		return ErrInvalidCollection
	}
	l := t.Limits
	if l.MaxKeyBytes <= 0 || l.MaxKeyBytes > replication.MaxMutationKeyBytes ||
		l.MaxDocumentBytes <= 0 || l.MaxDocumentBytes > replication.MaxMutationValueBytes ||
		l.MaxDistinctMutations <= 0 || l.MaxDistinctMutations > MaxDistinctMutations ||
		l.MaxBatchBytes <= 0 {
		return ErrInvalidCollection
	}
	switch spec.Kind {
	case RelationJSON:
		if spec.GlobalIndex != (GlobalIndexProfile{}) {
			return ErrInvalidCollection
		}
	case RelationGlobalIndex:
		profile := spec.GlobalIndex
		if profile.IndexID == 0 || profile.Incarnation == 0 ||
			profile.LocatorCount == 0 || profile.LocatorCount > 8 ||
			profile.KeyEncoding != GlobalIndexKeyCanonicalTuple || profile.KeyArity == 0 ||
			profile.KeyArity > distribution.KeyspaceWidth ||
			profile.TupleVersion != distribution.CurrentTupleVersion ||
			profile.MapperVersion != distribution.NativeMapperVersion ||
			!distribution.ValidVirtualBucketBits(profile.BucketBits) ||
			len(spec.LocalIndexes) != 0 {
			return ErrInvalidCollection
		}
	default:
		return ErrInvalidCollection
	}
	return nil
}

func canonicalLocalIndexes(input []store.IndexDefinition) ([]store.IndexDefinition, error) {
	if len(input) == 0 {
		return nil, nil
	}
	result := make([]store.IndexDefinition, len(input))
	for i := range input {
		compiled, err := store.CompileExactIndex(input[i])
		if err != nil {
			return nil, err
		}
		result[i].Name = strings.Clone(input[i].Name)
		result[i].Paths = make([]string, int(compiled.N))
		for column := 0; column < int(compiled.N); column++ {
			result[i].Paths[column] = strings.Clone(compiled.Specs[column])
		}
	}
	slices.SortFunc(result, func(left, right store.IndexDefinition) int {
		return strings.Compare(left.Name, right.Name)
	})
	for i := 1; i < len(result); i++ {
		if result[i-1].Name == result[i].Name {
			return nil, store.ErrIndexDefinition
		}
	}
	return result, nil
}

func validateRelationIndexCatalog(
	snapshot *durable.Snapshot,
	want []store.IndexDefinition,
) error {
	if snapshot == nil {
		return ErrInvalidCollection
	}
	got := snapshot.AppendIndexes(nil)
	if len(got) != len(want) {
		return ErrSchemaProfile
	}
	for i := range got {
		if got[i].Name != want[i].Name || got[i].Kind != store.IndexExact ||
			got[i].State != store.IndexReady || int(got[i].ColumnCount) != len(want[i].Paths) {
			return ErrSchemaProfile
		}
		for column := range want[i].Paths {
			if got[i].Columns[column] != want[i].Paths[column] {
				return ErrSchemaProfile
			}
		}
	}
	return nil
}

func openedRelationImageDigest(
	relation *relationCollection,
	snapshot *durable.Snapshot,
	owned distribution.KeyRange,
) ([sha256.Size]byte, relationPlacementAccumulator, error) {
	if relation == nil || snapshot == nil {
		return [sha256.Size]byte{}, relationPlacementAccumulator{}, ErrInvalidCollection
	}
	h, err := newCanonicalImageHasher(
		relation.name, relation.target.Validation,
		relation.target.ValidationDigest, relation.target.Validator,
	)
	if err != nil {
		return [sha256.Size]byte{}, relationPlacementAccumulator{}, err
	}
	placementValidator, placementEnabled :=
		relation.target.Validator.(GlobalIndexPlacementValidator)
	placementRange := owned
	if placementEnabled {
		placementRange = placementValidator.GlobalIndexPlacementRange()
		if !placementRange.Valid() {
			return [sha256.Size]byte{}, relationPlacementAccumulator{}, ErrSchemaProfile
		}
	}
	placement := newRelationPlacementAccumulator(placementRange, placementEnabled)
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		if relation.kind == RelationGlobalIndex &&
			!validGlobalIndexLocator(value, relation.globalIndex.LocatorCount) {
			return ErrSchemaProfile
		}
		if relation.kind == RelationGlobalIndex && placementEnabled {
			point, ok := placementValidator.GlobalIndexPlacementPoint(key)
			if !ok {
				return ErrSchemaProfile
			}
			placement.addRaw(point, key, value)
		}
		return h.add(key, value)
	}); err != nil {
		return [sha256.Size]byte{}, relationPlacementAccumulator{}, err
	}
	return h.sum(), placement, nil
}

var certifiedIncrementalRowDomain = []byte("vibedb/range-split/child-stage-row\x00")
var certifiedIncrementalEmptyDomain = []byte("vibedb/range-split/child-stage-empty\x00")

// openedRelationImageDigests validates each row once while deriving both the
// ordinary ordered digest and the constant-space split-import root. The latter
// is used only to authenticate an untouched imported split child at reopen.
func openedRelationImageDigests(
	relation *relationCollection,
	snapshot *durable.Snapshot,
	owned distribution.KeyRange,
) (
	ordered, incremental [sha256.Size]byte,
	placement relationPlacementAccumulator,
	err error,
) {
	if relation == nil || snapshot == nil {
		return ordered, incremental, placement, ErrInvalidCollection
	}
	canonical, err := newCanonicalImageHasher(
		relation.name, relation.target.Validation,
		relation.target.ValidationDigest, relation.target.Validator,
	)
	if err != nil {
		return ordered, incremental, placement, err
	}
	placementValidator, placementEnabled :=
		relation.target.Validator.(GlobalIndexPlacementValidator)
	placementRange := owned
	if placementEnabled {
		placementRange = placementValidator.GlobalIndexPlacementRange()
		if !placementRange.Valid() {
			return ordered, incremental, placement, ErrSchemaProfile
		}
	}
	placement = newRelationPlacementAccumulator(placementRange, placementEnabled)
	rowHasher := sha256.New()
	var size [8]byte
	err = snapshot.RangeRaw(func(key, value []byte) error {
		if relation.kind == RelationGlobalIndex &&
			!validGlobalIndexLocator(value, relation.globalIndex.LocatorCount) {
			return ErrSchemaProfile
		}
		if relation.kind == RelationGlobalIndex && placementEnabled {
			point, ok := placementValidator.GlobalIndexPlacementPoint(key)
			if !ok {
				return ErrSchemaProfile
			}
			placement.addRaw(point, key, value)
		}
		if err := canonical.add(key, value); err != nil {
			return err
		}
		row := certifiedIncrementalRowDigest(rowHasher, &size, key, value)
		addCertifiedIncrementalDigest(&incremental, row)
		return nil
	})
	if err != nil {
		return ordered, [sha256.Size]byte{}, relationPlacementAccumulator{}, err
	}
	if snapshot.Len() == 0 {
		incremental = sha256.Sum256(certifiedIncrementalEmptyDomain)
	}
	return canonical.sum(), incremental, placement, nil
}

func certifiedIncrementalRowDigest(
	h hash.Hash, size *[8]byte, key, value []byte,
) [sha256.Size]byte {
	h.Reset()
	_, _ = h.Write(certifiedIncrementalRowDomain)
	writeHashFrame(h, key)
	binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	valueDigest := sha256.Sum256(value)
	_, _ = h.Write(valueDigest[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func addCertifiedIncrementalDigest(target *[sha256.Size]byte, value [sha256.Size]byte) {
	carry := uint64(0)
	for offset := 0; offset < sha256.Size; offset += 8 {
		next, nextCarry := bits.Add64(
			binary.LittleEndian.Uint64(target[offset:offset+8]),
			binary.LittleEndian.Uint64(value[offset:offset+8]), carry,
		)
		binary.LittleEndian.PutUint64(target[offset:offset+8], next)
		carry = nextCarry
	}
}

func canonicalRelationImageDigest(
	relations []relationCollection,
) ([sha256.Size]byte, error) {
	if len(relations) == 0 {
		return [sha256.Size]byte{}, ErrInvalidCollection
	}
	if len(relations) == 1 {
		if relations[0].openedImage == ([sha256.Size]byte{}) {
			return [sha256.Size]byte{}, ErrInconsistentSnapshot
		}
		return relations[0].openedImage, nil
	}
	h := sha256.New()
	_, _ = h.Write(relationImageDigestDomain)
	var fixed [10]byte
	binary.LittleEndian.PutUint64(fixed[:8], uint64(len(relations)))
	_, _ = h.Write(fixed[:8])
	for i := range relations {
		relation := &relations[i]
		if relation.openedImage == ([sha256.Size]byte{}) {
			return [sha256.Size]byte{}, ErrInconsistentSnapshot
		}
		binary.LittleEndian.PutUint16(fixed[8:10], uint16(relation.id))
		_, _ = h.Write(fixed[8:10])
		_, _ = h.Write(relation.openedImage[:])
	}
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result, nil
}

func relationManifestDigest(
	schemaGeneration uint64,
	relations []relationCollection,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(relationManifestDigestDomain)
	var fixed [32]byte
	binary.LittleEndian.PutUint64(fixed[0:8], schemaGeneration)
	binary.LittleEndian.PutUint64(fixed[8:16], uint64(len(relations)))
	_, _ = h.Write(fixed[:16])
	for i := range relations {
		relation := &relations[i]
		binary.LittleEndian.PutUint16(fixed[0:2], uint16(relation.id))
		fixed[2] = byte(relation.kind)
		if relation.globalIndex.Unique {
			fixed[3] = 1
		} else {
			fixed[3] = 0
		}
		fixed[4] = relation.globalIndex.LocatorCount
		fixed[5] = byte(relation.globalIndex.KeyEncoding)
		fixed[6] = relation.globalIndex.KeyArity
		fixed[7] = relation.globalIndex.BucketBits
		binary.LittleEndian.PutUint64(fixed[8:16], relation.globalIndex.IndexID)
		binary.LittleEndian.PutUint64(fixed[16:24], relation.globalIndex.Incarnation)
		binary.LittleEndian.PutUint32(fixed[24:28], uint32(relation.globalIndex.TupleVersion))
		binary.LittleEndian.PutUint32(fixed[28:32], uint32(relation.globalIndex.MapperVersion))
		_, _ = h.Write(fixed[:])
		writeHashFrame(h, []byte(relation.name))
		_, _ = h.Write([]byte{byte(relation.target.Validation)})
		_, _ = h.Write(relation.target.ValidationDigest[:])
		var limits [32]byte
		binary.LittleEndian.PutUint64(limits[0:8], uint64(relation.target.Limits.MaxKeyBytes))
		binary.LittleEndian.PutUint64(limits[8:16], uint64(relation.target.Limits.MaxDocumentBytes))
		binary.LittleEndian.PutUint64(limits[16:24], uint64(relation.target.Limits.MaxDistinctMutations))
		binary.LittleEndian.PutUint64(limits[24:32], uint64(relation.target.Limits.MaxBatchBytes))
		_, _ = h.Write(limits[:])
		binary.LittleEndian.PutUint64(fixed[0:8], uint64(len(relation.localIndexes)))
		_, _ = h.Write(fixed[:8])
		for j := range relation.localIndexes {
			index := &relation.localIndexes[j]
			writeHashFrame(h, []byte(index.Name))
			binary.LittleEndian.PutUint64(fixed[0:8], uint64(len(index.Paths)))
			_, _ = h.Write(fixed[:8])
			for _, path := range index.Paths {
				writeHashFrame(h, []byte(path))
			}
		}
	}
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func relationContractDigest(
	manifest [sha256.Size]byte,
	relation *relationCollection,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(relationManifestDigestDomain)
	_, _ = h.Write(manifest[:])
	var id [2]byte
	binary.LittleEndian.PutUint16(id[:], uint16(relation.id))
	_, _ = h.Write(id[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func sameRelationNames(left, right []relationCollection) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].id != right[i].id || !bytes.Equal([]byte(left[i].name), []byte(right[i].name)) {
			return false
		}
	}
	return true
}
