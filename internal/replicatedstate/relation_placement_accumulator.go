package replicatedstate

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/bits"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	relationPlacementRowDomain   = []byte("vibedb/replicated-state/relation-placement-row\x00")
	relationPlacementProofDomain = []byte("vibedb/replicated-state/relation-placement-proof\x00")
	relationPlacementStateDomain = []byte("vibedb/replicated-state/relation-placement-state\x00")
)

// relationPlacementAccumulator is a fixed-size, order-independent commitment
// to one global-index image. XOR catches bitwise divergence while the
// independent 256-bit modular sum prevents equal-pair cancellation; Rows
// authenticates multiplicity. Outside counts rows that are canonical but do
// not belong to the immutable range used for this audit.
type relationPlacementAccumulator struct {
	enabled    bool
	rangeOwned distribution.KeyRange
	rows       uint64
	outside    uint64
	xor        [sha256.Size]byte
	sum        [4]uint64
}

func relationPlacementStateDigest(
	schemaGeneration uint64,
	manifest [sha256.Size]byte,
	relations []relationCollection,
) [sha256.Size]byte {
	return relationPlacementStateDigestWith(schemaGeneration, manifest, relations, nil)
}

func relationPlacementStateDigestWith(
	schemaGeneration uint64,
	manifest [sha256.Size]byte,
	relations []relationCollection,
	placements *[replication.MaxRelationsPerBundle]relationPlacementAccumulator,
) [sha256.Size]byte {
	count := 0
	for i := range relations {
		placement := relations[i].placement
		if placements != nil {
			placement = placements[i]
		}
		if placement.enabled {
			count++
		}
	}
	if count == 0 {
		return [sha256.Size]byte{}
	}
	h := sha256.New()
	_, _ = h.Write(relationPlacementStateDomain)
	_, _ = h.Write(manifest[:])
	var fixed [64]byte
	binary.LittleEndian.PutUint64(fixed[0:8], schemaGeneration)
	binary.LittleEndian.PutUint64(fixed[8:16], uint64(count))
	_, _ = h.Write(fixed[:16])
	for i := range relations {
		relation := &relations[i]
		placement := relation.placement
		if placements != nil {
			placement = placements[i]
		}
		if !placement.enabled {
			continue
		}
		clear(fixed[:])
		binary.LittleEndian.PutUint16(fixed[0:2], uint16(relation.id))
		copy(fixed[8:16], placement.rangeOwned.Start[:])
		copy(fixed[16:24], placement.rangeOwned.End.Point[:])
		if placement.rangeOwned.End.Max {
			fixed[24] = 1
		}
		binary.LittleEndian.PutUint64(fixed[32:40], placement.rows)
		binary.LittleEndian.PutUint64(fixed[40:48], placement.outside)
		_, _ = h.Write(fixed[:48])
		_, _ = h.Write(relation.contract[:])
		_, _ = h.Write(placement.xor[:])
		for word := range placement.sum {
			binary.LittleEndian.PutUint64(fixed[word*8:word*8+8], placement.sum[word])
		}
		_, _ = h.Write(fixed[:32])
	}
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func newRelationPlacementAccumulator(
	owned distribution.KeyRange,
	enabled bool,
) relationPlacementAccumulator {
	return relationPlacementAccumulator{enabled: enabled, rangeOwned: owned}
}

func relationPlacementRowDigest(key []byte, valueLength uint64, valueDigest [sha256.Size]byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(relationPlacementRowDomain)
	var fixed [16]byte
	binary.LittleEndian.PutUint64(fixed[0:8], uint64(len(key)))
	binary.LittleEndian.PutUint64(fixed[8:16], valueLength)
	_, _ = h.Write(fixed[:])
	_, _ = h.Write(key)
	_, _ = h.Write(valueDigest[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func (a *relationPlacementAccumulator) addRaw(point distribution.KeyspacePoint, key, value []byte) {
	digest := sha256.Sum256(value)
	a.addDescriptor(point, key, uint64(len(value)), digest)
}

func (a *relationPlacementAccumulator) addDescriptor(point distribution.KeyspacePoint, key []byte, valueLength uint64, valueDigest [sha256.Size]byte) {
	digest := relationPlacementRowDigest(key, valueLength, valueDigest)
	a.rows++
	if !a.rangeOwned.Contains(point) {
		a.outside++
	}
	for i := range a.xor {
		a.xor[i] ^= digest[i]
	}
	var carry uint64
	for i := range a.sum {
		word := binary.LittleEndian.Uint64(digest[i*8 : i*8+8])
		next, c1 := bits.Add64(a.sum[i], word, 0)
		next, c2 := bits.Add64(next, carry, 0)
		a.sum[i], carry = next, c1|c2
	}
}

func (a *relationPlacementAccumulator) removeDescriptor(point distribution.KeyspacePoint, key []byte, valueLength uint64, valueDigest [sha256.Size]byte) error {
	if a.rows == 0 || !a.rangeOwned.Contains(point) && a.outside == 0 {
		return ErrInconsistentSnapshot
	}
	digest := relationPlacementRowDigest(key, valueLength, valueDigest)
	a.rows--
	if !a.rangeOwned.Contains(point) {
		a.outside--
	}
	for i := range a.xor {
		a.xor[i] ^= digest[i]
	}
	var borrow uint64
	for i := range a.sum {
		word := binary.LittleEndian.Uint64(digest[i*8 : i*8+8])
		next, b1 := bits.Sub64(a.sum[i], word, 0)
		next, b2 := bits.Sub64(next, borrow, 0)
		a.sum[i], borrow = next, b1|b2
	}
	return nil
}

func globalIndexProfilePoint(profile GlobalIndexProfile, key []byte) (distribution.KeyspacePoint, bool) {
	if profile.IndexID == 0 || profile.Incarnation == 0 || profile.LocatorCount == 0 ||
		profile.LocatorCount > 8 || profile.KeyEncoding != GlobalIndexKeyCanonicalTuple ||
		profile.KeyArity == 0 || profile.KeyArity > distribution.KeyspaceWidth ||
		profile.TupleVersion != distribution.CurrentTupleVersion ||
		profile.MapperVersion != distribution.NativeMapperVersion ||
		!distribution.ValidVirtualBucketBits(profile.BucketBits) {
		return distribution.KeyspacePoint{}, false
	}
	point, consumed, ok := distribution.NativePointForEncodedTuplePrefix(
		key, int(profile.KeyArity), profile.BucketBits,
	)
	if !ok {
		return distribution.KeyspacePoint{}, false
	}
	if profile.Unique {
		return point, consumed == len(key)
	}
	locatorBytes, ok := distribution.CanonicalTuplePrefixLen(
		key[consumed:], int(profile.LocatorCount),
	)
	return point, ok && consumed+locatorBytes == len(key)
}

func (m *Machine) nextRelationPlacements(
	changes []finalMutation,
	plan commandPlan,
) ([replication.MaxRelationsPerBundle]relationPlacementAccumulator, error) {
	var next [replication.MaxRelationsPerBundle]relationPlacementAccumulator
	for i := range m.relations {
		next[i] = m.relations[i].placement
	}
	for i := range plan.relations {
		span := plan.relations[i]
		ordinal := int(span.ordinal)
		if ordinal < 0 || ordinal >= len(m.relations) ||
			uint64(span.end) > uint64(len(changes)) || span.start > span.end {
			return next, ErrAdmissionBound
		}
		relation := &m.relations[ordinal]
		if relation.kind != RelationGlobalIndex || !relation.placement.enabled {
			continue
		}
		validator, ok := relation.target.Validator.(GlobalIndexPlacementValidator)
		if !ok {
			return next, ErrSchemaProfile
		}
		for mutationIndex := span.start; mutationIndex < span.end; mutationIndex++ {
			mutation := &changes[mutationIndex]
			point, ok := validator.GlobalIndexPlacementPoint(mutation.key)
			if !ok {
				return next, ErrSchemaProfile
			}
			if mutation.beforeFound {
				beforeDigest := sha256.Sum256(mutation.before)
				if err := next[ordinal].removeDescriptor(
					point, mutation.key, uint64(len(mutation.before)), beforeDigest,
				); err != nil {
					return next, err
				}
			}
			if !mutation.delete {
				next[ordinal].addRaw(point, mutation.key, mutation.value)
			}
		}
	}
	return next, nil
}

func (o *logicalOverlay) updateRelationPlacement(
	validator GlobalIndexPlacementValidator,
	accumulator *relationPlacementAccumulator,
) error {
	if o == nil || validator == nil || accumulator == nil || !accumulator.enabled {
		return ErrInconsistentSnapshot
	}
	for index := range o.entries {
		entry := &o.entries[index]
		if !o.contributes(entry) {
			continue
		}
		key := o.key(entry)
		point, ok := validator.GlobalIndexPlacementPoint(key)
		if !ok {
			return ErrSchemaProfile
		}
		if entry.baseFound {
			if err := accumulator.removeDescriptor(
				point, key, entry.baseLength, entry.baseDigest,
			); err != nil {
				return err
			}
		}
		if !entry.deleted {
			value := o.value(entry)
			accumulator.addRaw(point, key, value)
		}
	}
	return nil
}

// GlobalIndexPlacementProof returns a constant-size commitment to a coherent
// relation image and split operation. It performs no row scan. The accumulator
// is accepted only at the exact durable relation generation and state cut from
// which this ReadSnapshot was captured.
func (s *ReadSnapshot) GlobalIndexPlacementProof(
	id replication.RelationID,
	source distribution.KeyRange,
	splitPlan [sha256.Size]byte,
) ([sha256.Size]byte, error) {
	if s == nil || id == 0 || int(id) > len(s.relations) || !source.Valid() ||
		splitPlan == ([sha256.Size]byte{}) || source != s.state.Binding.OwnedRange {
		return [sha256.Size]byte{}, ErrInconsistentSnapshot
	}
	relation := &s.relations[int(id)-1]
	if relation.id != id || relation.kind != RelationGlobalIndex ||
		relation.contract == ([sha256.Size]byte{}) || relation.placement.rangeOwned != source ||
		!relation.placement.enabled || relation.placement.outside != 0 ||
		relation.placementApplied != s.state.Applied ||
		relation.placementGen == 0 {
		return [sha256.Size]byte{}, ErrInconsistentSnapshot
	}
	if relationPlacementStateDigest(
		s.state.Binding.SchemaGeneration, s.manifestDigest, s.relations,
	) != s.state.RelationPlacementDigest {
		return [sha256.Size]byte{}, ErrInconsistentSnapshot
	}
	snapshot, ok := s.cut.Collection(relation.name)
	if !ok || snapshot == nil || snapshot.Generation() != relation.placementGen ||
		snapshot.Len() != relation.placement.rows {
		return [sha256.Size]byte{}, ErrInconsistentSnapshot
	}
	h := sha256.New()
	_, _ = h.Write(relationPlacementProofDomain)
	_, _ = h.Write(splitPlan[:])
	_, _ = h.Write(s.manifestDigest[:])
	_, _ = h.Write(relation.contract[:])
	_, _ = h.Write(s.state.DataChainDigest[:])
	var fixed [64]byte
	binary.LittleEndian.PutUint64(fixed[0:8], s.state.Binding.SchemaGeneration)
	binary.LittleEndian.PutUint64(fixed[8:16], s.state.Applied)
	binary.LittleEndian.PutUint16(fixed[16:18], uint16(id))
	copy(fixed[24:32], source.Start[:])
	copy(fixed[32:40], source.End.Point[:])
	if source.End.Max {
		fixed[40] = 1
	}
	binary.LittleEndian.PutUint64(fixed[48:56], relation.placement.rows)
	_, _ = h.Write(fixed[:])
	_, _ = h.Write(relation.placement.xor[:])
	for i := range relation.placement.sum {
		binary.LittleEndian.PutUint64(fixed[i*8:i*8+8], relation.placement.sum[i])
	}
	_, _ = h.Write(fixed[:32])
	var proof [sha256.Size]byte
	_ = h.Sum(proof[:0])
	if proof == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, errors.New("replicatedstate: zero relation placement proof")
	}
	return proof, nil
}
