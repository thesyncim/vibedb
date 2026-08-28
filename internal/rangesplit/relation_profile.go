package rangesplit

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

// RelationProfile is one dense physical slot. JSON relations use the
// partitioner's compiled shard-key program; global indexes use their own exact
// stored-key mapper. Collection names are cold metadata, never hot lookups.
type RelationProfile struct {
	Relation    replication.RelationID             `json:"relation"`
	Kind        replicatedstate.RelationKind       `json:"kind"`
	Collection  string                             `json:"collection"`
	GlobalIndex replicatedstate.GlobalIndexProfile `json:"global_index"`
}

// BundleProfile binds the routing recipe to one schema generation and exact
// source/child machine manifests, in child ordinal order. These commitments do
// not prove that the descriptors were derived from the schema: the controller
// must derive them from authenticated identities, and each source/child opener
// must compare its actual coherent machine manifest before using the recipe.
type BundleProfile struct {
	SchemaGeneration uint64              `json:"schema_generation"`
	SourceManifest   [sha256.Size]byte   `json:"source_manifest"`
	ChildManifests   [][sha256.Size]byte `json:"child_manifests"`
	Relations        []RelationProfile   `json:"relations"`
}

// BindRelations returns an owned immutable recipe. Rebinding a different
// profile is rejected; binding the same profile is idempotent. It grants no
// serving authority and does not enable bundle artifacts by itself.
func (p *Partitioner) BindRelations(profile BundleProfile) (*Partitioner, error) {
	if p == nil || profile.SchemaGeneration == 0 || profile.SourceManifest == ([sha256.Size]byte{}) ||
		len(profile.ChildManifests) != int(p.childCount) || len(profile.Relations) == 0 ||
		len(profile.Relations) > replication.MaxRelationsPerBundle {
		return nil, ErrInvalidPartition
	}
	for _, manifest := range profile.ChildManifests {
		if manifest == ([sha256.Size]byte{}) {
			return nil, ErrInvalidPartition
		}
	}
	for ordinal, relation := range profile.Relations {
		if relation.Relation != replication.RelationID(ordinal+1) || relation.Collection == "" ||
			len(relation.Collection) > replication.MaxIdentityBytes || !utf8.ValidString(relation.Collection) ||
			strings.IndexByte(relation.Collection, 0) >= 0 {
			return nil, ErrInvalidPartition
		}
		if ordinal == 0 && (relation.Kind != replicatedstate.RelationJSON || relation.Collection != p.collection) {
			return nil, ErrInvalidPartition
		}
		switch relation.Kind {
		case replicatedstate.RelationJSON:
			if relation.GlobalIndex != (replicatedstate.GlobalIndexProfile{}) {
				return nil, ErrInvalidPartition
			}
		case replicatedstate.RelationGlobalIndex:
			if !relation.GlobalIndex.Valid() {
				return nil, ErrInvalidPartition
			}
		default:
			return nil, ErrInvalidPartition
		}
		for prior := 0; prior < ordinal; prior++ {
			if profile.Relations[prior].Collection == relation.Collection {
				return nil, ErrInvalidPartition
			}
		}
	}
	digest := p.bundleDigest(profile)
	if p.bundle != nil {
		if digest != p.relationDigest {
			return nil, ErrInvalidPartition
		}
		return p, nil
	}
	owned := profile
	owned.ChildManifests = append([][sha256.Size]byte(nil), profile.ChildManifests...)
	owned.Relations = make([]RelationProfile, len(profile.Relations))
	for ordinal, relation := range profile.Relations {
		owned.Relations[ordinal] = relation
		owned.Relations[ordinal].Collection = strings.Clone(relation.Collection)
	}
	bound := *p
	bound.geometryDigest = p.GeometryDigest()
	bound.bundle, bound.relationDigest = &owned, digest
	bound.bindRelationDigest()
	return &bound, nil
}

func (p *Partitioner) bundleDigest(profile BundleProfile) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/range-split/relation-profile\x00"))
	var fixed [32]byte
	binary.LittleEndian.PutUint64(fixed[:8], profile.SchemaGeneration)
	binary.LittleEndian.PutUint16(fixed[8:10], uint16(len(profile.Relations)))
	fixed[10] = p.childCount
	_, _ = h.Write(fixed[:11])
	_, _ = h.Write(profile.SourceManifest[:])
	program := p.program.Digest()
	_, _ = h.Write(program[:])
	for _, manifest := range profile.ChildManifests {
		_, _ = h.Write(manifest[:])
	}
	for _, relation := range profile.Relations {
		clear(fixed[:])
		binary.LittleEndian.PutUint16(fixed[:2], uint16(relation.Relation))
		fixed[2] = byte(relation.Kind)
		binary.LittleEndian.PutUint16(fixed[3:5], uint16(len(relation.Collection)))
		_, _ = h.Write(fixed[:5])
		_, _ = h.Write([]byte(relation.Collection))
		global := relation.GlobalIndex
		binary.LittleEndian.PutUint64(fixed[:8], global.IndexID)
		binary.LittleEndian.PutUint64(fixed[8:16], global.Incarnation)
		fixed[16], fixed[17] = global.LocatorCount, byte(global.KeyEncoding)
		fixed[18], fixed[19] = global.KeyArity, global.BucketBits
		if global.Unique {
			fixed[20] = 1
		} else {
			fixed[20] = 0
		}
		binary.LittleEndian.PutUint32(fixed[21:25], uint32(global.TupleVersion))
		binary.LittleEndian.PutUint32(fixed[25:29], uint32(global.MapperVersion))
		_, _ = h.Write(fixed[:29])
	}
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	return digest
}

func (p *Partitioner) bindRelationDigest() {
	if p.bundle == nil {
		return
	}
	const domain = "vibedb/range-split/bound-relations\x00"
	var raw [len(domain) + 2*sha256.Size]byte
	at := copy(raw[:], domain)
	at += copy(raw[at:], p.digest[:])
	copy(raw[at:], p.relationDigest[:])
	p.digest = sha256.Sum256(raw[:])
}

func (p *Partitioner) RelationCount() int {
	if p == nil {
		return 0
	}
	if p.bundle == nil {
		return 1
	}
	return len(p.bundle.Relations)
}

// Relation returns a value descriptor, never a mutable slice or map alias.
func (p *Partitioner) Relation(id replication.RelationID) (RelationProfile, bool) {
	if p == nil || id == 0 || int(id) > p.RelationCount() {
		return RelationProfile{}, false
	}
	if p.bundle == nil {
		return RelationProfile{Relation: 1, Kind: replicatedstate.RelationJSON, Collection: p.collection}, true
	}
	return p.bundle.Relations[int(id)-1], true
}

// RelationPoint performs a dense slot lookup, then maps borrowed row bytes.
// A global index never interprets its value or locator tuple as a JSON key.
func (p *Partitioner) RelationPoint(id replication.RelationID, key, value []byte,
	workspace *distribution.DocumentPointWorkspace,
) (distribution.KeyspacePoint, error) {
	relation, ok := p.Relation(id)
	if !ok {
		return distribution.KeyspacePoint{}, ErrInvalidPartition
	}
	if relation.Kind == replicatedstate.RelationGlobalIndex {
		point, valid := relation.GlobalIndex.GlobalIndexStorageKeyPoint(key)
		if !valid {
			return distribution.KeyspacePoint{}, ErrInvalidPartition
		}
		return point, nil
	}
	return p.program.Point(value, workspace)
}

func (p *Partitioner) SchemaGeneration() uint64 {
	if p == nil || p.bundle == nil {
		return 0
	}
	return p.bundle.SchemaGeneration
}

func (p *Partitioner) SourceManifest() [sha256.Size]byte {
	if p == nil || p.bundle == nil {
		return [sha256.Size]byte{}
	}
	return p.bundle.SourceManifest
}

func (p *Partitioner) ChildManifest(child int) ([sha256.Size]byte, bool) {
	if p == nil || p.bundle == nil || child < 0 || child >= int(p.childCount) {
		return [sha256.Size]byte{}, false
	}
	return p.bundle.ChildManifests[child], true
}

// MatchesSourceSchema compares the recipe's commitments with an actual
// coherent source cut. Unbound singleton recipes have no schema authority.
func (p *Partitioner) MatchesSourceSchema(generation uint64, manifest [sha256.Size]byte) bool {
	return p != nil && p.bundle != nil && generation == p.bundle.SchemaGeneration && manifest == p.bundle.SourceManifest
}
