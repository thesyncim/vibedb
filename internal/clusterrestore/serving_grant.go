package clusterrestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const ServingGrantBytes = 8 + 32 + 32 + 72 + 8 + 16 + 16 + 8 + 32

var servingGrantMagic = [8]byte{'V', 'B', 'R', 'S', 'G', 'R', 'N', 'T'}

func ServingGrantDiscriminator() [8]byte { return servingGrantMagic }

// ServingGrant is a transient, per-process capability minted only from a
// catalog-observed ServingAuthority. It is never durable shard state.
type ServingGrant struct {
	operation       [sha256.Size]byte
	catalog         [sha256.Size]byte
	group           raftmember.GroupKey
	member          uint64
	node            rafttransport.NodeID
	store           [16]byte
	nodeIncarnation uint64
	digest          [sha256.Size]byte
}

func (authority *ServingAuthority) Grant(group raftmember.GroupKey, member uint64) (ServingGrant, error) {
	if authority == nil || member == 0 {
		return ServingGrant{}, ErrActivation
	}
	replicas, found := authority.replicas[group]
	if !found || member > uint64(len(replicas)) {
		return ServingGrant{}, ErrActivation
	}
	replica := replicas[member-1]
	grant := ServingGrant{operation: authority.operation, catalog: authority.catalog,
		group: group, member: replica.Member, node: replica.Node, store: replica.Store,
		nodeIncarnation: replica.NodeIncarnation}
	raw, err := AppendServingGrant(nil, grant)
	if err != nil {
		return ServingGrant{}, err
	}
	copy(grant.digest[:], raw[len(raw)-sha256.Size:])
	return grant, nil
}

func (grant ServingGrant) Operation() [32]byte        { return grant.operation }
func (grant ServingGrant) CatalogWitness() [32]byte   { return grant.catalog }
func (grant ServingGrant) Group() raftmember.GroupKey { return grant.group }
func (grant ServingGrant) Member() uint64             { return grant.member }
func (grant ServingGrant) Node() rafttransport.NodeID { return grant.node }
func (grant ServingGrant) Store() [16]byte            { return grant.store }
func (grant ServingGrant) NodeIncarnation() uint64    { return grant.nodeIncarnation }
func (grant ServingGrant) Digest() [sha256.Size]byte  { return grant.digest }

func AppendServingGrant(dst []byte, grant ServingGrant) ([]byte, error) {
	if grant.operation == ([32]byte{}) || grant.catalog == ([32]byte{}) ||
		grant.group == (raftmember.GroupKey{}) || grant.member == 0 ||
		grant.node == (rafttransport.NodeID{}) || grant.store == ([16]byte{}) ||
		grant.nodeIncarnation == 0 {
		return dst, ErrActivation
	}
	start := len(dst)
	dst = append(dst, make([]byte, ServingGrantBytes)...)
	raw := dst[start:]
	copy(raw[:8], servingGrantMagic[:])
	copy(raw[8:40], grant.operation[:])
	copy(raw[40:72], grant.catalog[:])
	appendGroupKey(raw[72:144], grant.group)
	binary.BigEndian.PutUint64(raw[144:152], grant.member)
	copy(raw[152:168], grant.node[:])
	copy(raw[168:184], grant.store[:])
	binary.BigEndian.PutUint64(raw[184:192], grant.nodeIncarnation)
	digest := sha256.Sum256(raw[:192])
	copy(raw[192:], digest[:])
	return dst, nil
}

func OpenServingGrant(raw []byte) (ServingGrant, error) {
	if len(raw) != ServingGrantBytes || !bytes.Equal(raw[:8], servingGrantMagic[:]) ||
		sha256.Sum256(raw[:192]) != [32]byte(raw[192:]) {
		return ServingGrant{}, ErrActivation
	}
	grant := ServingGrant{group: openGroupKey(raw[72:144]),
		member:          binary.BigEndian.Uint64(raw[144:152]),
		nodeIncarnation: binary.BigEndian.Uint64(raw[184:192])}
	copy(grant.operation[:], raw[8:40])
	copy(grant.catalog[:], raw[40:72])
	copy(grant.node[:], raw[152:168])
	copy(grant.store[:], raw[168:184])
	copy(grant.digest[:], raw[192:])
	canonical, err := AppendServingGrant(nil, grant)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ServingGrant{}, ErrActivation
	}
	return grant, nil
}
