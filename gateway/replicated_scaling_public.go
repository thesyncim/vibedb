package gateway

import (
	"context"
	"crypto/sha256"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
)

// ReplicatedCatalogHeadDigest returns the digest of the exact control-plane
// head envelope that the catalog authority commits. It is distinct from the
// nested SnapshotDocument digest and is suitable for
// GroupEnrollmentIntent.ExpectedCatalogHeadDigest.
func ReplicatedCatalogHeadDigest(snapshot *Snapshot) (replication.Digest, error) {
	raw, err := appendReplicatedCatalogDocument(nil, snapshot, maxReplicatedCatalogBytes)
	if err != nil {
		return replication.Digest{}, err
	}
	return replication.Digest(sha256.Sum256(raw)), nil
}

// ReadReplicatedCatalogHead reads one authoritative snapshot and returns the
// digest of the same canonical head bytes. The digest is derived only after
// the authority has validated its authenticated head/witness cut; callers
// must not hash an arbitrary local catalog file as a control-plane witness.
func (authority *ReplicatedCatalogAuthority) ReadReplicatedCatalogHead(ctx context.Context) (*Snapshot, replication.Digest, error) {
	if authority == nil || ctx == nil {
		return nil, replication.Digest{}, ErrReplicatedCatalog
	}
	snapshot, err := authority.Read(ctx)
	if err != nil {
		return nil, replication.Digest{}, err
	}
	digest, err := ReplicatedCatalogHeadDigest(snapshot)
	if err != nil {
		return nil, replication.Digest{}, err
	}
	return snapshot, digest, nil
}

// ReplicatedInitialMembershipDigests returns the catalog-certified serving
// RF3 roster and complete descriptor witnesses for one group. A missing or
// malformed group returns ok=false; callers must never substitute a made-up
// nonzero digest.
func ReplicatedInitialMembershipDigests(
	snapshot *Snapshot, group raftmember.GroupKey,
) (roster, descriptor replication.Digest, ok bool) {
	if snapshot == nil || group == (raftmember.GroupKey{}) {
		return replication.Digest{}, replication.Digest{}, false
	}
	for index, entry := range snapshot.replicatedShards {
		if entry.group != group || int(entry.replicaCount) != ServingReplicaCount {
			continue
		}
		roster = replication.Digest(replicatedCatalogInitialRosterDigest(snapshot, index))
		descriptor = replication.Digest(replicatedCatalogInitialDescriptorDigest(snapshot, index))
		if roster == (replication.Digest{}) || descriptor == (replication.Digest{}) {
			return replication.Digest{}, replication.Digest{}, false
		}
		return roster, descriptor, true
	}
	return replication.Digest{}, replication.Digest{}, false
}
