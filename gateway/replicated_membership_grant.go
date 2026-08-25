package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	replicatedMembershipGrantKeyByte     = byte(3)
	replicatedMembershipGrantPageKeyByte = byte(4)
	replicatedMembershipGrantPages       = 64
	maxReplicatedMembershipGrantsPerPage = 64
	// The distributed quota is explicit without a global counter/CAS hot spot.
	// Each independently updated page rejects its 65th collision, so retained
	// active authority is bounded by this product under every key distribution.
	maxReplicatedMembershipGrants         = replicatedMembershipGrantPages * maxReplicatedMembershipGrantsPerPage
	maxReplicatedMembershipGrantBytes     = 2 << 10
	maxReplicatedMembershipGrantPageBytes = 32 << 10
)

type persistedMembershipGrant struct {
	ClusterID             [16]byte `json:"cluster_id"`
	ClusterIncarnation    [16]byte `json:"cluster_incarnation"`
	TopologyRecoveryEpoch uint64   `json:"topology_recovery_epoch"`
	ShardIncarnation      [16]byte `json:"shard_incarnation"`
	GroupID               [16]byte `json:"group_id"`
	TransitionID          [16]byte `json:"transition_id"`
	MetadataEpoch         uint64   `json:"metadata_epoch"`
	CatalogGeneration     uint64   `json:"catalog_generation"`
	SourceMember          uint64   `json:"source_member"`
	TargetMember          uint64   `json:"target_member"`
}

type persistedMembershipGrantGroup struct {
	ClusterID             [16]byte `json:"cluster_id"`
	ClusterIncarnation    [16]byte `json:"cluster_incarnation"`
	TopologyRecoveryEpoch uint64   `json:"topology_recovery_epoch"`
	ShardIncarnation      [16]byte `json:"shard_incarnation"`
	GroupID               [16]byte `json:"group_id"`
}

type persistedMembershipGrantPage struct {
	Groups []persistedMembershipGrantGroup `json:"groups"`
}

// ReadMembershipGrant performs one linearizable independently keyed lookup.
// Missing is explicit so runtime revocation can distinguish authenticated
// absence from an unavailable catalog.
func (authority *ReplicatedCatalogAuthority) ReadMembershipGrant(ctx context.Context,
	group raftmember.GroupKey) (membershipgrant.Grant, bool, error) {
	if authority == nil || ctx == nil || !validMembershipGrantGroup(group) {
		return membershipgrant.Grant{}, false, ErrReplicatedCatalog
	}
	key, _ := replicatedMembershipGrantKeys(group)
	result, err := authority.readRaw(ctx, key[:], maxReplicatedMembershipGrantBytes)
	if err != nil {
		return membershipgrant.Grant{}, false, err
	}
	if !result.Found {
		return membershipgrant.Grant{}, false, nil
	}
	grant, err := openReplicatedMembershipGrant(result.Value)
	if err != nil || grant.Group != group {
		return membershipgrant.Grant{}, false, errors.Join(err, ErrReplicatedCatalog)
	}
	currentCatalog, err := authority.Read(ctx)
	if err != nil {
		return membershipgrant.Grant{}, false, err
	}
	if currentCatalog == nil || grant.CatalogGeneration != currentCatalog.Generation() ||
		!replicatedCatalogContainsGroup(currentCatalog, group) {
		return membershipgrant.Grant{}, false, ErrReplicatedCatalogConflict
	}
	return grant, true, nil
}

// PublishMembershipGrant atomically inserts one exact per-group record and its
// bounded hash-page occupancy witness. Unrelated pages never contend.
func (authority *ReplicatedCatalogAuthority) PublishMembershipGrant(ctx context.Context,
	grant membershipgrant.Grant) error {
	if authority == nil || authority.session == nil || ctx == nil || !grant.Valid() {
		return ErrReplicatedCatalog
	}
	currentCatalog, err := authority.Read(ctx)
	if err != nil {
		return err
	}
	if currentCatalog == nil || grant.CatalogGeneration != currentCatalog.Generation() ||
		!replicatedCatalogContainsGroup(currentCatalog, grant.Group) {
		return ErrReplicatedCatalogConflict
	}
	ctx, err = authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	recordKey, pageKey := replicatedMembershipGrantKeys(grant.Group)
	record, err := authority.readRaw(ctx, recordKey[:], maxReplicatedMembershipGrantBytes)
	if err != nil {
		return err
	}
	page, err := authority.readRaw(ctx, pageKey[:], maxReplicatedMembershipGrantPageBytes)
	if err != nil {
		return err
	}
	var groups []raftmember.GroupKey
	if page.Found {
		groups, err = openReplicatedMembershipGrantPage(pageKey[1], page.Value)
		if err != nil {
			return err
		}
	}
	index, found := findReplicatedMembershipGrantGroup(groups, grant.Group)
	if record.Found {
		current, openErr := openReplicatedMembershipGrant(record.Value)
		if openErr != nil || current.Group != grant.Group || !found {
			return errors.Join(openErr, ErrReplicatedCatalogConflict)
		}
		if current == grant {
			return nil
		}
		return ErrReplicatedCatalogConflict
	}
	if found || len(groups) >= maxReplicatedMembershipGrantsPerPage {
		return ErrReplicatedCatalogConflict
	}
	groups = append(groups, raftmember.GroupKey{})
	copy(groups[index+1:], groups[index:])
	groups[index] = grant.Group
	recordBytes, err := appendReplicatedMembershipGrant(nil, grant)
	if err != nil {
		return err
	}
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedMembershipGrantPage(authority.scratch, pageKey[1], groups)
	if err != nil {
		return err
	}
	mutations := []NativeMutation{{Kind: replication.MutationPutAbsentOrEqual,
		Key: recordKey[:], Value: recordBytes}}
	if !page.Found {
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutAbsentOrEqual,
			Key: pageKey[:], Value: authority.scratch})
	} else {
		digest := sha256.Sum256(page.Value)
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutDigestEqual,
			Key: pageKey[:], Value: authority.scratch,
			ExpectedValueLength: uint64(len(page.Value)), ExpectedValueDigest: replication.Digest(digest)})
	}
	native, err := authority.session.MutateBatch(ctx, mutations)
	return finishReplicatedMembershipGrantMutation(authority, native, err)
}

func replicatedCatalogContainsGroup(snapshot *Snapshot, group raftmember.GroupKey) bool {
	if snapshot == nil || group == (raftmember.GroupKey{}) {
		return false
	}
	for index := range snapshot.replicatedShards {
		if snapshot.replicatedShards[index].group == group {
			return true
		}
	}
	return false
}

// RevokeMembershipGrant removes only the byte-identical expected grant. An
// unknown outcome is resolved from NativeSession's retained exact command.
func (authority *ReplicatedCatalogAuthority) RevokeMembershipGrant(ctx context.Context,
	expected membershipgrant.Grant) error {
	if authority == nil || authority.session == nil || ctx == nil || !expected.Valid() {
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
	recordKey, pageKey := replicatedMembershipGrantKeys(expected.Group)
	record, err := authority.readRaw(ctx, recordKey[:], maxReplicatedMembershipGrantBytes)
	if err != nil {
		return err
	}
	if !record.Found {
		return ErrReplicatedCatalogConflict
	}
	current, err := openReplicatedMembershipGrant(record.Value)
	if err != nil || current != expected {
		return ErrReplicatedCatalogConflict
	}
	page, err := authority.readRaw(ctx, pageKey[:], maxReplicatedMembershipGrantPageBytes)
	if err != nil || !page.Found {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	groups, err := openReplicatedMembershipGrantPage(pageKey[1], page.Value)
	if err != nil {
		return err
	}
	index, found := findReplicatedMembershipGrantGroup(groups, expected.Group)
	if !found {
		return ErrReplicatedCatalogConflict
	}
	copy(groups[index:], groups[index+1:])
	groups = groups[:len(groups)-1]
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedMembershipGrantPage(authority.scratch, pageKey[1], groups)
	if err != nil {
		return err
	}
	recordDigest, pageDigest := sha256.Sum256(record.Value), sha256.Sum256(page.Value)
	native, err := authority.session.MutateBatch(ctx, []NativeMutation{
		{Kind: replication.MutationDeleteDigestEqual, Key: recordKey[:],
			ExpectedValueLength: uint64(len(record.Value)), ExpectedValueDigest: replication.Digest(recordDigest)},
		{Kind: replication.MutationPutDigestEqual, Key: pageKey[:], Value: authority.scratch,
			ExpectedValueLength: uint64(len(page.Value)), ExpectedValueDigest: replication.Digest(pageDigest)},
	})
	return finishReplicatedMembershipGrantMutation(authority, native, err)
}

func finishReplicatedMembershipGrantMutation(authority *ReplicatedCatalogAuthority,
	result NativeResult, err error) error {
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

func appendReplicatedMembershipGrant(dst []byte,
	grant membershipgrant.Grant) ([]byte, error) {
	if !grant.Valid() {
		return dst, ErrReplicatedCatalog
	}
	persisted := persistMembershipGrant(grant)
	raw, err := vibejson.Marshal(&persisted)
	if err != nil {
		return dst, err
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start == 0 || len(dst)-start > maxReplicatedMembershipGrantBytes {
		return dst[:start], errors.Join(err, ErrReplicatedCatalog)
	}
	return dst, nil
}

func openReplicatedMembershipGrant(raw []byte) (membershipgrant.Grant, error) {
	if len(raw) == 0 || len(raw) > maxReplicatedMembershipGrantBytes {
		return membershipgrant.Grant{}, ErrReplicatedCatalog
	}
	var persisted persistedMembershipGrant
	if err := vibejson.Unmarshal(raw, &persisted); err != nil {
		return membershipgrant.Grant{}, errors.Join(err, ErrReplicatedCatalog)
	}
	grant := openPersistedMembershipGrant(persisted)
	canonical, err := appendReplicatedMembershipGrant(nil, grant)
	if err != nil || !bytes.Equal(raw, canonical) {
		return membershipgrant.Grant{}, errors.Join(err, ErrReplicatedCatalog)
	}
	return grant, nil
}

func appendReplicatedMembershipGrantPage(dst []byte, pageIndex byte,
	groups []raftmember.GroupKey) ([]byte, error) {
	if pageIndex >= replicatedMembershipGrantPages ||
		len(groups) > maxReplicatedMembershipGrantsPerPage {
		return dst, ErrReplicatedCatalog
	}
	page := persistedMembershipGrantPage{
		Groups: make([]persistedMembershipGrantGroup, len(groups)),
	}
	for index := range groups {
		_, pageKey := replicatedMembershipGrantKeys(groups[index])
		if !validMembershipGrantGroup(groups[index]) || pageKey[1] != pageIndex || index != 0 &&
			compareMembershipGrantGroup(groups[index-1], groups[index]) >= 0 {
			return dst, ErrReplicatedCatalog
		}
		page.Groups[index] = persistMembershipGrantGroup(groups[index])
	}
	raw, err := vibejson.Marshal(&page)
	if err != nil {
		return dst, err
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start == 0 ||
		len(dst)-start > maxReplicatedMembershipGrantPageBytes {
		return dst[:start], errors.Join(err, ErrReplicatedCatalog)
	}
	return dst, nil
}

func openReplicatedMembershipGrantPage(pageIndex byte, raw []byte) ([]raftmember.GroupKey, error) {
	if pageIndex >= replicatedMembershipGrantPages || len(raw) == 0 ||
		len(raw) > maxReplicatedMembershipGrantPageBytes {
		return nil, ErrReplicatedCatalog
	}
	var page persistedMembershipGrantPage
	if err := vibejson.Unmarshal(raw, &page); err != nil ||
		len(page.Groups) > maxReplicatedMembershipGrantsPerPage {
		return nil, errors.Join(err, ErrReplicatedCatalog)
	}
	groups := make([]raftmember.GroupKey, len(page.Groups))
	for index := range page.Groups {
		groups[index] = openPersistedMembershipGrantGroup(page.Groups[index])
		_, pageKey := replicatedMembershipGrantKeys(groups[index])
		if !validMembershipGrantGroup(groups[index]) || pageKey[1] != pageIndex || index != 0 &&
			compareMembershipGrantGroup(groups[index-1], groups[index]) >= 0 {
			return nil, ErrReplicatedCatalog
		}
	}
	canonical, err := appendReplicatedMembershipGrantPage(nil, pageIndex, groups)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.Join(err, ErrReplicatedCatalog)
	}
	return groups, nil
}

func findReplicatedMembershipGrantGroup(groups []raftmember.GroupKey,
	group raftmember.GroupKey) (int, bool) {
	index := sort.Search(len(groups), func(index int) bool {
		return compareMembershipGrantGroup(groups[index], group) >= 0
	})
	return index, index < len(groups) && groups[index] == group
}

func replicatedMembershipGrantKeys(group raftmember.GroupKey) ([33]byte, [2]byte) {
	var input [96]byte
	offset := copy(input[:], []byte("vibedb/membership-grant\x00"))
	offset += copy(input[offset:], group.ClusterID[:])
	offset += copy(input[offset:], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(input[offset:], group.TopologyRecoveryEpoch)
	offset += 8
	offset += copy(input[offset:], group.ShardIncarnation[:])
	offset += copy(input[offset:], group.GroupID[:])
	digest := sha256.Sum256(input[:offset])
	var record [33]byte
	record[0] = replicatedMembershipGrantKeyByte
	copy(record[1:], digest[:])
	page := [2]byte{replicatedMembershipGrantPageKeyByte,
		digest[0] & (replicatedMembershipGrantPages - 1)}
	return record, page
}

func compareMembershipGrantGroup(left, right raftmember.GroupKey) int {
	if order := bytes.Compare(left.ClusterID[:], right.ClusterID[:]); order != 0 {
		return order
	}
	if order := bytes.Compare(left.ClusterIncarnation[:], right.ClusterIncarnation[:]); order != 0 {
		return order
	}
	if left.TopologyRecoveryEpoch < right.TopologyRecoveryEpoch {
		return -1
	}
	if left.TopologyRecoveryEpoch > right.TopologyRecoveryEpoch {
		return 1
	}
	if order := bytes.Compare(left.ShardIncarnation[:], right.ShardIncarnation[:]); order != 0 {
		return order
	}
	return bytes.Compare(left.GroupID[:], right.GroupID[:])
}

func validMembershipGrantGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func persistMembershipGrantGroup(group raftmember.GroupKey) persistedMembershipGrantGroup {
	return persistedMembershipGrantGroup{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		ShardIncarnation:      group.ShardIncarnation, GroupID: group.GroupID,
	}
}

func openPersistedMembershipGrantGroup(persisted persistedMembershipGrantGroup) raftmember.GroupKey {
	return raftmember.GroupKey{
		ClusterID: persisted.ClusterID, ClusterIncarnation: persisted.ClusterIncarnation,
		TopologyRecoveryEpoch: persisted.TopologyRecoveryEpoch,
		ShardIncarnation:      persisted.ShardIncarnation, GroupID: persisted.GroupID,
	}
}

func persistMembershipGrant(grant membershipgrant.Grant) persistedMembershipGrant {
	return persistedMembershipGrant{
		ClusterID: grant.Group.ClusterID, ClusterIncarnation: grant.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: grant.Group.TopologyRecoveryEpoch,
		ShardIncarnation:      grant.Group.ShardIncarnation, GroupID: grant.Group.GroupID,
		TransitionID: grant.TransitionID, MetadataEpoch: grant.MetadataEpoch,
		CatalogGeneration: grant.CatalogGeneration,
		SourceMember:      grant.SourceMember, TargetMember: grant.TargetMember,
	}
}

func openPersistedMembershipGrant(persisted persistedMembershipGrant) membershipgrant.Grant {
	return membershipgrant.Grant{
		Group: raftmember.GroupKey{
			ClusterID: persisted.ClusterID, ClusterIncarnation: persisted.ClusterIncarnation,
			TopologyRecoveryEpoch: persisted.TopologyRecoveryEpoch,
			ShardIncarnation:      persisted.ShardIncarnation, GroupID: persisted.GroupID,
		},
		TransitionID: persisted.TransitionID, MetadataEpoch: persisted.MetadataEpoch,
		CatalogGeneration: persisted.CatalogGeneration,
		SourceMember:      persisted.SourceMember, TargetMember: persisted.TargetMember,
	}
}
