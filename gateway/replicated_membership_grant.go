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
	replicatedMembershipGrantKeyByte               = byte(3)
	replicatedMembershipGrantPageKeyByte           = byte(4)
	replicatedReplicaReplacementReceiptKeyByte     = byte(5)
	replicatedReplicaReplacementReceiptPageKeyByte = byte(6)
	replicatedMembershipGrantPages                 = 64
	maxReplicatedMembershipGrantsPerPage           = 64
	// The distributed quota is explicit without a global counter/CAS hot spot.
	// Each independently updated page rejects its 65th collision, so retained
	// active grants plus completed receipts are bounded by this product under
	// every key distribution.
	maxReplicatedMembershipGrants               = replicatedMembershipGrantPages * maxReplicatedMembershipGrantsPerPage
	maxReplicatedMembershipGrantBytes           = 2 << 10
	maxReplicatedMembershipGrantPageBytes       = 32 << 10
	maxReplicatedReplicaReplacementReceiptBytes = 4 << 10
	maxReplicatedMembershipLifecycleBytes       = maxReplicatedMembershipGrants*(maxReplicatedMembershipGrantBytes+maxReplicatedReplicaReplacementReceiptBytes) +
		2*replicatedMembershipGrantPages*maxReplicatedMembershipGrantPageBytes
)

type persistedMembershipGrant struct {
	ClusterID                [16]byte  `json:"cluster_id"`
	ClusterIncarnation       [16]byte  `json:"cluster_incarnation"`
	TopologyRecoveryEpoch    uint64    `json:"topology_recovery_epoch"`
	ShardIncarnation         [16]byte  `json:"shard_incarnation"`
	GroupID                  [16]byte  `json:"group_id"`
	TransitionID             [16]byte  `json:"transition_id"`
	MetadataEpoch            uint64    `json:"metadata_epoch"`
	CatalogGeneration        uint64    `json:"catalog_generation"`
	InitialReplicaSetVersion uint64    `json:"initial_replica_set_version"`
	InitialVoters            [3]uint64 `json:"initial_voters"`
	InitialRosterDigest      [32]byte  `json:"initial_roster_digest"`
	InitialDescriptorDigest  [32]byte  `json:"initial_descriptor_digest"`
	SourceMember             uint64    `json:"source_member"`
	TargetMember             uint64    `json:"target_member"`
	TargetNode               [16]byte  `json:"target_node"`
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

// persistedReplicaReplacementReceipt is the compact durable bridge between
// two adjacent catalog generations. It occupies a distinct stable per-group
// row and bounded receipt page, so catalog publication cannot revoke the grant
// which still authorizes removal of the old source.
type persistedReplicaReplacementReceipt struct {
	Grant         persistedMembershipGrant `json:"grant"`
	OldGeneration uint64                   `json:"old_generation"`
	NewGeneration uint64                   `json:"new_generation"`
	OldHeadBytes  uint64                   `json:"old_head_bytes"`
	NewHeadBytes  uint64                   `json:"new_head_bytes"`
	OldHeadDigest [32]byte                 `json:"old_head_digest"`
	NewHeadDigest [32]byte                 `json:"new_head_digest"`
}

type replicaReplacementReceipt struct {
	Grant         membershipgrant.Grant
	OldGeneration uint64
	NewGeneration uint64
	OldHeadBytes  uint64
	NewHeadBytes  uint64
	OldHeadDigest [32]byte
	NewHeadDigest [32]byte
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
	authorized, authorizeErr := authority.catalogAuthorizesMembershipGrant(
		ctx, currentCatalog, grant,
	)
	if authorizeErr != nil {
		return membershipgrant.Grant{}, false, authorizeErr
	}
	if !authorized {
		return membershipgrant.Grant{}, false, ErrReplicatedCatalogConflict
	}
	return grant, true, nil
}

func (authority *ReplicatedCatalogAuthority) catalogAuthorizesMembershipGrant(
	ctx context.Context, current *Snapshot, grant membershipgrant.Grant,
) (bool, error) {
	if current == nil || !grant.Valid() {
		return false, nil
	}
	if current.Generation() == grant.CatalogGeneration {
		return replicatedCatalogCertifiesInitialGrant(current, grant), nil
	}
	if grant.CatalogGeneration == ^uint64(0) ||
		current.Generation() != grant.CatalogGeneration+1 {
		return false, nil
	}
	key, _ := replicatedReplicaReplacementReceiptKeys(grant.Group)
	result, err := authority.readRaw(
		ctx, key[:], uint32(maxReplicatedReplicaReplacementReceiptBytes),
	)
	if err != nil || !result.Found {
		return false, err
	}
	receipt, err := openReplicaReplacementReceipt(result.Value)
	if err != nil {
		return false, err
	}
	currentRaw, err := appendReplicatedCatalogDocument(nil, current, maxReplicatedCatalogBytes)
	if err != nil {
		return false, err
	}
	return receipt.Grant == grant && receipt.NewGeneration == current.Generation() &&
		receipt.NewHeadBytes == uint64(len(currentRaw)) &&
		receipt.NewHeadDigest == sha256.Sum256(currentRaw), nil
}

// PublishMembershipGrant atomically inserts one exact per-group record and its
// bounded hash-page occupancy witness. Unrelated pages never contend.
func (authority *ReplicatedCatalogAuthority) PublishMembershipGrant(ctx context.Context,
	grant membershipgrant.Grant) error {
	if authority == nil || authority.session == nil || ctx == nil || !grant.Valid() {
		return ErrReplicatedCatalog
	}
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return err
	}
	currentCatalog := cut.snapshot
	if currentCatalog == nil || grant.CatalogGeneration != currentCatalog.Generation() ||
		!replicatedCatalogCertifiesInitialGrant(currentCatalog, grant) {
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
	witness := cut.witness
	witnessDigest := sha256.Sum256(witness)
	mutations := []NativeMutation{{Kind: replication.MutationPutDigestEqual,
		Key: replicatedCatalogHeadWitnessKey[:], Value: witness,
		ExpectedValueLength: uint64(len(witness)), ExpectedValueDigest: replication.Digest(witnessDigest)},
		{Kind: replication.MutationPutAbsentOrEqual, Key: recordKey[:], Value: recordBytes}}
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

func replicatedCatalogCertifiesInitialGrant(snapshot *Snapshot, grant membershipgrant.Grant) bool {
	if snapshot == nil || !grant.Valid() {
		return false
	}
	for index := range snapshot.replicatedShards {
		entry := snapshot.replicatedShards[index]
		if entry.group != grant.Group || int(entry.replicaCount) != len(grant.InitialVoters) ||
			entry.command.ReplicaSetVersion != grant.InitialReplicaSetVersion ||
			int(entry.replicaBase)+int(entry.replicaCount) > len(snapshot.replicatedReplicas) {
			continue
		}
		var voters [3]uint64
		for replica := range int(entry.replicaCount) {
			voters[replica] = snapshot.replicatedReplicas[int(entry.replicaBase)+replica].Member
		}
		sort.Slice(voters[:], func(left, right int) bool { return voters[left] < voters[right] })
		return voters == grant.InitialVoters &&
			replicatedCatalogInitialRosterDigest(snapshot, index) == grant.InitialRosterDigest &&
			replicatedCatalogInitialDescriptorDigest(snapshot, index) == grant.InitialDescriptorDigest
	}
	return false
}

func replicatedCatalogInitialRosterDigest(snapshot *Snapshot, shardIndex int) [sha256.Size]byte {
	if snapshot == nil || shardIndex < 0 || shardIndex >= len(snapshot.replicatedShards) {
		return [sha256.Size]byte{}
	}
	entry := snapshot.replicatedShards[shardIndex]
	if int(entry.replicaCount) != ServingReplicaCount ||
		int(entry.replicaBase)+int(entry.replicaCount) > len(snapshot.replicatedReplicas) {
		return [sha256.Size]byte{}
	}
	var voters [3]membershipgrant.RosterMember
	for index := range voters {
		replica := snapshot.replicatedReplicas[int(entry.replicaBase)+index]
		voters[index] = membershipgrant.RosterMember{Member: replica.Member, Node: [16]byte(replica.Node)}
	}
	sort.Slice(voters[:], func(left, right int) bool { return voters[left].Member < voters[right].Member })
	return membershipgrant.CertifiedRosterDigest(entry.group, entry.command.ReplicaSetVersion, voters)
}

func replicatedCatalogInitialDescriptorDigest(snapshot *Snapshot, shardIndex int) [sha256.Size]byte {
	if snapshot == nil || shardIndex < 0 || shardIndex >= len(snapshot.replicatedShards) {
		return [sha256.Size]byte{}
	}
	entry := snapshot.replicatedShards[shardIndex]
	if int(entry.replicaCount) != ServingReplicaCount ||
		int(entry.replicaBase)+int(entry.replicaCount) > len(snapshot.replicatedReplicas) ||
		int(entry.manifest) >= len(snapshot.config.Manifests) {
		return [sha256.Size]byte{}
	}
	manifest := snapshot.config.Manifests[entry.manifest]
	metadata, ok := manifest.ShardMetadataAt(int(entry.shard))
	if !ok {
		return [sha256.Size]byte{}
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/catalog/certified-initial-rf3\x00"))
	var scalar [8]byte
	writeUint64 := func(value uint64) {
		binary.LittleEndian.PutUint64(scalar[:], value)
		_, _ = hash.Write(scalar[:])
	}
	writeString := func(value string) {
		writeUint64(uint64(len(value)))
		_, _ = hash.Write([]byte(value))
	}
	_, _ = hash.Write(entry.group.ClusterID[:])
	_, _ = hash.Write(entry.group.ClusterIncarnation[:])
	writeUint64(entry.group.TopologyRecoveryEpoch)
	_, _ = hash.Write(entry.group.ShardIncarnation[:])
	_, _ = hash.Write(entry.group.GroupID[:])
	writeString(string(manifest.Distribution()))
	writeString(string(metadata.ID))
	writeUint64(uint64(entry.allocation))
	writeUint64(entry.command.ReplicaSetVersion)
	writeUint64(entry.command.ActivePolicyGeneration)
	writeUint64(entry.command.ProtectionEpoch)
	writeUint64(entry.command.OwnershipEpoch)
	writeUint64(entry.command.SchemaGeneration)
	_, _ = hash.Write(entry.command.RelationManifestDigest[:])
	writeUint64(entry.command.RoutingVersion)
	writeUint64(entry.command.RouteGeneration)
	for ordinal := 0; ordinal < int(entry.replicaCount); ordinal++ {
		replica := snapshot.replicatedReplicas[int(entry.replicaBase)+ordinal]
		endpoint, endpointOK := manifest.ShardLeaderAt(int(entry.shard), ordinal)
		if !endpointOK {
			return [sha256.Size]byte{}
		}
		writeUint64(replica.Member)
		_, _ = hash.Write(replica.Node[:])
		_, _ = hash.Write(replica.StoreID[:])
		writeUint64(replica.NodeIncarnation)
		writeString(string(endpoint))
		writeString(snapshot.endpoints[endpoint])
		writeString(replica.NativeEndpoint)
		writeString(replica.Address)
		writeString(replica.ControlEndpoint)
		writeString(replica.ControlAddress)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

// RevokeMembershipGrant deliberately has no standalone success path. Runtime-
// local state is not a durable deletion proof: deleting an observed grant at
// an intermediate learner/RF4 cut would strand recovery after restart. Use
// PublishReplicaReplacement to certify the final RF3 catalog, remove the old
// source, then FinalizeReplicaReplacement to revoke the exact grant.
func (authority *ReplicatedCatalogAuthority) RevokeMembershipGrant(ctx context.Context,
	expected membershipgrant.Grant) error {
	if authority == nil || authority.session == nil || ctx == nil || !expected.Valid() {
		return ErrReplicatedCatalog
	}
	if _, err := authority.authorizedContext(ctx); err != nil {
		return err
	}
	return ErrReplicatedCatalogConflict
}

// PublishReplicaReplacement atomically publishes one certified final RF3
// catalog and a distinct compact receipt. The active grant and its page remain
// unchanged so the subsequent source removal is still authorized. A lost
// response is retained by NativeSession and settled byte-identically through
// RetryPending; no uncertified catalog can become visible.
func (authority *ReplicatedCatalogAuthority) PublishReplicaReplacement(
	ctx context.Context,
	expectedGeneration uint64,
	next *Snapshot,
	expected membershipgrant.Grant,
) error {
	if authority == nil || authority.session == nil || ctx == nil || next == nil ||
		!expected.Valid() || expected.CatalogGeneration != expectedGeneration ||
		expectedGeneration == ^uint64(0) || authority.session.bundle.maxMutations < 4 {
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
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return err
	}
	if cut.snapshot.Generation() != expectedGeneration ||
		next.Generation() != expectedGeneration+1 {
		return ErrCatalogGenerationMismatch
	}
	current, err := initialCatalogState(cut.snapshot)
	if err != nil {
		return err
	}
	certified, err := advanceCatalogStateReplicaReplacement(current, next, expected)
	if err != nil {
		return err
	}
	if certified.Generation() != next.Generation() {
		return ErrReplicatedCatalogConflict
	}

	recordKey, pageKey := replicatedMembershipGrantKeys(expected.Group)
	record, err := authority.readRaw(ctx, recordKey[:], maxReplicatedMembershipGrantBytes)
	if err != nil {
		return err
	}
	if !record.Found {
		return ErrReplicatedCatalogConflict
	}
	stored, err := openReplicatedMembershipGrant(record.Value)
	if err != nil || stored != expected {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	page, err := authority.readRaw(ctx, pageKey[:], maxReplicatedMembershipGrantPageBytes)
	if err != nil {
		return err
	}
	if !page.Found {
		return ErrReplicatedCatalogConflict
	}
	groups, err := openReplicatedMembershipGrantPage(pageKey[1], page.Value)
	if err != nil {
		return err
	}
	_, found := findReplicatedMembershipGrantGroup(groups, expected.Group)
	if !found {
		return ErrReplicatedCatalogConflict
	}
	receiptKey, receiptPageKey := replicatedReplicaReplacementReceiptKeys(expected.Group)
	priorReceipt, err := authority.readRaw(
		ctx, receiptKey[:], maxReplicatedReplicaReplacementReceiptBytes,
	)
	if err != nil {
		return err
	}
	receiptPage, err := authority.readRaw(
		ctx, receiptPageKey[:], maxReplicatedMembershipGrantPageBytes,
	)
	if err != nil {
		return err
	}
	var receiptGroups []raftmember.GroupKey
	if receiptPage.Found {
		receiptGroups, err = openReplicaReplacementReceiptPage(
			receiptPageKey[1], receiptPage.Value,
		)
		if err != nil {
			return err
		}
	}
	receiptPosition, receiptFound := findReplicatedMembershipGrantGroup(
		receiptGroups, expected.Group,
	)
	if priorReceipt.Found != receiptFound {
		return ErrReplicatedCatalogConflict
	}
	if priorReceipt.Found {
		prior, openErr := openReplicaReplacementReceipt(priorReceipt.Value)
		if openErr != nil || prior.Grant.Group != expected.Group ||
			prior.NewGeneration != cut.snapshot.Generation() ||
			prior.NewHeadBytes != uint64(len(cut.head)) ||
			prior.NewHeadDigest != sha256.Sum256(cut.head) {
			return errors.Join(openErr, ErrReplicatedCatalogConflict)
		}
	} else {
		if len(receiptGroups) >= maxReplicatedMembershipGrantsPerPage {
			return ErrReplicatedCatalogConflict
		}
		receiptGroups = append(receiptGroups, raftmember.GroupKey{})
		copy(receiptGroups[receiptPosition+1:], receiptGroups[receiptPosition:])
		receiptGroups[receiptPosition] = expected.Group
	}

	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedCatalogDocument(
		authority.scratch, certified, maxReplicatedCatalogBytes,
	)
	if err != nil {
		return ErrCatalogTooLarge
	}
	nextWitness, err := appendReplicatedCatalogHeadWitness(
		nil, certified.Generation(), authority.scratch,
	)
	if err != nil {
		return err
	}
	receipt, err := appendReplicaReplacementReceipt(
		nil, expected, cut.head, authority.scratch,
	)
	if err != nil {
		return err
	}
	headDigest := sha256.Sum256(cut.head)
	witnessDigest := sha256.Sum256(cut.witness)
	mutations := make([]NativeMutation, 0, 4)
	mutations = append(mutations,
		NativeMutation{Kind: replication.MutationPutDigestEqual,
			Key: replicatedCatalogHeadKey, Value: authority.scratch,
			ExpectedValueLength: uint64(len(cut.head)),
			ExpectedValueDigest: replication.Digest(headDigest)},
		NativeMutation{Kind: replication.MutationPutDigestEqual,
			Key: replicatedCatalogHeadWitnessKey, Value: nextWitness,
			ExpectedValueLength: uint64(len(cut.witness)),
			ExpectedValueDigest: replication.Digest(witnessDigest)},
	)
	if priorReceipt.Found {
		digest := sha256.Sum256(priorReceipt.Value)
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutDigestEqual,
			Key: receiptKey[:], Value: receipt,
			ExpectedValueLength: uint64(len(priorReceipt.Value)),
			ExpectedValueDigest: replication.Digest(digest)})
	} else {
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutAbsentOrEqual,
			Key: receiptKey[:], Value: receipt})
		receiptPageBytes, pageErr := appendReplicaReplacementReceiptPage(
			nil, receiptPageKey[1], receiptGroups,
		)
		if pageErr != nil {
			return pageErr
		}
		if receiptPage.Found {
			digest := sha256.Sum256(receiptPage.Value)
			mutations = append(mutations, NativeMutation{Kind: replication.MutationPutDigestEqual,
				Key: receiptPageKey[:], Value: receiptPageBytes,
				ExpectedValueLength: uint64(len(receiptPage.Value)),
				ExpectedValueDigest: replication.Digest(digest)})
		} else {
			mutations = append(mutations, NativeMutation{Kind: replication.MutationPutAbsentOrEqual,
				Key: receiptPageKey[:], Value: receiptPageBytes})
		}
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	if err != nil {
		if errors.Is(err, ErrNativeCommandPending) || authority.session.Status().Pending {
			authority.pendingCatalog, authority.pendingExpected = certified, expectedGeneration
			authority.pendingGrant = expected
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
	return authority.holder.publishReplicaReplacementAfter(
		expectedGeneration, certified, expected,
	)
}

// FinalizeReplicaReplacement revokes one exact grant only after the caller has
// durably proved source removal. The catalog head and replacement receipt are
// re-read linearly; the CAS deletes the grant and removes only its active-page
// slot while retaining the separately bounded receipt for stale gateways.
func (authority *ReplicatedCatalogAuthority) FinalizeReplicaReplacement(
	ctx context.Context, expected membershipgrant.Grant,
) error {
	if authority == nil || authority.session == nil || ctx == nil ||
		!expected.Valid() || expected.CatalogGeneration == ^uint64(0) ||
		authority.session.bundle.maxMutations < 3 {
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
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return err
	}
	if cut.snapshot.Generation() != expected.CatalogGeneration+1 {
		return ErrCatalogGenerationMismatch
	}
	receiptKey, _ := replicatedReplicaReplacementReceiptKeys(expected.Group)
	receiptResult, err := authority.readRaw(
		ctx, receiptKey[:], maxReplicatedReplicaReplacementReceiptBytes,
	)
	if err != nil {
		return err
	}
	if !receiptResult.Found {
		return ErrReplicatedCatalogConflict
	}
	receipt, err := openReplicaReplacementReceipt(receiptResult.Value)
	if err != nil || receipt.Grant != expected ||
		receipt.NewGeneration != cut.snapshot.Generation() ||
		receipt.NewHeadBytes != uint64(len(cut.head)) ||
		receipt.NewHeadDigest != sha256.Sum256(cut.head) {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	recordKey, pageKey := replicatedMembershipGrantKeys(expected.Group)
	record, err := authority.readRaw(ctx, recordKey[:], maxReplicatedMembershipGrantBytes)
	if err != nil {
		return err
	}
	if record.Found {
		stored, openErr := openReplicatedMembershipGrant(record.Value)
		if openErr != nil || stored != expected {
			return errors.Join(openErr, ErrReplicatedCatalogConflict)
		}
	}
	page, err := authority.readRaw(ctx, pageKey[:], maxReplicatedMembershipGrantPageBytes)
	if err != nil {
		return err
	}
	if !page.Found && !record.Found {
		return nil
	}
	if !page.Found {
		return ErrReplicatedCatalogConflict
	}
	groups, err := openReplicatedMembershipGrantPage(pageKey[1], page.Value)
	if err != nil {
		return err
	}
	position, found := findReplicatedMembershipGrantGroup(groups, expected.Group)
	if !record.Found && !found {
		return nil
	}
	if !found {
		return ErrReplicatedCatalogConflict
	}
	groups = append(groups[:position], groups[position+1:]...)
	witnessDigest := sha256.Sum256(cut.witness)
	recordDigest := sha256.Sum256(record.Value)
	pageDigest := sha256.Sum256(page.Value)
	mutations := []NativeMutation{
		{Kind: replication.MutationPutDigestEqual,
			Key: replicatedCatalogHeadWitnessKey, Value: cut.witness,
			ExpectedValueLength: uint64(len(cut.witness)),
			ExpectedValueDigest: replication.Digest(witnessDigest)},
		{Kind: replication.MutationDeleteDigestEqual,
			Key: recordKey[:], ExpectedValueLength: uint64(len(record.Value)),
			ExpectedValueDigest: replication.Digest(recordDigest)},
	}
	if len(groups) == 0 {
		mutations = append(mutations, NativeMutation{Kind: replication.MutationDeleteDigestEqual,
			Key: pageKey[:], ExpectedValueLength: uint64(len(page.Value)),
			ExpectedValueDigest: replication.Digest(pageDigest)})
	} else {
		pageBytes, pageErr := appendReplicatedMembershipGrantPage(nil, pageKey[1], groups)
		if pageErr != nil {
			return pageErr
		}
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutDigestEqual,
			Key: pageKey[:], Value: pageBytes,
			ExpectedValueLength: uint64(len(page.Value)),
			ExpectedValueDigest: replication.Digest(pageDigest)})
	}
	native, err := authority.session.MutateBatch(ctx, mutations)
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

func appendReplicaReplacementReceipt(dst []byte, grant membershipgrant.Grant,
	oldHead, newHead []byte) ([]byte, error) {
	if !grant.Valid() || grant.CatalogGeneration == ^uint64(0) || len(oldHead) == 0 ||
		len(newHead) == 0 || len(oldHead) > maxReplicatedCatalogBytes ||
		len(newHead) > maxReplicatedCatalogBytes {
		return dst, ErrReplicatedCatalog
	}
	receipt := replicaReplacementReceipt{
		Grant: grant, OldGeneration: grant.CatalogGeneration,
		NewGeneration: grant.CatalogGeneration + 1,
		OldHeadBytes:  uint64(len(oldHead)), NewHeadBytes: uint64(len(newHead)),
		OldHeadDigest: sha256.Sum256(oldHead), NewHeadDigest: sha256.Sum256(newHead),
	}
	return appendReplicaReplacementReceiptRecord(dst, receipt)
}

func appendReplicaReplacementReceiptRecord(dst []byte,
	receipt replicaReplacementReceipt) ([]byte, error) {
	if !validReplicaReplacementReceipt(receipt) {
		return dst, ErrReplicatedCatalog
	}
	persisted := persistedReplicaReplacementReceipt{
		Grant:         persistMembershipGrant(receipt.Grant),
		OldGeneration: receipt.OldGeneration, NewGeneration: receipt.NewGeneration,
		OldHeadBytes: receipt.OldHeadBytes, NewHeadBytes: receipt.NewHeadBytes,
		OldHeadDigest: receipt.OldHeadDigest, NewHeadDigest: receipt.NewHeadDigest,
	}
	payload, err := vibejson.Marshal(&persisted)
	if err != nil {
		return dst, err
	}
	return appendControlPlaneDocument(dst,
		replicatedReplicaReplacementReceiptDocumentID[:], payload,
		maxReplicatedReplicaReplacementReceiptBytes)
}

func openReplicaReplacementReceipt(raw []byte) (replicaReplacementReceipt, error) {
	payload, err := openTypedControlPlaneDocument(raw,
		replicatedReplicaReplacementReceiptDocumentID[:],
		maxReplicatedReplicaReplacementReceiptBytes)
	if err != nil {
		return replicaReplacementReceipt{}, err
	}
	var persisted persistedReplicaReplacementReceipt
	if err = vibejson.Unmarshal(payload, &persisted); err != nil {
		return replicaReplacementReceipt{}, errors.Join(err, ErrReplicatedCatalog)
	}
	receipt := replicaReplacementReceipt{
		Grant:         openPersistedMembershipGrant(persisted.Grant),
		OldGeneration: persisted.OldGeneration, NewGeneration: persisted.NewGeneration,
		OldHeadBytes: persisted.OldHeadBytes, NewHeadBytes: persisted.NewHeadBytes,
		OldHeadDigest: persisted.OldHeadDigest, NewHeadDigest: persisted.NewHeadDigest,
	}
	canonical, canonicalErr := appendReplicaReplacementReceiptRecord(nil, receipt)
	if canonicalErr != nil || !bytes.Equal(raw, canonical) {
		return replicaReplacementReceipt{}, errors.Join(canonicalErr, ErrReplicatedCatalog)
	}
	return receipt, nil
}

func validReplicaReplacementReceipt(receipt replicaReplacementReceipt) bool {
	return receipt.Grant.Valid() && receipt.OldGeneration != 0 &&
		receipt.OldGeneration != ^uint64(0) &&
		receipt.Grant.CatalogGeneration == receipt.OldGeneration &&
		receipt.NewGeneration == receipt.OldGeneration+1 &&
		receipt.OldHeadBytes != 0 && receipt.NewHeadBytes != 0 &&
		receipt.OldHeadBytes <= maxReplicatedCatalogBytes &&
		receipt.NewHeadBytes <= maxReplicatedCatalogBytes &&
		receipt.OldHeadDigest != ([32]byte{}) && receipt.NewHeadDigest != ([32]byte{})
}

func validateReplicaReplacementReceipt(raw, oldHead, newHead []byte,
	oldGeneration, newGeneration uint64) (membershipgrant.Grant, error) {
	receipt, err := openReplicaReplacementReceipt(raw)
	if err != nil || receipt.OldGeneration != oldGeneration ||
		receipt.NewGeneration != newGeneration ||
		receipt.OldHeadBytes != uint64(len(oldHead)) ||
		receipt.NewHeadBytes != uint64(len(newHead)) ||
		receipt.OldHeadDigest != sha256.Sum256(oldHead) ||
		receipt.NewHeadDigest != sha256.Sum256(newHead) {
		return membershipgrant.Grant{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	return receipt.Grant, nil
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

func appendReplicaReplacementReceiptPage(dst []byte, pageIndex byte,
	groups []raftmember.GroupKey) ([]byte, error) {
	if pageIndex >= replicatedMembershipGrantPages ||
		len(groups) > maxReplicatedMembershipGrantsPerPage {
		return dst, ErrReplicatedCatalog
	}
	page := persistedMembershipGrantPage{
		Groups: make([]persistedMembershipGrantGroup, len(groups)),
	}
	for index := range groups {
		_, pageKey := replicatedReplicaReplacementReceiptKeys(groups[index])
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

func openReplicaReplacementReceiptPage(pageIndex byte, raw []byte) ([]raftmember.GroupKey, error) {
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
		_, pageKey := replicatedReplicaReplacementReceiptKeys(groups[index])
		if !validMembershipGrantGroup(groups[index]) || pageKey[1] != pageIndex || index != 0 &&
			compareMembershipGrantGroup(groups[index-1], groups[index]) >= 0 {
			return nil, ErrReplicatedCatalog
		}
	}
	canonical, err := appendReplicaReplacementReceiptPage(nil, pageIndex, groups)
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
	return replicatedMembershipLifecycleKeys(
		group, replicatedMembershipGrantKeyByte, replicatedMembershipGrantPageKeyByte,
	)
}

func replicatedReplicaReplacementReceiptKeys(group raftmember.GroupKey) ([33]byte, [2]byte) {
	return replicatedMembershipLifecycleKeys(
		group, replicatedReplicaReplacementReceiptKeyByte,
		replicatedReplicaReplacementReceiptPageKeyByte,
	)
}

func replicatedMembershipLifecycleKeys(group raftmember.GroupKey,
	recordKind, pageKind byte) ([33]byte, [2]byte) {
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
	record[0] = recordKind
	copy(record[1:], digest[:])
	page := [2]byte{pageKind,
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
		CatalogGeneration:        grant.CatalogGeneration,
		InitialReplicaSetVersion: grant.InitialReplicaSetVersion,
		InitialVoters:            grant.InitialVoters,
		InitialRosterDigest:      grant.InitialRosterDigest,
		InitialDescriptorDigest:  grant.InitialDescriptorDigest,
		SourceMember:             grant.SourceMember, TargetMember: grant.TargetMember,
		TargetNode: grant.TargetNode,
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
		CatalogGeneration:        persisted.CatalogGeneration,
		InitialReplicaSetVersion: persisted.InitialReplicaSetVersion,
		InitialVoters:            persisted.InitialVoters,
		InitialRosterDigest:      persisted.InitialRosterDigest,
		InitialDescriptorDigest:  persisted.InitialDescriptorDigest,
		SourceMember:             persisted.SourceMember, TargetMember: persisted.TargetMember,
		TargetNode: persisted.TargetNode,
	}
}
