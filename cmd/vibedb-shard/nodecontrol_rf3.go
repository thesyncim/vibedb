package main

// The RF3 node-control adapter owns the physical-node half of enrollment. It
// deliberately stops at a durable SQL/schema reservation. A target cannot
// manufacture a Raft snapshot before the source has observed AddLearner and
// exported a certified cut; snapshottransfer owns that later, out-of-band
// stream and its cursor. This file is also the boundary that keeps request
// supplied paths out of the node filesystem.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

const (
	rf3EnrollmentPayloadKind     = "vibedb/rf3-enrollment-reservation/v1"
	rf3EnrollmentReservationFile = "enrollment-reservation.vibejson"
	rf3EnrollmentReceiverFile    = "enrollment-receiver.vibejson"
	rf3EnrollmentSpecFile        = "preparation-spec.vibejson"
	rf3EnrollmentDescriptorFile  = "enrollment-descriptor.vibejson"
	rf3EnrollmentRuntimeFile     = "enrollment-runtime.vibejson"
	maxRF3EnrollmentPayloadBytes = 4 << 20
)

type rf3EnrollmentReservation struct {
	Kind                  string                                 `json:"kind"`
	IntentID              [32]byte                               `json:"intent_id"`
	IntentDigest          replication.Digest                     `json:"intent_digest"`
	ManifestDigest        replication.Digest                     `json:"manifest_digest"`
	Group                 raftmember.GroupKey                    `json:"group"`
	TargetMember          uint64                                 `json:"target_member"`
	TargetNode            rafttransport.NodeID                   `json:"target_node"`
	TargetNodeIncarnation uint64                                 `json:"target_node_incarnation"`
	TargetStoreID         [16]byte                               `json:"target_store_id"`
	SQL                   sqldriver.ReplicatedShardStoreIdentity `json:"sql"`
	Apply                 sqldriver.ReplicatedApplyIdentity      `json:"apply"`
}

type rf3EnrollmentReceiverReceipt struct {
	Kind                  string               `json:"kind"`
	IntentID              [32]byte             `json:"intent_id"`
	IntentDigest          replication.Digest   `json:"intent_digest"`
	Group                 raftmember.GroupKey  `json:"group"`
	TargetMember          uint64               `json:"target_member"`
	TargetNode            rafttransport.NodeID `json:"target_node"`
	TargetNodeIncarnation uint64               `json:"target_node_incarnation"`
	TargetStoreID         [16]byte             `json:"target_store_id"`
	ProofDigest           replication.Digest   `json:"proof_digest"`
}

// rf3EnrollmentDescriptorReceipt is the durable handoff between the
// post-AddLearner source and the target receiver.  The fixed descriptor wire
// bytes are retained below the exact reservation directory so a process
// restart never has to reconstruct an artifact identity from a peer address
// or a local snapshot cursor.
type rf3EnrollmentDescriptorReceipt struct {
	Kind              string               `json:"kind"`
	IntentID          [32]byte             `json:"intent_id"`
	IntentDigest      replication.Digest   `json:"intent_digest"`
	Group             raftmember.GroupKey  `json:"group"`
	TargetMember      uint64               `json:"target_member"`
	TargetNode        rafttransport.NodeID `json:"target_node"`
	TargetIncarnation uint64               `json:"target_incarnation"`
	TargetStoreID     [16]byte             `json:"target_store_id"`
	Descriptor        []byte               `json:"descriptor"`
	DescriptorDigest  replication.Digest   `json:"descriptor_digest"`
}

// rf3EnrollmentRuntimeReceipt is written only after the shared peer has
// accepted the adopted runtime.  It is evidence for restart repair; it is
// never consumed as membership authority and therefore cannot make a group
// serve without a fresh committed intent/receipt and transport registration.
type rf3EnrollmentRuntimeReceipt struct {
	Kind              string                     `json:"kind"`
	IntentID          [32]byte                   `json:"intent_id"`
	IntentDigest      replication.Digest         `json:"intent_digest"`
	Group             raftmember.GroupKey        `json:"group"`
	TargetMember      uint64                     `json:"target_member"`
	TargetNode        rafttransport.NodeID       `json:"target_node"`
	TargetIncarnation uint64                     `json:"target_incarnation"`
	TargetStoreID     [16]byte                   `json:"target_store_id"`
	ProofDigest       replication.Digest         `json:"proof_digest"`
	DescriptorDigest  replication.Digest         `json:"descriptor_digest"`
	Identity          raftmember.RuntimeIdentity `json:"identity"`
}

func rf3EnrollmentReservationPath(nodeRoot string, intentID [32]byte) string {
	return filepath.Join(nodeRoot, "enrollments", hex.EncodeToString(intentID[:]))
}

func rf3EnrollmentPayloadBytes(payload []byte) (nodecontrol.PreparationSpec, error) {
	if len(payload) == 0 || len(payload) > maxRF3EnrollmentPayloadBytes {
		return nodecontrol.PreparationSpec{}, nodecontrol.ErrBound
	}
	return nodecontrol.OpenPreparationSpec(payload)
}

// validateRF3EnrollmentPayload binds the manifest to the committed intent and
// to a node-owned deterministic directory. No caller supplied Root is used as
// a filesystem destination until these exact checks have succeeded.
func validateRF3EnrollmentPayload(
	_ context.Context,
	intent gateway.GroupEnrollmentIntent,
	payload []byte,
	nodeRoot string,
	template rf3NodePreparationTemplate,
) (prepareRF3Manifest, error) {
	if !intent.Valid() || nodeRoot == "" || !filepath.IsAbs(nodeRoot) || filepath.Clean(nodeRoot) != nodeRoot || nodeRoot == string(filepath.Separator) {
		return prepareRF3Manifest{}, nodecontrol.ErrControl
	}
	spec, err := rf3EnrollmentPayloadBytes(payload)
	if err != nil {
		return prepareRF3Manifest{}, err
	}
	if err := spec.ValidateAgainst(intent); err != nil {
		return prepareRF3Manifest{}, err
	}
	manifest, err := prepareRF3ManifestFromSpec(spec, intent, nodeRoot, template)
	if err != nil {
		return prepareRF3Manifest{}, err
	}
	if err := validateRF3Reservation(manifest); err != nil {
		return prepareRF3Manifest{}, errors.Join(nodecontrol.ErrControl, err)
	}
	return manifest, nil
}

// rf3NodePreparationTemplate is supplied by the local physical-node
// manifest. It contains no controller-provided paths or credentials.
type rf3NodePreparationTemplate struct {
	WAL                 prepareRF3WAL
	TLS                 rf3ManifestTLS
	AuthorizationPolicy string
	SplitControl        prepareRF3SplitControl
}

// rf3NodePreparationTemplateFromManifest extracts only local node-owned
// preparation settings.  The public preparation document supplies all group
// identities and schema geometry; this function supplies paths, credentials,
// and the physical node's bounded split controls from the already validated
// node manifest.  Keeping this conversion at the node boundary prevents a
// remote controller from smuggling a filesystem path or key into a request.
func rf3NodePreparationTemplateFromManifest(manifest rf3Manifest) (rf3NodePreparationTemplate, error) {
	if manifest.NodeLog == nil || manifest.NodeIncarnation == 0 ||
		manifest.ReplicaControl.SourceDataRoot == "" ||
		!filepath.IsAbs(manifest.ReplicaControl.SourceDataRoot) ||
		filepath.Clean(manifest.ReplicaControl.SourceDataRoot) != manifest.ReplicaControl.SourceDataRoot {
		return rf3NodePreparationTemplate{}, nodecontrol.ErrControl
	}
	grants := make([]prepareRF3ActionGrant, len(manifest.SplitControl.Grants))
	for index, grant := range manifest.SplitControl.Grants {
		if grant.Node == (rafttransport.NodeID{}) || grant.Actions == 0 {
			return rf3NodePreparationTemplate{}, nodecontrol.ErrControl
		}
		grants[index] = prepareRF3ActionGrant{
			NodeID:  hex.EncodeToString(grant.Node[:]),
			Actions: grant.Actions,
		}
	}
	if len(grants) == 0 || manifest.SplitControl.MaxRecords <= 0 ||
		manifest.SplitControl.MaxFileBytes <= 0 || manifest.SplitControl.operationLimit() <= 0 {
		return rf3NodePreparationTemplate{}, nodecontrol.ErrControl
	}
	return rf3NodePreparationTemplate{
		WAL: prepareRF3WAL{
			KeyID: manifest.NodeLog.KeyID, KeyMaterialPath: manifest.NodeLog.KeyMaterialPath,
			WrappedKey: manifest.NodeLog.WrappedKey,
		},
		TLS:                 manifest.TLS,
		AuthorizationPolicy: manifest.AuthorizationPolicy,
		SplitControl: prepareRF3SplitControl{
			MaxRecords:           manifest.SplitControl.MaxRecords,
			MaxFileBytes:         manifest.SplitControl.MaxFileBytes,
			Grants:               grants,
			MaxChildOperations:   manifest.SplitControl.operationLimit(),
			StageCheckpointBytes: rangesplit.MaxChildArtifactChunkBytes,
		},
	}, nil
}

func prepareRF3ManifestFromSpec(
	spec nodecontrol.PreparationSpec, intent gateway.GroupEnrollmentIntent, nodeRoot string,
	template rf3NodePreparationTemplate,
) (prepareRF3Manifest, error) {
	if !filepath.IsAbs(nodeRoot) || filepath.Clean(nodeRoot) != nodeRoot {
		return prepareRF3Manifest{}, nodecontrol.ErrUnauthorized
	}
	encode := func(value [16]byte) string { return hex.EncodeToString(value[:]) }
	manifest := prepareRF3Manifest{
		Root: nodeRoot, Distribution: string(spec.Distribution), Shard: string(spec.Shard),
		ClusterID: encode(spec.Group.ClusterID), ClusterIncarnation: encode(spec.Group.ClusterIncarnation),
		TopologyRecoveryEpoch: spec.Group.TopologyRecoveryEpoch, AllocationGeneration: uint64(spec.AllocationGeneration),
		ShardIncarnation: encode(spec.Group.ShardIncarnation), GroupID: encode(spec.Group.GroupID),
		MemberID: intent.Target.Member, StoreID: encode(intent.Target.StoreID), Table: spec.Table,
		CreateTable: spec.CreateTable, SchemaStatements: append([]string(nil), spec.SchemaStatements...),
		Authority: prepareRF3Authority{ActivePolicyGeneration: spec.SourceCommand.ActivePolicyGeneration,
			ProtectionEpoch: spec.SourceCommand.ProtectionEpoch, OwnershipEpoch: spec.SourceCommand.OwnershipEpoch,
			SchemaGeneration: spec.SourceCommand.SchemaGeneration, RoutingVersion: spec.SourceCommand.RoutingVersion,
			RouteGeneration: spec.SourceCommand.RouteGeneration},
		WAL: template.WAL, TLS: template.TLS, AuthorizationPolicy: template.AuthorizationPolicy,
		SplitControl: template.SplitControl, DevelopmentOnly: false,
	}
	if manifest.SplitControl.StageCheckpointBytes == 0 {
		manifest.SplitControl.StageCheckpointBytes = rangesplit.MaxChildArtifactChunkBytes
	}
	manifest.WAL.KeyMaterialPath = template.WAL.KeyMaterialPath
	manifest.WAL.MaxFileBytes, manifest.WAL.MaxRecordBytes = spec.Log.MaxFileBytes, spec.Log.MaxRecordBytes
	manifest.WAL.MaxRecords, manifest.WAL.MaxEntries, manifest.WAL.MaxLiveBytes = spec.Log.MaxRecords, spec.Log.MaxEntries, spec.Log.MaxLiveBytes
	manifest.Apply = prepareRF3Apply{MaxSessions: spec.Apply.MaxSessions, RetryWindow: spec.Apply.RetryWindow,
		MaxCollections: spec.Apply.MaxCollections, MaxDocuments: spec.Apply.MaxDocuments, MaxBytes: spec.Apply.MaxBytes,
		ShardKey: spec.Apply.ShardKey, RequestLedgerCapacityBytes: spec.Apply.RequestLedgerCapacityBytes,
		RequestLedgerCleanupReserveBytes: spec.Apply.RequestLedgerCleanupReserveBytes}
	// Zero fixed-width wire fields represent an absent ledger profile. Encoding
	// them as nonempty hex strings would accidentally enable ledger validation.
	if spec.Apply.RequestLedgerCapacityBytes != 0 || spec.Apply.RequestLedgerCleanupReserveBytes != 0 ||
		spec.Apply.RequestLedgerRangeStart != ([32]byte{}) || spec.Apply.RequestLedgerRangeEnd != ([32]byte{}) ||
		spec.Apply.RequestLedgerRangeIdentity != ([32]byte{}) {
		manifest.Apply.RequestLedgerRangeStart = hex.EncodeToString(spec.Apply.RequestLedgerRangeStart[:])
		manifest.Apply.RequestLedgerRangeEnd = hex.EncodeToString(spec.Apply.RequestLedgerRangeEnd[:])
		manifest.Apply.RequestLedgerRangeIdentity = hex.EncodeToString(spec.Apply.RequestLedgerRangeIdentity[:])
	}
	manifest.GlobalIndexes = make([]sqldriver.ReplicatedGlobalIndexRelation, len(spec.GlobalIndexes))
	for index, relation := range spec.GlobalIndexes {
		manifest.GlobalIndexes[index] = sqldriver.ReplicatedGlobalIndexRelation{Relation: relation.Relation, Table: relation.Table,
			IndexID: relation.IndexID, Incarnation: relation.Incarnation, LocatorCount: relation.LocatorCount, Unique: relation.Unique,
			KeyEncoding: sqldriver.ReplicatedRelationKeyEncoding(relation.KeyEncoding), KeyArity: relation.KeyArity,
			TupleVersion: distribution.TupleVersion(relation.TupleVersion), MapperVersion: distribution.MapperVersion(relation.MapperVersion), BucketBits: relation.BucketBits}
	}
	manifest.Members = make([]prepareRF3Member, len(spec.InitialVoters))
	for index, member := range spec.InitialVoters {
		manifest.Members[index] = prepareRF3Member{MemberID: member.MemberID, NodeID: encode(member.Node), PeerAddress: member.PeerAddress}
	}
	manifest.TargetMember = &prepareRF3Member{MemberID: spec.Target.MemberID, NodeID: encode(spec.Target.Node), PeerAddress: spec.Target.PeerAddress}
	return manifest, nil
}

// validateRF3Reservation uses the existing schema validator only as a local
// SQL/profile checker. Its temporary three-member view always includes the
// explicit target and never becomes a Raft bootstrap or ConfState.
func validateRF3Reservation(input prepareRF3Manifest) error {
	_, _, _, _, _, _, err := validatePrepareRF3(rf3ReservationValidationView(input))
	return err
}

func validateRF3ReservationInput(input prepareRF3Manifest) (
	raftstore.Identity, sqldriver.ReplicatedAuthorityProfile, [3]rafttransport.NodeID,
	raftstore.Options, sqldriver.ReplicatedApplyOptions, []byte, error,
) {
	return validatePrepareRF3(rf3ReservationValidationView(input))
}

func rf3ReservationValidationView(input prepareRF3Manifest) prepareRF3Manifest {
	if input.TargetMember == nil {
		return input
	}
	view := input
	view.Members = append([]prepareRF3Member{*input.TargetMember}, input.Members...)
	sort.Slice(view.Members, func(i, j int) bool { return view.Members[i].MemberID < view.Members[j].MemberID })
	if len(view.Members) > rf3ManifestMembers {
		trimmed := []prepareRF3Member{*input.TargetMember}
		for _, member := range input.Members {
			if len(trimmed) == rf3ManifestMembers {
				break
			}
			trimmed = append(trimmed, member)
		}
		view.Members = trimmed
		sort.Slice(view.Members, func(i, j int) bool { return view.Members[i].MemberID < view.Members[j].MemberID })
	}
	return view
}

// rf3NodeControlPreparer reserves SQL/schema state and reopens it by exact
// receipt after a crash. The proof is a reservation proof; AppliedIndex is
// intentionally zero until the later certified snapshot installation.
type rf3NodeControlPreparer struct {
	mu       sync.Mutex
	NodeRoot string
	Template rf3NodePreparationTemplate
}

func (preparer *rf3NodeControlPreparer) Prepare(
	ctx context.Context, intent gateway.GroupEnrollmentIntent, payload []byte,
) (gateway.PreparedReplicaProof, error) {
	if preparer == nil || ctx == nil {
		return gateway.PreparedReplicaProof{}, nodecontrol.ErrControl
	}
	preparer.mu.Lock()
	defer preparer.mu.Unlock()
	manifest, err := validateRF3EnrollmentPayload(ctx, intent, payload, preparer.NodeRoot, preparer.Template)
	if err != nil {
		return gateway.PreparedReplicaProof{}, err
	}
	// Every immutable intent owns one private reservation directory below the
	// node root.  The node root itself is the process-level runtime namespace
	// and must never be reused as a group's SQL target.
	manifest.Root = rf3EnrollmentReservationPath(preparer.NodeRoot, intent.IntentID)
	payloadDigest := sha256.Sum256(payload)
	reservation := rf3EnrollmentReservation{
		Kind: rf3EnrollmentPayloadKind, IntentID: intent.IntentID, IntentDigest: intent.Digest(),
		ManifestDigest: payloadDigest, Group: intent.Group, TargetMember: intent.Target.Member,
		TargetNode: intent.Target.Node, TargetNodeIncarnation: intent.Target.NodeIncarnation,
		TargetStoreID: intent.Target.StoreID,
	}
	if err := provisionRF3SnapshotTargetInto(manifest, manifest.Root, reservation); err != nil {
		return gateway.PreparedReplicaProof{}, err
	}
	if err := persistRF3EnrollmentSpec(manifest.Root, payload); err != nil {
		return gateway.PreparedReplicaProof{}, err
	}
	return rf3ReservationProof(intent), nil
}

func (preparer *rf3NodeControlPreparer) ObservePrepared(
	ctx context.Context, intent gateway.GroupEnrollmentIntent, payload []byte,
) (gateway.PreparedReplicaProof, bool, error) {
	if preparer == nil || ctx == nil {
		return gateway.PreparedReplicaProof{}, false, nodecontrol.ErrControl
	}
	preparer.mu.Lock()
	defer preparer.mu.Unlock()
	manifest, err := validateRF3EnrollmentPayload(ctx, intent, payload, preparer.NodeRoot, preparer.Template)
	if err != nil {
		return gateway.PreparedReplicaProof{}, false, err
	}
	manifest.Root = rf3EnrollmentReservationPath(preparer.NodeRoot, intent.IntentID)
	reservation, found, err := readRF3EnrollmentReservation(manifest.Root)
	if err != nil || !found {
		return gateway.PreparedReplicaProof{}, found, err
	}
	want := rf3EnrollmentReservation{
		Kind: rf3EnrollmentPayloadKind, IntentID: intent.IntentID, IntentDigest: intent.Digest(),
		ManifestDigest: sha256.Sum256(payload), Group: intent.Group, TargetMember: intent.Target.Member,
		TargetNode: intent.Target.Node, TargetNodeIncarnation: intent.Target.NodeIncarnation,
		TargetStoreID: intent.Target.StoreID,
	}
	if reservation.Kind != want.Kind || reservation.IntentID != want.IntentID ||
		reservation.IntentDigest != want.IntentDigest || reservation.ManifestDigest != want.ManifestDigest ||
		reservation.Group != want.Group || reservation.TargetMember != want.TargetMember ||
		reservation.TargetNode != want.TargetNode || reservation.TargetNodeIncarnation != want.TargetNodeIncarnation ||
		reservation.TargetStoreID != want.TargetStoreID {
		return gateway.PreparedReplicaProof{}, false, nodecontrol.ErrConflict
	}
	storedSpec, readSpecErr := readRF3BoundedFile(filepath.Join(manifest.Root, rf3EnrollmentSpecFile), maxRF3EnrollmentPayloadBytes)
	if readSpecErr != nil || !bytes.Equal(storedSpec, payload) {
		return gateway.PreparedReplicaProof{}, false, errors.Join(nodecontrol.ErrConflict, readSpecErr)
	}
	return rf3ReservationProof(intent), true, nil
}

func persistRF3EnrollmentSpec(root string, payload []byte) error {
	if root == "" || len(payload) == 0 || len(payload) > maxRF3EnrollmentPayloadBytes {
		return nodecontrol.ErrControl
	}
	path := filepath.Join(root, rf3EnrollmentSpecFile)
	if prior, err := readRF3BoundedFile(path, maxRF3EnrollmentPayloadBytes); err == nil {
		if bytes.Equal(prior, payload) {
			return nil
		}
		return nodecontrol.ErrConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeRF3DurableMarker(path, payload)
}

func rf3EnrollmentDescriptorBytes(descriptor snapshottransfer.Descriptor) ([]byte, replication.Digest, error) {
	raw, err := snapshottransfer.AppendDescriptor(nil, descriptor)
	if err != nil {
		return nil, replication.Digest{}, err
	}
	return raw, sha256.Sum256(raw), nil
}

func persistRF3EnrollmentDescriptor(
	root string, intent gateway.GroupEnrollmentIntent, descriptor snapshottransfer.Descriptor,
) error {
	if root == "" || !intent.Valid() || !descriptor.Valid() || descriptor.Group != intent.Group ||
		descriptor.TargetMember != intent.Target.Member || descriptor.TargetStore != intent.Target.StoreID ||
		descriptor.TargetIncarnation != intent.Target.NodeIncarnation {
		return nodecontrol.ErrControl
	}
	rawDescriptor, descriptorDigest, err := rf3EnrollmentDescriptorBytes(descriptor)
	if err != nil {
		return err
	}
	receipt := rf3EnrollmentDescriptorReceipt{
		Kind: rf3EnrollmentPayloadKind, IntentID: intent.IntentID, IntentDigest: intent.Digest(),
		Group: intent.Group, TargetMember: intent.Target.Member, TargetNode: intent.Target.Node,
		TargetIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID,
		Descriptor: rawDescriptor, DescriptorDigest: descriptorDigest,
	}
	encoded, err := vibejson.Marshal(&receipt)
	if err != nil {
		return err
	}
	path := filepath.Join(root, rf3EnrollmentDescriptorFile)
	if prior, readErr := readRF3BoundedFile(path, 256<<10); readErr == nil {
		if bytes.Equal(prior, encoded) {
			return nil
		}
		return nodecontrol.ErrConflict
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	return writeRF3DurableMarker(path, encoded)
}

func readRF3EnrollmentDescriptor(
	root string, intent gateway.GroupEnrollmentIntent,
) (snapshottransfer.Descriptor, bool, error) {
	if root == "" || !intent.Valid() {
		return snapshottransfer.Descriptor{}, false, nodecontrol.ErrControl
	}
	raw, err := readRF3BoundedFile(filepath.Join(root, rf3EnrollmentDescriptorFile), 256<<10)
	if errors.Is(err, os.ErrNotExist) {
		return snapshottransfer.Descriptor{}, false, nil
	}
	if err != nil {
		return snapshottransfer.Descriptor{}, false, err
	}
	var receipt rf3EnrollmentDescriptorReceipt
	if err = vibejson.Unmarshal(raw, &receipt); err != nil {
		return snapshottransfer.Descriptor{}, false, errors.Join(nodecontrol.ErrJournalCorrupt, err)
	}
	canonical, marshalErr := vibejson.Marshal(&receipt)
	if marshalErr != nil || !bytes.Equal(canonical, raw) || receipt.Kind != rf3EnrollmentPayloadKind ||
		receipt.IntentID != intent.IntentID || receipt.IntentDigest != intent.Digest() ||
		receipt.Group != intent.Group || receipt.TargetMember != intent.Target.Member ||
		receipt.TargetNode != intent.Target.Node || receipt.TargetIncarnation != intent.Target.NodeIncarnation ||
		receipt.TargetStoreID != intent.Target.StoreID || len(receipt.Descriptor) != snapshottransfer.DescriptorBytes ||
		receipt.DescriptorDigest != sha256.Sum256(receipt.Descriptor) {
		return snapshottransfer.Descriptor{}, false, errors.Join(nodecontrol.ErrJournalCorrupt, marshalErr)
	}
	descriptor, err := snapshottransfer.OpenDescriptor(receipt.Descriptor)
	if err != nil || descriptor.Group != intent.Group || descriptor.TargetMember != intent.Target.Member ||
		descriptor.TargetStore != intent.Target.StoreID || descriptor.TargetIncarnation != intent.Target.NodeIncarnation {
		return snapshottransfer.Descriptor{}, false, errors.Join(nodecontrol.ErrConflict, err)
	}
	return descriptor, true, nil
}

func persistRF3EnrollmentRuntime(
	root string, intent gateway.GroupEnrollmentIntent, proof gateway.PreparedReplicaProof,
	descriptor snapshottransfer.Descriptor, identity raftmember.RuntimeIdentity,
) error {
	if root == "" || !intent.Valid() || !proof.Valid() || !descriptor.Valid() ||
		!rf3BootstrapIntentProofMatches(intent, proof) || descriptor.Group != intent.Group ||
		descriptor.TargetMember != intent.Target.Member || descriptor.TargetStore != intent.Target.StoreID ||
		descriptor.TargetIncarnation != intent.Target.NodeIncarnation || identity.Group != intent.Group ||
		identity.MemberID != intent.Target.Member || identity.StoreID != intent.Target.StoreID ||
		identity.NodeIncarnation != intent.Target.NodeIncarnation ||
		identity.RelationManifestDigest != intent.ExpectedCommand.RelationManifestDigest {
		return nodecontrol.ErrControl
	}
	_, descriptorDigest, err := rf3EnrollmentDescriptorBytes(descriptor)
	if err != nil {
		return err
	}
	receipt := rf3EnrollmentRuntimeReceipt{
		Kind: rf3EnrollmentPayloadKind, IntentID: intent.IntentID, IntentDigest: intent.Digest(),
		Group: intent.Group, TargetMember: intent.Target.Member, TargetNode: intent.Target.Node,
		TargetIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID,
		ProofDigest: proof.EnrollmentDigest, DescriptorDigest: descriptorDigest, Identity: identity,
	}
	encoded, err := vibejson.Marshal(&receipt)
	if err != nil {
		return err
	}
	path := filepath.Join(root, rf3EnrollmentRuntimeFile)
	if prior, readErr := readRF3BoundedFile(path, 256<<10); readErr == nil {
		if bytes.Equal(prior, encoded) {
			return nil
		}
		return nodecontrol.ErrConflict
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	return writeRF3DurableMarker(path, encoded)
}

func readRF3EnrollmentRuntime(
	root string, intent gateway.GroupEnrollmentIntent, proof gateway.PreparedReplicaProof,
	descriptor snapshottransfer.Descriptor,
) (raftmember.RuntimeIdentity, bool, error) {
	if root == "" || !intent.Valid() || !proof.Valid() || !descriptor.Valid() {
		return raftmember.RuntimeIdentity{}, false, nodecontrol.ErrControl
	}
	raw, err := readRF3BoundedFile(filepath.Join(root, rf3EnrollmentRuntimeFile), 256<<10)
	if errors.Is(err, os.ErrNotExist) {
		return raftmember.RuntimeIdentity{}, false, nil
	}
	if err != nil {
		return raftmember.RuntimeIdentity{}, false, err
	}
	var receipt rf3EnrollmentRuntimeReceipt
	if err = vibejson.Unmarshal(raw, &receipt); err != nil {
		return raftmember.RuntimeIdentity{}, false, errors.Join(nodecontrol.ErrJournalCorrupt, err)
	}
	canonical, marshalErr := vibejson.Marshal(&receipt)
	if marshalErr != nil || !bytes.Equal(canonical, raw) {
		return raftmember.RuntimeIdentity{}, false, errors.Join(nodecontrol.ErrJournalCorrupt, marshalErr)
	}
	_, descriptorDigest, digestErr := rf3EnrollmentDescriptorBytes(descriptor)
	if digestErr != nil || receipt.Kind != rf3EnrollmentPayloadKind || receipt.IntentID != intent.IntentID ||
		receipt.IntentDigest != intent.Digest() || receipt.Group != intent.Group ||
		receipt.TargetMember != intent.Target.Member || receipt.TargetNode != intent.Target.Node ||
		receipt.TargetIncarnation != intent.Target.NodeIncarnation || receipt.TargetStoreID != intent.Target.StoreID ||
		receipt.ProofDigest != proof.EnrollmentDigest || receipt.DescriptorDigest != descriptorDigest ||
		receipt.Identity.Group != intent.Group || receipt.Identity.MemberID != intent.Target.Member ||
		receipt.Identity.StoreID != intent.Target.StoreID || receipt.Identity.NodeIncarnation != intent.Target.NodeIncarnation ||
		receipt.Identity.RelationManifestDigest != intent.ExpectedCommand.RelationManifestDigest {
		return raftmember.RuntimeIdentity{}, false, errors.Join(nodecontrol.ErrConflict, digestErr)
	}
	return receipt.Identity, true, nil
}

func rf3ReservationProof(intent gateway.GroupEnrollmentIntent) gateway.PreparedReplicaProof {
	proof := gateway.PreparedReplicaProof{
		IntentID: intent.IntentID, Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard,
		ReplicaOrdinal: intent.ReplicaOrdinal, TargetMember: intent.Target.Member, TargetNode: intent.Target.Node,
		TargetNodeIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID,
		TargetEndpoint: intent.Target.Endpoint, TargetNativeEndpoint: intent.Target.NativeEndpoint,
		TargetControlEndpoint: intent.Target.ControlEndpoint, ExpectedRosterDigest: intent.ExpectedRosterDigest,
		ExpectedDescriptorDigest: intent.ExpectedDescriptorDigest, ExpectedManifestDigest: intent.ExpectedManifestDigest,
		RelationManifestDigest: intent.ExpectedCommand.RelationManifestDigest,
		DescriptorDigest:       intent.ExpectedDescriptorDigest, ManifestDigest: intent.ExpectedManifestDigest,
		Command: intent.ExpectedCommand, AllocationGeneration: intent.AllocationGeneration,
		CatalogGeneration: intent.CatalogGeneration,
		// Preparation is a pre-membership reservation.  The source has not
		// added the learner yet, so no applied index or replica-set version is
		// certifiable here; snapshottransfer fills that fence after AddLearner.
		AppliedIndex: 0, ReplicaSetVersion: 0,
		CertifiedDirectoryRevision: intent.TargetNodeRevision,
	}
	proof.EnrollmentDigest = proof.ComputedEnrollmentDigest()
	return proof
}

// rf3NodeControlAdopter activates only the pre-Raft bootstrap receiver. The
// snapshot transfer service calls InstallPublishedLearner later, after its
// descriptor and artifact are certified; this method never starts a voter or
// publishes a serving route.
type rf3NodeControlAdopter struct {
	NodeRoot         string
	ActivateReceiver func(context.Context, gateway.GroupEnrollmentIntent, gateway.PreparedReplicaProof) error
}

func (adopter *rf3NodeControlAdopter) Adopt(
	ctx context.Context, intent gateway.GroupEnrollmentIntent, proof gateway.PreparedReplicaProof,
) error {
	if adopter == nil || ctx == nil || !intent.Valid() || !proof.Valid() || proof.IntentID != intent.IntentID {
		return nodecontrol.ErrInvalidProof
	}
	path := rf3EnrollmentReservationPath(adopter.NodeRoot, intent.IntentID)
	reservation, found, err := readRF3EnrollmentReservation(path)
	if err != nil || !found {
		return nodecontrol.ErrNotPrepared
	}
	if reservation.IntentID != intent.IntentID || reservation.Group != intent.Group || reservation.TargetMember != intent.Target.Member ||
		reservation.TargetNode != intent.Target.Node || reservation.TargetNodeIncarnation != intent.Target.NodeIncarnation || reservation.TargetStoreID != intent.Target.StoreID {
		return nodecontrol.ErrConflict
	}
	// Register the authenticated pre-Raft receiver before publishing the local
	// terminal receipt. The callback is intentionally separate from learner
	// installation: AddLearner and the certified snapshot descriptor do not
	// exist at this stage. It must be idempotent because a crash between this
	// callback and the marker fsync is resolved by the next exact retry.
	if adopter.ActivateReceiver == nil {
		return nodecontrol.ErrControl
	}
	if err := adopter.ActivateReceiver(ctx, intent, proof); err != nil {
		return err
	}
	receipt := rf3EnrollmentReceiverReceipt{
		Kind: rf3EnrollmentPayloadKind, IntentID: intent.IntentID, IntentDigest: intent.Digest(),
		Group: intent.Group, TargetMember: intent.Target.Member, TargetNode: intent.Target.Node,
		TargetNodeIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID,
		ProofDigest: proof.EnrollmentDigest,
	}
	raw, err := vibejson.Marshal(&receipt)
	if err != nil {
		return err
	}
	return writeRF3DurableMarker(filepath.Join(path, rf3EnrollmentReceiverFile), raw)
}

func (adopter *rf3NodeControlAdopter) ObserveAdopted(
	ctx context.Context, intent gateway.GroupEnrollmentIntent, proof gateway.PreparedReplicaProof,
) (bool, error) {
	if adopter == nil || ctx == nil || !intent.Valid() || !proof.Valid() {
		return false, nodecontrol.ErrControl
	}
	path := filepath.Join(rf3EnrollmentReservationPath(adopter.NodeRoot, intent.IntentID), rf3EnrollmentReceiverFile)
	raw, err := readRF3BoundedFile(path, 16<<10)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var receipt rf3EnrollmentReceiverReceipt
	if err = vibejson.Unmarshal(raw, &receipt); err != nil {
		return false, err
	}
	want := rf3EnrollmentReceiverReceipt{
		Kind: rf3EnrollmentPayloadKind, IntentID: intent.IntentID, IntentDigest: intent.Digest(),
		Group: intent.Group, TargetMember: intent.Target.Member, TargetNode: intent.Target.Node,
		TargetNodeIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID,
		ProofDigest: proof.EnrollmentDigest,
	}
	return receipt == want, nil
}

func readRF3EnrollmentReservation(root string) (rf3EnrollmentReservation, bool, error) {
	raw, err := readRF3BoundedFile(filepath.Join(root, rf3EnrollmentReservationFile), 256<<10)
	if errors.Is(err, os.ErrNotExist) {
		return rf3EnrollmentReservation{}, false, nil
	}
	if err != nil {
		return rf3EnrollmentReservation{}, false, err
	}
	var reservation rf3EnrollmentReservation
	if err = vibejson.Unmarshal(raw, &reservation); err != nil || reservation.Kind != rf3EnrollmentPayloadKind ||
		reservation.IntentID == ([32]byte{}) || reservation.IntentDigest == (replication.Digest{}) ||
		reservation.ManifestDigest == (replication.Digest{}) || reservation.TargetMember == 0 ||
		reservation.TargetNode == (rafttransport.NodeID{}) || reservation.TargetNodeIncarnation == 0 ||
		reservation.TargetStoreID == ([16]byte{}) {
		return rf3EnrollmentReservation{}, false, errors.Join(nodecontrol.ErrJournalCorrupt, err)
	}
	canonical, marshalErr := vibejson.Marshal(&reservation)
	if marshalErr != nil || !bytes.Equal(canonical, raw) {
		return rf3EnrollmentReservation{}, false, errors.Join(nodecontrol.ErrJournalCorrupt, marshalErr)
	}
	return reservation, true, nil
}

func writeRF3DurableMarker(path string, raw []byte) error {
	if len(raw) == 0 || len(raw) > 256<<10 || filepath.Clean(path) != path {
		return nodecontrol.ErrControl
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".enrollment-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(raw)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(temporaryName, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err == nil {
		err = errors.Join(directory.Sync(), directory.Close())
	}
	return err
}

// provisionRF3SnapshotTargetInto creates only the SQL/schema reservation.
// It intentionally does not call raftstore.Create or emit node-bootstrap.pb.
func provisionRF3SnapshotTargetInto(
	input prepareRF3Manifest, destination string, reservation rf3EnrollmentReservation,
) (resultErr error) {
	if destination != input.Root || !filepath.IsAbs(destination) || filepath.Clean(destination) != destination ||
		reservation.TargetMember != input.MemberID || reservation.IntentID == ([32]byte{}) {
		return nodecontrol.ErrUnauthorized
	}
	if retained, found, err := readRF3EnrollmentReservation(destination); found || err != nil {
		if err != nil {
			return err
		}
		raw, marshalErr := vibejson.Marshal(&reservation)
		if marshalErr != nil {
			return marshalErr
		}
		retainedRaw, readErr := readRF3BoundedFile(filepath.Join(destination, rf3EnrollmentReservationFile), 256<<10)
		if readErr != nil || !bytes.Equal(retainedRaw, raw) {
			return nodecontrol.ErrConflict
		}
		_ = retained
		return nil
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(nodecontrol.ErrConflict, err)
	}
	identity, authority, _, _, applyOptions, keyMaterial, err := validateRF3ReservationInput(input)
	if err != nil {
		return err
	}
	defer clear(keyMaterial)
	binding, err := raftmember.BindingForNewWAL(identity, input.TopologyRecoveryEpoch, authority)
	if err != nil {
		return err
	}
	parent := filepath.Dir(destination)
	if err = os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, ".enrollment-prepare-")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			resultErr = errors.Join(resultErr, os.RemoveAll(stage))
		}
	}()
	if err = os.Chmod(stage, 0o700); err != nil {
		return err
	}
	sqlPath := filepath.Join(stage, "member.vdb")
	database, err := sqldriver.InitializeShardStore(sqlPath, sqldriver.ShardStoreBinding{
		Distribution: distribution.DistributionName(input.Distribution), Shard: distribution.ShardID(input.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(input.AllocationGeneration),
	})
	if err != nil {
		return err
	}
	closeDatabase := func(cause error) error { return errors.Join(cause, database.Close()) }
	session, err := database.NewSession(ctxOrBackground())
	if err != nil {
		return closeDatabase(err)
	}
	for _, statementText := range append([]string{input.CreateTable}, input.SchemaStatements...) {
		statement, prepareErr := session.Prepare(ctxOrBackground(), statementText)
		if prepareErr == nil {
			_, prepareErr = statement.Exec(ctxOrBackground(), nil)
		}
		if statement != nil {
			prepareErr = errors.Join(prepareErr, statement.Close())
		}
		if prepareErr != nil {
			err = prepareErr
			break
		}
	}
	err = errors.Join(err, session.Close())
	if err != nil {
		return closeDatabase(err)
	}
	var base sqldriver.ReplicatedShardStoreIdentity
	if len(input.GlobalIndexes) == 0 {
		base, err = database.BindReplicatedShardStore(binding, input.Table)
	} else {
		base, err = database.BindReplicatedShardStoreBundle(binding, input.Table, input.GlobalIndexes)
	}
	if err != nil {
		return closeDatabase(err)
	}
	if err = sqldriver.ValidateReplicatedChildSchema(base, input.CreateTable, input.SchemaStatements, input.GlobalIndexes); err != nil {
		return closeDatabase(err)
	}
	var storage, capture [32]byte
	if _, err = rand.Read(storage[:]); err != nil {
		return closeDatabase(err)
	}
	if _, err = rand.Read(capture[:]); err != nil {
		return closeDatabase(err)
	}
	reserved, err := sqldriver.NewReplicatedChildApplyIdentity(base, hex.EncodeToString(storage[:]), hex.EncodeToString(capture[:]), applyOptions)
	if err == nil {
		err = database.PrepareReplicatedSnapshotTarget(base, reserved)
	}
	if err != nil {
		return closeDatabase(err)
	}
	if err = database.Close(); err != nil {
		return err
	}
	reservation.SQL, reservation.Apply = base, reserved
	reservationRaw, err := vibejson.Marshal(&reservation)
	if err != nil {
		return err
	}
	if err = writePrepareRF3File(filepath.Join(stage, rf3EnrollmentReservationFile), reservationRaw, 0o600); err != nil {
		return err
	}
	baseRaw, err := base.MarshalJSON()
	if err != nil {
		return err
	}
	applyRaw, err := reserved.MarshalJSON()
	if err != nil {
		return err
	}
	if err = writePrepareRF3File(filepath.Join(stage, "sql-identity.vibejson"), baseRaw, 0o600); err != nil {
		return err
	}
	if err = writePrepareRF3File(filepath.Join(stage, "apply-identity.vibejson"), applyRaw, 0o600); err != nil {
		return err
	}
	if err = syncPrepareRF3Directory(stage); err != nil {
		return err
	}
	if err = os.Rename(stage, destination); err != nil {
		return errors.Join(nodecontrol.ErrOutcomeUnknown, err)
	}
	published = true
	return syncPrepareRF3Directory(parent)
}

// ctxOrBackground keeps schema setup independent from a process-local
// callback context. The nodecontrol service has already checked cancellation
// before invoking this bounded filesystem operation.
func ctxOrBackground() context.Context { return context.Background() }

var _ nodecontrol.Preparer = (*rf3NodeControlPreparer)(nil)
var _ nodecontrol.Adopter = (*rf3NodeControlAdopter)(nil)
