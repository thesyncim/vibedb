package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson"
)

const (
	rf3CatalogGenesisPlanFormat       = 1
	rf3CatalogGenesisPlanMaxBytes     = 16 << 10
	rf3CatalogGenesisDirectoryMaxSize = 4 << 20
	rf3CatalogGenesisTenant           = "control-plane"
)

var (
	errRF3CatalogGenesis          = errors.New("vibedb-shard: invalid catalog genesis")
	errRF3CatalogGenesisNotLeader = errors.New("vibedb-shard: catalog genesis owner is not leader")
)

// rf3CatalogGenesisPlan mirrors the producer's sidecar. It is deliberately
// decoded and compared canonically; a changed operator input must require a
// new physical preparation rather than silently changing startup authority.
type rf3CatalogGenesisPlan struct {
	Format                     uint16 `json:"format"`
	CatalogPath                string `json:"catalog_path"`
	InitialNodeDirectoryPath   string `json:"initial_node_directory"`
	CatalogDigest              string `json:"catalog_digest"`
	InitialNodeDirectoryDigest string `json:"initial_node_directory_digest"`
	ConfigDigest               string `json:"config_digest"`
	CombinedDigest             string `json:"combined_digest"`
}

type rf3CatalogGenesisInputs struct {
	Snapshot  *gateway.Snapshot
	Records   []gateway.NodeRecord
	Mutations []gateway.NativeMutation
	Route     gateway.ReplicatedRoute
	// Existing marks a nonempty committed relation with a retained private
	// session journal. The session runner may settle only that journal; it must
	// never open a fresh session or publish a second genesis mutation.
	Existing bool
}

const rf3CatalogGenesisRetryDelay = 100 * time.Millisecond

func loadRF3CatalogGenesisInputs(config *rf3CatalogGenesisConfig) (rf3CatalogGenesisInputs, error) {
	if config == nil || !validRF3CatalogGenesisConfig(*config) {
		return rf3CatalogGenesisInputs{}, errRF3CatalogGenesis
	}
	planRaw, err := readRF3BoundedFile(config.PlanPath, rf3CatalogGenesisPlanMaxBytes)
	if err != nil {
		return rf3CatalogGenesisInputs{}, errors.Join(errRF3CatalogGenesis, err)
	}
	var plan rf3CatalogGenesisPlan
	if err = vibejson.Unmarshal(planRaw, &plan); err != nil {
		return rf3CatalogGenesisInputs{}, errors.Join(errRF3CatalogGenesis, err)
	}
	canonicalPlan, err := vibejson.Marshal(&plan)
	if err != nil || !bytes.Equal(canonicalPlan, planRaw) ||
		plan.Format != rf3CatalogGenesisPlanFormat ||
		plan.CatalogPath != config.CatalogPath ||
		plan.InitialNodeDirectoryPath != config.InitialNodeDirectoryPath {
		return rf3CatalogGenesisInputs{}, errRF3CatalogGenesis
	}
	configRaw, err := vibejson.Marshal(config)
	if err != nil || !rf3CatalogGenesisDigestMatches(plan.ConfigDigest, configRaw) {
		return rf3CatalogGenesisInputs{}, errRF3CatalogGenesis
	}

	catalogRaw, err := readRF3BoundedFile(config.CatalogPath, gateway.RestoreCatalogReadAdmissionBytes)
	if err != nil {
		return rf3CatalogGenesisInputs{}, errors.Join(errRF3CatalogGenesis, err)
	}
	directoryRaw, err := readRF3BoundedFile(config.InitialNodeDirectoryPath, rf3CatalogGenesisDirectoryMaxSize)
	if err != nil {
		return rf3CatalogGenesisInputs{}, errors.Join(errRF3CatalogGenesis, err)
	}
	if !rf3CatalogGenesisDigestMatches(plan.CatalogDigest, catalogRaw) ||
		!rf3CatalogGenesisDigestMatches(plan.InitialNodeDirectoryDigest, directoryRaw) ||
		!rf3CatalogGenesisCombinedDigestMatches(plan.CombinedDigest, catalogRaw, directoryRaw) {
		return rf3CatalogGenesisInputs{}, errRF3CatalogGenesis
	}

	// The provisioning digest binds the exact durable catalog bytes. Durable
	// catalog files use the pretty-printed file grammar accepted by
	// LoadSnapshot; OpenSnapshotDocument is reserved for compact replicated
	// relation values and would reject this valid on-disk artifact as
	// non-canonical.
	snapshot, err := gateway.LoadSnapshot(config.CatalogPath)
	if err != nil || snapshot.Generation() != 1 {
		return rf3CatalogGenesisInputs{}, errors.Join(errRF3CatalogGenesis, err)
	}
	var records []gateway.NodeRecord
	if err = vibejson.Unmarshal(directoryRaw, &records); err != nil || len(records) == 0 {
		return rf3CatalogGenesisInputs{}, errors.Join(errRF3CatalogGenesis, err)
	}
	canonicalDirectory, err := vibejson.Marshal(&records)
	if err != nil || !bytes.Equal(canonicalDirectory, directoryRaw) {
		return rf3CatalogGenesisInputs{}, errors.Join(errRF3CatalogGenesis, err)
	}
	mutations, err := gateway.BuildReplicatedCatalogGenesisMutations(snapshot, records)
	if err != nil {
		return rf3CatalogGenesisInputs{}, errors.Join(errRF3CatalogGenesis, err)
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(
		gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard, replicas[:0],
	)
	if !ok {
		return rf3CatalogGenesisInputs{}, errRF3CatalogGenesis
	}
	if !rf3CatalogGenesisRouteMatches(*config, route) {
		return rf3CatalogGenesisInputs{}, errRF3CatalogGenesis
	}
	return rf3CatalogGenesisInputs{Snapshot: snapshot, Records: records, Mutations: mutations, Route: route}, nil
}

func rf3CatalogGenesisDigestMatches(encoded string, raw []byte) bool {
	if len(encoded) != hex.EncodedLen(sha256.Size) {
		return false
	}
	want, err := hex.DecodeString(encoded)
	if err != nil || len(want) != sha256.Size {
		return false
	}
	got := sha256.Sum256(raw)
	return bytes.Equal(want, got[:])
}

func rf3CatalogGenesisCombinedDigestMatches(encoded string, catalog, directory []byte) bool {
	if len(encoded) != hex.EncodedLen(sha256.Size) {
		return false
	}
	want, err := hex.DecodeString(encoded)
	if err != nil || len(want) != sha256.Size {
		return false
	}
	got := rf3CatalogGenesisCombinedDigest(catalog, directory)
	return bytes.Equal(want, got[:])
}

func rf3CatalogGenesisCombinedDigest(catalog, directory []byte) [sha256.Size]byte {
	// Keep this byte grammar identical to cmd/vibedb's physical producer.
	var input []byte
	input = append(input, []byte("vibedb/catalog-genesis-plan\x00")...)
	input = appendRF3CatalogGenesisLength(input, len(catalog))
	input = append(input, catalog...)
	input = appendRF3CatalogGenesisLength(input, len(directory))
	input = append(input, directory...)
	return sha256.Sum256(input)
}

func appendRF3CatalogGenesisLength(dst []byte, length int) []byte {
	return append(dst,
		byte(uint64(length)>>56), byte(uint64(length)>>48), byte(uint64(length)>>40), byte(uint64(length)>>32),
		byte(uint64(length)>>24), byte(uint64(length)>>16), byte(uint64(length)>>8), byte(length),
	)
}

func rf3CatalogGenesisRouteMatches(config rf3CatalogGenesisConfig, route gateway.ReplicatedRoute) bool {
	group, ok := rf3CatalogGenesisGroup(config)
	if !ok {
		return false
	}
	var store [16]byte
	if !decodeRF3FixedHex(config.StoreID, store[:], false) {
		return false
	}
	return route.Group == group && route.AllocationGeneration == config.AllocationGeneration &&
		route.Distribution == gateway.ReplicatedCatalogDistribution &&
		route.Shard == gateway.ReplicatedCatalogShard &&
		len(route.Replicas) > 0 && config.MemberID != 0 &&
		rf3CatalogGenesisRouteHasMember(route, config.MemberID, store)
}

func rf3CatalogGenesisGroup(config rf3CatalogGenesisConfig) (raftmember.GroupKey, bool) {
	var group raftmember.GroupKey
	if !decodeRF3FixedHex(config.ClusterID, group.ClusterID[:], false) ||
		!decodeRF3FixedHex(config.ClusterIncarnation, group.ClusterIncarnation[:], false) ||
		!decodeRF3FixedHex(config.ShardIncarnation, group.ShardIncarnation[:], false) ||
		!decodeRF3FixedHex(config.GroupID, group.GroupID[:], false) ||
		config.TopologyRecoveryEpoch == 0 {
		return raftmember.GroupKey{}, false
	}
	group.TopologyRecoveryEpoch = config.TopologyRecoveryEpoch
	return group, true
}

func rf3CatalogGenesisRouteHasMember(route gateway.ReplicatedRoute, member uint64, store [16]byte) bool {
	for _, endpoint := range route.Replicas {
		if endpoint.Member == member {
			return endpoint.StoreID == store && endpoint.NodeIncarnation != 0
		}
	}
	return false
}

func rf3CatalogGenesisCutHasPhysicalNode(
	cut gateway.FrontendDrainRuntimeCut, node rafttransport.NodeID, incarnation uint64,
) bool {
	if node == (rafttransport.NodeID{}) || incarnation == 0 || !cut.Nodes.Valid() {
		return false
	}
	for _, record := range cut.Nodes.CurrentNodes() {
		if record.NodeID == node && record.Incarnation == incarnation {
			return true
		}
	}
	return false
}

// rf3CatalogGenesisExistingCutReader is the canonical source used after the
// reserved catalog relation has already proved nonempty.  Keeping this
// callback at the existing-state boundary makes the no-write recovery path
// testable without replacing the production owner or source implementation.
// The production caller supplies ReadFrontendDrainRuntimeCutFromRows; tests
// can only supply a detached, fully validated cut.
type rf3CatalogGenesisExistingCutReader func(
	context.Context, gateway.FrontendDrainRuntimeCatalogRoute,
) (gateway.FrontendDrainRuntimeCut, error)

// rf3CatalogGenesisOwners is the closed owner capability used by the private
// bootstrap session.  The production implementation is ExecutionOwners; the
// narrow interface keeps the actual initializer's session/retry ordering
// directly testable without exposing a general proposal path.
type rf3CatalogGenesisOwners interface {
	Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)
	SubmitOwnedAuthorized(
		context.Context, raftservice.ServingFence, []byte, raftservice.ProposalAuthorization,
	) (raftservice.Result, error)
}

// initializeRF3CatalogGenesisExisting validates an already committed cut and
// returns without opening the private genesis session.  It is deliberately a
// separate production helper so the existing-state branch cannot accidentally
// fall through to the generation-one writer when a node is Enforcing or
// Retired.  The runtime owner command may advance independently of the
// physical lifecycle incarnation; the route passed to the source therefore
// uses the current owner command while node membership remains bound to the
// immutable physical incarnation in config.
func initializeRF3CatalogGenesisExisting(
	ctx context.Context,
	config *rf3CatalogGenesisConfig,
	localNode rafttransport.NodeID,
	identity raftmember.RuntimeIdentity,
	state raftservice.ServingState,
	readCut rf3CatalogGenesisExistingCutReader,
) error {
	if ctx == nil || config == nil || readCut == nil ||
		localNode == (rafttransport.NodeID{}) ||
		identity.Group == (raftmember.GroupKey{}) || identity.NodeIncarnation == 0 ||
		identity.MemberID == 0 || identity.StoreID == ([16]byte{}) ||
		state.Identity != identity || state.Status.LeaderID != identity.MemberID ||
		state.Status.Term == 0 || !state.Command.Valid() || config.Relation == 0 {
		return errRF3CatalogGenesis
	}
	route := gateway.FrontendDrainRuntimeCatalogRoute{
		Group: identity.Group, AllocationGeneration: identity.AllocationGeneration,
		Command: state.Command, Relation: replication.RelationID(config.Relation),
	}
	if !route.Valid() {
		return errRF3CatalogGenesis
	}
	cut, err := readCut(ctx, route)
	if err != nil {
		return err
	}
	if cut.Catalog == nil || cut.CatalogHeadDigest == (replication.Digest{}) ||
		!cut.Nodes.Valid() || cut.Nodes.CatalogGeneration != cut.Catalog.Generation() ||
		cut.ServiceDirectoryRevision == 0 ||
		!rf3CatalogGenesisCutHasPhysicalNode(cut, localNode, config.NodeIncarnation) {
		return errRF3CatalogGenesis
	}
	headDigest, err := gateway.ReplicatedCatalogHeadDigest(cut.Catalog)
	if err != nil || headDigest != cut.CatalogHeadDigest {
		return errors.Join(errRF3CatalogGenesis, err)
	}
	return nil
}

// rf3LocalCatalogGenesisClient is an in-process, closed round tripper for the
// provisioning session. It deliberately exposes only Probe and Propose for
// the exact reserved catalog owner; no network endpoint or general server
// dispatch is involved in the physical genesis lane.
type rf3LocalCatalogGenesisClient struct {
	owners               rf3CatalogGenesisOwners
	group                raftmember.GroupKey
	allocation           uint64
	command              raftservice.CommandFence
	member               uint64
	store                [16]byte
	node                 rafttransport.NodeID
	nodeIncarnationValue uint64
	relation             replication.RelationID
	tenant               []byte
	clientID             replication.ID128
	retryHome            replication.RetryHome
	mutations            []gateway.NativeMutation
	proposals            int
	lastSubmitErr        error
}

func (client *rf3LocalCatalogGenesisClient) DoReplicated(
	ctx context.Context, endpoint gateway.ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if client == nil || client.owners == nil || ctx == nil || request == nil ||
		endpoint.Member != client.member || endpoint.Node != client.node || endpoint.StoreID != client.store ||
		endpoint.NodeIncarnation == 0 || endpoint.NodeIncarnation != client.nodeIncarnationValue ||
		request.Capability != serviceauthz.CapabilityTopology || request.Fence.Group != client.group ||
		request.Fence.AllocationGeneration != client.allocation || request.Fence.Command != client.command {
		return nil, errRF3CatalogGenesis
	}
	state, err := client.owners.Probe(ctx, client.group)
	if err != nil {
		return nil, err
	}
	wireState := rf3CatalogGenesisWireState(state)
	if state.Identity.Group != client.group || state.Identity.MemberID != client.member ||
		state.Identity.StoreID != client.store ||
		state.Identity.NodeIncarnation != client.nodeIncarnationValue || state.Command != client.command {
		return nil, errRF3CatalogGenesis
	}
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: wireState}, nil
	}
	if request.Operation != shardservice.ReplicatedPropose || len(request.Command) == 0 {
		return nil, errRF3CatalogGenesis
	}
	if state.Status.LeaderID != state.Identity.MemberID {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedNotLeader, HasState: true, State: wireState}, nil
	}
	if request.Fence.MemberID != state.Identity.MemberID || request.Fence.StoreID != state.Identity.StoreID ||
		request.Fence.NodeIncarnation != state.Identity.NodeIncarnation || request.Fence.Term != state.Status.Term {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalStaleFence, HasState: true, State: wireState}, nil
	}
	view, err := replication.OpenCommand(request.Command)
	if err != nil || !client.commandMatches(view) {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalUnauthorized, HasState: true, State: wireState}, nil
	}
	client.proposals++
	result, err := client.owners.SubmitOwnedAuthorized(ctx, raftservice.ServingFence{
		Group: client.group, AllocationGeneration: client.allocation, Command: client.command,
		MemberID: client.member, StoreID: client.store, NodeIncarnation: client.nodeIncarnationValue, Term: state.Status.Term,
	}, request.Command, func(candidate raftservice.ServingState) bool {
		return candidate.Identity.Group == client.group && candidate.Identity.MemberID == client.member &&
			candidate.Identity.StoreID == client.store &&
			candidate.Identity.NodeIncarnation == client.nodeIncarnationValue && candidate.Command == client.command
	})
	if err != nil {
		client.lastSubmitErr = err
	}
	if err == nil {
		applied := result.State.Status.Applied
		if result.Outcome.AppliedIndex > applied {
			applied = result.Outcome.AppliedIndex
		}
		commit := result.State.Status.Commit
		if commit < applied {
			commit = applied
		}
		wireState.Applied, wireState.Commit = applied, commit
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedCompletion, HasState: true,
			State: wireState, Outcome: result.Outcome, Completion: result.Completion,
			RequestDigest: sha256.Sum256(request.Command)}, nil
	}
	if result.Outcome.Code > raftserve.OutcomeCompletion &&
		result.Outcome.Code < raftserve.OutcomeProposalRefused {
		wireState = rf3CatalogGenesisWireState(result.State)
		wireState.Fence = request.Fence
		if wireState.Applied < result.Outcome.AppliedIndex {
			wireState.Applied = result.Outcome.AppliedIndex
		}
		if wireState.Commit < wireState.Applied {
			wireState.Commit = wireState.Applied
		}
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalDeterministic, HasState: true, State: wireState,
			Outcome: result.Outcome, RequestDigest: sha256.Sum256(request.Command)}, nil
	}
	if errors.Is(err, raftmodel.ErrNotLeader) {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedNotLeader, HasState: true, State: wireState}, nil
	}
	if errors.Is(err, raftservice.ErrOutcomeUnknown) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: wireState}, nil
	}
	if errors.Is(err, raftservice.ErrServingFence) {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalStaleFence, HasState: true, State: wireState}, nil
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
		Refusal: shardservice.ReplicatedRefusalUnavailable, HasState: true, State: wireState}, nil
}

func (client *rf3LocalCatalogGenesisClient) commandMatches(view replication.CommandView) bool {
	if view.AuthorityClass != replication.CommandAuthorityTopology ||
		view.ClusterID != replication.ID128(client.group.ClusterID) ||
		view.ClusterIncarnation != replication.ID128(client.group.ClusterIncarnation) ||
		view.TopologyRecoveryEpoch != client.group.TopologyRecoveryEpoch ||
		string(view.Distribution) != string(gateway.ReplicatedCatalogDistribution) ||
		string(view.Shard) != string(gateway.ReplicatedCatalogShard) ||
		view.AllocationGeneration != client.allocation || view.ShardIncarnation != replication.ID128(client.group.ShardIncarnation) ||
		view.GroupID != replication.ID128(client.group.GroupID) || view.ClientID != client.clientID ||
		view.ReplicaSetVersion != client.command.ReplicaSetVersion ||
		view.ActivePolicyGeneration != client.command.ActivePolicyGeneration ||
		view.ProtectionEpoch != client.command.ProtectionEpoch ||
		view.OwnershipEpoch != client.command.OwnershipEpoch ||
		view.SchemaGeneration != client.command.SchemaGeneration ||
		view.RoutingVersion != client.command.RoutingVersion ||
		view.RouteGeneration != client.command.RouteGeneration ||
		view.RetryHome != client.retryHome || !bytes.Equal(view.Tenant, client.tenant) {
		return false
	}
	switch view.Kind() {
	case replication.CommandSessionOpen, replication.CommandSessionRenew, replication.CommandSessionRetire,
		replication.CommandSessionRelease:
		return view.MutationCount() == 0 && view.RelationCount() == 0
	case replication.CommandMutationBatch:
		if view.RelationCount() != 1 || view.MutationCount() != len(client.mutations) {
			return false
		}
		batches := view.RelationBatches()
		if !batches.Next() || batches.Batch().Relation != client.relation {
			return false
		}
		mutations := batches.Batch().Mutations()
		for _, expected := range client.mutations {
			if !mutations.Next() {
				return false
			}
			actual := mutations.Mutation()
			if actual.Kind != expected.Kind || !bytes.Equal(actual.Key, expected.Key) ||
				!bytes.Equal(actual.Value, expected.Value) || actual.ExpectedValueLength != expected.ExpectedValueLength ||
				actual.ExpectedValueDigest != expected.ExpectedValueDigest {
				return false
			}
		}
		return !mutations.Next() && !batches.Next()
	default:
		return false
	}
}

func rf3CatalogGenesisWireState(state raftservice.ServingState) shardservice.ReplicatedMemberState {
	return shardservice.ReplicatedMemberState{
		Fence: shardservice.ReplicatedFence{Group: state.Identity.Group,
			AllocationGeneration: state.Identity.AllocationGeneration, Command: state.Command,
			MemberID: state.Identity.MemberID, StoreID: state.Identity.StoreID,
			NodeIncarnation: state.Identity.NodeIncarnation, Term: state.Status.Term},
		LeaderID: state.Status.LeaderID, Commit: state.Status.Commit,
		Applied: state.Status.Applied, CheckpointApplied: state.Status.CheckpointApplied,
	}
}

type rf3CatalogGenesisProbe func(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)

type rf3CatalogGenesisRelationReader func(
	context.Context, raftservice.ServingFence, replication.RelationID,
) (found, full bool, rows int, err error)

// initializeRF3CatalogGenesis performs the one private physical publication
// allowed by the immutable preparation plan. Followers return a typed retry
// result; the supervisor may invoke this again after a later election.
func initializeRF3CatalogGenesis(
	ctx context.Context,
	config *rf3CatalogGenesisConfig,
	owners *raftservice.ExecutionOwners,
	localNode rafttransport.NodeID,
	identity raftmember.RuntimeIdentity,
	command raftservice.CommandFence,
) error {
	if owners == nil {
		return errRF3CatalogGenesis
	}
	return initializeRF3CatalogGenesisWithReaders(
		ctx, config, owners, localNode, identity, command,
		owners.Probe,
		func(readCtx context.Context, fence raftservice.ServingFence, relation replication.RelationID) (bool, bool, int, error) {
			var cut raftservice.LinearizableDataReadCut
			if err := owners.ReadLinearizableDataInto(readCtx, raftservice.LinearizableDataReadRequest{
				Fence: fence, Capability: serviceauthz.CapabilityDataRead, Relations: []replication.RelationID{relation},
				Authorize: func(candidate raftservice.ServingState) bool { return candidate.Fence() == fence },
			}, &cut); err != nil {
				return false, false, 0, err
			}
			value, found := cut.Data().Relation(relation)
			full := cut.Data().FullOwnership()
			rows := 0
			if found && value != nil {
				rows = int(value.Len())
			}
			return found, full, rows, cut.Close()
		},
		func(readCtx context.Context, route gateway.FrontendDrainRuntimeCatalogRoute) (gateway.FrontendDrainRuntimeCut, error) {
			reader, readerErr := gateway.NewFrontendDrainRuntimeCatalogRowReaderForCatalogRoute(owners, route)
			if readerErr != nil {
				return gateway.FrontendDrainRuntimeCut{}, readerErr
			}
			return gateway.ReadFrontendDrainRuntimeCutFromRows(readCtx, reader)
		},
	)
}

// initializeRF3CatalogGenesisWithReaders contains the complete preflight and
// existing-state orchestration. The three read callbacks are production
// adapters in the wrapper above; keeping them injectable lets focused tests
// execute the exact branch before plan loading/session creation while still
// exercising the real initializer ordering and validation.
func initializeRF3CatalogGenesisWithReaders(
	ctx context.Context,
	config *rf3CatalogGenesisConfig,
	owners rf3CatalogGenesisOwners,
	localNode rafttransport.NodeID,
	identity raftmember.RuntimeIdentity,
	command raftservice.CommandFence,
	probe rf3CatalogGenesisProbe,
	readRelation rf3CatalogGenesisRelationReader,
	readCut rf3CatalogGenesisExistingCutReader,
) error {
	return initializeRF3CatalogGenesisWithDependencies(
		ctx, config, owners, localNode, identity, command,
		probe, readRelation, readCut,
		loadRF3CatalogGenesisInputs, initializeRF3CatalogGenesisSession,
	)
}

type rf3CatalogGenesisInputLoader func(*rf3CatalogGenesisConfig) (rf3CatalogGenesisInputs, error)

type rf3CatalogGenesisSessionRunner func(
	context.Context,
	*rf3CatalogGenesisConfig,
	rf3CatalogGenesisInputs,
	rf3CatalogGenesisOwners,
	rafttransport.NodeID,
	raftmember.RuntimeIdentity,
	raftservice.ServingState,
) error

// initializeRF3CatalogGenesisWithDependencies contains the complete preflight
// and branch ordering.  The production wrapper supplies the immutable plan
// loader and local-owner session runner; tests use the same orchestration with
// a closed fixture loader so pending-journal and leader-transition behavior can
// be exercised without manufacturing on-disk operator input.
func initializeRF3CatalogGenesisWithDependencies(
	ctx context.Context,
	config *rf3CatalogGenesisConfig,
	owners rf3CatalogGenesisOwners,
	localNode rafttransport.NodeID,
	identity raftmember.RuntimeIdentity,
	command raftservice.CommandFence,
	probe rf3CatalogGenesisProbe,
	readRelation rf3CatalogGenesisRelationReader,
	readCut rf3CatalogGenesisExistingCutReader,
	loadInputs rf3CatalogGenesisInputLoader,
	runSession rf3CatalogGenesisSessionRunner,
) error {
	if ctx == nil || config == nil || localNode == (rafttransport.NodeID{}) || identity.Group == (raftmember.GroupKey{}) ||
		identity.MemberID == 0 || identity.StoreID == ([16]byte{}) ||
		identity.NodeIncarnation == 0 || !command.Valid() || probe == nil ||
		readRelation == nil || readCut == nil || loadInputs == nil || runSession == nil {
		return errRF3CatalogGenesis
	}
	configuredNode, nodeOK := config.nodeID()
	if !nodeOK || configuredNode != localNode {
		return errRF3CatalogGenesis
	}
	var configuredStore [16]byte
	if !decodeRF3FixedHex(config.StoreID, configuredStore[:], false) ||
		configuredStore != identity.StoreID || config.MemberID != identity.MemberID ||
		config.AllocationGeneration != identity.AllocationGeneration ||
		config.Distribution != string(gateway.ReplicatedCatalogDistribution) ||
		config.Shard != string(gateway.ReplicatedCatalogShard) {
		return errRF3CatalogGenesis
	}
	configuredGroup, groupOK := rf3CatalogGenesisGroup(*config)
	if !groupOK || configuredGroup != identity.Group {
		return errRF3CatalogGenesis
	}
	// Probe the current retained owner before loading the immutable generation
	// one plan. RuntimeIdentity.NodeIncarnation is a per-group Raft boot
	// incarnation; config.NodeIncarnation and the plan's endpoint incarnation
	// are the physical lifecycle identity. Existing committed state must be
	// recoverable when the former has advanced or catalog placement has moved.
	state, err := probe(ctx, identity.Group)
	if err != nil {
		return err
	}
	if state.Identity.Group != identity.Group || state.Identity.AllocationGeneration != identity.AllocationGeneration ||
		state.Identity.MemberID != identity.MemberID || state.Identity.StoreID != identity.StoreID ||
		state.Identity.NodeIncarnation != identity.NodeIncarnation || state.Command != command {
		return errRF3CatalogGenesis
	}
	if state.Status.LeaderID != identity.MemberID {
		return errRF3CatalogGenesisNotLeader
	}
	// A linearizable read of the reserved relation is the absence proof. A
	// missing head is not enough: partial state is corruption and is handled by
	// the canonical reader below rather than repaired by this initializer.
	fence := state.Fence()
	found, full, rows, err := readRelation(ctx, fence, replication.RelationID(config.Relation))
	if err != nil {
		return err
	}
	if !found || !full {
		return errRF3CatalogGenesis
	}
	journaling, err := gateway.NativeSessionJournalPresent(config.SessionJournal)
	if err != nil {
		return err
	}
	if rows != 0 {
		if !journaling {
			return initializeRF3CatalogGenesisExisting(ctx, config, localNode, identity, state, readCut)
		}
		if owners == nil {
			return errRF3CatalogGenesis
		}
		// The journal is an immutable private-session binding. Load and validate
		// its exact producer inputs before opening it; a nonempty relation never
		// authorizes reconstructing a new mutation from current catalog bytes.
		inputs, err := loadInputs(config)
		if err != nil || !validRF3CatalogGenesisInputs(config, inputs, identity, state, localNode) {
			return errors.Join(errRF3CatalogGenesis, err)
		}
		inputs.Existing = true
		if err = runSession(ctx, config, inputs, owners, localNode, identity, state); err != nil {
			return err
		}
		return initializeRF3CatalogGenesisExisting(ctx, config, localNode, identity, state, readCut)
	}
	if owners == nil {
		return errRF3CatalogGenesis
	}

	// The only path allowed to write is a proven empty generation-one
	// relation. Validate the immutable producer now, after the existing-state
	// read, so a changed historical command or boot incarnation cannot strand a
	// complete catalog. Placement coordinates may have advanced monotonically;
	// the current owner fence is used for this private proposal session.
	inputs, err := loadInputs(config)
	if err != nil {
		return err
	}
	if !validRF3CatalogGenesisInputs(config, inputs, identity, state, localNode) {
		return errRF3CatalogGenesis
	}

	return runSession(ctx, config, inputs, owners, localNode, identity, state)
}

func validRF3CatalogGenesisInputs(
	config *rf3CatalogGenesisConfig,
	inputs rf3CatalogGenesisInputs,
	identity raftmember.RuntimeIdentity,
	state raftservice.ServingState,
	localNode rafttransport.NodeID,
) bool {
	if config == nil || inputs.Snapshot == nil || len(inputs.Mutations) == 0 ||
		inputs.Route.Group != identity.Group ||
		inputs.Route.AllocationGeneration != identity.AllocationGeneration ||
		!gateway.CatalogCommandProgression(inputs.Route.Command, state.Command) ||
		!rf3CatalogGenesisRouteHasMember(inputs.Route, identity.MemberID, identity.StoreID) {
		return false
	}
	var endpoint gateway.ReplicatedEndpoint
	for _, candidate := range inputs.Route.Replicas {
		if candidate.Member == identity.MemberID {
			endpoint = candidate
			break
		}
	}
	return endpoint.Node == localNode && endpoint.StoreID == identity.StoreID && endpoint.NodeIncarnation != 0
}

// initializeRF3CatalogGenesisSession is the production private session
// runner. It owns the durable journal retry order: a retained pending command
// is settled before Open or the one genesis mutation, and a lost lifecycle
// response remains retryable until the exact command is settled.
func initializeRF3CatalogGenesisSession(
	ctx context.Context,
	config *rf3CatalogGenesisConfig,
	inputs rf3CatalogGenesisInputs,
	owners rf3CatalogGenesisOwners,
	localNode rafttransport.NodeID,
	identity raftmember.RuntimeIdentity,
	state raftservice.ServingState,
) error {
	if ctx == nil || config == nil || owners == nil || inputs.Snapshot == nil || len(inputs.Mutations) == 0 ||
		state.Identity != identity || state.Status.LeaderID != identity.MemberID ||
		!state.Command.Valid() || localNode == (rafttransport.NodeID{}) {
		return errRF3CatalogGenesis
	}
	var endpoint gateway.ReplicatedEndpoint
	for _, candidate := range inputs.Route.Replicas {
		if candidate.Member == identity.MemberID {
			endpoint = candidate
			break
		}
	}
	if endpoint.Node != localNode || endpoint.StoreID != identity.StoreID || endpoint.NodeIncarnation == 0 {
		return errRF3CatalogGenesis
	}
	// The private route carries the current owner boot fence. Canonical
	// NodeRecords in inputs remain bound to the physical lifecycle incarnation.
	endpoint.NodeIncarnation = identity.NodeIncarnation
	route := inputs.Route
	route.Command = state.Command
	route.Replicas = []gateway.ReplicatedEndpoint{endpoint}
	client := &rf3LocalCatalogGenesisClient{
		owners: owners, group: identity.Group, allocation: identity.AllocationGeneration,
		command: state.Command, member: identity.MemberID, store: identity.StoreID, node: localNode,
		nodeIncarnationValue: identity.NodeIncarnation,
		relation:             replication.RelationID(config.Relation), tenant: []byte(rf3CatalogGenesisTenant), mutations: inputs.Mutations,
	}
	if !decodeRF3FixedHex(config.ClientID, client.clientID[:], false) ||
		!decodeRF3FixedHex(config.RetryHome, client.retryHome[:], false) {
		return errRF3CatalogGenesis
	}
	executor, err := gateway.NewReplicatedExecutor(client, 3, 2*time.Second)
	if err != nil {
		return err
	}
	binding, err := gateway.NativeSessionJournalBinding(route, string(gateway.ReplicatedCatalogDistribution),
		string(gateway.ReplicatedCatalogShard), []byte(rf3CatalogGenesisTenant), replication.RelationID(config.Relation),
		serviceauthz.CapabilityTopology)
	if err != nil {
		return err
	}
	journal, err := gateway.OpenNativeSessionJournal(gateway.NativeSessionJournalOptions{
		Path: config.SessionJournal, ClientID: client.clientID, RetryHome: client.retryHome,
		MaxCommandBytes: replication.MaxCommandBytes, Binding: binding,
	})
	if err != nil {
		return err
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{
		Executor: executor, Route: route, Distribution: string(gateway.ReplicatedCatalogDistribution),
		Shard: string(gateway.ReplicatedCatalogShard), Tenant: []byte(rf3CatalogGenesisTenant),
		ClientID: client.clientID, RetryHome: client.retryHome, Resolver: gateway.BaseRelationResolver{Relation: replication.RelationID(config.Relation)},
		Journal: journal, ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: len(inputs.Mutations), MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		return err
	}
	var settledPendingKind replication.CommandKind
	var settledPending bool
	if status := session.Status(); status.Pending {
		pending, pendingErr := replication.OpenCommand(session.PendingCommand())
		if pendingErr != nil {
			return errors.Join(errRF3CatalogGenesis, pendingErr)
		}
		settledPending = true
		settledPendingKind = pending.Kind()
		if _, err = session.RetryPending(ctx); err != nil {
			return err
		}
		if status = session.Status(); status.Pending {
			return gateway.ErrNativeCommandPending
		}
	}
	status := session.Status()
	if inputs.Existing {
		// A committed relation plus a retained journal is cleanup-only. A fresh
		// journal state is malformed partial authority and must never be promoted
		// into a new Open or Mutation command.
		if !status.Active && !status.Retired && !status.Released {
			return gateway.ErrNativeSessionState
		}
		return session.RetireReleaseAndDestroy(ctx)
	}
	if !status.Active && !status.Retired && !status.Released {
		if _, err = session.Open(ctx, time.Now().Add(2*time.Second).UnixNano()); err != nil {
			return fmt.Errorf("rf3 catalog genesis session open: %w", err)
		}
	}
	status = session.Status()
	if status.Active && (!settledPending || settledPendingKind != replication.CommandMutationBatch) {
		if _, err = session.MutateBatch(ctx, inputs.Mutations); err != nil {
			return fmt.Errorf("rf3 catalog genesis mutation batch: %w", err)
		}
	}
	phase := "retire-release"
	statusBeforeRetireRelease := session.Status()
	switch {
	case statusBeforeRetireRelease.Pending:
		phase = "pending-retry"
	case statusBeforeRetireRelease.Active:
		phase = "retire"
	case statusBeforeRetireRelease.Retired:
		phase = "release"
	case statusBeforeRetireRelease.Released:
		phase = "journal-destroy"
	}
	if err = session.RetireReleaseAndDestroy(ctx); err != nil {
		statusAfterRetireRelease := session.Status()
		if client.lastSubmitErr != nil {
			return fmt.Errorf("rf3 catalog genesis session %s: client=%x retry=%x epoch=%d next=%d ack=%d pending=%t active=%t retired=%t released=%t local owner: %v: %w",
				phase, client.clientID, client.retryHome, statusAfterRetireRelease.Epoch,
				statusAfterRetireRelease.NextSequence, statusAfterRetireRelease.AckThrough,
				statusAfterRetireRelease.Pending, statusAfterRetireRelease.Active,
				statusAfterRetireRelease.Retired, statusAfterRetireRelease.Released,
				client.lastSubmitErr, err)
		}
		return fmt.Errorf("rf3 catalog genesis session %s: client=%x retry=%x epoch=%d next=%d ack=%d pending=%t active=%t retired=%t released=%t: %w",
			phase, client.clientID, client.retryHome, statusAfterRetireRelease.Epoch,
			statusAfterRetireRelease.NextSequence, statusAfterRetireRelease.AckThrough,
			statusAfterRetireRelease.Pending, statusAfterRetireRelease.Active,
			statusAfterRetireRelease.Retired, statusAfterRetireRelease.Released, err)
	}
	return nil
}

func startRF3CatalogGenesis(
	parent context.Context,
	config *rf3CatalogGenesisConfig,
	owners *raftservice.ExecutionOwners,
	localNode rafttransport.NodeID,
	identity raftmember.RuntimeIdentity,
	command raftservice.CommandFence,
) <-chan error {
	if parent == nil || config == nil || owners == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- runRF3CatalogGenesis(parent, config, owners, localNode, identity, command) }()
	return done
}

// runRF3CatalogGenesis keeps the private initializer live across elections.
// Only the local catalog voter that currently owns the leader fence may
// publish; followers stay in the physical supervisor and retry without
// opening any public bootstrap path.
func runRF3CatalogGenesis(
	ctx context.Context,
	config *rf3CatalogGenesisConfig,
	owners *raftservice.ExecutionOwners,
	localNode rafttransport.NodeID,
	identity raftmember.RuntimeIdentity,
	command raftservice.CommandFence,
) error {
	if ctx == nil {
		return errRF3CatalogGenesis
	}
	for {
		err := initializeRF3CatalogGenesis(ctx, config, owners, localNode, identity, command)
		if err == nil {
			return nil
		}
		if !rf3CatalogGenesisRetryable(err) {
			return err
		}
		timer := time.NewTimer(rf3CatalogGenesisRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
}

// rf3CatalogGenesisRetryable keeps an outcome-unknown private command from
// becoming a permanent startup failure. The journal is the exact replay
// authority in that case; the next owner term retries the same bytes. This is
// deliberately narrower than retrying arbitrary initializer errors.
func rf3CatalogGenesisRetryable(err error) bool {
	return errors.Is(err, errRF3CatalogGenesisNotLeader) ||
		errors.Is(err, raftmodel.ErrNotLeader) ||
		errors.Is(err, raftservice.ErrOwnerClosed) ||
		errors.Is(err, raftservice.ErrOutcomeUnknown)
}

// Ensure the compile-time dependency remains explicit while this private
// adapter is kept separate from the authenticated network ReplicatedServer.
var _ gateway.ReplicatedRoundTripper = (*rf3LocalCatalogGenesisClient)(nil)
