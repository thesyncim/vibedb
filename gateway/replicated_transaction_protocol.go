package gateway

import (
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	ErrReplicatedTransaction         = errors.New("gateway: invalid replicated transaction")
	ErrReplicatedTransactionConflict = errors.New("gateway: replicated transaction mutation conflict")
)

// ReplicatedTransactionParticipant is one already grouped, byte-native shard
// mutation. Relation IDs are authenticated by Route.Command's schema
// generation; SQL text and relation names never enter this boundary.
type ReplicatedTransactionParticipant struct {
	Route        ReplicatedRoute
	Batches      []replication.RelationMutationBatch
	BucketBits   uint8
	IntentScopes []distributedtxn.IntentScope
}

// ReplicatedTransactionResult is the compact committed result shared by the
// durable request service and its SQL adapter.
type ReplicatedTransactionResult struct {
	ID           distributedtxn.ID
	Committed    bool
	AffectedRows int64
}

type replicatedTransactionCommandEncoder struct {
	tenant           []byte
	controlScratch   []byte
	membershipStable bool
}

// replicatedTransactionRetainedControlBytes keeps the ordinary transaction
// lane allocation-free after its first wave without pinning a maximum-width
// manifest page pack for the lifetime of an active request. Oversized control
// bodies are still bounded by distributedtxn.MaxReplicatedCommandBytes and are
// released after the outer proposal has copied them.
const replicatedTransactionRetainedControlBytes = 64 << 10

func (encoder *replicatedTransactionCommandEncoder) appendExact(
	dst []byte,
	retryHome replication.RetryHome,
	route ReplicatedRoute,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
) ([]byte, error) {
	controlSize, err := distributedtxn.ReplicatedCommandSize(control)
	if err != nil {
		return dst, err
	}
	if encoder == nil {
		return dst, ErrReplicatedTransaction
	}
	controlBytes := encoder.controlScratch[:0]
	if cap(controlBytes) < controlSize {
		controlBytes = make([]byte, 0, controlSize)
	}
	controlBytes, err = distributedtxn.AppendReplicatedCommand(controlBytes, control)
	if err != nil {
		return dst, err
	}
	sequence, err := replication.TransactionClientSequence(controlBytes)
	if err != nil {
		return dst, err
	}
	command := replicatedTransactionCommandHeader(
		route, encoder.tenant, retryHome, replication.ID128(control.ID),
		uint64(control.Role), sequence,
	)
	command.Kind = replication.CommandTransaction
	if encoder.membershipStable {
		command.AuthorityClass = replication.CommandAuthorityMembershipStableData
	}
	command.Transaction = controlBytes
	command.Batches = batches
	command.Fingerprint = nativeCommandFingerprint(command)
	commandSize, err := replication.CommandSize(command)
	if err != nil {
		return dst, err
	}
	if cap(dst)-len(dst) < commandSize {
		dst = slices.Grow(dst, commandSize)
	}
	start := len(dst)
	dst, err = replication.AppendCommand(dst, command)
	if cap(controlBytes) <= replicatedTransactionRetainedControlBytes {
		encoder.controlScratch = controlBytes[:0]
	} else {
		encoder.controlScratch = nil
	}
	if err != nil || len(dst)-start != commandSize {
		return dst[:start], errors.Join(err, ErrReplicatedTransaction)
	}
	return dst, nil
}

func replicatedTransactionCommandHeader(
	route ReplicatedRoute,
	tenant []byte,
	retryHome replication.RetryHome,
	clientID replication.ID128,
	epoch, sequence uint64,
) replication.Command {
	fence := route.Command
	return replication.Command{
		AuthorityClass:         replication.CommandAuthorityData,
		ClusterID:              route.Group.ClusterID,
		ClusterIncarnation:     route.Group.ClusterIncarnation,
		TopologyRecoveryEpoch:  route.Group.TopologyRecoveryEpoch,
		Distribution:           string(route.Distribution),
		Shard:                  string(route.Shard),
		AllocationGeneration:   route.AllocationGeneration,
		ShardIncarnation:       route.Group.ShardIncarnation,
		GroupID:                route.Group.GroupID,
		ReplicaSetVersion:      fence.ReplicaSetVersion,
		ActivePolicyGeneration: fence.ActivePolicyGeneration,
		ProtectionEpoch:        fence.ProtectionEpoch,
		OwnershipEpoch:         fence.OwnershipEpoch,
		SchemaGeneration:       fence.SchemaGeneration,
		RoutingVersion:         fence.RoutingVersion,
		RouteGeneration:        fence.RouteGeneration,
		Tenant:                 tenant,
		ClientID:               clientID,
		ClientEpoch:            epoch,
		ClientSequence:         sequence,
		RetryHome:              retryHome,
	}
}

func replicatedRouteAuthorityWitness(route ReplicatedRoute) distributedtxn.AuthorityWitness {
	digest := replicatedRouteAuthorityDigest(route)
	var witness distributedtxn.AuthorityWitness
	copy(witness[:], digest[:])
	return witness
}

func replicatedTransactionRouteAuthorityWitness(route ReplicatedRoute, stableMembership bool) distributedtxn.AuthorityWitness {
	if !stableMembership {
		return replicatedRouteAuthorityWitness(route)
	}
	// Normalize only membership. All logical authority, including the exact
	// schema/manifest and ownership generation, remains in the witness.
	route.Command.ReplicaSetVersion = 0
	digest := replication.MembershipStableRouteAuthorityDigest(replicatedRouteAuthorityDigest(route))
	var witness distributedtxn.AuthorityWitness
	copy(witness[:], digest[:])
	return witness
}

func replicatedRouteAuthorityDigest(route ReplicatedRoute) replication.Digest {
	return replication.RouteAuthorityDigest(replicatedRouteAuthority(route))
}

// replicatedRouteAuthority copies the exact authority coordinates covered by
// the route digest. The struct is value-comparable, so batch readers can gate
// same-route points with == instead of re-hashing identical bytes per point.
func replicatedRouteAuthority(route ReplicatedRoute) replication.RouteAuthority {
	return replication.RouteAuthority{
		ClusterID:              route.Group.ClusterID,
		ClusterIncarnation:     route.Group.ClusterIncarnation,
		TopologyRecoveryEpoch:  route.Group.TopologyRecoveryEpoch,
		ShardIncarnation:       route.Group.ShardIncarnation,
		GroupID:                route.Group.GroupID,
		AllocationGeneration:   route.AllocationGeneration,
		ReplicaSetVersion:      route.Command.ReplicaSetVersion,
		ActivePolicyGeneration: route.Command.ActivePolicyGeneration,
		ProtectionEpoch:        route.Command.ProtectionEpoch,
		OwnershipEpoch:         route.Command.OwnershipEpoch,
		SchemaGeneration:       route.Command.SchemaGeneration,
		RelationManifestDigest: replication.Digest(route.Command.RelationManifestDigest),
		RoutingVersion:         route.Command.RoutingVersion,
		RouteGeneration:        route.Command.RouteGeneration,
	}
}
