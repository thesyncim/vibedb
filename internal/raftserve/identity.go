package raftserve

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/maphash"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

const attemptDigestDomain = "vibedb/raft-serving/attempt/format-0\x00"

const maxAttemptDigestBytes = len(attemptDigestDomain) + 32 + 8 + 8 +
	2*(8+replication.MaxIdentityBytes) + 8 + 16 + 16 + 7*8

type requestNamespace uint8

const (
	requestNamespaceSequenced requestNamespace = iota + 1
	requestNamespaceOpen
	requestNamespaceRelease
)

type requestPosition struct {
	group         raftmember.GroupKey
	sessionDigest [32]byte
	clientID      replication.ID128
	epoch         uint64
	sequence      uint64
	namespace     requestNamespace
}

type commandIdentity struct {
	position             requestPosition
	fingerprint          replication.Digest
	logical              [32]byte
	attempt              [32]byte
	tenant               []byte
	transactionRole      distributedtxn.ReplicatedRole
	transactionOperation distributedtxn.ReplicatedOperation
}

func openCommandIdentity(
	group raftmember.GroupKey,
	data []byte,
) (commandIdentity, error) {
	command, err := replication.OpenCommand(data)
	if err != nil {
		return commandIdentity{}, err
	}
	if !commandMatchesGroup(command, group) {
		return commandIdentity{}, ErrCommandGroupMismatch
	}
	transactionRole, transactionOperation, _ := command.TransactionIdentity()
	logical := replicatedstate.LogicalCommandDigest(command)
	return commandIdentity{
		position: requestPosition{
			group:         group,
			sessionDigest: replicatedstate.SessionKey(command.AuthorityClass, command.Tenant, command.ClientID),
			clientID:      command.ClientID,
			epoch:         command.ClientEpoch,
			sequence:      command.ClientSequence,
			namespace:     namespaceForCommand(command.Kind()),
		},
		fingerprint:          command.Fingerprint,
		logical:              logical,
		attempt:              attemptDigest(command, logical),
		tenant:               command.Tenant,
		transactionRole:      transactionRole,
		transactionOperation: transactionOperation,
	}, nil
}

func namespaceForCommand(kind replication.CommandKind) requestNamespace {
	switch kind {
	case replication.CommandSessionOpen:
		return requestNamespaceOpen
	case replication.CommandSessionRelease:
		return requestNamespaceRelease
	default:
		return requestNamespaceSequenced
	}
}

func commandMatchesGroup(
	command replication.CommandView,
	group raftmember.GroupKey,
) bool {
	return command.ClusterID == group.ClusterID &&
		command.ClusterIncarnation == group.ClusterIncarnation &&
		command.TopologyRecoveryEpoch == group.TopologyRecoveryEpoch &&
		command.ShardIncarnation == group.ShardIncarnation &&
		command.GroupID == group.GroupID
}

func attemptDigest(
	command replication.CommandView,
	logical [32]byte,
) [32]byte {
	var framed [maxAttemptDigestBytes]byte
	cursor := copy(framed[:], attemptDigestDomain)
	cursor += copy(framed[cursor:], logical[:])
	binary.LittleEndian.PutUint64(framed[cursor:cursor+8], command.AckThrough)
	cursor += 8
	binary.LittleEndian.PutUint64(
		framed[cursor:cursor+8], command.TopologyRecoveryEpoch,
	)
	cursor += 8
	for _, value := range [...][]byte{command.Distribution, command.Shard} {
		binary.LittleEndian.PutUint64(framed[cursor:cursor+8], uint64(len(value)))
		cursor += 8
		cursor += copy(framed[cursor:], value)
	}
	binary.LittleEndian.PutUint64(
		framed[cursor:cursor+8], command.AllocationGeneration,
	)
	cursor += 8
	cursor += copy(framed[cursor:], command.ShardIncarnation[:])
	cursor += copy(framed[cursor:], command.GroupID[:])
	for _, value := range [...]uint64{
		command.ReplicaSetVersion,
		command.ActivePolicyGeneration,
		command.ProtectionEpoch,
		command.OwnershipEpoch,
		command.SchemaGeneration,
		command.RoutingVersion,
		command.RouteGeneration,
	} {
		binary.LittleEndian.PutUint64(framed[cursor:cursor+8], value)
		cursor += 8
	}
	return sha256.Sum256(framed[:cursor])
}

func hashPosition(
	seed maphash.Seed,
	position requestPosition,
	tenant []byte,
) uint64 {
	var fixed [16 + 16 + 8 + 16 + 16 + 32 + 16 + 8 + 8 + 1]byte
	cursor := 0
	cursor += copy(fixed[cursor:], position.group.ClusterID[:])
	cursor += copy(fixed[cursor:], position.group.ClusterIncarnation[:])
	binary.LittleEndian.PutUint64(
		fixed[cursor:cursor+8], position.group.TopologyRecoveryEpoch,
	)
	cursor += 8
	cursor += copy(fixed[cursor:], position.group.ShardIncarnation[:])
	cursor += copy(fixed[cursor:], position.group.GroupID[:])
	cursor += copy(fixed[cursor:], position.sessionDigest[:])
	cursor += copy(fixed[cursor:], position.clientID[:])
	binary.LittleEndian.PutUint64(fixed[cursor:cursor+8], position.epoch)
	cursor += 8
	binary.LittleEndian.PutUint64(fixed[cursor:cursor+8], position.sequence)
	cursor += 8
	fixed[cursor] = byte(position.namespace)
	var h maphash.Hash
	h.SetSeed(seed)
	_, _ = h.Write(fixed[:])
	_, _ = h.Write(tenant)
	return h.Sum64()
}

func hashGroup(seed maphash.Seed, group raftmember.GroupKey) uint64 {
	var fixed [16 + 16 + 8 + 16 + 16]byte
	cursor := copy(fixed[:], group.ClusterID[:])
	cursor += copy(fixed[cursor:], group.ClusterIncarnation[:])
	binary.LittleEndian.PutUint64(
		fixed[cursor:cursor+8], group.TopologyRecoveryEpoch,
	)
	cursor += 8
	cursor += copy(fixed[cursor:], group.ShardIncarnation[:])
	copy(fixed[cursor:], group.GroupID[:])
	var h maphash.Hash
	h.SetSeed(seed)
	_, _ = h.Write(fixed[:])
	return h.Sum64()
}
