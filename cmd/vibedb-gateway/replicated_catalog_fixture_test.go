package main

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
)

// bindTestReplicatedShardIdentity fills the three independent logical-range
// authorities required by an RF3 catalog descriptor. Each authority has its
// own domain and is bound to the complete group, shard, and allocation tuple;
// fixtures cannot accidentally reuse a valid-looking constant across shards.
func bindTestReplicatedShardIdentity(
	t testing.TB,
	descriptor *gateway.ReplicatedShardDescriptor,
) {
	t.Helper()
	if descriptor == nil || descriptor.Distribution == "" || descriptor.Shard == "" ||
		descriptor.AllocationGeneration == 0 ||
		descriptor.Group.ClusterID == ([16]byte{}) ||
		descriptor.Group.ClusterIncarnation == ([16]byte{}) ||
		descriptor.Group.TopologyRecoveryEpoch == 0 ||
		descriptor.Group.ShardIncarnation == ([16]byte{}) ||
		descriptor.Group.GroupID == ([16]byte{}) {
		t.Fatal("test RF3 descriptor identity requires a complete group, shard, and allocation")
	}
	descriptor.RangeIdentity = testReplicatedShardIdentityDigest(
		"vibedb/test/rf3/range-identity/1\x00", descriptor,
	)
	descriptor.LineageDigest = testReplicatedShardIdentityDigest(
		"vibedb/test/rf3/lineage-digest/1\x00", descriptor,
	)
	descriptor.ForwardingRuleDigest = testReplicatedShardIdentityDigest(
		"vibedb/test/rf3/forwarding-rule-digest/1\x00", descriptor,
	)
	if descriptor.RangeIdentity == (replication.Digest{}) ||
		descriptor.LineageDigest == (replication.Digest{}) ||
		descriptor.ForwardingRuleDigest == (replication.Digest{}) ||
		descriptor.RangeIdentity == descriptor.LineageDigest ||
		descriptor.RangeIdentity == descriptor.ForwardingRuleDigest ||
		descriptor.LineageDigest == descriptor.ForwardingRuleDigest {
		t.Fatal("test RF3 descriptor identity domains collided")
	}
}

func testReplicatedShardIdentityDigest(
	domain string,
	descriptor *gateway.ReplicatedShardDescriptor,
) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	_, _ = hash.Write(descriptor.Group.ClusterID[:])
	_, _ = hash.Write(descriptor.Group.ClusterIncarnation[:])
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], descriptor.Group.TopologyRecoveryEpoch)
	_, _ = hash.Write(number[:])
	_, _ = hash.Write(descriptor.Group.ShardIncarnation[:])
	_, _ = hash.Write(descriptor.Group.GroupID[:])
	writeTestReplicatedShardIdentityText(hash, string(descriptor.Distribution))
	writeTestReplicatedShardIdentityText(hash, string(descriptor.Shard))
	binary.BigEndian.PutUint64(number[:], uint64(descriptor.AllocationGeneration))
	_, _ = hash.Write(number[:])
	var digest replication.Digest
	_ = hash.Sum(digest[:0])
	return digest
}

func writeTestReplicatedShardIdentityText(
	hash interface{ Write([]byte) (int, error) },
	value string,
) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = hash.Write(size[:])
	_, _ = hash.Write([]byte(value))
}

func TestBindTestReplicatedShardIdentityBindsCompleteDescriptor(t *testing.T) {
	base := gateway.ReplicatedShardDescriptor{
		Distribution: "data", Shard: "all", AllocationGeneration: 1,
		Group: raftmember.GroupKey{
			ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
			TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
		},
	}
	bindTestReplicatedShardIdentity(t, &base)
	same := base
	same.RangeIdentity = replication.Digest{}
	same.LineageDigest = replication.Digest{}
	same.ForwardingRuleDigest = replication.Digest{}
	bindTestReplicatedShardIdentity(t, &same)
	if same.RangeIdentity != base.RangeIdentity || same.LineageDigest != base.LineageDigest ||
		same.ForwardingRuleDigest != base.ForwardingRuleDigest {
		t.Fatal("identical RF3 descriptor tuple produced different authorities")
	}
	mutations := []func(*gateway.ReplicatedShardDescriptor){
		func(value *gateway.ReplicatedShardDescriptor) { value.Group.ClusterID[0]++ },
		func(value *gateway.ReplicatedShardDescriptor) { value.Group.ClusterIncarnation[0]++ },
		func(value *gateway.ReplicatedShardDescriptor) { value.Group.TopologyRecoveryEpoch++ },
		func(value *gateway.ReplicatedShardDescriptor) { value.Group.ShardIncarnation[0]++ },
		func(value *gateway.ReplicatedShardDescriptor) { value.Group.GroupID[0]++ },
		func(value *gateway.ReplicatedShardDescriptor) { value.Distribution = "other" },
		func(value *gateway.ReplicatedShardDescriptor) { value.Shard = "other" },
		func(value *gateway.ReplicatedShardDescriptor) { value.AllocationGeneration++ },
	}
	for index, mutate := range mutations {
		candidate := base
		mutate(&candidate)
		bindTestReplicatedShardIdentity(t, &candidate)
		if candidate.RangeIdentity == base.RangeIdentity ||
			candidate.LineageDigest == base.LineageDigest ||
			candidate.ForwardingRuleDigest == base.ForwardingRuleDigest {
			t.Fatalf("RF3 descriptor tuple mutation %d was not bound by every authority", index)
		}
	}
}
