package membershipgrant

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

func TestCanonicalGrantRoundTripAndCorruption(t *testing.T) {
	grant := Grant{
		Group: raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
			TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}},
		TransitionID: [16]byte{6}, MetadataEpoch: 7, CatalogGeneration: 8,
		InitialReplicaSetVersion: 9, InitialVoters: [3]uint64{1, 2, 3},
		InitialRosterDigest: [32]byte{10}, InitialDescriptorDigest: [32]byte{11},
		SourceMember: 1, TargetMember: 4, TargetNode: [16]byte{12},
	}
	raw, err := AppendCanonical([]byte{0xaa}, grant)
	if err != nil || len(raw) != 1+CanonicalGrantBytes || raw[0] != 0xaa {
		t.Fatalf("append bytes=%d err=%v", len(raw), err)
	}
	opened, err := OpenCanonical(raw[1:])
	if err != nil || opened != grant {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	for _, corrupt := range [][]byte{nil, raw[1 : len(raw)-1], append(bytes.Clone(raw[1:]), 0)} {
		if _, err := OpenCanonical(corrupt); !errors.Is(err, ErrCodec) {
			t.Fatalf("corrupt length %d err=%v", len(corrupt), err)
		}
	}
	corrupt := bytes.Clone(raw[1:])
	corrupt[0] ^= 0xff
	if _, err := OpenCanonical(corrupt); !errors.Is(err, ErrCodec) {
		t.Fatalf("corrupt magic err=%v", err)
	}
}
