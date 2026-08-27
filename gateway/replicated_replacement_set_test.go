package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
)

func newReplacementSetFixture(t *testing.T) (*ReplicatedCatalogAuthority, *catalogAuthorityClient, *Snapshot, []ReplicaReplacementChange) {
	t.Helper()
	authority, client, _ := newCatalogAuthorityFixture(t)
	config, endpoints, first := testReplicatedCatalogInput(t)
	manifest := config.Manifests[0]
	var shards []distribution.Shard
	for i := 0; i < manifest.ShardCount(); i++ {
		shard, _ := manifest.ShardInfo(i)
		shards = append(shards, shard)
	}
	other, err := distribution.NewManifest("other_data", manifest.Version(), shards)
	if err != nil {
		t.Fatal(err)
	}
	config.Manifests = append(config.Manifests, other)
	config.Distributions = append(config.Distributions, distribution.DistributionSpec{Name: "other_data", Arity: 1, MapperVersion: 1})
	config.Placements = append(config.Placements, distribution.TablePlacement{Table: "logs", Distribution: "other_data", Columns: []string{"/tenant_id"}})
	second := first
	second.Distribution = "other_data"
	second.Group.GroupID[0] ^= 0x80
	second.Group.ShardIncarnation[0] ^= 0x80
	second.RequestLedgerRanges = nil
	second.RangeIdentity[0] ^= 0x80
	current, err := NewSnapshotWithReplicatedMetadata(config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{first, second})
	if err != nil {
		t.Fatal(err)
	}
	current, err = initialCatalogState(current)
	if err != nil {
		t.Fatal(err)
	}
	head, err := appendReplicatedCatalogDocument(nil, current, maxReplicatedCatalogBytes)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := appendReplicatedCatalogHeadWitness(nil, current.Generation(), head)
	if err != nil {
		t.Fatal(err)
	}
	client.rows[string(replicatedCatalogHeadKey)] = head
	client.rows[string(replicatedCatalogHeadWitnessKey)] = witness
	authority.holder = NewCatalogHolder(current)
	// This fixture exercises two independent receipt mutations in one batch.
	// Production continues to use its existing bounded session profile.
	authority.session.bundle.maxMutations = 16
	changes := make([]ReplicaReplacementChange, 0, 2)
	for _, descriptor := range current.replicatedDescriptors() {
		grant, nextManifest, target, command := testCertifiedReplicaReplacement(t, current, descriptor)
		for i, shard := range current.replicatedShards {
			if shard.group == grant.Group {
				grant.InitialRosterDigest = replicatedCatalogInitialRosterDigest(current, i)
				grant.InitialDescriptorDigest = replicatedCatalogInitialDescriptorDigest(current, i)
			}
		}
		if err := authority.PublishMembershipGrant(t.Context(), grant); err != nil {
			t.Fatal(err)
		}
		changes = append(changes, ReplicaReplacementChange{Grant: grant, Manifest: nextManifest, Target: target, Command: command})
	}
	return authority, client, current, changes
}

func TestReplicaReplacementSetPublishesBothCertifiedCutsAtomically(t *testing.T) {
	authority, client, current, changes := newReplacementSetFixture(t)
	observer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x80)
	grants := []membershipgrant.Grant{changes[0].Grant, changes[1].Grant}
	for _, postRemove := range []bool{false, true} {
		if postRemove {
			for i := range changes {
				changes[i].Command.ReplicaSetVersion++
			}
		}
		next, err := BuildReplicaReplacementSetTransition(current, changes, postRemove)
		if err != nil {
			t.Fatal(err)
		}
		client.unknownNext = true
		err = authority.PublishReplicaReplacementSet(t.Context(), current.Generation(), next, grants, postRemove)
		if !errors.Is(err, ErrReplicatedCatalogPending) {
			t.Fatalf("unknown post=%t: %v", postRemove, err)
		}
		retained := authority.session.PendingCommand()
		read, err := observer.Read(t.Context())
		if err != nil || read.Generation() != next.Generation() {
			t.Fatalf("observer post=%t: %v", postRemove, err)
		}
		client.holdUnknown = false
		if err := authority.RetryPending(t.Context()); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(retained, client.unknownCommand) {
			t.Fatal("set replay changed command bytes")
		}
		for _, grant := range grants {
			stored, found, err := authority.ReadMembershipGrant(t.Context(), grant.Group)
			if err != nil || !found || stored != grant {
				t.Fatalf("grant changed: %v", err)
			}
			key, _ := replicatedReplicaReplacementReceiptKeys(grant.Group)
			receipt, err := openReplicaReplacementReceipt(client.rows[string(key[:])])
			if err != nil || receipt.Grant != grant || postRemove && receipt.PostRemoveGeneration != next.Generation() {
				t.Fatalf("receipt: %+v %v", receipt, err)
			}
		}
		current = next
	}
	for _, grant := range grants {
		if err := authority.FinalizeReplicaReplacement(context.Background(), grant); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReplicaReplacementSetRejectsMissingOrMutatedSiblingProof(t *testing.T) {
	for _, corrupt := range []string{"grant", "receipt", "unrelated-fence", "duplicate"} {
		t.Run(corrupt, func(t *testing.T) {
			authority, client, current, changes := newReplacementSetFixture(t)
			next, err := BuildReplicaReplacementSetTransition(current, changes, false)
			if err != nil {
				t.Fatal(err)
			}
			grants := []membershipgrant.Grant{changes[0].Grant, changes[1].Grant}
			if corrupt == "grant" {
				key, _ := replicatedMembershipGrantKeys(grants[1].Group)
				delete(client.rows, string(key[:]))
			} else if corrupt == "duplicate" {
				grants[1] = grants[0]
			} else if corrupt == "unrelated-fence" {
				descriptors := next.replicatedDescriptors()
				descriptors[1].Command.ProtectionEpoch++
				next, err = NewSnapshotWithReplicatedMetadata(next.config, next.endpoints, next.Generation(), nil, nil, descriptors)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := bytes.Clone(client.rows[string(replicatedCatalogHeadKey)])
			err = authority.PublishReplicaReplacementSet(t.Context(), current.Generation(), next, grants, false)
			if corrupt != "receipt" {
				if err == nil || !bytes.Equal(before, client.rows[string(replicatedCatalogHeadKey)]) {
					t.Fatalf("partial/uncertified set published: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			key, _ := replicatedReplicaReplacementReceiptKeys(grants[1].Group)
			delete(client.rows, string(key[:]))
			observer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x80)
			if _, err := observer.Read(t.Context()); err == nil || observer.holder.Current().Generation() != current.Generation() {
				t.Fatalf("reader accepted incomplete set receipt: %v", err)
			}
		})
	}
}
