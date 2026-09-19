package raftservice

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestInstallSchemaGenerationCanceledBeforeAdmission(t *testing.T) {
	owner := newDeliveryTestOwner()
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("schema installation canceled")
	cancel(cause)
	err := owner.InstallSchemaGeneration(ctx, peerServerTestGroup(),
		&sqldriver.Database{}, &sqldriver.ReplicatedApply{},
		sqldriver.ReplicatedShardStoreIdentity{}, sqldriver.ReplicatedApplyIdentity{})
	if !errors.Is(err, cause) {
		t.Fatalf("installation error = %v, want %v", err, cause)
	}
	if len(owner.ingress) != 0 || owner.ingressItems != 0 {
		t.Fatal("canceled installation published SQL handles")
	}
}

type schemaQuiesceDeliveryHost struct {
	ownerHost
	quiesced bool
}

func (host *schemaQuiesceDeliveryHost) ObserveSchemaTransition(raftmember.GroupKey, []byte) (uint64, bool, error) {
	if host.quiesced {
		return 0, false, multiraft.ErrGroupBusy
	}
	return 1, true, nil
}

func (host *schemaQuiesceDeliveryHost) QuiesceSQLGeneration(raftmember.GroupKey) error {
	if host.quiesced {
		return multiraft.ErrGroupBusy
	}
	host.quiesced = true
	return nil
}

func TestQuiesceSchemaGenerationCancellationWaitsForClosedSource(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "serving"
		if committed {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fence := testLinearizablePointServingFence(peerServerTestGroup())
				from := testLinearizablePointSnapshotFence(fence, 1).Binding
				from.Distribution, from.Shard = "docs", "0000-ffff"
				from.OwnedRange = distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
				command, err := replicatedstate.AppendSchemaTransition(nil, replicatedstate.SchemaTransition{
					From: from, ToSchemaGeneration: from.SchemaGeneration + 1,
					ExpectedReplicaSetVersion: fence.Command.ReplicaSetVersion,
					MembershipSequence:        1, MembershipSource: [32]byte{1}, MembershipTarget: [32]byte{2},
					FromManifest: fence.Command.RelationManifestDigest, FromApplyContract: [32]byte{5},
					ToManifest: [32]byte{6}, ToApplyContract: [32]byte{7},
					RequestDigest: [32]byte{8}, AuthorizationDigest: [32]byte{9}, CatalogCASDigest: [32]byte{10},
				})
				if err != nil {
					t.Fatal(err)
				}
				host := &schemaQuiesceDeliveryHost{}
				owner := newDeliveryTestOwner()
				owner.host = host
				owner.limits.MaxIngressBytes = int64(len(command))
				owner.members = map[raftmember.GroupKey]ownerMember{fence.Group: {
					identity: raftmember.RuntimeIdentity{Group: fence.Group,
						AllocationGeneration: fence.AllocationGeneration, MemberID: fence.MemberID,
						StoreID: fence.StoreID, NodeIncarnation: fence.NodeIncarnation,
						RelationManifestDigest: fence.Command.RelationManifestDigest},
					command: fence.Command, generation: &ownerGeneration{},
				}}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					if committed {
						done <- owner.QuiesceCommittedSchemaGeneration(ctx, fence.Group, command)
					} else {
						done <- owner.QuiesceSchemaGeneration(ctx, fence, command)
					}
				}()
				request := <-owner.ingress
				cancel()
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("quiesce returned before the source close settled: %v", err)
				default:
				}
				if err := owner.handle(request); err != nil {
					t.Fatal(err)
				}
				owner.release(request.bytes)
				if err := <-done; err != nil {
					t.Fatalf("lost completed source quiesce: %v", err)
				}
				if !host.quiesced || !owner.members[fence.Group].generation.quiescing.Load() {
					t.Fatal("successful quiesce left the source active")
				}
			})
		})
	}
}

type schemaGenerationDeliveryHost struct{ ownerHost }

func (*schemaGenerationDeliveryHost) Close() error { return nil }

func TestInstallSchemaGenerationCancellationWaitsForOwnershipOutcome(t *testing.T) {
	for _, outcome := range []string{"adopted", "refused", "shutdown"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				owner := newDeliveryTestOwner()
				owner.host = &schemaGenerationDeliveryHost{}
				owner.done = make(chan struct{})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				database := &sqldriver.Database{}
				apply := &sqldriver.ReplicatedApply{}
				done := make(chan error, 1)
				go func() {
					done <- owner.InstallSchemaGeneration(ctx, peerServerTestGroup(), database, apply,
						sqldriver.ReplicatedShardStoreIdentity{}, sqldriver.ReplicatedApplyIdentity{})
				}()
				request := <-owner.ingress
				if request.kind != requestInstallSchemaGeneration ||
					request.database != database || request.apply != apply {
					t.Fatal("installation did not publish the target handles")
				}
				cancel()
				// Let the caller observe cancellation before the owner dispatches.
				// It must retain the handles until adoption or definite rejection.
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("installation returned before ownership settled: %v", err)
				default:
				}

				var want error
				switch outcome {
				case "adopted":
					// Model the owner completion boundary independently of SQL
					// storage: the same pointers have become runtime-owned.
					request.reply <- ownerReply{}
					owner.release(request.bytes)
				case "refused":
					want = ErrServingFence
					if err := owner.handle(request); !errors.Is(err, want) {
						t.Fatalf("owner refusal = %v, want %v", err, want)
					}
					owner.release(request.bytes)
				case "shutdown":
					want = ErrOwnerClosed
					owner.ingress <- request
					if err := owner.stop(nil); !errors.Is(err, want) {
						t.Fatalf("owner shutdown = %v, want %v", err, want)
					}
				}
				if err := <-done; !errors.Is(err, want) {
					t.Fatalf("installation outcome = %v, want %v", err, want)
				}
				if owner.ingressItems != 0 {
					t.Fatalf("installation left %d ingress reservations", owner.ingressItems)
				}
			})
		})
	}
}
