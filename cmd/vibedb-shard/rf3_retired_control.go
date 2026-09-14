package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
)

// This process owns no execution runtime. The action service admits only an
// exact durable retirement tombstone and can settle an interrupted final ACK.
// A manifest or an absent group alone never grants that authority.
type rf3RetiredControlOwner struct{}

func (rf3RetiredControlOwner) ProposeOwnershipTransition(context.Context, raftservice.ServingFence, []byte) error {
	return multiraft.ErrGroupNotFound
}
func (rf3RetiredControlOwner) ObserveReplica(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaObservation, error) {
	return raftservice.ReplicaObservation{}, multiraft.ErrGroupNotFound
}
func (rf3RetiredControlOwner) RetireReplicaSource(context.Context, raftservice.ReplicaRetirementRequest) error {
	return multiraft.ErrGroupNotFound
}

func newRF3RetiredControlMux(journal replicaaction.Journal, registry *rafttransport.StaticRegistry,
	policy *serviceauthz.Policy, profile *rafttransport.PeerTLS, deadline rafttransport.DeadlineFunc,
) (*shardcontrol.Mux, error) {
	action, err := newRF3ReplicaActionControl(journal, rf3RetiredControlOwner{}, registry, policy, deadline, profile, nil)
	if err != nil {
		return nil, err
	}
	return shardcontrol.New(shardcontrol.Route{Discriminator: replicaaction.RequestDiscriminator(), Handler: action})
}

// Split receipts are retained independently of source retirement. Resolve only
// their certified identities before deciding whether any live adopted child
// still requires execution recovery.
func rf3RetiredControlEligible(manifest rf3Manifest, records []replicaaction.Record, inventory *rf3AdoptedGroupInventory, local rafttransport.NodeID) (bool, error) {
	allRetired, err := rf3AllManifestSourcesRetired(manifest, records)
	if err != nil || !allRetired {
		return false, err
	}
	if inventory == nil {
		return false, errRF3Serving
	}
	children, err := inventory.recoveryGroups(local)
	if err != nil {
		return false, err
	}
	for _, child := range children {
		binding := child.base.Binding
		if !rf3ReplicaSourceRetired(records, groupFromBinding(binding), binding.MemberID, binding.StoreID, binding.AllocationGeneration) {
			return false, nil
		}
	}
	return true, nil
}

// The caller has proved every retained manifest storage identity permanently
// retired and that no adopted group remains. This branch precedes opening the
// physical WAL or any SQL runtime and never advertises native serving readiness.
func serveRF3RetiredControl(ctx context.Context, manifest rf3Manifest, listen rf3ListenFunc,
	profile *rafttransport.PeerTLS, policy *serviceauthz.Policy, server *servicetls.Server,
	journal *replicaaction.FileJournal,
) error {
	if ctx == nil || listen == nil || profile == nil || policy == nil || server == nil || journal == nil {
		return errRF3Serving
	}
	if cause := context.Cause(ctx); cause != nil {
		return componentShutdownError(cause)
	}
	local := profile.LocalIdentity()
	registry, err := rafttransport.NewEmptyRegistry(local.Node, local.TrustDomain, rf3TransportRegistryLimits())
	if err != nil {
		return err
	}
	deadline := func() time.Time { return time.Now().Add(rf3NetworkTimeout) }
	mux, err := newRF3RetiredControlMux(journal, registry, policy, profile, deadline)
	if err != nil {
		return err
	}
	listener, err := listen("tcp", manifest.Listeners.Control)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "vibedb-shard RF3 retired-source control listening node=%x control=%s\n", local.Node, listener.Addr())
	err = server.Serve(ctx, listener, servicetls.Limits{MaxConnections: 32, MaxHandshakes: 8, HandshakeDeadline: deadline},
		func(ctx context.Context, connection rafttransport.PeerConnection) {
			if err := mux.Serve(ctx, connection); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "RF3 retired-source control request failed: %v\n", err)
			}
		})
	if ctx.Err() != nil {
		return componentShutdownError(err)
	}
	return errors.Join(errRF3Serving, err)
}
