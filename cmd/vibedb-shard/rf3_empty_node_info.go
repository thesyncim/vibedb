package main

import (
	"context"
	"errors"
	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

func newRF3EmptyNodeInfo(store *raftstore.NodeStore, profile *rafttransport.PeerTLS, manifest rf3Manifest, policy *serviceauthz.Policy, budget *migrationbudget.Budget, serving *atomic.Int64, preparer *rf3NodeControlPreparer, deadline rafttransport.DeadlineFunc) (*nodecontrol.NodeInfoService, error) {
	if store == nil || profile == nil || policy == nil || budget == nil || serving == nil || preparer == nil {
		return nil, nodecontrol.ErrNodeInfoUnavailable
	}
	identity, err := nodecontrol.NodeInfoStoreIdentityFromNodeStore(store)
	if err != nil {
		return nil, err
	}
	var revision atomic.Uint64
	local := profile.LocalIdentity()
	return nodecontrol.NewNodeInfoService(nodecontrol.NodeInfoServiceOptions{
		TrustDomain: local.TrustDomain, LocalNode: local.Node, Incarnation: manifest.NodeIncarnation,
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 16,
		Authorize: func(peer rafttransport.PeerIdentity, _ nodecontrol.NodeInfoRequest) bool {
			return policy.Check(peer.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow || policy.Check(peer.Node, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow
		},
		Provider: nodecontrol.NodeInfoProviderFunc(func(ctx context.Context, request nodecontrol.NodeInfoRequest) (nodecontrol.NodeInfoObservation, error) {
			if err := context.Cause(ctx); err != nil {
				return nodecontrol.NodeInfoObservation{}, err
			}
			preparer.mu.Lock()
			defer preparer.mu.Unlock()
			reservations, openErr := os.Open(filepath.Join(preparer.NodeRoot, "enrollments"))
			if openErr == nil {
				entries, readErr := reservations.ReadDir(1)
				_ = reservations.Close()
				if len(entries) != 0 || !errors.Is(readErr, io.EOF) {
					return nodecontrol.NodeInfoObservation{}, nodecontrol.ErrNodeInfoUnavailable
				}
			} else if !errors.Is(openErr, os.ErrNotExist) {
				return nodecontrol.NodeInfoObservation{}, openErr
			}
			// This provider certifies the initial empty capacity form only. An adopted
			// serving group requires the live inventory provider, never a zero-use claim.
			if serving.Load() != 0 {
				return nodecontrol.NodeInfoObservation{}, nodecontrol.ErrNodeInfoUnavailable
			}
			capacity, err := store.CapacityReservationBytes()
			if err != nil {
				return nodecontrol.NodeInfoObservation{}, err
			}
			metrics := budget.Metrics()
			if metrics.Active != 0 || metrics.ActiveCapacity <= 0 {
				return nodecontrol.NodeInfoObservation{}, nodecontrol.ErrNodeInfoUnavailable
			}
			current := revision.Add(1)
			if current == 0 {
				return nodecontrol.NodeInfoObservation{}, nodecontrol.ErrNodeInfoUnavailable
			}
			vector := autosplit.CapacityVector{autosplit.ResourceLiveBytes: capacity}
			return (nodecontrol.NodeInfoStoreFacts{
				Identity: identity, SPKIPinDigest: replication.Digest(profile.LocalPeerKeyDigest()),
				Endpoints: nodecontrol.NodeInfoEndpoints{Peer: manifest.Listeners.Peer, Native: manifest.Listeners.Native, Snapshot: manifest.Listeners.Snapshot, Control: manifest.Listeners.Control},
				Readiness: nodecontrol.NodeInfoReadiness{NodeJournalReady: true, PhysicalStoreReady: true, BoundListenersReady: true}, InventoryRevision: current,
				ActualCapacity: vector, DeclaredCapacity: vector, ActualMigrationCapacity: capacity, DeclaredMigrationCapacity: capacity, DeclaredMaxReceives: uint32(metrics.ActiveCapacity),
			}).Observation(request)
		}),
	})
}
