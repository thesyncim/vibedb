package gatewayruntime

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

// publishCanonicalFrontendDrainCut distributes a complete catalog/service cut
// after a publication callback. A cut without a drain subject is a normal
// recovery/publication install: the exact InstallExact request carries zero
// DrainID and GrantDigest and never invents a child proof.
//
// The source and roster are re-read after every round. A concurrent catalog or
// receiver change is folded into the next bounded round, so callers only
// report publication complete after the same cut is installed everywhere that
// is currently serving physical routes or storage bindings.
func (runtime *Runtime) publishCanonicalFrontendDrainCut(
	ctx context.Context, nodeCut gateway.NodeDirectoryCut, sourceCut frontenddrain.PreparedAckCut,
	catalog *gateway.Snapshot,
) error {
	if runtime == nil || ctx == nil || !nodeCut.Valid() || !sourceCut.Valid() ||
		catalog == nil || catalog.Generation() != sourceCut.CatalogGeneration ||
		nodeCut.Revision != sourceCut.DirectoryRevision ||
		nodeCut.Digest != sourceCut.DirectoryDigest ||
		nodeCut.CatalogGeneration != sourceCut.CatalogGeneration {
		return fmt.Errorf("%w: invalid canonical publication input", gateway.ErrScalingRevision)
	}
	currentNodes, currentCut, currentCatalog := nodeCut, sourceCut, catalog
	const maxPublicationRounds = 3
	var lastPublishErr error
	for round := 0; round < maxPublicationRounds; round++ {
		publishErr := runtime.publishCanonicalFrontendDrainCutOnce(ctx, currentNodes, currentCut, currentCatalog)
		if publishErr == nil {
			return nil
		}
		lastPublishErr = publishErr
		moved := errors.Is(publishErr, frontenddrain.ErrPreparedAckCutMoved)
		revision := errors.Is(publishErr, gateway.ErrScalingRevision) && !errors.Is(publishErr, gateway.ErrScalingIdentity)
		if !moved && !revision {
			return publishErr
		}
		if round+1 == maxPublicationRounds {
			return publishErr
		}
		projection, readErr := runtime.readLiveControlDirectoryProjection(ctx)
		if readErr != nil {
			return readErr
		}
		if !projection.fullCut.Valid() {
			return fmt.Errorf("%w: reread canonical publication cut is invalid after round=%d initial-directory=%d initial-digest=%x initial-catalog=%d initial-full=%x",
				gateway.ErrScalingRevision, round, currentNodes.Revision, currentNodes.Digest,
				currentNodes.CatalogGeneration, currentCut.Digest())
		}
		if projection.fullCut.Digest() == currentCut.Digest() {
			// A moved response is retryable only after the authority has
			// advanced. Never spin the same exact request against an unchanged
			// floor after a lost/ambiguous receiver response.
			return publishErr
		}
		// Keep the local semantic receiver at the same complete cut that the
		// next physical round will install. This call is intentionally below
		// refreshLiveControlDirectory's gate; the caller already owns it.
		if applyErr := runtime.applyLiveControlDirectoryProjection(ctx, projection); applyErr != nil {
			return applyErr
		}
		currentNodes = gateway.NodeDirectoryCut{
			Revision: projection.cut.Revision, Digest: projection.fullCut.DirectoryDigest,
			CatalogGeneration: projection.cut.CatalogGeneration, Nodes: slices.Clone(projection.cut.Nodes),
		}
		currentCut = projection.fullCut
		currentCatalog = projection.catalog
	}
	if lastPublishErr != nil {
		return lastPublishErr
	}
	return fmt.Errorf("%w: canonical publication exhausted without a round", gateway.ErrScalingRevision)
}

func (runtime *Runtime) publishCanonicalFrontendDrainCutOnce(
	ctx context.Context, nodeCut gateway.NodeDirectoryCut, sourceCut frontenddrain.PreparedAckCut,
	catalog *gateway.Snapshot,
) error {
	if runtime == nil || ctx == nil || runtime.config.TLSProfile == nil ||
		runtime.config.Authorization == nil || !nodeCut.Valid() || !sourceCut.Valid() ||
		catalog == nil || catalog.Generation() != sourceCut.CatalogGeneration ||
		nodeCut.Revision != sourceCut.DirectoryRevision ||
		nodeCut.Digest != sourceCut.DirectoryDigest ||
		nodeCut.CatalogGeneration != sourceCut.CatalogGeneration {
		return fmt.Errorf("%w: invalid canonical publication input", gateway.ErrScalingRevision)
	}
	receivers, err := runtime.frontendDrainPreparedAckPublicationReceiversFromServiceCut(
		nodeCut, catalog, &sourceCut.ServiceDirectory,
	)
	if err != nil {
		return fmt.Errorf("derive canonical prepared-ack receiver roster: %w", err)
	}
	// A source with no serving physical receiver has nothing to install. This
	// is valid for a gateway-only control-plane cut and avoids requiring a
	// fabricated gateway publisher merely to acknowledge an empty roster.
	if len(receivers) != 0 {
		sourcePrincipal, sourceKey, ok := runtime.frontendDrainPreparedAckPublisher(sourceCut)
		if !ok {
			return gateway.ErrScalingIdentity
		}
		acked := 0
		var lastUnreachable error
		// A failed round is retried every tick until it completes. Receivers
		// that already installed this exact cut need no second exchange, so a
		// retry costs only the outstanding receivers rather than the roster.
		cutDigest := replication.Digest(sourceCut.Digest())
		if runtime.frontendDrainAckedCut != cutDigest || runtime.frontendDrainAckedReceivers == nil {
			runtime.frontendDrainAckedCut = cutDigest
			runtime.frontendDrainAckedReceivers = make(map[frontendDrainAckedReceiver]struct{}, len(receivers))
		}
		for _, receiver := range receivers {
			ackedKey := frontendDrainAckedReceiver{node: receiver.node.NodeID,
				incarnation: receiver.node.Incarnation, revision: receiver.node.Revision,
				serviceKey: receiver.node.ServiceKeyDigest}
			if _, done := runtime.frontendDrainAckedReceivers[ackedKey]; done {
				acked++
				continue
			}
			var nonce [16]byte
			if _, err := cryptorand.Read(nonce[:]); err != nil {
				return err
			}
			request := frontenddrain.PreparedAckRequest{
				Nonce: nonce, SourcePrincipal: sourcePrincipal, SourcePrincipalKeyDigest: sourceKey,
				ReceiverNode: receiver.node.NodeID, ReceiverIncarnation: receiver.node.Incarnation,
				ReceiverServiceKeyDigest: [32]byte(receiver.node.ServiceKeyDigest),
				ReceiverNodeRevision:     receiver.node.Revision, SourceCut: sourceCut,
			}
			if !request.Valid() {
				return gateway.ErrScalingState
			}
			if err := runtime.acknowledgeFrontendDrainPreparedAckPhysicalReceiver(ctx, receiver, request); err != nil {
				// A recovery/publication install carries no drain subject. A
				// serving replica that is already gone must not strand the
				// remaining live barrier or the periodic directory refresh.
				if request.DrainID == ([32]byte{}) && frontendDrainPreparedAckReceiverUnreachable(err) {
					if runtime.config.Logf != nil {
						runtime.config.Logf("gatewayruntime: skip unreachable prepared-ack receiver %s incarnation %d: %v",
							receiver.node.NodeID, receiver.node.Incarnation, err)
					}
					lastUnreachable = err
					continue
				}
				return fmt.Errorf("prepared-ack receiver %s incarnation %d: %w", receiver.node.NodeID,
					receiver.node.Incarnation, err)
			}
			runtime.frontendDrainAckedReceivers[ackedKey] = struct{}{}
			acked++
		}
		if acked == 0 {
			if lastUnreachable != nil {
				return fmt.Errorf("prepared-ack receivers unreachable: %w", lastUnreachable)
			}
			return gateway.ErrScalingState
		}
	}
	latest, err := runtime.readLiveControlDirectoryProjection(ctx)
	if err != nil || !latest.fullCut.Valid() {
		if err != nil {
			return fmt.Errorf("read canonical publication cut after receiver barrier: %w", err)
		}
		return fmt.Errorf("%w: canonical publication cut after receiver barrier is invalid", gateway.ErrScalingRevision)
	}
	if latest.cut.Revision != nodeCut.Revision || latest.fullCut.DirectoryDigest != nodeCut.Digest ||
		latest.cut.CatalogGeneration != nodeCut.CatalogGeneration ||
		!slices.Equal(latest.cut.Nodes, nodeCut.Nodes) ||
		latest.fullCut.Digest() != sourceCut.Digest() {
		return fmt.Errorf("%w: canonical source cut changed during receiver barrier initial directory=%d digest=%x catalog=%d full=%x latest directory=%d digest=%x catalog=%d full=%x",
			gateway.ErrScalingRevision, nodeCut.Revision, nodeCut.Digest, nodeCut.CatalogGeneration,
			sourceCut.Digest(), latest.cut.Revision, latest.fullCut.DirectoryDigest,
			latest.cut.CatalogGeneration, latest.fullCut.Digest())
	}
	return nil
}

// frontendDrainAckedReceiver identifies one receiver incarnation that has
// durably installed the current canonical cut. A restart, re-enrollment, or
// key rotation changes the key and forces a fresh exchange.
type frontendDrainAckedReceiver struct {
	node        rafttransport.NodeID
	incarnation uint64
	revision    uint64
	serviceKey  replication.Digest
}
