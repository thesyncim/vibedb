package shardservice

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var (
	errServiceDirectoryRefreshUnavailable = errors.New("shardservice: service-directory refresh unavailable")
	errServiceDirectoryRefreshState       = errors.New("shardservice: service-directory refresh state mismatch")
)

// FrontendDrainServiceCutReader is the authenticated physical source used to
// refresh a receiver whose retained service-directory gate is missing an
// exact resource. The reader must obtain the cut through the canonical
// ReadLatest wire route; it cannot be replaced by a caller supplied cut.
type FrontendDrainServiceCutReader interface {
	ReadLatestServiceCut(context.Context, frontenddrain.ServiceCutReadLatestRequest) (frontenddrain.ServiceCut, error)
}

// BindServiceDirectoryRefresh installs the one physical source used for
// service-directory miss recovery. It must be called before ServeAuthenticated
// or DispatchReplicated begins serving. The receiver identity is bound into
// each ReadLatest request and is checked again by the source transport.
func (server *ReplicatedServer) BindServiceDirectoryRefresh(
	reader FrontendDrainServiceCutReader,
	receiverNode rafttransport.NodeID,
	receiverIncarnation uint64,
	receiverServiceKey [32]byte,
) error {
	if server == nil || reader == nil || receiverNode == (rafttransport.NodeID{}) ||
		receiverIncarnation == 0 || receiverServiceKey == ([32]byte{}) ||
		server.state.Load() != replicatedServerReady {
		return ErrReplicatedWire
	}
	server.serviceDirectoryRefreshMu.Lock()
	defer server.serviceDirectoryRefreshMu.Unlock()
	if server.serviceDirectoryRefreshReader != nil {
		return ErrReplicatedWire
	}
	server.serviceDirectoryRefreshReader = reader
	server.serviceDirectoryRefreshNode = receiverNode
	server.serviceDirectoryRefreshIncarnation = receiverIncarnation
	server.serviceDirectoryRefreshKey = receiverServiceKey
	return nil
}

// authorizeReplicatedPeerWithDirectoryRefresh performs one complete
// authorization attempt, optionally followed by one coalesced canonical
// service-cut refresh, and then a complete second authorization attempt. The
// refresh occurs before owner admission, so a request is never replayed after
// execution has begun.
func (server *ReplicatedServer) authorizeReplicatedPeerWithDirectoryRefresh(
	ctx context.Context, peer serviceauthz.AuthenticatedPeer, request *ReplicatedRequest,
) bool {
	if server == nil || ctx == nil || request == nil {
		return false
	}
	directory := server.directory.Load()
	if directory == nil {
		// The caller handles a required-but-not-yet-installed directory as a
		// typed Unavailable response. Optional development/local callers retain
		// the pre-directory Policy path until a committed gate is required.
		return server.authorizeReplicatedPeerWithDirectory(nil, peer, request)
	}
	if server.authorizeReplicatedPeerWithDirectory(directory, peer, request) {
		return true
	}
	scope, scoped := FrontendContinuationScopeForReplicatedRequest(request)
	if scoped && request.Continuation != nil {
		var scopeOK bool
		scope, scopeOK = FrontendContinuationScopeForReplicatedRequestWithProtocol(request,
			request.Continuation.Scope.Protocol)
		if !scopeOK {
			return false
		}
	}
	if request.Continuation != nil && scope.Action == serviceauthz.FrontendActionForwardedData &&
		server.authorization.Check(request.Authority.Node, request.Authority.Generation, request.Capability) != serviceauthz.DecisionAllow {
		// A stale resource for an otherwise valid forwarded authority may trigger
		// one source read. An unknown or already revoked inner authority must be
		// denied before that read so refresh cannot become an oracle for callers.
		return false
	}
	if !directory.ServiceCutRefreshNeeded(peer, request.Authority, request.Continuation, scope) {
		// The gate's state is replaced in place when a cut is installed, so a
		// concurrent refresh can land between the failed authorization above
		// and this check, which then sees the resource and reports no refresh
		// needed. Authorize once more against the current state before denying;
		// this reads no source and so exposes nothing beyond a normal check.
		return server.authorizeReplicatedPeerWithDirectory(server.directory.Load(), peer, request)
	}
	if err := server.refreshServiceDirectoryCut(ctx); err != nil {
		return false
	}
	current := server.directory.Load()
	return current != nil && server.authorizeReplicatedPeerWithDirectory(current, peer, request)
}

// refreshServiceDirectoryCut executes one canonical source read for all
// concurrent service-cut misses. Waiters share the result and always retry
// authorization against the retained gate, while an unsuccessful source read
// leaves the request denied. A source response is installed only when its
// complete floor is at least the receiver's currently retained floor.
func (server *ReplicatedServer) refreshServiceDirectoryCut(ctx context.Context) error {
	if server == nil || ctx == nil {
		return errServiceDirectoryRefreshUnavailable
	}
	server.serviceDirectoryRefreshMu.Lock()
	if flight := server.serviceDirectoryRefreshFlight; flight != nil {
		server.serviceDirectoryRefreshMu.Unlock()
		select {
		case <-flight:
			return nil
		case <-ctx.Done():
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			return context.Canceled
		}
	}
	reader := server.serviceDirectoryRefreshReader
	receiverNode := server.serviceDirectoryRefreshNode
	receiverIncarnation := server.serviceDirectoryRefreshIncarnation
	receiverServiceKey := server.serviceDirectoryRefreshKey
	flight := make(chan struct{})
	server.serviceDirectoryRefreshFlight = flight
	server.serviceDirectoryRefreshMu.Unlock()

	defer func() {
		server.serviceDirectoryRefreshMu.Lock()
		if server.serviceDirectoryRefreshFlight == flight {
			server.serviceDirectoryRefreshFlight = nil
			close(flight)
		}
		server.serviceDirectoryRefreshMu.Unlock()
	}()
	if reader == nil || receiverNode == (rafttransport.NodeID{}) || receiverIncarnation == 0 ||
		receiverServiceKey == ([32]byte{}) {
		return errServiceDirectoryRefreshUnavailable
	}
	floor, ok := server.ServiceCutCoordinates()
	if !ok || floor == (frontenddrain.PreparedAckCutReadFloor{}) || !floor.Valid() {
		return errServiceDirectoryRefreshState
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errors.Join(errServiceDirectoryRefreshUnavailable, err)
	}
	query := frontenddrain.ServiceCutReadLatestRequest{
		Operation: frontenddrain.ReadLatest, Nonce: nonce,
		ReceiverNode: receiverNode, ReceiverIncarnation: receiverIncarnation,
		ReceiverServiceKeyDigest: receiverServiceKey, SourceFloor: floor,
	}
	cut, err := reader.ReadLatestServiceCut(ctx, query)
	if err != nil {
		return errors.Join(errServiceDirectoryRefreshUnavailable, err)
	}
	if !cut.Valid() || !cut.AtLeastFloor(floor) {
		return errServiceDirectoryRefreshState
	}
	applied, err := server.InstallFrontendDrainServiceCut(ctx, cut)
	if err != nil {
		return errors.Join(errServiceDirectoryRefreshState, err)
	}
	if applied != cut.ServiceDirectoryRevision {
		return fmt.Errorf("%w: applied revision=%d cut revision=%d", errServiceDirectoryRefreshState,
			applied, cut.ServiceDirectoryRevision)
	}
	got, ok := server.ServiceCutCoordinates()
	if !ok || got != cut.ReadFloor() {
		return errServiceDirectoryRefreshState
	}
	return nil
}
