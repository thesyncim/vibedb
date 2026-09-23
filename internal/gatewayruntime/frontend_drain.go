package gatewayruntime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
)

// FrontendDrainIdentity is the immutable participant identity carried by a
// frontend admission acknowledgement. Physical and gateway identities are
// deliberately separate: a process restart may retain the same physical node
// while publishing a new gateway session, and a lifecycle CAS may move without
// changing the catalog generation.
type FrontendDrainIdentity struct {
	NodeID                  rafttransport.NodeID
	Incarnation             uint64
	GatewayNodeID           rafttransport.NodeID
	GatewayIncarnation      uint64
	GatewayServiceKeyDigest replication.Digest
	SessionID               [16]byte
	SessionRevision         uint64
	NodeRevision            uint64
	CatalogGeneration       uint64
	DirectoryRevision       uint64
}

func (identity FrontendDrainIdentity) Valid() bool {
	return identity.NodeID != (rafttransport.NodeID{}) && identity.Incarnation != 0 &&
		identity.GatewayNodeID != (rafttransport.NodeID{}) && identity.GatewayIncarnation != 0 &&
		identity.GatewayServiceKeyDigest != (replication.Digest{}) &&
		identity.SessionID != ([16]byte{}) && identity.SessionRevision != 0 &&
		identity.NodeRevision != 0 && identity.CatalogGeneration != 0 &&
		identity.DirectoryRevision != 0
}

// FrontendDrainAck is the authoritative local frontend cut used by a
// decommission controller. Native connections include accepted TLS and
// plaintext streams; PostgreSQL connections include sockets in startup,
// authentication, and active SQL sessions. Active session counts are a subset
// of their corresponding connection counts. No timeout or forced close is part
// of this acknowledgement.
type FrontendDrainAck struct {
	Identity FrontendDrainIdentity
	Revision uint64

	AdmissionDrained        bool
	NativeAdmissionDrained  bool
	PGAdmissionDrained      bool
	ActiveNativeConnections uint64
	ActiveNativeSessions    uint64
	ActivePGConnections     uint64
	ActivePGSessions        uint64

	SafeToStop bool
}

func (ack FrontendDrainAck) Valid() bool {
	return ack.Identity.Valid() && ack.Revision != 0 && ack.AdmissionDrained &&
		ack.NativeAdmissionDrained && ack.PGAdmissionDrained && ack.SafeToStop ==
		(ack.ActiveNativeConnections == 0 && ack.ActiveNativeSessions == 0 &&
			ack.ActivePGConnections == 0 && ack.ActivePGSessions == 0)
}

// errFrontendAdmissionDrained is carried through net.Listener.Accept and the
// TLS accept wrapper. It means only the public listener was intentionally
// closed; the owning runtime context remains live and existing connections
// must continue serving.
var errFrontendAdmissionDrained = errors.New("gatewayruntime: frontend admission drained")

const (
	// These are deliberately below serviceauthz's wire-format absolutes. A
	// frontend drain stores both the immutable token proof and its scope set in
	// one 4 MiB replicated catalog document; admission must therefore stop
	// before a valid but unpersistable proof can exist. Rejection at admission
	// is explicit -- a drain never silently drops an accepted token.
	maxFrontendContinuationTokens = 8192
	maxFrontendContinuationScopes = 4096
)

type frontendAdmission struct {
	mu sync.Mutex

	identity FrontendDrainIdentity
	revision uint64
	draining bool

	nativeAdmissionDrained bool
	pgAdmissionDrained     bool
	pgRequired             bool
	nativeActive           uint64
	pg                     *pgwire.Server
	tokens                 map[serviceauthz.FrontendConnToken]frontendTokenState
	grants                 [3]frontendGrantState
}

type frontendTokenState struct {
	scope    serviceauthz.FrontendContinuationScope
	eligible bool
}

type frontendGrantState struct {
	digest [32]byte
	ready  bool
}

func newFrontendAdmission(identity FrontendDrainIdentity, initiallyDraining, pgRequired bool) *frontendAdmission {
	frontend := &frontendAdmission{
		identity: identity, draining: initiallyDraining, pgRequired: pgRequired,
		tokens: make(map[serviceauthz.FrontendConnToken]frontendTokenState),
	}
	if initiallyDraining {
		frontend.revision = 1
	}
	if !pgRequired {
		// No PostgreSQL listener is configured, so that side of the admission
		// fence is vacuously closed even before a frontend drain begins.
		frontend.pgAdmissionDrained = true
	}
	frontend.nativeAdmissionDrained = initiallyDraining
	return frontend
}

// BeginFrontendDrain closes only public native and PostgreSQL admission. It
// leaves control listeners, controller contexts, and every already-admitted
// session untouched. The operation is idempotent and safe before Serve starts.
func (runtime *Runtime) BeginFrontendDrain() FrontendDrainAck {
	if runtime == nil {
		return FrontendDrainAck{}
	}
	return runtime.frontend.begin(runtime.listener)
}

// FrontendDrainStatus returns a consistent acknowledgement cut without
// changing lifecycle state. It is intentionally usable while the runtime is
// serving and while an idle PostgreSQL session is held open by a retiring
// client.
func (runtime *Runtime) FrontendDrainStatus() FrontendDrainAck {
	if runtime == nil {
		return FrontendDrainAck{}
	}
	return runtime.frontend.status()
}

// SetFrontendDrainIdentity publishes the exact catalog identity used by later
// safe-to-stop checks. It does not open or close listeners and is intended for
// the catalog directory reconciler after a fresh revision cut.
func (runtime *Runtime) SetFrontendDrainIdentity(identity FrontendDrainIdentity) {
	if runtime == nil || runtime.frontend == nil {
		return
	}
	runtime.frontend.mu.Lock()
	runtime.frontend.identity = identity
	runtime.frontend.mu.Unlock()
}

// syncFrontendDrainFromDirectory binds this frontend's gateway principal to
// the physical node record that owns it. Gateway and storage identities are
// deliberately distinct, so matching the TLS principal against NodeRecord's
// NodeID would leave the participant acknowledgement unauthenticated. A
// lifecycle transition to Draining is also the durable admission fence: close
// public admission while retaining already accepted sessions for the live
// reference scan.
func (runtime *Runtime) syncFrontendDrainFromDirectory(nodes []gateway.NodeRecord, directoryRevision uint64) bool {
	return runtime.syncFrontendDrainFromDirectoryWithAdmission(nodes, directoryRevision, true)
}

func (runtime *Runtime) syncFrontendDrainFromDirectoryWithAdmission(
	nodes []gateway.NodeRecord, directoryRevision uint64, beginAdmission bool,
) bool {
	if runtime == nil || runtime.frontend == nil || directoryRevision == 0 {
		return false
	}
	var gatewayNode rafttransport.NodeID
	var gatewayServiceKey replication.Digest
	haveGatewayServiceKey := false
	if runtime.config.TLSProfile != nil {
		gatewayNode = runtime.config.TLSProfile.LocalIdentity().Node
		gatewayServiceKey = replication.Digest(runtime.config.TLSProfile.LocalServiceKeyDigest())
		haveGatewayServiceKey = gatewayServiceKey != (replication.Digest{})
	}
	if gatewayNode == (rafttransport.NodeID{}) {
		gatewayNode = runtime.config.InternalAuthority.Node
	}
	if gatewayNode == (rafttransport.NodeID{}) {
		return false
	}
	for _, record := range nodes {
		if !record.Valid() || record.Gateway.NodeID != gatewayNode ||
			haveGatewayServiceKey && record.Gateway.ServiceKeyDigest != gatewayServiceKey {
			continue
		}
		identity := FrontendDrainIdentity{
			NodeID: record.NodeID, Incarnation: record.Incarnation,
			GatewayNodeID: record.Gateway.NodeID, GatewayIncarnation: record.Gateway.Incarnation,
			GatewayServiceKeyDigest: record.Gateway.ServiceKeyDigest,
			SessionID:               record.Gateway.SessionID, SessionRevision: record.Gateway.SessionRevision,
			NodeRevision: record.Revision, CatalogGeneration: record.CatalogGeneration,
			DirectoryRevision: directoryRevision,
		}
		runtime.frontend.mu.Lock()
		wasDraining := runtime.frontend.draining
		priorIdentity := runtime.frontend.identity
		if priorIdentity.GatewayNodeID != (rafttransport.NodeID{}) &&
			(priorIdentity.NodeID != identity.NodeID || priorIdentity.Incarnation != identity.Incarnation ||
				priorIdentity.GatewayNodeID != identity.GatewayNodeID ||
				priorIdentity.GatewayIncarnation != identity.GatewayIncarnation ||
				priorIdentity.GatewayServiceKeyDigest != identity.GatewayServiceKeyDigest) {
			runtime.frontend.mu.Unlock()
			continue
		}
		runtime.frontend.identity = identity
		if record.Lifecycle >= gateway.NodeDraining && !wasDraining {
			runtime.frontend.draining = true
			if runtime.frontend.revision == 0 {
				runtime.frontend.revision = 1
			}
			runtime.frontend.nativeAdmissionDrained = true
			if !runtime.frontend.pgRequired {
				runtime.frontend.pgAdmissionDrained = true
			}
		}
		runtime.frontend.mu.Unlock()
		if record.Lifecycle >= gateway.NodeDraining && !wasDraining && beginAdmission {
			runtime.BeginFrontendDrain()
		}
		return true
	}
	return false
}

// InstallFrontendContinuationGrant publishes the committed grant digest for
// sockets that were already accepted when the frontend admission fence began.
// It never creates a token and never makes a newly accepted socket eligible;
// the catalog/controller owner must call this only after durable publication
// and receiver acknowledgements have completed.
func (runtime *Runtime) InstallFrontendContinuationGrant(
	digest [32]byte, scope serviceauthz.FrontendContinuationScope,
) bool {
	if runtime == nil || runtime.frontend == nil || digest == ([32]byte{}) || !scope.Valid() {
		return false
	}
	if runtime.serviceDirectory == nil {
		return false
	}
	cut, ok := runtime.serviceDirectory.Cut()
	if !ok {
		return false
	}
	committed := false
	for _, grant := range cut.ContinuationGrants {
		if grant.GrantDigest != digest || (grant.State != serviceauthz.ContinuationGrantPrepared &&
			grant.State != serviceauthz.ContinuationGrantEnforcing) {
			continue
		}
		for _, protocol := range grant.AcceptedConnectionProtocols {
			if protocol == scope {
				committed = true
				break
			}
		}
		break
	}
	if !committed {
		return false
	}
	return runtime.frontend.installGrant(digest, scope)
}

// installPublishedFrontendContinuation is the receiver acknowledgement
// boundary for a local frontend. The catalog grant may be durable before a
// receiver has applied its complete service cut; credentials become visible
// to accepted sockets only after ApplyCommittedCut succeeds.
func (runtime *Runtime) installPublishedFrontendContinuation(
	cut serviceauthz.ServiceDirectoryCut,
) {
	if runtime == nil || runtime.frontend == nil || !cut.Valid() {
		return
	}
	runtime.frontend.mu.Lock()
	identity := runtime.frontend.identity
	runtime.frontend.mu.Unlock()
	for _, grant := range cut.ContinuationGrants {
		if grant.State != serviceauthz.ContinuationGrantEnforcing ||
			grant.PhysicalNode != identity.NodeID || grant.PhysicalIncarnation != identity.Incarnation ||
			grant.GatewayServiceID != identity.GatewayNodeID ||
			grant.GatewaySessionID != identity.SessionID ||
			grant.GatewaySessionRevision != identity.SessionRevision {
			continue
		}
		seen := [3]bool{}
		for _, protocol := range grant.AcceptedConnectionProtocols {
			if !protocol.Valid() || seen[protocol] {
				continue
			}
			seen[protocol] = true
			runtime.InstallFrontendContinuationGrant(grant.GrantDigest, protocol)
		}
	}
}

// ScanGatewayParticipant implements the catalog's optional live participant
// scanner. Retirement uses this cold path to bind the drain cut to the exact
// NodeRecord identity; it never infers liveness from a role bit or disk
// capacity. A local record is scanned directly. A record owned by another
// gateway is scanned over the authenticated gateway-control stream so the
// authority never compares a remote participant with the caller's TLS key.
func (runtime *Runtime) ScanGatewayParticipant(
	ctx context.Context, record gateway.NodeRecord,
) (gateway.GatewayParticipantEvidence, error) {
	if runtime == nil || ctx == nil || !record.Valid() {
		return gateway.GatewayParticipantEvidence{}, gateway.ErrInvalidScalingMetadata
	}
	select {
	case <-ctx.Done():
		return gateway.GatewayParticipantEvidence{}, ctx.Err()
	default:
	}
	profile := runtime.config.TLSProfile
	if localGatewayParticipantMatches(profile, record) {
		return runtime.scanLocalGatewayParticipant(ctx, record)
	}
	return runtime.scanRemoteGatewayParticipant(ctx, record)
}

func localGatewayParticipantMatches(profile *rafttransport.PeerTLS, record gateway.NodeRecord) bool {
	if profile == nil {
		return false
	}
	authenticatedGatewayNode := profile.LocalIdentity().Node
	authenticatedGatewayKey := replication.Digest(profile.LocalServiceKeyDigest())
	return authenticatedGatewayNode == record.Gateway.NodeID &&
		authenticatedGatewayKey != (replication.Digest{}) &&
		authenticatedGatewayKey == record.Gateway.ServiceKeyDigest
}

func (runtime *Runtime) scanLocalGatewayParticipant(
	ctx context.Context, record gateway.NodeRecord,
) (gateway.GatewayParticipantEvidence, error) {
	if runtime == nil || ctx == nil || !record.Valid() {
		return gateway.GatewayParticipantEvidence{}, gateway.ErrInvalidScalingMetadata
	}
	profile := runtime.config.TLSProfile
	if profile == nil {
		return gateway.GatewayParticipantEvidence{}, fmt.Errorf(
			"%w: no authenticated gateway TLS profile", gateway.ErrScalingRevision,
		)
	}
	authenticatedGatewayNode := profile.LocalIdentity().Node
	authenticatedGatewayKey := replication.Digest(profile.LocalServiceKeyDigest())
	if authenticatedGatewayNode != record.Gateway.NodeID ||
		authenticatedGatewayKey == (replication.Digest{}) ||
		authenticatedGatewayKey != record.Gateway.ServiceKeyDigest {
		return gateway.GatewayParticipantEvidence{}, fmt.Errorf(
			"%w: gateway TLS identity node=%s key=%x; directory gateway node=%s key=%x",
			gateway.ErrScalingRevision, nodeIDHex(authenticatedGatewayNode), authenticatedGatewayKey,
			nodeIDHex(record.Gateway.NodeID), record.Gateway.ServiceKeyDigest,
		)
	}
	ack := runtime.FrontendDrainStatus()
	identity := ack.Identity
	if identity.NodeID != record.NodeID || identity.Incarnation != record.Incarnation ||
		identity.GatewayNodeID != record.Gateway.NodeID ||
		identity.GatewayIncarnation != record.Gateway.Incarnation ||
		identity.GatewayServiceKeyDigest != record.Gateway.ServiceKeyDigest ||
		identity.SessionID != record.Gateway.SessionID ||
		identity.SessionRevision != record.Gateway.SessionRevision ||
		identity.NodeRevision != record.Revision ||
		identity.CatalogGeneration != record.CatalogGeneration {
		return gateway.GatewayParticipantEvidence{}, fmt.Errorf(
			"%w: frontend identity node=%s/%d gateway=%s/%d key=%x session=%x/%d revision=%d generation=%d; directory node=%s/%d gateway=%s/%d key=%x session=%x/%d revision=%d generation=%d",
			gateway.ErrScalingRevision,
			nodeIDHex(identity.NodeID), identity.Incarnation, nodeIDHex(identity.GatewayNodeID), identity.GatewayIncarnation,
			identity.GatewayServiceKeyDigest, identity.SessionID, identity.SessionRevision,
			identity.NodeRevision, identity.CatalogGeneration,
			nodeIDHex(record.NodeID), record.Incarnation, nodeIDHex(record.Gateway.NodeID), record.Gateway.Incarnation,
			record.Gateway.ServiceKeyDigest, record.Gateway.SessionID, record.Gateway.SessionRevision,
			record.Revision, record.CatalogGeneration,
		)
	}
	if identity.DirectoryRevision == 0 {
		return gateway.GatewayParticipantEvidence{}, fmt.Errorf(
			"%w: frontend identity has no directory revision (node=%s/%d)",
			gateway.ErrScalingRevision, nodeIDHex(identity.NodeID), identity.Incarnation,
		)
	}
	active := !ack.AdmissionDrained || ack.ActiveNativeConnections != 0 ||
		ack.ActiveNativeSessions != 0 || ack.ActivePGConnections != 0 || ack.ActivePGSessions != 0
	return gateway.GatewayParticipantEvidence{
		NodeID: record.NodeID, Incarnation: record.Incarnation,
		ServiceKeyDigest: record.ServiceKeyDigest,
		NodeRevision:     record.Revision, CatalogGeneration: record.CatalogGeneration,
		GatewayNodeID: record.Gateway.NodeID, GatewayIncarnation: record.Gateway.Incarnation,
		GatewayServiceKeyDigest: record.Gateway.ServiceKeyDigest,
		ServiceID:               record.Gateway.ServiceID,
		SessionID:               identity.SessionID, SessionRevision: identity.SessionRevision,
		ParticipantDigest: record.Gateway.ParticipantDigest,
		DirectoryRevision: identity.DirectoryRevision, Active: active,
		Digest: frontendDrainDigest(ack),
	}, nil
}

func frontendDrainDigest(ack FrontendDrainAck) (digest replication.Digest) {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/gateway-frontend-drain/v1\x00"))
	_, _ = hash.Write(ack.Identity.NodeID[:])
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], ack.Identity.Incarnation)
	_, _ = hash.Write(scalar[:])
	_, _ = hash.Write(ack.Identity.GatewayNodeID[:])
	binary.LittleEndian.PutUint64(scalar[:], ack.Identity.GatewayIncarnation)
	_, _ = hash.Write(scalar[:])
	_, _ = hash.Write(ack.Identity.GatewayServiceKeyDigest[:])
	_, _ = hash.Write(ack.Identity.SessionID[:])
	binary.LittleEndian.PutUint64(scalar[:], ack.Identity.SessionRevision)
	_, _ = hash.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], ack.Identity.NodeRevision)
	_, _ = hash.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], ack.Identity.CatalogGeneration)
	_, _ = hash.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], ack.Identity.DirectoryRevision)
	_, _ = hash.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], ack.Revision)
	_, _ = hash.Write(scalar[:])
	for _, count := range []uint64{
		ack.ActiveNativeConnections, ack.ActiveNativeSessions,
		ack.ActivePGConnections, ack.ActivePGSessions,
	} {
		binary.LittleEndian.PutUint64(scalar[:], count)
		_, _ = hash.Write(scalar[:])
	}
	if ack.AdmissionDrained {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	copy(digest[:], hash.Sum(nil))
	return digest
}

func (frontend *frontendAdmission) begin(native net.Listener) FrontendDrainAck {
	if frontend == nil {
		return FrontendDrainAck{}
	}
	frontend.mu.Lock()
	if !frontend.draining {
		frontend.draining = true
		frontend.revision++
		if frontend.revision == 0 {
			frontend.revision = 1
		}
	}
	for token, state := range frontend.tokens {
		state.eligible = true
		frontend.tokens[token] = state
	}
	pg := frontend.pg
	frontend.mu.Unlock()

	// Set the drain bit before closing the listener. The accept wrapper uses
	// that same bit to decide the race between a queued connection and the
	// lifecycle cut, so a connection is either counted as an existing stream or
	// rejected as a new admission, never silently omitted from the proof.
	if native != nil {
		_ = native.Close()
	}
	frontend.mu.Lock()
	frontend.nativeAdmissionDrained = true
	frontend.mu.Unlock()
	if pg != nil {
		pg.BeginDrain()
		frontend.mu.Lock()
		frontend.pgAdmissionDrained = true
		frontend.mu.Unlock()
	}
	return frontend.status()
}

func (frontend *frontendAdmission) bindPG(server *pgwire.Server) {
	if frontend == nil || server == nil {
		return
	}
	frontend.mu.Lock()
	frontend.pg = server
	draining := frontend.draining
	frontend.mu.Unlock()
	if draining {
		server.BeginDrain()
		frontend.mu.Lock()
		frontend.pgAdmissionDrained = true
		frontend.mu.Unlock()
	}
}

func (frontend *frontendAdmission) installGrant(
	digest [32]byte, scope serviceauthz.FrontendContinuationScope,
) bool {
	if frontend == nil {
		return false
	}
	frontend.mu.Lock()
	defer frontend.mu.Unlock()
	if !frontend.draining || digest == ([32]byte{}) || !scope.Valid() {
		return false
	}
	grant := &frontend.grants[scope]
	if grant.ready && grant.digest != digest {
		return false
	}
	grant.digest, grant.ready = digest, true
	return true
}

// FrontendContinuationCredential resolves only a token that was open at the
// admission cut. This is the provider used by operation contexts on accepted
// native and PostgreSQL sockets; it returns false until the committed grant is
// published, preserving the Active legacy envelope.
func (frontend *frontendAdmission) FrontendContinuationCredential(
	token serviceauthz.FrontendConnToken, scope serviceauthz.FrontendContinuationScope,
) (serviceauthz.FrontendContinuationCredential, bool) {
	if frontend == nil || !scope.Valid() {
		return serviceauthz.FrontendContinuationCredential{}, false
	}
	frontend.mu.Lock()
	state, found := frontend.tokens[token]
	grant := frontend.grants[scope]
	ready := grant.ready && state.eligible && state.scope == scope
	digest := grant.digest
	frontend.mu.Unlock()
	if !found || !ready {
		return serviceauthz.FrontendContinuationCredential{}, false
	}
	credential := serviceauthz.FrontendContinuationCredential{GrantDigest: digest, ConnToken: token, Protocol: scope}
	return credential, credential.Valid()
}

func (frontend *frontendAdmission) admitNative() (serviceauthz.FrontendConnToken, bool) {
	if frontend == nil {
		return serviceauthz.FrontendConnToken{}, false
	}
	frontend.mu.Lock()
	defer frontend.mu.Unlock()
	if frontend.draining {
		return serviceauthz.FrontendConnToken{}, false
	}
	if len(frontend.tokens) >= maxFrontendContinuationTokens {
		return serviceauthz.FrontendConnToken{}, false
	}
	token, err := serviceauthz.MintFrontendConnToken()
	if err != nil {
		return serviceauthz.FrontendConnToken{}, false
	}
	frontend.tokens[token] = frontendTokenState{scope: serviceauthz.FrontendScopeNative}
	frontend.nativeActive++
	return token, true
}

func (frontend *frontendAdmission) admitPG() (serviceauthz.FrontendConnToken, bool) {
	if frontend == nil {
		return serviceauthz.FrontendConnToken{}, false
	}
	frontend.mu.Lock()
	defer frontend.mu.Unlock()
	if frontend.draining {
		return serviceauthz.FrontendConnToken{}, false
	}
	if len(frontend.tokens) >= maxFrontendContinuationTokens {
		return serviceauthz.FrontendConnToken{}, false
	}
	token, err := serviceauthz.MintFrontendConnToken()
	if err != nil {
		return serviceauthz.FrontendConnToken{}, false
	}
	frontend.tokens[token] = frontendTokenState{scope: serviceauthz.FrontendScopePostgreSQL}
	return token, true
}

func (frontend *frontendAdmission) releaseNative(token serviceauthz.FrontendConnToken) {
	if frontend == nil {
		return
	}
	frontend.mu.Lock()
	delete(frontend.tokens, token)
	if frontend.nativeActive > 0 {
		frontend.nativeActive--
	}
	frontend.mu.Unlock()
}

func (frontend *frontendAdmission) releasePG(token serviceauthz.FrontendConnToken) {
	if frontend == nil {
		return
	}
	frontend.mu.Lock()
	delete(frontend.tokens, token)
	frontend.mu.Unlock()
}

func (frontend *frontendAdmission) isDraining() bool {
	if frontend == nil {
		return false
	}
	frontend.mu.Lock()
	draining := frontend.draining
	frontend.mu.Unlock()
	return draining
}

func (frontend *frontendAdmission) status() FrontendDrainAck {
	if frontend == nil {
		return FrontendDrainAck{}
	}
	frontend.mu.Lock()
	identity, revision := frontend.identity, frontend.revision
	draining := frontend.draining
	nativeDrained, pgDrained := frontend.nativeAdmissionDrained, frontend.pgAdmissionDrained
	nativeActive, pg := frontend.nativeActive, frontend.pg
	frontend.mu.Unlock()

	var pgState pgwire.AdmissionDrainState
	if pg != nil {
		pgState = pg.AdmissionState()
	}
	ack := FrontendDrainAck{
		Identity: identity, Revision: revision,
		AdmissionDrained:        draining && nativeDrained && pgDrained,
		NativeAdmissionDrained:  nativeDrained,
		PGAdmissionDrained:      pgDrained,
		ActiveNativeConnections: nativeActive,
		ActiveNativeSessions:    nativeActive,
		ActivePGConnections:     uint64(pgState.ActiveConnections),
		ActivePGSessions:        uint64(pgState.ActiveSessions),
	}
	ack.SafeToStop = ack.Identity.Valid() && ack.AdmissionDrained &&
		ack.ActiveNativeConnections == 0 && ack.ActiveNativeSessions == 0 &&
		ack.ActivePGConnections == 0 && ack.ActivePGSessions == 0
	return ack
}

// restoreFrontendDrainFromDirectory makes NodeDraining durable across a
// process restart. The directory remains the authority: an unavailable or
// incomplete cut never turns admissions back on, and the exact session and
// revision identity is retained for the controller to fence.
func (runtime *Runtime) restoreFrontendDrainFromDirectory(ctx context.Context) error {
	if runtime == nil || runtime.frontend == nil || ctx == nil ||
		(runtime.controlDirectory == nil && runtime.config.ControlDirectory == nil && runtime.authority == nil) {
		return nil
	}
	var nodes []gateway.NodeRecord
	var directoryRevision uint64
	if runtime.controlDirectory != nil {
		nodes = runtime.controlDirectory.Nodes()
		directoryRevision = runtime.controlDirectory.Revision()
	} else {
		reader := runtime.config.ControlDirectory
		if reader == nil {
			reader = runtime.authority
		}
		if cutReader, ok := reader.(gateway.NodeDirectoryCutReader); ok {
			cut, err := cutReader.ReadNodeDirectoryCut(ctx)
			if errors.Is(err, gateway.ErrScalingNodeMissing) {
				return nil
			}
			if err != nil {
				return err
			}
			nodes, directoryRevision = cut.CurrentNodes(), cut.Revision
		} else {
			var err error
			nodes, err = reader.ListNodes(ctx)
			if errors.Is(err, gateway.ErrScalingNodeMissing) {
				return nil
			}
			if err != nil {
				return err
			}
			directoryRevision = runtime.config.FrontendDrainIdentity.DirectoryRevision
		}
	}
	if !runtime.syncFrontendDrainFromDirectoryWithAdmission(nodes, directoryRevision, false) {
		return nil
	}

	// The node lifecycle tells us whether admission must be closed; the child
	// row tells us which immutable continuation proof may be installed after a
	// restart. Never revive a token from a different physical or gateway
	// session, even when the process-local frontend starts with an empty token
	// map.
	reader := runtime.config.ControlDirectory
	if reader == nil && runtime.authority != nil {
		reader = runtime.authority
	}
	if reader == nil {
		return nil
	}
	drainReader, ok := any(reader).(gateway.FrontendDrainRecordReader)
	if !ok && runtime.authority != nil {
		drainReader, ok = any(runtime.authority).(gateway.FrontendDrainRecordReader)
	}
	if !ok {
		return nil
	}
	_, records, err := drainReader.ReadFrontendDrainRecordCut(ctx)
	if errors.Is(err, gateway.ErrScalingNodeMissing) {
		return nil
	}
	if err != nil {
		return err
	}
	var currentNode gateway.NodeRecord
	for _, candidate := range nodes {
		if candidate.Valid() && runtime.frontend.identityMatchesNode(candidate) {
			currentNode = candidate
			break
		}
	}
	if currentNode.NodeID == (rafttransport.NodeID{}) {
		return nil
	}
	var record *gateway.FrontendDrainRecord
	for index := range records {
		candidate := records[index]
		if candidate.PhysicalNode != currentNode.NodeID || candidate.PhysicalIncarnation != currentNode.Incarnation ||
			candidate.GatewayServiceID != currentNode.Gateway.NodeID || candidate.GatewayIncarnation != currentNode.Gateway.Incarnation ||
			candidate.GatewayIdentityServiceID != currentNode.Gateway.ServiceID ||
			candidate.GatewaySessionID != currentNode.Gateway.SessionID ||
			candidate.GatewaySessionRevision != currentNode.Gateway.SessionRevision {
			continue
		}
		if record != nil {
			return gateway.ErrScalingIdentity
		}
		copyOfRecord := candidate
		record = &copyOfRecord
		break
	}
	if record == nil {
		return nil
	}
	if !record.Valid() || record.Lifecycle != gateway.FrontendDrainRetired && record.NodeRevision != currentNode.Revision ||
		record.Lifecycle == gateway.FrontendDrainRetired &&
			(record.NodeRevision == ^uint64(0) || record.NodeRevision+1 != currentNode.Revision) {
		return gateway.ErrScalingIdentity
	}
	if currentNode.Lifecycle == gateway.NodeActive && record.Lifecycle != gateway.FrontendDrainPrepared {
		return gateway.ErrScalingState
	}
	if currentNode.Lifecycle == gateway.NodeDraining && record.Lifecycle != gateway.FrontendDrainEnforcing {
		return gateway.ErrScalingState
	}
	if currentNode.Lifecycle == gateway.NodeDecommissioned && record.Lifecycle != gateway.FrontendDrainRetired {
		return gateway.ErrScalingState
	}
	if currentNode.Lifecycle >= gateway.NodeDraining {
		runtime.BeginFrontendDrain()
	} else if record.Lifecycle == gateway.FrontendDrainPrepared {
		// The process may have crashed after persisting Prepared and before the
		// node CAS. Keep admission closed until the retry can complete that CAS.
		runtime.BeginFrontendDrain()
	}
	if record.ContinuationGrant != nil && record.Lifecycle == gateway.FrontendDrainEnforcing {
		seen := [3]bool{}
		for _, protocol := range record.ContinuationGrant.AcceptedConnectionProtocols {
			if !protocol.Valid() || seen[protocol] {
				continue
			}
			seen[protocol] = true
			runtime.InstallFrontendContinuationGrant(record.ContinuationGrant.GrantDigest, protocol)
		}
	}
	if runtime.serviceDirectory != nil {
		if cut, ok := runtime.serviceDirectory.Cut(); ok {
			runtime.installPublishedFrontendContinuation(cut)
		}
	}
	return nil
}

func (frontend *frontendAdmission) identityMatchesNode(node gateway.NodeRecord) bool {
	if frontend == nil {
		return false
	}
	frontend.mu.Lock()
	identity := frontend.identity
	frontend.mu.Unlock()
	return identity.NodeID == node.NodeID && identity.Incarnation == node.Incarnation &&
		identity.GatewayNodeID == node.Gateway.NodeID && identity.GatewayIncarnation == node.Gateway.Incarnation &&
		identity.GatewayServiceKeyDigest == node.Gateway.ServiceKeyDigest &&
		identity.SessionID == node.Gateway.SessionID && identity.SessionRevision == node.Gateway.SessionRevision
}

// frontendAdmissionListener fences native listener admission and attaches a
// release callback to each accepted connection. The callback is on the raw
// net.Conn so TLS handshake failures cannot leak the count.
type frontendAdmissionListener struct {
	net.Listener
	frontend *frontendAdmission
}

func (listener *frontendAdmissionListener) Accept() (net.Conn, error) {
	conn, err := listener.Listener.Accept()
	if err != nil {
		if listener.frontend != nil && listener.frontend.isDraining() {
			return nil, errFrontendAdmissionDrained
		}
		return nil, err
	}
	token, admitted := listener.frontend.admitNative()
	if listener.frontend == nil || !admitted {
		_ = conn.Close()
		return nil, errFrontendAdmissionDrained
	}
	return &frontendTrackedConn{Conn: conn, token: token, scope: serviceauthz.FrontendScopeNative,
		frontend: listener.frontend, release: func() { listener.frontend.releaseNative(token) }}, nil
}

// frontendPGAdmissionListener mints the connection token before pgwire reads
// startup. pgwire owns its own exact connection/session counters; this wrapper
// only preserves the token and rejects new sockets at the same lifecycle fence.
type frontendPGAdmissionListener struct {
	net.Listener
	frontend *frontendAdmission
}

func (listener *frontendPGAdmissionListener) Accept() (net.Conn, error) {
	conn, err := listener.Listener.Accept()
	if err != nil {
		if listener.frontend != nil && listener.frontend.isDraining() {
			return nil, errFrontendAdmissionDrained
		}
		return nil, err
	}
	if listener.frontend == nil {
		_ = conn.Close()
		return nil, errFrontendAdmissionDrained
	}
	token, admitted := listener.frontend.admitPG()
	if !admitted {
		_ = conn.Close()
		return nil, errFrontendAdmissionDrained
	}
	return &frontendTrackedConn{Conn: conn, token: token, scope: serviceauthz.FrontendScopePostgreSQL,
		frontend: listener.frontend, release: func() { listener.frontend.releasePG(token) }}, nil
}

type frontendTrackedConn struct {
	net.Conn
	token    serviceauthz.FrontendConnToken
	scope    serviceauthz.FrontendContinuationScope
	frontend *frontendAdmission
	release  func()
	once     sync.Once
}

func (conn *frontendTrackedConn) FrontendConnectionContext(parent context.Context) context.Context {
	if conn == nil || conn.frontend == nil {
		return parent
	}
	ctx, err := serviceauthz.WithFrontendConnection(parent, conn.token, conn.scope, conn.frontend)
	if err != nil {
		return parent
	}
	return ctx
}

func (conn *frontendTrackedConn) Close() error {
	err := conn.Conn.Close()
	conn.once.Do(func() {
		if conn.release != nil {
			conn.release()
		}
	})
	return err
}
