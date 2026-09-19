package shardservice

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var (
	ErrFrontendDrainPreparedAck     = errors.New("shardservice: invalid frontend drain prepared acknowledgement")
	ErrFrontendDrainPreparedAckAuth = errors.New("shardservice: frontend drain prepared acknowledgement authentication failed")
)

// FrontendDrainPreparedAckCutReader reads one exact source cut for a storage
// receiver. The request already carries the canonical cut produced by the
// gateway authority; a production reader may independently resolve it from a
// replicated catalog and must return the same fences and service revision.
type FrontendDrainPreparedAckCutReader interface {
	ReadFrontendDrainPreparedAckCut(context.Context, frontenddrain.PreparedAckRequest) (frontenddrain.PreparedAckCut, error)
}

// FrontendDrainServiceCutInstaller is the canonical full-coordinate install
// seam. Implementations retain the same native gate pointer while also
// fencing directory and catalog coordinates that ServiceDirectoryCut alone
// does not carry.
type FrontendDrainServiceCutInstaller interface {
	InstallFrontendDrainServiceCut(context.Context, frontenddrain.PreparedAckCut) (uint64, error)
}

// FrontendDrainPreparedAckServiceOptions configures the physical storage
// receiver ACK endpoint. TLS authenticates the stream; Authorize applies the
// operator's capability policy to the source principal after the exact key
// and principal equality checks below.
type FrontendDrainPreparedAckServiceOptions struct {
	Reader        FrontendDrainPreparedAckCutReader
	Installer     FrontendDrainServiceCutInstaller
	TrustDomain   rafttransport.TrustDomain
	Authorize     func(rafttransport.PeerIdentity, frontenddrain.PreparedAckRequest) bool
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
}

// FrontendDrainPreparedAckService is a fixed grammar carried on the
// authenticated shard-control listener. A storage-only process owns this
// handler; it does not need a gateway-control listener or a gateway service
// identity.
type FrontendDrainPreparedAckService struct {
	reader    FrontendDrainPreparedAckCutReader
	installer FrontendDrainServiceCutInstaller
	trust     rafttransport.TrustDomain
	authorize func(rafttransport.PeerIdentity, frontenddrain.PreparedAckRequest) bool
	read      rafttransport.DeadlineFunc
	write     rafttransport.DeadlineFunc
}

func NewFrontendDrainPreparedAckService(
	options FrontendDrainPreparedAckServiceOptions,
) (*FrontendDrainPreparedAckService, error) {
	if options.Reader == nil || options.Installer == nil ||
		options.TrustDomain.ClusterID == ([16]byte{}) || options.TrustDomain.ClusterIncarnation == ([16]byte{}) ||
		options.Authorize == nil || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrFrontendDrainPreparedAck
	}
	return &FrontendDrainPreparedAckService{
		reader: options.Reader, installer: options.Installer, trust: options.TrustDomain,
		authorize: options.Authorize, read: options.ReadDeadline, write: options.WriteDeadline,
	}, nil
}

func (service *FrontendDrainPreparedAckService) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrFrontendDrainPreparedAck
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := frontendDrainPreparedAckDeadline(ctx, service.read()); deadline.IsZero() {
		return ErrFrontendDrainPreparedAck
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	// The request is length-prefixed by the exact 32-bit cut length in its
	// fixed header. Read the header first, then the bounded source cut and
	// trailing frame digest. No generic JSON stream parser is accepted here.
	header := make([]byte, frontenddrain.PreparedAckRequestHeaderBytes)
	if _, err := io.ReadFull(connection, header); err != nil {
		return err
	}
	cutBytes := uint64(header[288]) | uint64(header[289])<<8 | uint64(header[290])<<16 | uint64(header[291])<<24
	if cutBytes == 0 || cutBytes > frontenddrain.MaxPreparedAckCutBytes {
		return frontenddrain.ErrPreparedAckWire
	}
	raw := make([]byte, len(header)+int(cutBytes)+32)
	copy(raw, header)
	if _, err := io.ReadFull(connection, raw[len(header):]); err != nil {
		return err
	}
	request, err := frontenddrain.OpenPreparedAckRequest(raw)
	if err != nil {
		return err
	}
	peer := connection.PeerIdentity()
	if peer.TrustDomain != service.trust || peer.Node != request.SourcePrincipal ||
		connection.PeerKeyDigest() != request.SourcePrincipalKeyDigest ||
		!service.authorize(peer, request) {
		return frontenddrain.ErrPreparedAckAuth
	}
	cut, err := service.reader.ReadFrontendDrainPreparedAckCut(ctx, request)
	if err != nil {
		if errors.Is(err, frontenddrain.ErrPreparedAckCutMoved) {
			if deadline := frontendDrainPreparedAckDeadline(ctx, service.write()); deadline.IsZero() {
				return ErrFrontendDrainPreparedAck
			} else if err := connection.SetWriteDeadline(deadline); err != nil {
				return err
			}
			response := frontenddrain.PreparedAckResponse{
				Nonce: request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
				ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
				ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
				ReceiverNodeRevision:     request.ReceiverNodeRevision,
				DirectoryRevision:        request.SourceCut.DirectoryRevision,
				DirectoryDigest:          request.SourceCut.DirectoryDigest,
				CatalogGeneration:        request.SourceCut.CatalogGeneration,
				CatalogHeadDigest:        request.SourceCut.CatalogHeadDigest,
				ServiceDirectoryRevision: request.SourceCut.ServiceDirectoryRevision,
				ServiceDirectoryDigest:   request.SourceCut.ServiceDirectoryDigestValue(),
				AppliedRevision:          frontenddrain.PreparedAckCutMovedAppliedRevision,
				SourceCutDigest:          request.SourceCutDigest(),
			}
			if !response.Valid(request) {
				return frontenddrain.ErrPreparedAckState
			}
			return writeFrontendDrainPreparedAckFrame(connection, response.Marshal())
		}
		return err
	}
	if err := validateFrontendDrainPreparedAckCut(request, cut, service.trust); err != nil {
		return err
	}
	applied, err := service.installer.InstallFrontendDrainServiceCut(ctx, cut)
	if err != nil || applied == 0 {
		return errors.Join(frontenddrain.ErrPreparedAckState, err)
	}
	if deadline := frontendDrainPreparedAckDeadline(ctx, service.write()); deadline.IsZero() {
		return ErrFrontendDrainPreparedAck
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	response := frontenddrain.PreparedAckResponse{
		Nonce: request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     request.ReceiverNodeRevision,
		DirectoryRevision:        request.SourceCut.DirectoryRevision,
		DirectoryDigest:          request.SourceCut.DirectoryDigest,
		CatalogGeneration:        request.SourceCut.CatalogGeneration,
		CatalogHeadDigest:        request.SourceCut.CatalogHeadDigest,
		ServiceDirectoryRevision: request.SourceCut.ServiceDirectoryRevision,
		ServiceDirectoryDigest:   request.SourceCut.ServiceDirectoryDigestValue(),
		AppliedRevision:          applied, SourceCutDigest: request.SourceCutDigest(),
	}
	if !response.Valid(request) {
		return frontenddrain.ErrPreparedAckState
	}
	return writeFrontendDrainPreparedAckFrame(connection, response.Marshal())
}

func writeFrontendDrainPreparedAckFrame(connection io.Writer, frame []byte) error {
	for len(frame) != 0 {
		written, err := connection.Write(frame)
		if written > 0 {
			frame = frame[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func frontendDrainPreparedAckDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(configured) {
		return deadline
	}
	return configured
}

func validateFrontendDrainPreparedAckCut(
	request frontenddrain.PreparedAckRequest,
	cut frontenddrain.PreparedAckCut,
	trust rafttransport.TrustDomain,
) error {
	if !request.Valid() || !cut.Valid() || cut.Digest() != request.SourceCutDigest() {
		return frontenddrain.ErrPreparedAckState
	}
	if cut.ServiceDirectory.TrustDomain != trust {
		return frontenddrain.ErrPreparedAckAuth
	}
	var receiver *serviceauthz.ServiceBinding
	for index := range cut.ServiceDirectory.Bindings {
		binding := &cut.ServiceDirectory.Bindings[index]
		if binding.Principal != request.ReceiverNode {
			continue
		}
		if receiver != nil || binding.PhysicalNode != request.ReceiverNode ||
			binding.PhysicalIncarnation != request.ReceiverIncarnation ||
			binding.KeyDigest != request.ReceiverServiceKeyDigest ||
			binding.Roles&serviceauthz.ServiceRoleStorage == 0 ||
			(binding.Lifecycle != serviceauthz.ServiceActive && binding.Lifecycle != serviceauthz.ServiceDraining) {
			return frontenddrain.ErrPreparedAckState
		}
		receiver = binding
	}
	if receiver == nil {
		return frontenddrain.ErrPreparedAckState
	}
	if request.DrainID != ([32]byte{}) {
		if request.GrantDigest != ([32]byte{}) {
			foundGrant := false
			for _, grant := range cut.ServiceDirectory.ContinuationGrants {
				if grant.DrainID == request.DrainID && grant.GrantDigest == request.GrantDigest {
					if !grant.Valid() || (request.RequirePrepared && grant.State != serviceauthz.ContinuationGrantPrepared) || foundGrant {
						return frontenddrain.ErrPreparedAckState
					}
					foundGrant = true
				}
			}
			if !foundGrant {
				return frontenddrain.ErrPreparedAckState
			}
		} else {
			foundSubject := false
			for _, subject := range request.SourceCut.Subjects {
				if subject.DrainID != request.DrainID {
					continue
				}
				if !subject.Valid() || (request.RequirePrepared && subject.Lifecycle != 1) || foundSubject {
					return frontenddrain.ErrPreparedAckState
				}
				foundSubject = true
			}
			if !foundSubject {
				return frontenddrain.ErrPreparedAckState
			}
		}
	}
	return nil
}

// InstallFrontendDrainServiceCut validates and installs one complete source
// cut. The retained gate pointer and the full source coordinates advance as
// one serialized operation; a lower directory/catalog/service coordinate is
// rejected before it can replace a current lifecycle proof.
func (server *ReplicatedServer) InstallFrontendDrainServiceCut(
	ctx context.Context, cut frontenddrain.PreparedAckCut,
) (uint64, error) {
	if server == nil || ctx == nil || !cut.Valid() {
		return 0, ErrFrontendDrainPreparedAck
	}
	floor := cut.ReadFloor()
	if !floor.Valid() || floor == (frontenddrain.PreparedAckCutReadFloor{}) {
		return 0, frontenddrain.ErrPreparedAckState
	}
	server.directoryCutMu.Lock()
	defer server.directoryCutMu.Unlock()
	if server.directoryCoordinatesSet && !cut.AtLeastFloor(server.directoryCoordinates) {
		return 0, serviceauthz.ErrServiceDirectoryStale
	}
	directory, err := serviceauthz.NewServiceDirectoryGate(cut.ServiceDirectory)
	if err != nil {
		return 0, errors.Join(frontenddrain.ErrPreparedAckState, err)
	}
	if err := server.bindServiceDirectoryGateLocked(directory); err != nil {
		return 0, err
	}
	server.directoryCoordinates = floor
	server.directoryCoordinatesSet = true
	return server.ServiceDirectoryRevision(), nil
}

// ServiceCutCoordinates returns the complete source floor installed by the
// canonical service-cut path. It is used by fused gateway receivers to prove
// that their retained native gate has the same full epoch before ACK.
func (server *ReplicatedServer) ServiceCutCoordinates() (frontenddrain.PreparedAckCutReadFloor, bool) {
	if server == nil {
		return frontenddrain.PreparedAckCutReadFloor{}, false
	}
	server.directoryCutMu.Lock()
	defer server.directoryCutMu.Unlock()
	return server.directoryCoordinates, server.directoryCoordinatesSet
}
