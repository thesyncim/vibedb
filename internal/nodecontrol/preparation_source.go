package nodecontrol

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
	"io"
)

const maxPreparationSourceRequestBytes = 64 << 10

var preparationSourceRequestMagic = [8]byte{'V', 'D', 'P', 'R', 'E', 'P', 'Q', 1}
var preparationSourceReplyMagic = [8]byte{'V', 'D', 'P', 'R', 'E', 'P', 'R', 1}

func PreparationSourceRequestDiscriminator() [8]byte { return preparationSourceRequestMagic }

func validPreparationSourceIntent(intent gateway.GroupEnrollmentIntent) bool {
	if intent.ExpectedManifestDigest == (replication.Digest{}) {
		intent.ExpectedManifestDigest[0] = 1
	}
	return intent.Valid()
}

type PreparationSourceRequest struct {
	Intent gateway.GroupEnrollmentIntent
	Voters [3]PreparationMember
}

type PreparationSourceProvider func(context.Context, gateway.GroupEnrollmentIntent, [3]PreparationMember) ([]byte, error)

type PreparationSourceService struct {
	source      PreparationSourceProvider
	local       rafttransport.NodeID
	authorize   func(rafttransport.PeerIdentity, gateway.GroupEnrollmentIntent) bool
	read, write rafttransport.DeadlineFunc
	slots       chan struct{}
}

func NewPreparationSourceService(source PreparationSourceProvider, local rafttransport.NodeID, authorize func(rafttransport.PeerIdentity, gateway.GroupEnrollmentIntent) bool, read, write rafttransport.DeadlineFunc) (*PreparationSourceService, error) {
	if source == nil || local == (rafttransport.NodeID{}) || authorize == nil || read == nil || write == nil {
		return nil, ErrControl
	}
	return &PreparationSourceService{source: source, local: local, authorize: authorize, read: read, write: write, slots: make(chan struct{}, 4)}, nil
}

func (service *PreparationSourceService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil {
		return ErrControl
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if connection.TrafficClass() != rafttransport.TrafficShardControl {
		return ErrUnauthorized
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrBound
	}
	if err := connection.SetReadDeadline(nodeInfoBoundedDeadline(ctx, service.read())); err != nil {
		return err
	}
	raw, nonce, err := readPreparationSourceFrame(connection, preparationSourceRequestMagic, maxPreparationSourceRequestBytes)
	if err != nil {
		return err
	}
	var request PreparationSourceRequest
	if err = vibejson.Unmarshal(raw, &request); err != nil || !validPreparationSourceIntent(request.Intent) {
		return ErrControl
	}
	intent := request.Intent
	canonical, err := vibejson.Marshal(&request)
	if err != nil || !bytes.Equal(raw, canonical) {
		return ErrControl
	}
	peer := connection.PeerIdentity()
	domain := rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}
	if peer.TrustDomain != domain || intent.Source.Node != service.local || !service.authorize(peer, intent) {
		return ErrUnauthorized
	}
	payload, err := service.source(ctx, intent, request.Voters)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > MaxPayloadBytes {
		return ErrBound
	}
	if err := connection.SetWriteDeadline(nodeInfoBoundedDeadline(ctx, service.write())); err != nil {
		return err
	}
	return writePreparationSourceFrame(connection, preparationSourceReplyMagic, nonce, payload)
}

type PreparationSourceClient struct{ options ClientOptions }

func NewPreparationSourceClient(options ClientOptions) (*PreparationSourceClient, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrControl
	}
	return &PreparationSourceClient{options: options}, nil
}
func (client *PreparationSourceClient) Read(ctx context.Context, intent gateway.GroupEnrollmentIntent, voters [3]PreparationMember) ([]byte, error) {
	if client == nil || ctx == nil || !validPreparationSourceIntent(intent) {
		return nil, ErrControl
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	raw, err := vibejson.Marshal(&PreparationSourceRequest{Intent: intent, Voters: voters})
	if err != nil || len(raw) > maxPreparationSourceRequestBytes {
		return nil, errors.Join(ErrBound, err)
	}
	connection, err := client.options.Opener.OpenShardControl(ctx, intent.Source.Node)
	if err != nil {
		return nil, err
	}
	if connection == nil {
		return nil, ErrControl
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	domain := rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl || connection.PeerIdentity().Node != intent.Source.Node || connection.PeerIdentity().TrustDomain != domain {
		return nil, ErrUnauthorized
	}
	if err = connection.SetWriteDeadline(nodeInfoBoundedDeadline(ctx, client.options.WriteDeadline())); err != nil {
		return nil, err
	}
	if err = writePreparationSourceFrame(connection, preparationSourceRequestMagic, nonce, raw); err != nil {
		return nil, err
	}
	if err = connection.SetReadDeadline(nodeInfoBoundedDeadline(ctx, client.options.ReadDeadline())); err != nil {
		return nil, err
	}
	payload, replyNonce, err := readPreparationSourceFrame(connection, preparationSourceReplyMagic, MaxPayloadBytes)
	if err != nil {
		return nil, err
	}
	if replyNonce != nonce {
		return nil, ErrConflict
	}
	if _, err = OpenPreparationSpec(payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writePreparationSourceFrame(writer io.Writer, magic [8]byte, nonce [16]byte, payload []byte) error {
	var header [32]byte
	copy(header[:8], magic[:])
	copy(header[8:24], nonce[:])
	binary.BigEndian.PutUint32(header[24:28], uint32(len(payload)))
	if err := writeFull(writer, header[:]); err != nil {
		return err
	}
	return writeFull(writer, payload)
}
func readPreparationSourceFrame(reader io.Reader, magic [8]byte, maximum int) ([]byte, [16]byte, error) {
	var header [32]byte
	var nonce [16]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, nonce, err
	}
	copy(nonce[:], header[8:24])
	n := binary.BigEndian.Uint32(header[24:28])
	if !bytes.Equal(header[:8], magic[:]) || nonce == ([16]byte{}) || binary.BigEndian.Uint32(header[28:]) != 0 || n == 0 || uint64(n) > uint64(maximum) {
		return nil, nonce, ErrBound
	}
	payload := make([]byte, int(n))
	_, err := io.ReadFull(reader, payload)
	return payload, nonce, err
}
