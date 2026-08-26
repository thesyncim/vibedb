package shardservice

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var (
	ErrMembershipGrantControl        = errors.New("shardservice: invalid membership grant control request")
	ErrMembershipGrantUnauthorized   = errors.New("shardservice: membership grant principal is not authorized")
	ErrMembershipGrantOutcomeUnknown = errors.New("shardservice: membership grant outcome is unknown")
)

const (
	MembershipGrantRequestBytes  = 8 + membershipgrant.CanonicalGrantBytes
	MembershipGrantResponseBytes = 8 + 32
)

var (
	// The membership request discriminator is intentionally disjoint from the
	// snapshot bootstrap request magic carried by the same authenticated
	// shard-control traffic class. A bounded listener dispatcher can therefore
	// select either fixed grammar without interpreting either payload.
	membershipGrantRequestMagic  = [8]byte{'V', 'B', 'M', 'G', 'I', 'N', 'S', 'T'}
	membershipGrantResponseMagic = [8]byte{'V', 'B', 'M', 'G', 'A', 'C', 'K', 0}
)

type TransitionGrantInstaller interface {
	InstallTransitionGrant(membershipgrant.Grant) error
}

type MembershipGrantControlService struct {
	registry      TransitionGrantInstaller
	policy        *serviceauthz.Policy
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
}

func NewMembershipGrantControlService(
	registry TransitionGrantInstaller,
	policy *serviceauthz.Policy,
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
) (*MembershipGrantControlService, error) {
	if registry == nil || policy == nil || readDeadline == nil || writeDeadline == nil ||
		len(policy.NodesWith(serviceauthz.CapabilityMembership)) == 0 {
		return nil, ErrMembershipGrantControl
	}
	return &MembershipGrantControlService{
		registry: registry, policy: policy,
		readDeadline: readDeadline, writeDeadline: writeDeadline,
	}, nil
}

// Serve installs exactly one grant from one authenticated shard-control
// stream. The TLS principal needs the dedicated Membership capability; the
// registry then proves the group, stable RF3 roster, and enrolled target.
func (service *MembershipGrantControlService) Serve(
	ctx context.Context,
	connection rafttransport.PeerConnection,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		return ErrMembershipGrantUnauthorized
	}
	if deadline := boundedMembershipGrantDeadline(ctx, service.readDeadline()); deadline.IsZero() {
		return ErrMembershipGrantControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	grant, err := ReadMembershipGrantRequest(connection)
	if err != nil {
		return err
	}
	identity := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{
		ClusterID: grant.Group.ClusterID, ClusterIncarnation: grant.Group.ClusterIncarnation,
	}
	if identity.TrustDomain != wantDomain ||
		service.policy.Check(identity.Node, serviceauthz.CapabilityMembership) != serviceauthz.DecisionAllow {
		return ErrMembershipGrantUnauthorized
	}
	if err = service.registry.InstallTransitionGrant(grant); err != nil {
		return err
	}
	if deadline := boundedMembershipGrantDeadline(ctx, service.writeDeadline()); deadline.IsZero() {
		return ErrMembershipGrantControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return errors.Join(ErrMembershipGrantOutcomeUnknown, err)
	}
	if err = WriteMembershipGrantResponse(connection, grant.Digest()); err != nil {
		return errors.Join(ErrMembershipGrantOutcomeUnknown, err)
	}
	return nil
}

type MembershipGrantControlStreamOpener interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type MembershipGrantControlClient struct {
	opener        MembershipGrantControlStreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
}

func NewMembershipGrantControlClient(
	opener MembershipGrantControlStreamOpener,
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
) (*MembershipGrantControlClient, error) {
	if opener == nil || readDeadline == nil || writeDeadline == nil {
		return nil, ErrMembershipGrantControl
	}
	return &MembershipGrantControlClient{
		opener: opener, readDeadline: readDeadline, writeDeadline: writeDeadline,
	}, nil
}

func (client *MembershipGrantControlClient) InstallMembershipGrant(
	ctx context.Context,
	target rafttransport.NodeID,
	grant membershipgrant.Grant,
) error {
	if client == nil || ctx == nil || target == (rafttransport.NodeID{}) || !grant.Valid() {
		return ErrMembershipGrantControl
	}
	connection, err := client.opener.OpenShardControl(ctx, target)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return err
	}
	if connection == nil {
		return ErrMembershipGrantControl
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{
		ClusterID: grant.Group.ClusterID, ClusterIncarnation: grant.Group.ClusterIncarnation,
	}
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		peer.Node != target || peer.TrustDomain != wantDomain {
		return ErrMembershipGrantUnauthorized
	}
	if deadline := boundedMembershipGrantDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return ErrMembershipGrantControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err = WriteMembershipGrantRequest(connection, grant); err != nil {
		return errors.Join(ErrMembershipGrantOutcomeUnknown, err)
	}
	if deadline := boundedMembershipGrantDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return ErrMembershipGrantOutcomeUnknown
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return errors.Join(ErrMembershipGrantOutcomeUnknown, err)
	}
	digest, err := ReadMembershipGrantResponse(connection)
	if err != nil {
		return errors.Join(ErrMembershipGrantOutcomeUnknown, err)
	}
	if digest != grant.Digest() {
		return ErrMembershipGrantControl
	}
	return nil
}

func AppendMembershipGrantRequest(dst []byte, grant membershipgrant.Grant) ([]byte, error) {
	if !grant.Valid() {
		return dst, ErrMembershipGrantControl
	}
	start := len(dst)
	dst = append(dst, membershipGrantRequestMagic[:]...)
	var err error
	dst, err = membershipgrant.AppendCanonical(dst, grant)
	if err != nil || len(dst)-start != MembershipGrantRequestBytes {
		return dst[:start], errors.Join(ErrMembershipGrantControl, err)
	}
	return dst, nil
}

func OpenMembershipGrantRequest(raw []byte) (membershipgrant.Grant, error) {
	if len(raw) != MembershipGrantRequestBytes || !bytes.Equal(raw[:8], membershipGrantRequestMagic[:]) {
		return membershipgrant.Grant{}, ErrMembershipGrantControl
	}
	grant, err := membershipgrant.OpenCanonical(raw[8:])
	if err != nil {
		return membershipgrant.Grant{}, errors.Join(ErrMembershipGrantControl, err)
	}
	return grant, nil
}

func WriteMembershipGrantRequest(writer io.Writer, grant membershipgrant.Grant) error {
	var raw [MembershipGrantRequestBytes]byte
	encoded, err := AppendMembershipGrantRequest(raw[:0], grant)
	if err != nil {
		return err
	}
	return writeMembershipGrantFull(writer, encoded)
}

func ReadMembershipGrantRequest(reader io.Reader) (membershipgrant.Grant, error) {
	var raw [MembershipGrantRequestBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return membershipgrant.Grant{}, errors.Join(ErrMembershipGrantControl, err)
	}
	return OpenMembershipGrantRequest(raw[:])
}

func WriteMembershipGrantResponse(writer io.Writer, digest [32]byte) error {
	if digest == ([32]byte{}) {
		return ErrMembershipGrantControl
	}
	var raw [MembershipGrantResponseBytes]byte
	copy(raw[:8], membershipGrantResponseMagic[:])
	copy(raw[8:], digest[:])
	return writeMembershipGrantFull(writer, raw[:])
}

func ReadMembershipGrantResponse(reader io.Reader) ([32]byte, error) {
	var raw [MembershipGrantResponseBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return [32]byte{}, errors.Join(ErrMembershipGrantControl, err)
	}
	var digest [32]byte
	copy(digest[:], raw[8:])
	if !bytes.Equal(raw[:8], membershipGrantResponseMagic[:]) || digest == ([32]byte{}) {
		return [32]byte{}, ErrMembershipGrantControl
	}
	return digest, nil
}

func writeMembershipGrantFull(writer io.Writer, raw []byte) error {
	if writer == nil {
		return ErrMembershipGrantControl
	}
	for len(raw) != 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(raw) {
			return ErrMembershipGrantControl
		}
		raw = raw[written:]
	}
	return nil
}

func boundedMembershipGrantDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(configured) {
		return deadline
	}
	return configured
}
