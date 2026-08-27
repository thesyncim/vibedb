package shardservice

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var (
	ErrRestoreServingControl      = errors.New("shardservice: invalid restore serving grant")
	ErrRestoreServingUnauthorized = errors.New("shardservice: restore serving grant unauthorized")
	ErrRestoreServingUnknown      = errors.New("shardservice: restore serving grant outcome unknown")
)

const restoreServingResponseBytes = 8 + 32

var restoreServingResponseMagic = [8]byte{'V', 'B', 'R', 'S', 'G', 'A', 'C', 'K'}

func RestoreServingRequestDiscriminator() [8]byte {
	return clusterrestore.ServingGrantDiscriminator()
}

type restoreServingPublication struct{ digest [32]byte }

// RestoreServingGate is process-local by design. A restart closes serving and
// requires the gateway to repeat its linearizable catalog observation.
type RestoreServingGate struct {
	group           raftmember.GroupKey
	member          uint64
	node            rafttransport.NodeID
	store           [16]byte
	nodeIncarnation uint64
	active          atomic.Pointer[restoreServingPublication]
}

func NewRestoreServingGate(identity raftmember.RuntimeIdentity,
	node rafttransport.NodeID,
) (*RestoreServingGate, error) {
	if identity.Group == (raftmember.GroupKey{}) || identity.MemberID == 0 ||
		identity.StoreID == ([16]byte{}) || identity.NodeIncarnation == 0 ||
		node == (rafttransport.NodeID{}) {
		return nil, ErrRestoreServingControl
	}
	return &RestoreServingGate{group: identity.Group, member: identity.MemberID, node: node,
		store: identity.StoreID, nodeIncarnation: identity.NodeIncarnation}, nil
}

func (gate *RestoreServingGate) Install(grant clusterrestore.ServingGrant) error {
	if gate == nil || grant.Group() != gate.group || grant.Member() != gate.member ||
		grant.Node() != gate.node || grant.Store() != gate.store ||
		grant.NodeIncarnation() != gate.nodeIncarnation || grant.Digest() == ([32]byte{}) {
		return ErrRestoreServingControl
	}
	next := &restoreServingPublication{digest: grant.Digest()}
	for {
		current := gate.active.Load()
		if current != nil {
			if current.digest == next.digest {
				return nil
			}
			return ErrRestoreServingControl
		}
		if gate.active.CompareAndSwap(nil, next) {
			return nil
		}
	}
}

func (gate *RestoreServingGate) Allows(state raftservice.ServingState) bool {
	return gate != nil && gate.active.Load() != nil && state.Identity.Group == gate.group &&
		state.Identity.MemberID == gate.member && state.Identity.StoreID == gate.store &&
		state.Identity.NodeIncarnation == gate.nodeIncarnation
}

type RestoreServingControlService struct {
	gate          *RestoreServingGate
	policy        *serviceauthz.Policy
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
}

func NewRestoreServingControlService(gate *RestoreServingGate, policy *serviceauthz.Policy,
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
) (*RestoreServingControlService, error) {
	if gate == nil || policy == nil || readDeadline == nil || writeDeadline == nil ||
		len(policy.NodesWith(serviceauthz.CapabilityRestoreActivate)) == 0 {
		return nil, ErrRestoreServingControl
	}
	return &RestoreServingControlService{gate: gate, policy: policy,
		readDeadline: readDeadline, writeDeadline: writeDeadline}, nil
}

func (service *RestoreServingControlService) Serve(ctx context.Context,
	connection rafttransport.PeerConnection,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		return ErrRestoreServingUnauthorized
	}
	if deadline := boundedMembershipGrantDeadline(ctx, service.readDeadline()); deadline.IsZero() {
		return ErrRestoreServingControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	var raw [clusterrestore.ServingGrantBytes]byte
	if _, err := io.ReadFull(connection, raw[:]); err != nil {
		return errors.Join(ErrRestoreServingControl, err)
	}
	grant, err := clusterrestore.OpenServingGrant(raw[:])
	if err != nil {
		return errors.Join(ErrRestoreServingControl, err)
	}
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{ClusterID: grant.Group().ClusterID,
		ClusterIncarnation: grant.Group().ClusterIncarnation}
	if peer.TrustDomain != wantDomain || service.policy.Check(peer.Node,
		serviceauthz.CapabilityRestoreActivate) != serviceauthz.DecisionAllow {
		return ErrRestoreServingUnauthorized
	}
	if err := service.gate.Install(grant); err != nil {
		return err
	}
	if deadline := boundedMembershipGrantDeadline(ctx, service.writeDeadline()); deadline.IsZero() {
		return ErrRestoreServingUnknown
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return errors.Join(ErrRestoreServingUnknown, err)
	}
	var response [restoreServingResponseBytes]byte
	copy(response[:8], restoreServingResponseMagic[:])
	digest := grant.Digest()
	copy(response[8:], digest[:])
	if err := writeMembershipGrantFull(connection, response[:]); err != nil {
		return errors.Join(ErrRestoreServingUnknown, err)
	}
	return nil
}

type RestoreServingControlClient struct {
	opener        MembershipGrantControlStreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
}

func NewRestoreServingControlClient(opener MembershipGrantControlStreamOpener,
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
) (*RestoreServingControlClient, error) {
	if opener == nil || readDeadline == nil || writeDeadline == nil {
		return nil, ErrRestoreServingControl
	}
	return &RestoreServingControlClient{opener: opener,
		readDeadline: readDeadline, writeDeadline: writeDeadline}, nil
}

func (client *RestoreServingControlClient) Install(ctx context.Context,
	target rafttransport.NodeID, grant clusterrestore.ServingGrant,
) error {
	if client == nil || ctx == nil || target == (rafttransport.NodeID{}) || target != grant.Node() {
		return ErrRestoreServingControl
	}
	connection, err := client.opener.OpenShardControl(ctx, target)
	if err != nil || connection == nil {
		if connection != nil {
			_ = connection.Close()
		}
		return errors.Join(ErrRestoreServingControl, err)
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{ClusterID: grant.Group().ClusterID,
		ClusterIncarnation: grant.Group().ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		peer.Node != target || peer.TrustDomain != wantDomain {
		return ErrRestoreServingUnauthorized
	}
	if deadline := boundedMembershipGrantDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return ErrRestoreServingControl
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	raw, err := clusterrestore.AppendServingGrant(nil, grant)
	if err != nil {
		return err
	}
	if err = writeMembershipGrantFull(connection, raw); err != nil {
		return errors.Join(ErrRestoreServingUnknown, err)
	}
	if deadline := boundedMembershipGrantDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return ErrRestoreServingUnknown
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return errors.Join(ErrRestoreServingUnknown, err)
	}
	var response [restoreServingResponseBytes]byte
	if _, err = io.ReadFull(connection, response[:]); err != nil ||
		!bytes.Equal(response[:8], restoreServingResponseMagic[:]) ||
		[32]byte(response[8:]) != grant.Digest() {
		return errors.Join(ErrRestoreServingUnknown, err)
	}
	return nil
}
