package gatewayruntime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
)

var errGatewayBackupRemote = errors.New("vibedb-gateway: invalid backup leader resolution")

type gatewayBackupRoute struct {
	group             raftmember.GroupKey
	replicaSetVersion uint64
	replicas          []gateway.ReplicatedEndpoint
}

type gatewayBackupLeaderResolver struct {
	operation [sha256.Size]byte
	opener    *gatewayShardControlOpener
	observer  gatewayReplicaObservationClient
	read      rafttransport.DeadlineFunc
	write     rafttransport.DeadlineFunc
	routes    map[raftmember.GroupKey]gatewayBackupRoute
}

func newGatewayBackupLeaderResolver(operation [sha256.Size]byte, opener *gatewayShardControlOpener,
	observer gatewayReplicaObservationClient, read, write rafttransport.DeadlineFunc,
	routes []gateway.ReplicatedRoute,
) (*gatewayBackupLeaderResolver, error) {
	if operation == ([sha256.Size]byte{}) || opener == nil || observer == nil || read == nil || write == nil ||
		len(routes) == 0 || len(routes) > clusterbackup.AbsoluteMaxGroupCuts {
		return nil, errGatewayBackupRemote
	}
	directory := make(map[raftmember.GroupKey]gatewayBackupRoute, len(routes))
	for _, route := range routes {
		if route.Group == (raftmember.GroupKey{}) || route.Command.ReplicaSetVersion == 0 ||
			len(route.Replicas) != gateway.ServingReplicaCount {
			return nil, errGatewayBackupRemote
		}
		if _, duplicate := directory[route.Group]; duplicate {
			return nil, errGatewayBackupRemote
		}
		owned := append([]gateway.ReplicatedEndpoint(nil), route.Replicas...)
		for _, endpoint := range owned {
			if endpoint.Member == 0 || endpoint.Node == (rafttransport.NodeID{}) || endpoint.ControlAddress == "" {
				return nil, errGatewayBackupRemote
			}
		}
		directory[route.Group] = gatewayBackupRoute{group: route.Group,
			replicaSetVersion: route.Command.ReplicaSetVersion, replicas: owned}
	}
	return &gatewayBackupLeaderResolver{operation: operation, opener: opener, observer: observer,
		read: read, write: write, routes: directory}, nil
}

func (resolver *gatewayBackupLeaderResolver) ResolveBackupLeader(ctx context.Context,
	group raftmember.GroupKey,
) (uint64, clusterbackup.LiveArtifactExporter, error) {
	if resolver == nil || ctx == nil {
		return 0, nil, errGatewayBackupRemote
	}
	route, found := resolver.routes[group]
	if !found {
		return 0, nil, errGatewayBackupRemote
	}
	step := gatewayBackupObservationStep(resolver.operation, group)
	var observed error
	for _, endpoint := range route.replicas {
		request := replicacontrol.Request{Operation: resolver.operation, Step: step, Group: group,
			TargetMember: endpoint.Member, ExpectedReplicaSetVersion: route.replicaSetVersion}
		observation, err := resolver.observer.Observe(ctx, endpoint.Node, request)
		if err == nil && observation.Status.MemberID == endpoint.Member &&
			observation.Status.LeaderID == endpoint.Member && observation.Status.Term != 0 {
			node := endpoint.Node
			client := clusterbackup.LiveClient{Open: func(openCtx context.Context) (rafttransport.PeerConnection, error) {
				return resolver.opener.OpenShardControl(openCtx, node)
			}, ReadDeadline: resolver.read, WriteDeadline: resolver.write}
			return endpoint.Member, client, nil
		}
		observed = errors.Join(observed, err)
	}
	return 0, nil, errors.Join(errGatewayBackupRemote, observed)
}

func gatewayBackupObservationStep(operation [sha256.Size]byte, group raftmember.GroupKey) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb backup leader observation\x00"))
	_, _ = hash.Write(operation[:])
	_, _ = hash.Write(group.ClusterID[:])
	_, _ = hash.Write(group.ClusterIncarnation[:])
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], group.TopologyRecoveryEpoch)
	_, _ = hash.Write(scalar[:])
	_, _ = hash.Write(group.ShardIncarnation[:])
	_, _ = hash.Write(group.GroupID[:])
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

var _ gateway.BackupLeaderResolver = (*gatewayBackupLeaderResolver)(nil)
