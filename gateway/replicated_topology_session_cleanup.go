package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// NativeTopologySessionCleanup only retires an operation-owned
// topology session. The caller must first prove that the operation's work has
// completed and prevent new work on that identity. No new Open is possible.
// The exact session header/terminal slot is its replicated cleanup journal:
// another leader can retry retirement or release after a lost response without
// an old leader's local files. Native serving still checks every command fence.
type NativeTopologySessionCleanup struct {
	session *NativeSession
}

// NewNativeTopologySessionCleanup detaches the retirement state without
// proposing anything. The caller must close cut before Run: keeping the old
// system generation pinned across retirement and release can prevent the
// checkpoint needed to settle those very commands.
func NewNativeTopologySessionCleanup(options NativeSessionOptions, cut *replicatedstate.ReadSnapshot) (*NativeTopologySessionCleanup, error) {
	if cut == nil || options.Journal != nil || options.ProposalCapability != serviceauthz.CapabilityTopology {
		return nil, ErrNativeSession
	}
	fence := cut.Fence()
	binding, route, command := fence.Binding, options.Route, options.Route.Command
	if binding.ClusterID != route.Group.ClusterID || binding.ClusterIncarnation != route.Group.ClusterIncarnation ||
		binding.TopologyRecoveryEpoch != route.Group.TopologyRecoveryEpoch || binding.GroupID != route.Group.GroupID ||
		binding.ShardIncarnation != route.Group.ShardIncarnation || binding.Distribution != string(route.Distribution) ||
		binding.Shard != string(route.Shard) || binding.AllocationGeneration != route.AllocationGeneration ||
		fence.ReplicaSetVersion != command.ReplicaSetVersion || fence.RelationManifestDigest != command.RelationManifestDigest ||
		binding.ActivePolicyGeneration != command.ActivePolicyGeneration || binding.ProtectionEpoch != command.ProtectionEpoch ||
		binding.OwnershipEpoch != command.OwnershipEpoch || binding.SchemaGeneration != command.SchemaGeneration ||
		binding.RoutingVersion != command.RoutingVersion || binding.RouteGeneration != command.RouteGeneration {
		return nil, ErrNativeSession
	}
	session, err := NewNativeSession(options)
	if err != nil {
		return nil, err
	}
	header, slot, found, err := cut.TopologySession(options.Tenant, options.ClientID)
	if err != nil {
		return nil, err
	}
	if !found {
		return &NativeTopologySessionCleanup{}, nil
	}
	if header.RetryHome != options.RetryHome {
		return nil, ErrNativeSession
	}
	session.epoch, session.nextSequence = header.ClientEpoch, header.HighSequence+1
	session.ackThrough, session.leaseDeadline = header.AckThrough, header.LeaseDeadlineUnixNano
	switch header.Status {
	case replicatedstate.SessionActive:
		session.phase = nativeSessionActive
	case replicatedstate.SessionRetired:
		if slot.ResultCode != replicatedstate.ResultSessionRetired && slot.ResultCode != replicatedstate.ResultSessionRevoked {
			return nil, errors.Join(ErrNativeSession, replicatedstate.ErrSessionCorrupt)
		}
		session.phase = nativeSessionRetired
		session.terminalSequence, session.terminalFingerprint = slot.ClientSequence, slot.Fingerprint
	default:
		return nil, ErrNativeSession
	}
	return &NativeTopologySessionCleanup{session: session}, nil
}

func (cleanup *NativeTopologySessionCleanup) Run(ctx context.Context) error {
	if cleanup == nil || ctx == nil {
		return ErrNativeSession
	}
	if cleanup.session == nil {
		return nil
	}
	if cleanup.session.phase == nativeSessionActive {
		if _, err := cleanup.session.Retire(ctx); err != nil {
			return err
		}
	}
	_, err := cleanup.session.Release(ctx)
	if err == nil {
		cleanup.session = nil
	}
	return err
}
