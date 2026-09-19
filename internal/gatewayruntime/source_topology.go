package gatewayruntime

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

// openSourceTopologyService uses the gateway's existing authorized catalog
// and semantic/native transport. Storage sources receive only the closed,
// operation-bound RPC; the service validates their current committed role,
// physical identity and split step before forwarding with gateway authority.
func (runtime *Runtime) openSourceTopologyService(manifest gatewayReplicaControlManifest) error {
	if runtime == nil || runtime.authority == nil || runtime.serviceDirectory == nil ||
		runtime.config.TLSProfile == nil || runtime.config.Authorization == nil ||
		!runtime.config.InternalAuthority.Valid() || runtime.controlReadDeadline == nil || runtime.controlWriteDeadline == nil {
		return fmt.Errorf("%w: source topology requires authenticated catalog, directory and gateway authority", ErrInvalidConfig)
	}
	authority := runtime.config.InternalAuthority
	if authority.Node != runtime.config.TLSProfile.LocalIdentity().Node ||
		authority.Generation != runtime.config.Authorization.Generation() ||
		runtime.config.Authorization.Check(authority.Node, serviceauthz.CapabilityTopology|serviceauthz.CapabilityDelegate) != serviceauthz.DecisionAllow {
		return fmt.Errorf("%w: source topology gateway authority mismatch", ErrInvalidConfig)
	}
	native, err := runtime.sourceTopologyNativeClient()
	if err != nil {
		return err
	}
	service, err := splitcontroller.NewSourceTopologyService(splitcontroller.SourceTopologyServiceOptions{
		Catalog: runtime.authority, Directory: runtime.serviceDirectory,
		TrustDomain: runtime.config.TLSProfile.LocalIdentity().TrustDomain,
		Native:      native, Authority: authority,
		ReadDeadline: runtime.controlReadDeadline, WriteDeadline: runtime.controlWriteDeadline,
		MaxConcurrent: min(int(manifest.Bounds.MaxConnections), 16),
	})
	if err != nil {
		return fmt.Errorf("open source topology control service: %w", err)
	}
	runtime.sourceTopologyService = service
	return nil
}

func (runtime *Runtime) sourceTopologyNativeClient() (splitcontroller.SourceTopologyNativeClient, error) {
	if runtime == nil {
		return nil, ErrInvalidConfig
	}
	if runtime.config.Transport != nil {
		native, ok := runtime.config.Transport.(splitcontroller.SourceTopologyNativeClient)
		if !ok {
			return nil, fmt.Errorf("%w: injected source topology transport lacks authenticated probes", ErrInvalidConfig)
		}
		return native, nil
	}
	if runtime.replicatedPool == nil {
		return nil, fmt.Errorf("%w: source topology native transport unavailable", ErrInvalidConfig)
	}
	return runtime.replicatedPool, nil
}
