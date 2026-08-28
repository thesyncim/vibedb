package routeforward

import (
	"crypto/sha256"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
)

// BuildClearance derives the source-gate fields from the exact settled
// route-gate result instead of trusting scalar copies supplied by a caller.
// Admission authenticates both settlement channels before proposing the
// returned value; the retry certificate remains opaque to this package.
func BuildClearance(
	key Digest,
	catalogGeneration uint64,
	authorityRevision uint64,
	gateSettlement []byte,
	retry RetryCut,
) (Clearance, error) {
	gate, err := routegate.OpenOutcome(gateSettlement)
	if err != nil || key == (Digest{}) || catalogGeneration == 0 || authorityRevision == 0 ||
		gate.Status.ActivePins != 0 || retry.OldestApplied == 0 || retry.Certificate == (Digest{}) {
		return Clearance{}, ErrCorrupt
	}
	return Clearance{
		Key: key, CatalogGeneration: catalogGeneration,
		RouteGateEpoch: gate.Status.Epoch, RouteGateRevision: gate.Status.Revision,
		OldestRetryApplied: retry.OldestApplied, AuthorityRevision: authorityRevision,
		GateCertificate:  Digest(sha256.Sum256(gateSettlement)),
		RetryCertificate: retry.Certificate,
	}, nil
}

// BuildEntry binds an already canonical old command to one fixed target. The
// old relation-manifest digest comes from the authenticated logical plan;
// every fence carried by the command itself is rechecked here.
func BuildEntry(
	exactCommand []byte,
	kind TopologyKind,
	old RouteAuthority,
	plan Digest,
	target TargetRoute,
	validity Validity,
) (Entry, error) {
	command, err := replication.OpenCommand(exactCommand)
	if err != nil || !commandMatchesAuthority(command, old) || plan == (Digest{}) {
		return Entry{}, ErrCorrupt
	}
	entry := Entry{
		Kind: kind, Old: old,
		CommandFingerprint: Digest(command.Fingerprint),
		CommandDigest:      Digest(sha256.Sum256(exactCommand)),
		PlanDigest:         plan, Target: target, Validity: validity,
	}
	if !validEntry(entry) {
		return Entry{}, ErrCorrupt
	}
	return entry, nil
}

// Resolve validates one exact old command at a linearized catalog-RF3 cut.
// OriginalCommand aliases exactCommand with capacity clamped to its length;
// the bytes are never rebuilt or rewritten.
func (machine *Machine) Resolve(
	key Digest,
	exactCommand []byte,
	cut ReadCut,
) (Decision, Reason) {
	if machine == nil || key == (Digest{}) || len(exactCommand) == 0 {
		return Decision{}, ReasonInvalid
	}
	if cut.Authority != machine.authority || cut.AuthorityEpoch != machine.authorityEpoch {
		return Decision{}, ReasonUnauthorized
	}
	if cut.ReadIndex == 0 || cut.AppliedRevision < machine.revision {
		return Decision{}, ReasonStaleRead
	}
	retained, found := machine.entries[key]
	if !found {
		if _, retired := machine.retired[key]; retired {
			return Decision{}, ReasonRetired
		}
		return Decision{}, ReasonNotFound
	}
	if retained.State != EntryActive || cut.AppliedRevision < retained.ActiveRevision {
		return Decision{}, ReasonNotActive
	}
	entry := retained.Entry
	if cut.CatalogGeneration < entry.Validity.ValidFromCatalog {
		return Decision{}, ReasonTooEarly
	}
	if cut.CatalogGeneration > entry.Validity.ExpiresAfterCatalog {
		return Decision{}, ReasonExpired
	}
	if cut.TargetApplied < entry.Validity.TargetAppliedFloor {
		return Decision{}, ReasonTargetBehind
	}
	// BuildEntry already performed the full canonical command/fence parse. The
	// content address now proves that these are those exact bytes, avoiding a
	// second envelope walk on every response-loss retry.
	if Digest(sha256.Sum256(exactCommand)) != entry.CommandDigest {
		return Decision{}, ReasonConflict
	}
	return Decision{
		Target: entry.Target, MinimumApplied: entry.Validity.TargetAppliedFloor,
		Certificate:     entryCertificate(key, retained),
		OriginalCommand: exactCommand[:len(exactCommand):len(exactCommand)],
	}, ReasonActivated
}

func commandMatchesAuthority(command replication.CommandView, authority RouteAuthority) bool {
	group := raftmember.GroupKey{
		ClusterID:             [16]byte(command.ClusterID),
		ClusterIncarnation:    [16]byte(command.ClusterIncarnation),
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		ShardIncarnation:      [16]byte(command.ShardIncarnation),
		GroupID:               [16]byte(command.GroupID),
	}
	fence := authority.Command
	return command.AuthorityClass == replication.CommandAuthorityData &&
		authority.Group == group && authority.AllocationGeneration == command.AllocationGeneration &&
		fence.ReplicaSetVersion == command.ReplicaSetVersion &&
		fence.ActivePolicyGeneration == command.ActivePolicyGeneration &&
		fence.ProtectionEpoch == command.ProtectionEpoch &&
		fence.OwnershipEpoch == command.OwnershipEpoch &&
		fence.SchemaGeneration == command.SchemaGeneration &&
		fence.RoutingVersion == command.RoutingVersion &&
		fence.RouteGeneration == command.RouteGeneration
}
