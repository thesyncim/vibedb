package gatewayruntime

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	catalogSessionHandoffFormat       = 1
	maxCatalogSessionHandoffBytes     = 32 << 10
	maxCatalogSessionHandoffPathBytes = 4096
)

type catalogSessionHandoffPhase uint8

const (
	catalogSessionHandoffPrepared catalogSessionHandoffPhase = iota + 1
	catalogSessionHandoffOldSettled
	catalogSessionHandoffNewReady
	catalogSessionHandoffComplete
)

// catalogSessionHandoff is the durable predecessor/current transition. The
// route seed remains pending until NewReady and the authority session swap are
// complete. CurrentJournalPath is retained in the complete record as the
// durable pointer for the next process start.
type catalogSessionHandoff struct {
	Format               uint8                      `json:"format"`
	Phase                catalogSessionHandoffPhase `json:"phase"`
	Transition           [32]byte                   `json:"transition"`
	OldSessionGeneration uint64                     `json:"old_session_generation"`
	OldGeneration        uint64                     `json:"old_generation"`
	NextGeneration       uint64                     `json:"next_generation"`
	OldSnapshotDigest    [32]byte                   `json:"old_snapshot_digest"`
	NextSnapshotDigest   [32]byte                   `json:"next_snapshot_digest"`
	OldBinding           replication.Digest         `json:"old_binding"`
	NextBinding          replication.Digest         `json:"next_binding"`
	OldJournalPath       string                     `json:"old_journal_path"`
	NextJournalPath      string                     `json:"next_journal_path"`
	CurrentJournalPath   string                     `json:"current_journal_path"`
}

func catalogSessionHandoffPath(journalPath string) string {
	return journalPath + ".catalog-handoff"
}

func (handoff catalogSessionHandoff) valid() bool {
	if handoff.Format != catalogSessionHandoffFormat || handoff.Transition == ([32]byte{}) ||
		handoff.OldSessionGeneration == 0 || handoff.OldSessionGeneration > handoff.OldGeneration || handoff.OldGeneration == 0 || handoff.NextGeneration <= handoff.OldGeneration ||
		handoff.OldSnapshotDigest == ([32]byte{}) || handoff.NextSnapshotDigest == ([32]byte{}) ||
		handoff.OldBinding == (replication.Digest{}) || handoff.NextBinding == (replication.Digest{}) ||
		handoff.OldBinding == handoff.NextBinding || handoff.Phase < catalogSessionHandoffPrepared ||
		handoff.Phase > catalogSessionHandoffComplete || handoff.OldJournalPath == "" ||
		handoff.NextJournalPath == "" || handoff.CurrentJournalPath == "" ||
		handoff.OldJournalPath == handoff.NextJournalPath ||
		handoff.CurrentJournalPath != handoff.NextJournalPath &&
			handoff.Phase == catalogSessionHandoffComplete {
		return false
	}
	if handoff.Phase <= catalogSessionHandoffOldSettled &&
		handoff.CurrentJournalPath != handoff.OldJournalPath {
		return false
	}
	if handoff.Phase >= catalogSessionHandoffNewReady &&
		handoff.CurrentJournalPath != handoff.NextJournalPath {
		return false
	}
	for _, path := range []string{handoff.OldJournalPath, handoff.NextJournalPath, handoff.CurrentJournalPath} {
		if len(path) > maxCatalogSessionHandoffPathBytes || filepath.Clean(path) != path {
			return false
		}
	}
	return true
}

// catalogSessionJournalPath is the only journal path derivation accepted by a
// live handoff. Generation one uses the configured base journal; every later
// exact session has a deterministic generation suffix. Keeping this function
// beside the durable record prevents a tampered record from redirecting a
// recovery worker to an arbitrary clean file.
func catalogSessionJournalPath(base string, generation uint64) string {
	if base == "" || generation == 0 {
		return ""
	}
	if generation == 1 {
		return base
	}
	return fmt.Sprintf("%s.catalog-session-%020d", base, generation)
}

func catalogSessionHandoffPathsValid(handoff catalogSessionHandoff, base string) bool {
	if !handoff.valid() || base == "" || filepath.Clean(base) != base {
		return false
	}
	if handoff.OldJournalPath != catalogSessionJournalPath(base, handoff.OldSessionGeneration) ||
		handoff.NextJournalPath != catalogSessionJournalPath(base, handoff.NextGeneration) {
		return false
	}
	switch handoff.Phase {
	case catalogSessionHandoffPrepared, catalogSessionHandoffOldSettled:
		return handoff.CurrentJournalPath == handoff.OldJournalPath
	case catalogSessionHandoffNewReady, catalogSessionHandoffComplete:
		return handoff.CurrentJournalPath == handoff.NextJournalPath
	default:
		return false
	}
}

// catalogSessionResumeJournalPath selects the exact journal which belongs to
// the durable phase.  Recovery must derive this from the record rather than
// from whichever route-seed file happens to be active: a process can stop
// after NewReady and before the seed promotion/Complete writes.
func catalogSessionResumeJournalPath(handoff catalogSessionHandoff, base string) (string, error) {
	if !catalogSessionHandoffPathsValid(handoff, base) {
		return "", gateway.ErrReplicatedCatalogConflict
	}
	switch handoff.Phase {
	case catalogSessionHandoffPrepared, catalogSessionHandoffOldSettled:
		return handoff.OldJournalPath, nil
	case catalogSessionHandoffNewReady, catalogSessionHandoffComplete:
		return handoff.NextJournalPath, nil
	default:
		return "", gateway.ErrReplicatedCatalogConflict
	}
}

func catalogSessionHandoffFromRoutes(
	oldRoute, nextRoute gateway.ReplicatedRoute,
	oldSnapshot, nextSnapshot *gateway.Snapshot,
	oldJournalPath, nextJournalPath string,
	relation replication.RelationID,
	oldSessionGeneration uint64,
) (catalogSessionHandoff, error) {
	if oldSnapshot == nil || nextSnapshot == nil || oldJournalPath == "" || nextJournalPath == "" ||
		oldJournalPath == nextJournalPath || !gatewayCatalogRoutePairValid(oldRoute, nextRoute) ||
		!catalogSnapshotRouteMatches(oldSnapshot, oldRoute) ||
		!catalogSnapshotRouteMatches(nextSnapshot, nextRoute) {
		return catalogSessionHandoff{}, gateway.ErrReplicatedCatalog
	}
	oldBinding, err := gateway.NativeSessionJournalBinding(
		oldRoute, string(gateway.ReplicatedCatalogDistribution), string(gateway.ReplicatedCatalogShard),
		[]byte{replicatedCatalogControllerTenant}, relation, serviceauthz.CapabilityTopology,
	)
	if err != nil {
		return catalogSessionHandoff{}, err
	}
	nextBinding, err := gateway.NativeSessionJournalBinding(
		nextRoute, string(gateway.ReplicatedCatalogDistribution), string(gateway.ReplicatedCatalogShard),
		[]byte{replicatedCatalogControllerTenant}, relation, serviceauthz.CapabilityTopology,
	)
	if err != nil {
		return catalogSessionHandoff{}, err
	}
	oldDigest, err := gateway.CatalogSnapshotDigest(oldSnapshot)
	if err != nil {
		return catalogSessionHandoff{}, err
	}
	nextDigest, err := gateway.CatalogSnapshotDigest(nextSnapshot)
	if err != nil {
		return catalogSessionHandoff{}, err
	}
	transition := sha256.Sum256(append(append([]byte("vibedb/catalog-session-handoff/1\x00"), oldDigest[:]...), nextDigest[:]...))
	handoff := catalogSessionHandoff{
		Format: catalogSessionHandoffFormat, Phase: catalogSessionHandoffPrepared,
		OldSessionGeneration: oldSessionGeneration,
		Transition:           transition, OldGeneration: oldSnapshot.Generation(), NextGeneration: nextSnapshot.Generation(),
		OldSnapshotDigest: oldDigest, NextSnapshotDigest: nextDigest,
		OldBinding: oldBinding, NextBinding: nextBinding,
		OldJournalPath: oldJournalPath, NextJournalPath: nextJournalPath,
		CurrentJournalPath: oldJournalPath,
	}
	if !handoff.valid() {
		return catalogSessionHandoff{}, gateway.ErrReplicatedCatalog
	}
	return handoff, nil
}

func catalogSnapshotRouteMatches(snapshot *gateway.Snapshot, expected gateway.ReplicatedRoute) bool {
	if snapshot == nil {
		return false
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	actual, ok := snapshot.ResolveReplicatedRoute(
		gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard,
		replicas[:0],
	)
	return ok && sameReplicatedCatalogRoute(actual, expected)
}

func gatewayCatalogRoutePairValid(oldRoute, nextRoute gateway.ReplicatedRoute) bool {
	return gatewayCatalogRouteValid(oldRoute) && gatewayCatalogRouteValid(nextRoute) &&
		oldRoute.Group == nextRoute.Group && oldRoute.AllocationGeneration == nextRoute.AllocationGeneration &&
		oldRoute.RangeIdentity == nextRoute.RangeIdentity && oldRoute.LineageDigest == nextRoute.LineageDigest &&
		oldRoute.ForwardingRuleDigest == nextRoute.ForwardingRuleDigest &&
		gatewayCatalogCommandProgression(oldRoute, nextRoute)
}

func gatewayCatalogRouteValid(route gateway.ReplicatedRoute) bool {
	return route.Distribution == gateway.ReplicatedCatalogDistribution &&
		route.Shard == gateway.ReplicatedCatalogShard && len(route.Replicas) == gateway.ServingReplicaCount
}

func gatewayCatalogCommandProgression(oldRoute, nextRoute gateway.ReplicatedRoute) bool {
	return nextRoute.Command.ActivePolicyGeneration == oldRoute.Command.ActivePolicyGeneration &&
		nextRoute.Command.ProtectionEpoch == oldRoute.Command.ProtectionEpoch &&
		nextRoute.Command.SchemaGeneration == oldRoute.Command.SchemaGeneration &&
		nextRoute.Command.RelationManifestDigest == oldRoute.Command.RelationManifestDigest &&
		nextRoute.Command.ReplicaSetVersion >= oldRoute.Command.ReplicaSetVersion &&
		nextRoute.Command.OwnershipEpoch >= oldRoute.Command.OwnershipEpoch &&
		nextRoute.Command.RoutingVersion >= oldRoute.Command.RoutingVersion &&
		nextRoute.Command.RouteGeneration >= oldRoute.Command.RouteGeneration
}

func loadCatalogSessionHandoff(path string) (catalogSessionHandoff, bool, error) {
	if path == "" {
		return catalogSessionHandoff{}, false, gateway.ErrReplicatedCatalog
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return catalogSessionHandoff{}, false, nil
	}
	if err != nil || !info.Mode().IsRegular() {
		return catalogSessionHandoff{}, false, errors.Join(err, gateway.ErrReplicatedCatalog)
	}
	file, err := os.Open(path)
	if err != nil {
		return catalogSessionHandoff{}, false, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxCatalogSessionHandoffBytes+1))
	err = errors.Join(readErr, file.Close())
	if err != nil || len(raw) == 0 || len(raw) > maxCatalogSessionHandoffBytes {
		return catalogSessionHandoff{}, false, errors.Join(err, gateway.ErrReplicatedCatalog)
	}
	canonical, canonicalErr := vibejson.AppendCanonicalize(nil, raw)
	var handoff catalogSessionHandoff
	decodeErr := vibejson.Unmarshal(raw, &handoff)
	if canonicalErr != nil || decodeErr != nil || !bytes.Equal(canonical, raw) || !handoff.valid() {
		return catalogSessionHandoff{}, false, errors.Join(canonicalErr, decodeErr, gateway.ErrReplicatedCatalog)
	}
	return handoff, true, nil
}

func storeCatalogSessionHandoff(path string, handoff catalogSessionHandoff) error {
	if path == "" || !handoff.valid() {
		return gateway.ErrReplicatedCatalog
	}
	if info, statErr := os.Lstat(path); statErr == nil {
		if !info.Mode().IsRegular() {
			return gateway.ErrReplicatedCatalog
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	raw, err := vibejson.Marshal(&handoff)
	if err != nil {
		return err
	}
	raw, err = vibejson.AppendCanonicalize(nil, raw)
	if err != nil || len(raw) > maxCatalogSessionHandoffBytes {
		return errors.Join(err, gateway.ErrReplicatedCatalog)
	}
	temporary := path + ".pending"
	if info, statErr := os.Lstat(temporary); statErr == nil {
		if !info.Mode().IsRegular() {
			return gateway.ErrReplicatedCatalog
		}
		if err = os.Remove(temporary); err != nil {
			return err
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		_ = os.Remove(temporary)
		return err
	}
	if err = os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func catalogSessionHandoffPhaseAdvance(
	handoff catalogSessionHandoff, phase catalogSessionHandoffPhase, currentPath string,
) (catalogSessionHandoff, error) {
	if !handoff.valid() || phase != handoff.Phase && phase != handoff.Phase+1 ||
		phase > catalogSessionHandoffComplete {
		return catalogSessionHandoff{}, gateway.ErrReplicatedCatalog
	}
	handoff.Phase = phase
	if currentPath != "" {
		handoff.CurrentJournalPath = currentPath
	}
	if !handoff.valid() {
		return catalogSessionHandoff{}, gateway.ErrReplicatedCatalog
	}
	return handoff, nil
}

func validateCatalogSessionHandoffEvidence(
	handoff catalogSessionHandoff,
	oldRoute, nextRoute gateway.ReplicatedRoute,
	oldSnapshot, nextSnapshot *gateway.Snapshot,
	relation replication.RelationID,
) error {
	if !handoff.valid() || oldSnapshot == nil || nextSnapshot == nil ||
		handoff.OldGeneration != oldSnapshot.Generation() ||
		handoff.NextGeneration != nextSnapshot.Generation() ||
		!catalogSnapshotRouteMatches(oldSnapshot, oldRoute) ||
		!catalogSnapshotRouteMatches(nextSnapshot, nextRoute) {
		return gateway.ErrReplicatedCatalogConflict
	}
	expected, err := catalogSessionHandoffFromRoutes(
		oldRoute, nextRoute, oldSnapshot, nextSnapshot,
		handoff.OldJournalPath, handoff.NextJournalPath, relation, handoff.OldSessionGeneration,
	)
	if err != nil || expected.Transition != handoff.Transition ||
		expected.OldSnapshotDigest != handoff.OldSnapshotDigest ||
		expected.NextSnapshotDigest != handoff.NextSnapshotDigest ||
		expected.OldBinding != handoff.OldBinding || expected.NextBinding != handoff.NextBinding {
		return errors.Join(err, gateway.ErrReplicatedCatalogConflict)
	}
	return nil
}

// Decode only the canonical configured journal family. The session creation
// generation is independent of subsequent catalog heads with unchanged routes.
func catalogSessionJournalGeneration(base, path string) uint64 {
	if base == "" || filepath.Clean(base) != base {
		return 0
	}
	if path == base {
		return 1
	}
	suffix, found := strings.CutPrefix(path, base+".catalog-session-")
	if !found {
		return 0
	}
	generation, err := strconv.ParseUint(suffix, 10, 64)
	if err != nil || generation <= 1 || catalogSessionJournalPath(base, generation) != path {
		return 0
	}
	return generation
}
