package gateway

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/thesyncim/vibedb/distribution"
)

// The minimal authoritative catalog: one immutable snapshot generation of the
// routing configuration plus endpoint membership, a lock-free holder mirroring
// distribution.ManifestHolder, and durable JSON persistence using the same
// temp-file/sync/rename/directory-fsync recipe the SQL catalog uses.

// catalogVersion is the on-disk format version. A decoder rejects an unknown
// version rather than guessing an older or newer layout.
const catalogVersion = 1

// maxCatalogBytes bounds both an untrusted catalog read and a prospective
// rewrite, so a corrupt or hostile file cannot force an unbounded allocation.
const maxCatalogBytes = 16 << 20

// ErrCatalogTooLarge reports a catalog file or encoding beyond the size bound.
var ErrCatalogTooLarge = errors.New("gateway: catalog snapshot exceeds the maximum size")

// ErrInvalidCatalog is the sentinel every catalog snapshot malformation matches
// under errors.Is.
var ErrInvalidCatalog = errors.New("gateway: invalid catalog snapshot")

// CatalogError reports why a catalog snapshot was rejected. It wraps
// ErrInvalidCatalog.
type CatalogError struct {
	Reason string
}

func (e *CatalogError) Error() string { return "gateway: invalid catalog snapshot: " + e.Reason }

func (e *CatalogError) Unwrap() error { return ErrInvalidCatalog }

// Snapshot is one immutable, atomically published generation of authoritative
// cluster metadata. It embeds the routing configuration — distributions, table
// placements, and the manifests whose shards carry the per-shard ownership
// epochs — alongside the endpoint membership that resolves each opaque endpoint
// to a network address, and a monotonic publication generation.
//
// It has no exported mutators, and NewSnapshot defensively copies its inputs, so
// a published snapshot is safe to share across goroutines. The embedded
// ClusterConfig and its promoted read methods (Spec, Placement, Manifest,
// Validate) must be treated as read-only.
type Snapshot struct {
	distribution.ClusterConfig
	endpoints  map[distribution.EndpointID]string
	generation uint64
}

// NewSnapshot validates config and endpoints and returns an immutable snapshot
// pinned to generation. Validation reuses ClusterConfig.Validate and additionally
// requires every shard leader endpoint to resolve to an address, so a routed
// target always has a transport destination in the generation that routed it.
// Inputs are defensively copied.
func NewSnapshot(config distribution.ClusterConfig, endpoints map[distribution.EndpointID]string, generation uint64) (*Snapshot, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	for _, m := range config.Manifests {
		for i := 0; i < m.ShardCount(); i++ {
			shard, _ := m.ShardInfo(i)
			for _, ep := range shard.Leaders {
				if _, ok := endpoints[ep]; !ok {
					return nil, &CatalogError{Reason: fmt.Sprintf(
						"distribution %q shard %q leader endpoint %q resolves to no address",
						m.Distribution(), shard.ID, ep)}
				}
			}
		}
	}
	return &Snapshot{
		ClusterConfig: cloneConfig(config),
		endpoints:     maps.Clone(endpoints),
		generation:    generation,
	}, nil
}

// Generation reports this snapshot's monotonic publication generation.
func (s *Snapshot) Generation() uint64 { return s.generation }

// OwnershipEpoch returns the fencing epoch the named shard is served under in
// this generation, read from the distribution's manifest — the single source of
// truth for per-shard ownership. ok is false when the distribution or shard is
// absent.
func (s *Snapshot) OwnershipEpoch(dist distribution.DistributionName, shard distribution.ShardID) (distribution.OwnershipEpoch, bool) {
	m, ok := s.Manifest(dist)
	if !ok {
		return 0, false
	}
	for i := 0; i < m.ShardCount(); i++ {
		info, _ := m.ShardInfo(i)
		if info.ID == shard {
			return info.Epoch, true
		}
	}
	return 0, false
}

// cloneConfig returns a defensive copy of c. The manifests are already
// immutable, so only the pointer slice and the mutable placement columns are
// cloned.
func cloneConfig(c distribution.ClusterConfig) distribution.ClusterConfig {
	out := distribution.ClusterConfig{
		Distributions: slices.Clone(c.Distributions),
		Manifests:     slices.Clone(c.Manifests),
	}
	if c.Placements != nil {
		out.Placements = make([]distribution.TablePlacement, len(c.Placements))
		for i, p := range c.Placements {
			p.Columns = slices.Clone(p.Columns)
			out.Placements[i] = p
		}
	}
	return out
}

// CatalogHolder publishes immutable Snapshot generations for lock-free
// concurrent reads. A reader always observes one whole generation — the old or
// the new — never a mixed or partially published state, because each Snapshot is
// immutable and only its pointer is swapped. It mirrors
// distribution.ManifestHolder.
type CatalogHolder struct {
	ptr atomic.Pointer[Snapshot]
}

// NewCatalogHolder returns a holder seeded with initial, which may be nil.
func NewCatalogHolder(initial *Snapshot) *CatalogHolder {
	h := &CatalogHolder{}
	if initial != nil {
		h.ptr.Store(initial)
	}
	return h
}

// Publish atomically installs s as the current generation, unconditionally.
func (h *CatalogHolder) Publish(s *Snapshot) {
	h.ptr.Store(s)
}

// PublishNewer installs s only if it is a strictly newer generation than the
// current one (or none is published), and reports whether it did. It is the
// strongly ordered publication primitive: concurrent publishers converge on the
// highest generation and a stale republish is refused, while a reader still
// observes one whole generation.
func (h *CatalogHolder) PublishNewer(s *Snapshot) bool {
	for {
		cur := h.ptr.Load()
		if cur != nil && s.generation <= cur.generation {
			return false
		}
		if h.ptr.CompareAndSwap(cur, s) {
			return true
		}
	}
}

// Current returns the most recently published Snapshot, or nil if none has been
// published. The read path takes no lock; a caller pins one generation for a
// whole operation by reading Current once.
func (h *CatalogHolder) Current() *Snapshot {
	return h.ptr.Load()
}

// The on-disk catalog format. It is a versioned JSON document; keyspace points
// are big-endian hex so the file is inspectable and platform-independent, and
// the ordering of every collection is deterministic so equal snapshots persist
// to identical bytes.

type persistedCatalog struct {
	Version       int                     `json:"version"`
	Generation    uint64                  `json:"generation"`
	Distributions []persistedDistribution `json:"distributions"`
	Placements    []persistedPlacement    `json:"placements,omitempty"`
	Manifests     []persistedManifest     `json:"manifests"`
	Endpoints     []persistedEndpoint     `json:"endpoints"`
}

type persistedDistribution struct {
	Name          string `json:"name"`
	Arity         int    `json:"arity"`
	MapperVersion uint32 `json:"mapper_version"`
}

type persistedPlacement struct {
	Table        string   `json:"table"`
	Distribution string   `json:"distribution"`
	Columns      []string `json:"columns"`
}

type persistedManifest struct {
	Distribution string           `json:"distribution"`
	Version      uint64           `json:"version"`
	Shards       []persistedShard `json:"shards"`
}

type persistedShard struct {
	ID      string   `json:"id"`
	Start   string   `json:"start"`
	End     string   `json:"end,omitempty"`
	EndMax  bool     `json:"end_max,omitempty"`
	Leaders []string `json:"leaders"`
	Epoch   uint64   `json:"epoch,omitempty"`
}

type persistedEndpoint struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// toPersisted renders s in the on-disk form with every collection in a
// deterministic order.
func toPersisted(s *Snapshot) persistedCatalog {
	pc := persistedCatalog{Version: catalogVersion, Generation: s.generation}
	for _, d := range s.Distributions {
		pc.Distributions = append(pc.Distributions, persistedDistribution{
			Name: string(d.Name), Arity: d.Arity, MapperVersion: uint32(d.MapperVersion),
		})
	}
	for _, p := range s.Placements {
		pc.Placements = append(pc.Placements, persistedPlacement{
			Table: p.Table, Distribution: string(p.Distribution), Columns: p.Columns,
		})
	}
	for _, m := range s.Manifests {
		pm := persistedManifest{Distribution: string(m.Distribution()), Version: uint64(m.Version())}
		for i := 0; i < m.ShardCount(); i++ {
			shard, _ := m.ShardInfo(i)
			ps := persistedShard{
				ID:     string(shard.ID),
				Start:  hex.EncodeToString(shard.Range.Start[:]),
				EndMax: shard.Range.End.Max,
				Epoch:  uint64(shard.Epoch),
			}
			if !shard.Range.End.Max {
				ps.End = hex.EncodeToString(shard.Range.End.Point[:])
			}
			for _, ep := range shard.Leaders {
				ps.Leaders = append(ps.Leaders, string(ep))
			}
			pm.Shards = append(pm.Shards, ps)
		}
		pc.Manifests = append(pc.Manifests, pm)
	}
	ids := make([]string, 0, len(s.endpoints))
	for id := range s.endpoints {
		ids = append(ids, string(id))
	}
	slices.Sort(ids)
	for _, id := range ids {
		pc.Endpoints = append(pc.Endpoints, persistedEndpoint{ID: id, Address: s.endpoints[distribution.EndpointID(id)]})
	}
	return pc
}

// SaveSnapshot durably writes s to path with a crash-safe recipe: encode,
// bound the size, write a sibling temporary file, fsync it, rename it over the
// destination, then fsync the directory. A failure before the rename leaves the
// previous catalog intact and removes the temporary file.
func SaveSnapshot(path string, s *Snapshot) error {
	if s == nil {
		return errors.New("gateway: SaveSnapshot requires a non-nil snapshot")
	}
	raw, err := json.MarshalIndent(toPersisted(s), "", "  ")
	if err != nil {
		return err
	}
	if len(raw) > maxCatalogBytes {
		return fmt.Errorf("%w: encoded catalog is %d bytes, maximum is %d",
			ErrCatalogTooLarge, len(raw), maxCatalogBytes)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	ok = true
	return fsyncDir(dir)
}

// LoadSnapshot reads and validates the catalog persisted at path, returning the
// same immutable snapshot generation SaveSnapshot wrote. The read is size-bounded
// and every manifest is re-validated through NewManifest, so a corrupt or
// inconsistent file fails closed rather than routing.
func LoadSnapshot(path string) (*Snapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() > maxCatalogBytes {
		_ = file.Close()
		return nil, fmt.Errorf("%w: %s is %d bytes, maximum is %d",
			ErrCatalogTooLarge, path, info.Size(), maxCatalogBytes)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxCatalogBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(raw) > maxCatalogBytes {
		return nil, fmt.Errorf("%w: %s grew beyond %d bytes while it was read",
			ErrCatalogTooLarge, path, maxCatalogBytes)
	}
	var pc persistedCatalog
	if err := json.Unmarshal(raw, &pc); err != nil {
		return nil, err
	}
	if pc.Version != catalogVersion {
		return nil, &CatalogError{Reason: fmt.Sprintf("unsupported catalog version %d", pc.Version)}
	}
	config, endpoints, err := pc.toConfig()
	if err != nil {
		return nil, err
	}
	return NewSnapshot(config, endpoints, pc.Generation)
}

// toConfig reconstructs the routing configuration and endpoint membership from
// the on-disk form, validating every manifest through NewManifest.
func (pc persistedCatalog) toConfig() (distribution.ClusterConfig, map[distribution.EndpointID]string, error) {
	var config distribution.ClusterConfig
	for _, d := range pc.Distributions {
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{
			Name:          distribution.DistributionName(d.Name),
			Arity:         d.Arity,
			MapperVersion: distribution.MapperVersion(d.MapperVersion),
		})
	}
	for _, p := range pc.Placements {
		config.Placements = append(config.Placements, distribution.TablePlacement{
			Table:        p.Table,
			Distribution: distribution.DistributionName(p.Distribution),
			Columns:      slices.Clone(p.Columns),
		})
	}
	for _, pm := range pc.Manifests {
		shards := make([]distribution.Shard, len(pm.Shards))
		for i, ps := range pm.Shards {
			shard, err := ps.toShard()
			if err != nil {
				return config, nil, err
			}
			shards[i] = shard
		}
		m, err := distribution.NewManifest(
			distribution.DistributionName(pm.Distribution),
			distribution.RoutingVersion(pm.Version),
			shards,
		)
		if err != nil {
			return config, nil, err
		}
		config.Manifests = append(config.Manifests, m)
	}
	endpoints := make(map[distribution.EndpointID]string, len(pc.Endpoints))
	for _, e := range pc.Endpoints {
		if e.ID == "" {
			return config, nil, &CatalogError{Reason: "endpoint has an empty id"}
		}
		if _, dup := endpoints[distribution.EndpointID(e.ID)]; dup {
			return config, nil, &CatalogError{Reason: "duplicate endpoint id " + e.ID}
		}
		endpoints[distribution.EndpointID(e.ID)] = e.Address
	}
	return config, endpoints, nil
}

// toShard reconstructs one manifest shard, decoding its big-endian hex keyspace
// bounds.
func (ps persistedShard) toShard() (distribution.Shard, error) {
	start, err := decodePoint(ps.Start)
	if err != nil {
		return distribution.Shard{}, &CatalogError{Reason: "shard " + ps.ID + " start: " + err.Error()}
	}
	end := distribution.KeyspaceEnd{Max: ps.EndMax}
	if !ps.EndMax {
		point, err := decodePoint(ps.End)
		if err != nil {
			return distribution.Shard{}, &CatalogError{Reason: "shard " + ps.ID + " end: " + err.Error()}
		}
		end.Point = point
	}
	leaders := make([]distribution.EndpointID, len(ps.Leaders))
	for i, ep := range ps.Leaders {
		leaders[i] = distribution.EndpointID(ep)
	}
	return distribution.Shard{
		ID:      distribution.ShardID(ps.ID),
		Range:   distribution.KeyRange{Start: start, End: end},
		Leaders: leaders,
		Epoch:   distribution.OwnershipEpoch(ps.Epoch),
	}, nil
}

// decodePoint parses a big-endian hex keyspace point of exactly KeyspaceWidth
// bytes.
func decodePoint(s string) (distribution.KeyspacePoint, error) {
	var p distribution.KeyspacePoint
	b, err := hex.DecodeString(s)
	if err != nil {
		return p, err
	}
	if len(b) != distribution.KeyspaceWidth {
		return p, fmt.Errorf("keyspace point is %d bytes, want %d", len(b), distribution.KeyspaceWidth)
	}
	copy(p[:], b)
	return p, nil
}
