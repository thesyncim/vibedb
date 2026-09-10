package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	protocol "github.com/thesyncim/vibedb/shardcontrol"
	"github.com/thesyncim/vibejson"
)

const (
	maxRF3ManifestBytes       = 4 << 20
	maxRF3ManifestStringBytes = 4 << 10
	rf3ManifestMembers        = 3
	maxRF3ManifestGroups      = 64
)

var errInvalidRF3Manifest = errors.New("vibedb-shard: invalid RF3 manifest")

type rf3Manifest struct {
	NodeLog *rf3NodeLogManifest
	// NodeIncarnation is explicit for grouped physical-node manifests. It is
	// mandatory for the empty-capacity form and never inferred from a group
	// roster.
	NodeIncarnation     uint64
	reloadPath          string
	reloadSignals       <-chan os.Signal
	Digest              [sha256.Size]byte
	WAL                 rf3ManifestWAL
	SQL                 rf3ManifestSQL
	Listeners           rf3ManifestListeners
	TLS                 rf3ManifestTLS
	AuthorizationPolicy string
	GatewaySeeds        []nodecontrol.BootstrapGatewaySeed
	ReplicaControl      rf3ManifestReplicaControl
	SplitControl        rf3ManifestSplitControl
	Gateway             *rf3ManifestGateway
	Route               rf3ManifestGroupRoute
	DevelopmentOnly     bool
	ReadAuthority       *rf3ManifestReadAuthority
	Members             [rf3ManifestMembers]rf3ManifestMember
	MemberCount         uint8
	EnrolledTarget      *rf3ManifestEnrolledTarget
	Groups              []rf3ManifestGroup
}

type rf3ManifestGroup struct {
	WAL            rf3ManifestWAL
	SQL            rf3ManifestSQL
	Route          rf3ManifestGroupRoute
	ChildRegistry  rf3ManifestSplitChildRegistry
	Members        [rf3ManifestMembers]rf3ManifestMember
	MemberCount    uint8
	EnrolledTarget *rf3ManifestEnrolledTarget
}

// rf3ManifestReadAuthority is the persisted, all-voter feature contract.
// The section is part of the strict startup manifest so a feature-aware
// binary cannot silently ignore an enabled authority policy. The restart
// marker is derived from the member root and is checked before Raft starts;
// deployment must keep older binaries from restoring pre-enrollment files.
type rf3ManifestReadAuthority struct {
	Enabled              bool                         `json:"enabled"`
	FeatureVersion       uint32                       `json:"feature_version"`
	PolicyVersion        uint32                       `json:"policy_version"`
	MaxGrantMillis       uint64                       `json:"max_grant_millis"`
	ClockRatePPM         uint32                       `json:"clock_rate_ppm"`
	RoundingMarginMillis uint64                       `json:"rounding_margin_millis"`
	Voters               []uint64                     `json:"voters"`
	Capabilities         []rf3ManifestVoterCapability `json:"capabilities"`
}

type rf3ManifestVoterCapability struct {
	MemberID      uint64 `json:"member_id"`
	PolicyVersion uint32 `json:"policy_version"`
	Enabled       bool   `json:"enabled"`
}

func (manifest rf3Manifest) groupBundles() []rf3ManifestGroup {
	// A node log makes the grouped grammar unambiguous, including the valid
	// empty-capacity form.  Legacy manifests have no NodeLog and retain their
	// single-group fallback for compatibility.
	if manifest.NodeLog != nil || len(manifest.Groups) != 0 {
		return manifest.Groups
	}
	return []rf3ManifestGroup{{WAL: manifest.WAL, SQL: manifest.SQL,
		Route: manifest.Route, ChildRegistry: manifest.SplitControl.ChildRegistry,
		Members: manifest.Members, MemberCount: uint8(len(manifest.memberRoster())),
		EnrolledTarget: manifest.EnrolledTarget}}
}

func (manifest rf3Manifest) withGroup(group rf3ManifestGroup) rf3Manifest {
	split := manifest.SplitControl
	split.ChildRegistry = group.ChildRegistry
	selected := rf3Manifest{NodeLog: manifest.NodeLog, NodeIncarnation: manifest.NodeIncarnation, Digest: manifest.Digest, WAL: group.WAL, SQL: group.SQL, Listeners: manifest.Listeners,
		TLS: manifest.TLS, AuthorizationPolicy: manifest.AuthorizationPolicy,
		GatewaySeeds:   append([]nodecontrol.BootstrapGatewaySeed(nil), manifest.GatewaySeeds...),
		ReplicaControl: manifest.ReplicaControl,
		SplitControl:   split, Route: group.Route,
		Gateway:         manifest.Gateway,
		DevelopmentOnly: manifest.DevelopmentOnly, ReadAuthority: manifest.ReadAuthority, Members: group.Members,
		MemberCount: group.MemberCount, EnrolledTarget: group.EnrolledTarget}
	if manifest.NodeLog != nil {
		selected.Groups = []rf3ManifestGroup{group}
	}
	return selected
}

type rf3ManifestSplitControl struct {
	JournalPath   string
	MaxRecords    int
	MaxFileBytes  int64
	Grants        []protocol.ActionGrant
	MaxOperations int
	ChildRegistry rf3ManifestSplitChildRegistry
}

func (control rf3ManifestSplitControl) operationLimit() int {
	if control.MaxOperations != 0 {
		return control.MaxOperations
	}
	return control.ChildRegistry.MaxOperations
}

type rf3ManifestGroupRoute struct {
	Group                raftmember.GroupKey
	Distribution         string
	Shard                string
	AllocationGeneration uint64
	MemberID             uint64
	StoreID              [16]byte
	MemberRoot           string
	SplitRuntimeRoot     string
	MembershipGrantPath  string
}

func (manifest rf3Manifest) memberRoster() []rf3ManifestMember {
	count := manifest.MemberCount
	if count == 0 {
		// In-memory RF3 fixtures and callers predating the explicit count always
		// own the full fixed roster. Parsed manifests never rely on this fallback.
		count = rf3ManifestMembers
	}
	return manifest.Members[:count]
}

// rf3ManifestReplicaControl fixes every local disk and memory bound used by
// replica movement. Source fields are retained even before a target is
// enrolled so provisioning does not rewrite the control grammar mid-move.
type rf3ManifestReplicaControl struct {
	ActionJournalPath      string
	MaxActionRecords       int
	SourceDataRoot         string
	SourceJournalPath      string
	MaxSourceRecords       int
	SourceRepositoryPath   string
	MaxSourceArtifacts     int
	MaxSourceArtifactBytes uint64
	MaxSourceDiskBytes     uint64
	SourceChunkBytes       uint32
	MaxSourceConcurrent    int
	Migration              rf3ManifestMigrationBudget
}

// rf3ManifestMigrationBudget is the canonical wire representation of the
// node-wide transfer budget. It intentionally lives below replica_control so
// a grouped RF3 manifest has exactly one copy for the physical node rather
// than one allocation per group.
type rf3ManifestMigrationBudget struct {
	MaxActive      int                  `json:"max_active"`
	CPU            rf3ManifestRateLimit `json:"cpu"`
	DiskRead       rf3ManifestRateLimit `json:"disk_read"`
	DiskWrite      rf3ManifestRateLimit `json:"disk_write"`
	NetworkSend    rf3ManifestRateLimit `json:"network_send"`
	NetworkReceive rf3ManifestRateLimit `json:"network_receive"`
}

type rf3ManifestRateLimit struct {
	BytesPerSecond uint64 `json:"bytes_per_second"`
	BurstBytes     uint64 `json:"burst_bytes"`
}

func defaultRF3ManifestMigrationBudget() rf3ManifestMigrationBudget {
	return rf3ManifestMigrationBudgetFromConfig(migrationbudget.DefaultConfig())
}

func rf3ManifestMigrationBudgetFromConfig(config migrationbudget.Config) rf3ManifestMigrationBudget {
	return rf3ManifestMigrationBudget{
		MaxActive:      config.MaxActive,
		CPU:            rf3ManifestRateLimit{BytesPerSecond: config.CPU.BytesPerSecond, BurstBytes: config.CPU.BurstBytes},
		DiskRead:       rf3ManifestRateLimit{BytesPerSecond: config.DiskRead.BytesPerSecond, BurstBytes: config.DiskRead.BurstBytes},
		DiskWrite:      rf3ManifestRateLimit{BytesPerSecond: config.DiskWrite.BytesPerSecond, BurstBytes: config.DiskWrite.BurstBytes},
		NetworkSend:    rf3ManifestRateLimit{BytesPerSecond: config.NetworkSend.BytesPerSecond, BurstBytes: config.NetworkSend.BurstBytes},
		NetworkReceive: rf3ManifestRateLimit{BytesPerSecond: config.NetworkReceive.BytesPerSecond, BurstBytes: config.NetworkReceive.BurstBytes},
	}
}

func (budget rf3ManifestMigrationBudget) config() migrationbudget.Config {
	return migrationbudget.Config{
		MaxActive:      budget.MaxActive,
		CPU:            migrationbudget.RateLimit{BytesPerSecond: budget.CPU.BytesPerSecond, BurstBytes: budget.CPU.BurstBytes},
		DiskRead:       migrationbudget.RateLimit{BytesPerSecond: budget.DiskRead.BytesPerSecond, BurstBytes: budget.DiskRead.BurstBytes},
		DiskWrite:      migrationbudget.RateLimit{BytesPerSecond: budget.DiskWrite.BytesPerSecond, BurstBytes: budget.DiskWrite.BurstBytes},
		NetworkSend:    migrationbudget.RateLimit{BytesPerSecond: budget.NetworkSend.BytesPerSecond, BurstBytes: budget.NetworkSend.BurstBytes},
		NetworkReceive: migrationbudget.RateLimit{BytesPerSecond: budget.NetworkReceive.BytesPerSecond, BurstBytes: budget.NetworkReceive.BurstBytes},
	}
}

type rf3ManifestWAL struct {
	Path            string
	KeyID           string
	KeyMaterialPath string
	Options         raftstore.Options
}

type rf3ManifestSQL struct {
	Path              string
	IdentityPath      string
	ApplyIdentityPath string
}

type rf3ManifestListeners struct {
	Peer     string `json:"peer"`
	Native   string `json:"native"`
	Snapshot string `json:"snapshot"`
	Control  string `json:"control"`
}

type rf3ManifestTLS struct {
	Certificate string               `json:"certificate"`
	Key         string               `json:"key"`
	Roots       string               `json:"roots"`
	IdentityOID string               `json:"identity_oid"`
	PeerKeys    []rf3ManifestPeerKey `json:"peer_keys,omitempty"`
}

type rf3ManifestMember struct {
	MemberID      uint64
	NodeID        rafttransport.NodeID
	PeerAddress   string
	StoreID       [16]byte
	NativeAddress string
}

// rf3ManifestEnrolledTarget is the one replacement identity provisioning has
// enrolled beside the stable serving RF3. Keeping it separate from Members is
// intentional: merely retaining this descriptor must not make the target a
// voter, a transport member, or a data-serving endpoint.
type rf3ManifestEnrolledTarget struct {
	MemberID        uint64
	NodeID          rafttransport.NodeID
	StoreID         [16]byte
	NodeIncarnation uint64
	PeerAddress     string
	NativeAddress   string
	SnapshotAddress string
	ControlAddress  string
}

// loadRF3Manifest reads one exact, bounded startup manifest. The grammar is
// deliberately order-sensitive: accepting aliases, reordered members, or
// escaped field names would give provisioning systems more than one spelling
// for the same serving authority.
func loadRF3Manifest(path string) (rf3Manifest, error) {
	data, err := readRF3ManifestFile(path)
	if err != nil {
		return rf3Manifest{}, err
	}
	return parseRF3Manifest(data)
}

func readRF3ManifestFile(path string) ([]byte, error) {
	if path == "" {
		return nil, errInvalidRF3Manifest
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Join(errInvalidRF3Manifest, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > maxRF3ManifestBytes {
		return nil, errors.Join(errInvalidRF3Manifest, err)
	}
	data := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, errors.Join(errInvalidRF3Manifest, err)
	}
	var trailing [1]byte
	if count, readErr := file.Read(trailing[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return nil, errInvalidRF3Manifest
	}
	return data, nil
}

func parseRF3Manifest(data []byte) (rf3Manifest, error) {
	if len(data) == 0 || len(data) > maxRF3ManifestBytes {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	document, err := vibejson.ParseOptions(data, vibejson.Options{ZeroCopy: true, MaxDepth: 7})
	if err != nil {
		return rf3Manifest{}, errors.Join(errInvalidRF3Manifest, err)
	}
	fields, ok := document.Node().ObjectIter()
	if !ok {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	var manifest rf3Manifest
	key, node, present := fields.Next()
	if !present {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	if bytes.Equal(key.Raw().Bytes(), []byte(`"node_log"`)) {
		manifest.NodeLog, err = parseRF3NodeLogManifest(node)
		if err != nil {
			return rf3Manifest{}, err
		}
		key, node, present = fields.Next()
		if !present {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		if bytes.Equal(key.Raw().Bytes(), []byte(`"node_incarnation"`)) {
			value, parseErr := rf3ManifestPositiveUint64(node)
			if parseErr != nil {
				return rf3Manifest{}, errInvalidRF3Manifest
			}
			manifest.NodeIncarnation = value
			key, node, present = fields.Next()
			if !present {
				return rf3Manifest{}, errInvalidRF3Manifest
			}
		}
	}
	if bytes.Equal(key.Raw().Bytes(), []byte(`"listeners"`)) {
		if manifest.Listeners, err = parseRF3ManifestListeners(node); err != nil {
			return rf3Manifest{}, err
		}
		node, err = nextRF3Field(&fields, `"tls"`)
		if err != nil {
			return rf3Manifest{}, err
		}
		if manifest.TLS, err = parseRF3ManifestTLS(node); err != nil {
			return rf3Manifest{}, err
		}
		node, err = nextRF3Field(&fields, `"authorization_policy"`)
		if err != nil {
			return rf3Manifest{}, err
		}
		if manifest.AuthorizationPolicy, err = rf3ManifestString(node, maxRF3ManifestStringBytes); err != nil {
			return rf3Manifest{}, err
		}
		key, node, present = fields.Next()
		if !present {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		if bytes.Equal(key.Raw().Bytes(), []byte(`"bootstrap_gateway_seeds"`)) {
			if manifest.GatewaySeeds, err = parseRF3BootstrapGatewaySeeds(node); err != nil {
				return rf3Manifest{}, err
			}
			key, node, present = fields.Next()
			if !present {
				return rf3Manifest{}, errInvalidRF3Manifest
			}
		}
		if !bytes.Equal(key.Raw().Bytes(), []byte(`"replica_control"`)) {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		if manifest.ReplicaControl, err = parseRF3ManifestReplicaControl(node); err != nil {
			return rf3Manifest{}, err
		}
		node, err = nextRF3Field(&fields, `"split_control"`)
		if err != nil {
			return rf3Manifest{}, err
		}
		if manifest.SplitControl, err = parseRF3ManifestSplitControlMode(node, true); err != nil {
			return rf3Manifest{}, err
		}
		key, node, present = fields.Next()
		if !present {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		if bytes.Equal(key.Raw().Bytes(), []byte(`"read_authority"`)) {
			manifest.ReadAuthority, err = parseRF3ManifestReadAuthority(node)
			if err != nil {
				return rf3Manifest{}, err
			}
			key, node, present = fields.Next()
			if !present {
				return rf3Manifest{}, errInvalidRF3Manifest
			}
		}
		if bytes.Equal(key.Raw().Bytes(), []byte(`"gateway"`)) {
			manifest.Gateway, err = parseRF3ManifestGateway(node)
			if err != nil {
				return rf3Manifest{}, err
			}
			node, err = nextRF3Field(&fields, `"groups"`)
		} else if !bytes.Equal(key.Raw().Bytes(), []byte(`"groups"`)) {
			err = errInvalidRF3Manifest
		}
		if err != nil {
			return rf3Manifest{}, err
		}
		if manifest.Groups, err = parseRF3ManifestGroups(node, manifest.NodeLog); err != nil {
			return rf3Manifest{}, err
		}
		if len(manifest.Groups) == 0 && (manifest.NodeIncarnation == 0 || len(manifest.GatewaySeeds) == 0) {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		if err := validateRF3ReadAuthority(manifest.ReadAuthority, manifest.Groups, false); err != nil {
			return rf3Manifest{}, err
		}
		controlPaths := [...]string{
			manifest.ReplicaControl.ActionJournalPath, manifest.ReplicaControl.SourceJournalPath,
			manifest.ReplicaControl.SourceRepositoryPath, manifest.SplitControl.JournalPath,
		}
		for _, group := range manifest.Groups {
			if group.ChildRegistry.MaxOperations > manifest.SplitControl.MaxOperations {
				return rf3Manifest{}, errInvalidRF3Manifest
			}
			for _, path := range [...]string{group.WAL.Path, group.WAL.KeyMaterialPath, group.SQL.Path, group.SQL.IdentityPath, group.SQL.ApplyIdentityPath,
				group.ChildRegistry.Root, group.ChildRegistry.StaticBootstrapPath} {
				for _, control := range controlPaths {
					if path == control {
						return rf3Manifest{}, errInvalidRF3Manifest
					}
				}
			}
		}
		if _, _, extra := fields.Next(); extra {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		manifest.Digest = sha256.Sum256(data)
		return manifest, nil
	}
	if !bytes.Equal(key.Raw().Bytes(), []byte(`"wal"`)) {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	if manifest.WAL, err = parseRF3ManifestWAL(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"sql"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.SQL, err = parseRF3ManifestSQL(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"route"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.Route, err = parseRF3ManifestGroupRoute(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"listeners"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.Listeners, err = parseRF3ManifestListeners(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"tls"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.TLS, err = parseRF3ManifestTLS(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"authorization_policy"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.AuthorizationPolicy, err = rf3ManifestString(node, maxRF3ManifestStringBytes); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"replica_control"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.ReplicaControl, err = parseRF3ManifestReplicaControl(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"split_control"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.SplitControl, err = parseRF3ManifestSplitControl(node); err != nil {
		return rf3Manifest{}, err
	}
	if manifest.SplitControl.ChildRegistry.Root != filepath.Join(
		manifest.Route.MemberRoot, "split-children",
	) {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	key, node, present = fields.Next()
	if !present {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	if bytes.Equal(key.Raw().Bytes(), []byte(`"read_authority"`)) {
		manifest.ReadAuthority, err = parseRF3ManifestReadAuthority(node)
		if err != nil {
			return rf3Manifest{}, err
		}
		key, node, present = fields.Next()
		if !present {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
	}
	if bytes.Equal(key.Raw().Bytes(), []byte(`"development_only"`)) {
		development, ok := node.Bool()
		if !ok || !development {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		manifest.DevelopmentOnly = true
		node, err = nextRF3Field(&fields, `"members"`)
	} else if !bytes.Equal(key.Raw().Bytes(), []byte(`"members"`)) {
		err = errInvalidRF3Manifest
	}
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.Members, manifest.MemberCount, err = parseRF3ManifestMembers(node, manifest.DevelopmentOnly); err != nil {
		return rf3Manifest{}, err
	}
	if err := validateRF3ReadAuthority(manifest.ReadAuthority, []rf3ManifestGroup{{Members: manifest.Members, MemberCount: manifest.MemberCount}}, manifest.DevelopmentOnly); err != nil {
		return rf3Manifest{}, err
	}
	key, node, present = fields.Next()
	if present {
		if !bytes.Equal(key.Raw().Bytes(), []byte(`"enrolled_target"`)) {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		if manifest.DevelopmentOnly {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		target, err := parseRF3ManifestEnrolledTarget(node, manifest.memberRoster())
		if err != nil {
			return rf3Manifest{}, err
		}
		manifest.EnrolledTarget = &target
	}
	if _, _, extra := fields.Next(); extra {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	if err := validateRF3ManifestGroupRoute(rf3ManifestGroup{
		WAL: manifest.WAL, SQL: manifest.SQL, Route: manifest.Route,
		Members: manifest.Members, MemberCount: manifest.MemberCount,
		EnrolledTarget: manifest.EnrolledTarget,
	}); err != nil {
		return rf3Manifest{}, err
	}
	if !manifest.DevelopmentOnly || manifest.SplitControl.ChildRegistry.MemberCount != rf3ManifestMembers {
		if err := validateRF3SplitChildRegistryRoster(
			manifest.SplitControl.ChildRegistry,
			[]rf3ManifestGroup{{Members: manifest.Members, MemberCount: manifest.MemberCount}},
		); err != nil {
			return rf3Manifest{}, err
		}
	}
	manifest.Digest = sha256.Sum256(data)
	return manifest, nil
}

func parseRF3ManifestGroups(node vibejson.Node, nodeLog *rf3NodeLogManifest) ([]rf3ManifestGroup, error) {
	count, ok := node.ArrayLen()
	if !ok || count > maxRF3ManifestGroups || count == 0 && nodeLog == nil {
		return nil, errInvalidRF3Manifest
	}
	iter, _ := node.ArrayIter()
	groups := make([]rf3ManifestGroup, 0, count)
	paths := make(map[string]struct{}, count*5)
	if nodeLog != nil {
		paths[nodeLog.Path], paths[nodeLog.KeyMaterialPath] = struct{}{}, struct{}{}
	}
	groupKeys := make(map[raftmember.GroupKey]struct{}, count)
	nodes := make(map[rafttransport.NodeID]string, rf3ManifestMembers)
	addresses := make(map[string]rafttransport.NodeID, rf3ManifestMembers)
	nativeAddresses := make(map[rafttransport.NodeID]string, rf3ManifestMembers)
	nativeOwners := make(map[string]rafttransport.NodeID, rf3ManifestMembers)
	for index := 0; index < count; index++ {
		value, present := iter.Next()
		if !present {
			return nil, errInvalidRF3Manifest
		}
		group, err := parseRF3ManifestGroup(value)
		if err != nil {
			return nil, err
		}
		if _, duplicate := groupKeys[group.Route.Group]; duplicate {
			return nil, errInvalidRF3Manifest
		}
		groupKeys[group.Route.Group] = struct{}{}
		sharedKey := nodeLog != nil && group.WAL.KeyID == nodeLog.KeyID && group.WAL.KeyMaterialPath == nodeLog.KeyMaterialPath
		if !sharedKey {
			if _, duplicate := paths[group.WAL.KeyMaterialPath]; duplicate {
				return nil, errInvalidRF3Manifest
			}
			paths[group.WAL.KeyMaterialPath] = struct{}{}
		}
		for _, path := range [...]string{
			group.WAL.Path, group.SQL.Path,
			group.SQL.IdentityPath, group.SQL.ApplyIdentityPath,
			group.Route.MemberRoot, group.Route.SplitRuntimeRoot,
			group.Route.MembershipGrantPath,
			group.ChildRegistry.Root, group.ChildRegistry.StaticBootstrapPath,
		} {
			if _, duplicate := paths[path]; duplicate {
				return nil, errInvalidRF3Manifest
			}
			paths[path] = struct{}{}
		}
		for _, member := range group.Members {
			if address, found := nodes[member.NodeID]; found && address != member.PeerAddress {
				return nil, errInvalidRF3Manifest
			}
			if node, found := addresses[member.PeerAddress]; found && node != member.NodeID {
				return nil, errInvalidRF3Manifest
			}
			if member.NativeAddress != "" {
				if address, found := nativeAddresses[member.NodeID]; found && address != member.NativeAddress {
					return nil, errInvalidRF3Manifest
				}
				if node, found := nativeOwners[member.NativeAddress]; found && node != member.NodeID {
					return nil, errInvalidRF3Manifest
				}
				nativeAddresses[member.NodeID], nativeOwners[member.NativeAddress] = member.NativeAddress, member.NodeID
			}
			nodes[member.NodeID], addresses[member.PeerAddress] = member.PeerAddress, member.NodeID
		}
		groups = append(groups, group)
	}
	if _, extra := iter.Next(); extra {
		return nil, errInvalidRF3Manifest
	}
	return groups, nil
}

func parseRF3ManifestGroup(node vibejson.Node) (rf3ManifestGroup, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestGroup{}, errInvalidRF3Manifest
	}
	var group rf3ManifestGroup
	value, err := nextRF3Field(&fields, `"wal"`)
	if err != nil {
		return group, err
	}
	if group.WAL, err = parseRF3ManifestWAL(value); err != nil {
		return group, err
	}
	value, err = nextRF3Field(&fields, `"sql"`)
	if err != nil {
		return group, err
	}
	if group.SQL, err = parseRF3ManifestSQL(value); err != nil {
		return group, err
	}
	value, err = nextRF3Field(&fields, `"route"`)
	if err != nil {
		return group, err
	}
	if group.Route, err = parseRF3ManifestGroupRoute(value); err != nil {
		return group, err
	}
	value, err = nextRF3Field(&fields, `"child_registry"`)
	if err != nil {
		return group, err
	}
	if group.ChildRegistry, err = parseRF3ManifestSplitChildRegistry(value); err != nil {
		return group, err
	}
	if group.ChildRegistry.Root != filepath.Join(group.Route.MemberRoot, "split-children") {
		return group, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"members"`)
	if err != nil {
		return group, err
	}
	var count uint8
	if group.Members, count, err = parseRF3ManifestMembers(value, false); err != nil || count != rf3ManifestMembers {
		return group, err
	}
	group.MemberCount = count
	key, value, present := fields.Next()
	if present {
		if !bytes.Equal(key.Raw().Bytes(), []byte(`"enrolled_target"`)) {
			return group, errInvalidRF3Manifest
		}
		target, err := parseRF3ManifestEnrolledTarget(value, group.Members[:])
		if err != nil {
			return group, err
		}
		group.EnrolledTarget = &target
	}
	if _, _, extra := fields.Next(); extra {
		return group, errInvalidRF3Manifest
	}
	if err := validateRF3ManifestGroupRoute(group); err != nil {
		return group, err
	}
	if err := validateRF3SplitChildRegistryRoster(group.ChildRegistry, []rf3ManifestGroup{group}); err != nil {
		return group, err
	}
	return group, nil
}

func validateRF3ManifestGroupRoute(group rf3ManifestGroup) error {
	route := group.Route
	found := false
	for _, member := range group.Members[:group.MemberCount] {
		if member.MemberID == route.MemberID {
			found = true
			break
		}
	}
	if !found && group.EnrolledTarget != nil && group.EnrolledTarget.MemberID == route.MemberID &&
		group.EnrolledTarget.StoreID == route.StoreID {
		found = true
	}
	if !found {
		return errInvalidRF3Manifest
	}
	for _, path := range [...]string{
		group.WAL.Path, group.SQL.Path, group.SQL.IdentityPath, group.SQL.ApplyIdentityPath,
	} {
		relative, err := filepath.Rel(route.MemberRoot, path)
		if err != nil || relative == "." || relative == ".." ||
			strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return errInvalidRF3Manifest
		}
	}
	return nil
}

func parseRF3ManifestSplitControl(node vibejson.Node) (rf3ManifestSplitControl, error) {
	return parseRF3ManifestSplitControlMode(node, false)
}

func parseRF3ManifestSplitControlMode(node vibejson.Node, grouped bool) (rf3ManifestSplitControl, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestSplitControl{}, errInvalidRF3Manifest
	}
	var result rf3ManifestSplitControl
	value, err := nextRF3Field(&fields, `"journal_path"`)
	if err != nil {
		return result, err
	}
	if result.JournalPath, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil ||
		!filepath.IsAbs(result.JournalPath) || filepath.Clean(result.JournalPath) != result.JournalPath {
		return rf3ManifestSplitControl{}, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"max_records"`)
	if err != nil {
		return result, err
	}
	if result.MaxRecords, err = rf3ManifestPositiveInt(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"max_file_bytes"`)
	if err != nil {
		return result, err
	}
	if result.MaxFileBytes, err = rf3ManifestPositiveInt64(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"grants"`)
	if err != nil {
		return result, err
	}
	count, ok := value.ArrayLen()
	if !ok || count == 0 || count > protocol.AbsoluteMaxGrants {
		return rf3ManifestSplitControl{}, errInvalidRF3Manifest
	}
	grants, _ := value.ArrayIter()
	result.Grants = make([]protocol.ActionGrant, 0, count)
	for range count {
		grantNode, present := grants.Next()
		if !present {
			return rf3ManifestSplitControl{}, errInvalidRF3Manifest
		}
		grantFields, object := grantNode.ObjectIter()
		if !object {
			return rf3ManifestSplitControl{}, errInvalidRF3Manifest
		}
		nodeValue, fieldErr := nextRF3Field(&grantFields, `"node_id"`)
		if fieldErr != nil {
			return rf3ManifestSplitControl{}, fieldErr
		}
		nodeID, fieldErr := rf3ManifestNodeID(nodeValue)
		if fieldErr != nil {
			return rf3ManifestSplitControl{}, fieldErr
		}
		actionsValue, fieldErr := nextRF3Field(&grantFields, `"actions"`)
		if fieldErr != nil {
			return rf3ManifestSplitControl{}, fieldErr
		}
		actions, fieldErr := rf3ManifestPositiveUint64(actionsValue)
		if fieldErr != nil || actions > math.MaxUint16 {
			return rf3ManifestSplitControl{}, errInvalidRF3Manifest
		}
		if _, _, extra := grantFields.Next(); extra {
			return rf3ManifestSplitControl{}, errInvalidRF3Manifest
		}
		result.Grants = append(result.Grants, protocol.ActionGrant{Node: nodeID, Actions: uint16(actions)})
	}
	if _, extra := grants.Next(); extra {
		return rf3ManifestSplitControl{}, errInvalidRF3Manifest
	}
	if grouped {
		value, err = nextRF3Field(&fields, `"max_operations"`)
		if err != nil {
			return result, err
		}
		if result.MaxOperations, err = rf3ManifestPositiveInt(value); err != nil || result.MaxOperations > maxRF3SplitChildOperations {
			return result, errInvalidRF3Manifest
		}
	} else {
		value, err = nextRF3Field(&fields, `"child_registry"`)
		if err != nil {
			return result, err
		}
		if result.ChildRegistry, err = parseRF3ManifestSplitChildRegistry(value); err != nil {
			return result, err
		}
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestSplitControl{}, errInvalidRF3Manifest
	}
	if result.MaxRecords > 1<<20 || result.MaxFileBytes > 1<<40 ||
		result.MaxFileBytes < int64(protocol.MaxPayloadBytes) {
		return rf3ManifestSplitControl{}, errInvalidRF3Manifest
	}
	if _, err = protocol.NewAuthorizer(result.Grants); err != nil {
		return rf3ManifestSplitControl{}, errors.Join(errInvalidRF3Manifest, err)
	}
	return result, nil
}

func parseRF3ManifestGroupRoute(node vibejson.Node) (rf3ManifestGroupRoute, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestGroupRoute{}, errInvalidRF3Manifest
	}
	var route rf3ManifestGroupRoute
	fixed := []*[16]byte{
		&route.Group.ClusterID, &route.Group.ClusterIncarnation,
		&route.Group.ShardIncarnation, &route.Group.GroupID, &route.StoreID,
	}
	names := [...]string{
		`"cluster_id"`, `"cluster_incarnation"`, `"shard_incarnation"`, `"group_id"`, `"store_id"`,
	}
	for index := 0; index < 2; index++ {
		value, err := nextRF3Field(&fields, names[index])
		if err != nil {
			return route, err
		}
		decoded, err := rf3ManifestNodeID(value)
		if err != nil {
			return route, err
		}
		*fixed[index] = [16]byte(decoded)
	}
	value, err := nextRF3Field(&fields, `"topology_recovery_epoch"`)
	if err != nil {
		return route, err
	}
	if route.Group.TopologyRecoveryEpoch, err = rf3ManifestPositiveUint64(value); err != nil {
		return route, err
	}
	for index := 2; index < 4; index++ {
		value, err = nextRF3Field(&fields, names[index])
		if err != nil {
			return route, err
		}
		decoded, err := rf3ManifestNodeID(value)
		if err != nil {
			return route, err
		}
		*fixed[index] = [16]byte(decoded)
	}
	value, err = nextRF3Field(&fields, `"distribution"`)
	if err != nil {
		return route, err
	}
	if route.Distribution, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return route, err
	}
	value, err = nextRF3Field(&fields, `"shard"`)
	if err != nil {
		return route, err
	}
	if route.Shard, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return route, err
	}
	value, err = nextRF3Field(&fields, `"allocation_generation"`)
	if err != nil {
		return route, err
	}
	if route.AllocationGeneration, err = rf3ManifestPositiveUint64(value); err != nil {
		return route, err
	}
	value, err = nextRF3Field(&fields, `"member_id"`)
	if err != nil {
		return route, err
	}
	if route.MemberID, err = rf3ManifestPositiveUint64(value); err != nil {
		return route, err
	}
	value, err = nextRF3Field(&fields, names[4])
	if err != nil {
		return route, err
	}
	decodedStore, err := rf3ManifestNodeID(value)
	if err != nil {
		return route, err
	}
	route.StoreID = [16]byte(decodedStore)
	paths := []*string{&route.MemberRoot, &route.SplitRuntimeRoot, &route.MembershipGrantPath}
	pathNames := [...]string{`"member_root"`, `"split_runtime_root"`, `"membership_grant_path"`}
	for index := range paths {
		value, err = nextRF3Field(&fields, pathNames[index])
		if err != nil {
			return route, err
		}
		if *paths[index], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil ||
			!filepath.IsAbs(*paths[index]) || filepath.Clean(*paths[index]) != *paths[index] {
			return rf3ManifestGroupRoute{}, errInvalidRF3Manifest
		}
	}
	if _, _, extra := fields.Next(); extra || route.MemberRoot == string(filepath.Separator) ||
		route.SplitRuntimeRoot != filepath.Join(route.MemberRoot, "split-runtime") ||
		route.MembershipGrantPath != filepath.Join(route.MemberRoot, "membership-grant") {
		return rf3ManifestGroupRoute{}, errInvalidRF3Manifest
	}
	return route, nil
}

func parseRF3ManifestReplicaControl(node vibejson.Node) (rf3ManifestReplicaControl, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
	}
	var result rf3ManifestReplicaControl
	stringsOut := []*string{
		&result.ActionJournalPath, &result.SourceDataRoot,
		&result.SourceJournalPath, &result.SourceRepositoryPath,
	}
	stringNames := [...]string{
		`"action_journal_path"`, `"source_data_root"`,
		`"source_journal_path"`, `"source_repository_path"`,
	}
	value, err := nextRF3Field(&fields, stringNames[0])
	if err != nil {
		return result, err
	}
	if *stringsOut[0], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"max_action_records"`)
	if err != nil {
		return result, err
	}
	if result.MaxActionRecords, err = rf3ManifestPositiveInt(value); err != nil ||
		result.MaxActionRecords > replicaaction.AbsoluteMaxReplicaActionRecords {
		return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
	}
	for index := 1; index < len(stringsOut); index++ {
		value, err = nextRF3Field(&fields, stringNames[index])
		if err != nil {
			return result, err
		}
		if *stringsOut[index], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return result, err
		}
		if index == 2 {
			value, err = nextRF3Field(&fields, `"max_source_records"`)
			if err != nil {
				return result, err
			}
			if result.MaxSourceRecords, err = rf3ManifestPositiveInt(value); err != nil ||
				result.MaxSourceRecords > snapshottransfer.AbsoluteMaxSourceRecords {
				return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
			}
		}
	}
	positiveInts := []*int{&result.MaxSourceArtifacts, &result.MaxSourceConcurrent}
	positiveIntNames := [...]string{`"max_source_artifacts"`, `"max_source_concurrent"`}
	for index := range positiveInts {
		value, err = nextRF3Field(&fields, positiveIntNames[index])
		if err != nil {
			return result, err
		}
		if *positiveInts[index], err = rf3ManifestPositiveInt(value); err != nil {
			return result, err
		}
	}
	uint64Out := []*uint64{&result.MaxSourceArtifactBytes, &result.MaxSourceDiskBytes}
	uint64Names := [...]string{`"max_source_artifact_bytes"`, `"max_source_disk_bytes"`}
	for index := range uint64Out {
		value, err = nextRF3Field(&fields, uint64Names[index])
		if err != nil {
			return result, err
		}
		if *uint64Out[index], err = rf3ManifestPositiveUint64(value); err != nil {
			return result, err
		}
	}
	value, err = nextRF3Field(&fields, `"source_chunk_bytes"`)
	if err != nil {
		return result, err
	}
	chunk, err := rf3ManifestPositiveUint64(value)
	if err != nil || chunk > math.MaxUint32 {
		return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
	}
	result.SourceChunkBytes = uint32(chunk)
	value, err = nextRF3Field(&fields, `"migration_budget"`)
	if err != nil {
		return result, err
	}
	if result.Migration, err = parseRF3ManifestMigrationBudget(value); err != nil {
		return rf3ManifestReplicaControl{}, err
	}
	if _, _, extra := fields.Next(); extra ||
		result.MaxSourceArtifacts > 4096 ||
		result.MaxSourceConcurrent > snapshottransfer.AbsoluteMaxSourceConcurrency ||
		result.SourceChunkBytes < snapshottransfer.MinChunkBytes ||
		result.SourceChunkBytes > snapshottransfer.AbsoluteMaxChunkBytes ||
		result.MaxSourceArtifactBytes == 0 ||
		result.MaxSourceDiskBytes < result.MaxSourceArtifactBytes {
		return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
	}
	paths := [...]string{
		result.ActionJournalPath, result.SourceDataRoot,
		result.SourceJournalPath, result.SourceRepositoryPath,
	}
	for index, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
		}
		for prior := 0; prior < index; prior++ {
			if path == paths[prior] {
				return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
			}
		}
	}
	return result, nil
}

func parseRF3ManifestMigrationBudget(node vibejson.Node) (rf3ManifestMigrationBudget, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestMigrationBudget{}, errInvalidRF3Manifest
	}
	var result rf3ManifestMigrationBudget
	value, err := nextRF3Field(&fields, `"max_active"`)
	if err != nil {
		return result, err
	}
	if result.MaxActive, err = rf3ManifestPositiveInt(value); err != nil {
		return result, err
	}
	limits := []*rf3ManifestRateLimit{
		&result.CPU, &result.DiskRead, &result.DiskWrite,
		&result.NetworkSend, &result.NetworkReceive,
	}
	names := [...]string{`"cpu"`, `"disk_read"`, `"disk_write"`, `"network_send"`, `"network_receive"`}
	for index := range limits {
		value, err = nextRF3Field(&fields, names[index])
		if err != nil {
			return result, err
		}
		if *limits[index], err = parseRF3ManifestRateLimit(value); err != nil {
			return rf3ManifestMigrationBudget{}, err
		}
	}
	if _, _, extra := fields.Next(); extra || result.config().Validate() != nil {
		return rf3ManifestMigrationBudget{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestRateLimit(node vibejson.Node) (rf3ManifestRateLimit, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestRateLimit{}, errInvalidRF3Manifest
	}
	var result rf3ManifestRateLimit
	value, err := nextRF3Field(&fields, `"bytes_per_second"`)
	if err != nil {
		return result, err
	}
	if result.BytesPerSecond, err = rf3ManifestPositiveUint64(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"burst_bytes"`)
	if err != nil {
		return result, err
	}
	if result.BurstBytes, err = rf3ManifestPositiveUint64(value); err != nil {
		return result, err
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestRateLimit{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3BootstrapGatewaySeeds(node vibejson.Node) ([]nodecontrol.BootstrapGatewaySeed, error) {
	count, ok := node.ArrayLen()
	if !ok || count == 0 || count > nodecontrol.MaxBootstrapGatewaySeeds {
		return nil, errInvalidRF3Manifest
	}
	raw := node.Raw().Bytes()
	seeds := make([]nodecontrol.BootstrapGatewaySeed, count)
	if err := vibejson.Unmarshal(raw, &seeds); err != nil {
		return nil, errInvalidRF3Manifest
	}
	canonical, err := vibejson.Marshal(&seeds)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, errInvalidRF3Manifest
	}
	seen := make(map[rafttransport.NodeID]struct{}, len(seeds))
	for _, seed := range seeds {
		if !seed.Valid() {
			return nil, errInvalidRF3Manifest
		}
		if _, found := seen[seed.NodeID]; found {
			return nil, errInvalidRF3Manifest
		}
		seen[seed.NodeID] = struct{}{}
	}
	return seeds, nil
}

func parseRF3ManifestWAL(node vibejson.Node) (rf3ManifestWAL, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestWAL{}, errInvalidRF3Manifest
	}
	var result rf3ManifestWAL
	value, err := nextRF3Field(&fields, `"path"`)
	if err != nil {
		return result, err
	}
	if result.Path, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"key_id"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.KeyID, err = rf3ManifestString(value, raftstore.MaxKeyIDBytes); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"key_material_path"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.KeyMaterialPath, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_file_bytes"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxFileBytes, err = rf3ManifestPositiveInt64(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_record_bytes"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxRecordBytes, err = rf3ManifestPositiveInt(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_records"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxRecords, err = rf3ManifestPositiveUint64(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_entries"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxEntries, err = rf3ManifestPositiveUint64(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_live_bytes"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxLiveBytes, err = rf3ManifestPositiveInt64(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestWAL{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestSQL(node vibejson.Node) (rf3ManifestSQL, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestSQL{}, errInvalidRF3Manifest
	}
	var result rf3ManifestSQL
	values := []*string{&result.Path, &result.IdentityPath, &result.ApplyIdentityPath}
	names := [...]string{`"path"`, `"identity_path"`, `"apply_identity_path"`}
	for index := range names {
		value, err := nextRF3Field(&fields, names[index])
		if err != nil {
			return rf3ManifestSQL{}, err
		}
		if *values[index], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return rf3ManifestSQL{}, err
		}
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestSQL{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestListeners(node vibejson.Node) (rf3ManifestListeners, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestListeners{}, errInvalidRF3Manifest
	}
	var result rf3ManifestListeners
	value, err := nextRF3Field(&fields, `"peer"`)
	if err != nil {
		return result, err
	}
	if result.Peer, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestListeners{}, err
	}
	value, err = nextRF3Field(&fields, `"native"`)
	if err != nil {
		return rf3ManifestListeners{}, err
	}
	if result.Native, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestListeners{}, err
	}
	value, err = nextRF3Field(&fields, `"snapshot"`)
	if err != nil {
		return result, err
	}
	if result.Snapshot, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestListeners{}, err
	}
	value, err = nextRF3Field(&fields, `"control"`)
	if err != nil {
		return result, err
	}
	if result.Control, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestListeners{}, err
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestListeners{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestTLS(node vibejson.Node) (rf3ManifestTLS, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestTLS{}, errInvalidRF3Manifest
	}
	var result rf3ManifestTLS
	values := []*string{&result.Certificate, &result.Key, &result.Roots, &result.IdentityOID}
	names := [...]string{`"certificate"`, `"key"`, `"roots"`, `"identity_oid"`}
	for index := range names {
		value, err := nextRF3Field(&fields, names[index])
		if err != nil {
			return rf3ManifestTLS{}, err
		}
		if *values[index], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return rf3ManifestTLS{}, err
		}
	}
	if key, node, extra := fields.Next(); extra {
		if !bytes.Equal(key.Raw().Bytes(), []byte(`"peer_keys"`)) {
			return rf3ManifestTLS{}, errInvalidRF3Manifest
		}
		if err := vibejson.Unmarshal(node.Raw().Bytes(), &result.PeerKeys); err != nil {
			return rf3ManifestTLS{}, errInvalidRF3Manifest
		}
		if len(result.PeerKeys) == 0 || len(result.PeerKeys) > 256 {
			return rf3ManifestTLS{}, errInvalidRF3Manifest
		}
		seen := make(map[rafttransport.NodeID]bool, len(result.PeerKeys))
		for _, pin := range result.PeerKeys {
			var nodeID rafttransport.NodeID
			var digest [32]byte
			if !decodeRF3FixedHex(pin.NodeID, nodeID[:], false) || !decodeRF3FixedHex(pin.KeyDigest, digest[:], false) || seen[nodeID] {
				return rf3ManifestTLS{}, errInvalidRF3Manifest
			}
			seen[nodeID] = true
		}
		if _, _, extra := fields.Next(); extra {
			return rf3ManifestTLS{}, errInvalidRF3Manifest
		}
	}
	return result, nil
}

func parseRF3ManifestMembers(node vibejson.Node, developmentOnly bool) ([rf3ManifestMembers]rf3ManifestMember, uint8, error) {
	var result [rf3ManifestMembers]rf3ManifestMember
	count, ok := node.ArrayLen()
	if !ok || count != len(result) && (!developmentOnly || count != 1) {
		return result, 0, errInvalidRF3Manifest
	}
	members, _ := node.ArrayIter()
	for index := 0; index < count; index++ {
		node, present := members.Next()
		if !present {
			return [rf3ManifestMembers]rf3ManifestMember{}, 0, errInvalidRF3Manifest
		}
		member, err := parseRF3ManifestMember(node)
		if err != nil || index > 0 && member.MemberID <= result[index-1].MemberID {
			return [rf3ManifestMembers]rf3ManifestMember{}, 0, errInvalidRF3Manifest
		}
		for prior := 0; prior < index; prior++ {
			if member.NodeID == result[prior].NodeID || member.PeerAddress == result[prior].PeerAddress {
				return [rf3ManifestMembers]rf3ManifestMember{}, 0, errInvalidRF3Manifest
			}
		}
		result[index] = member
	}
	if _, extra := members.Next(); extra {
		return [rf3ManifestMembers]rf3ManifestMember{}, 0, errInvalidRF3Manifest
	}
	return result, uint8(count), nil
}

func parseRF3ManifestMember(node vibejson.Node) (rf3ManifestMember, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestMember{}, errInvalidRF3Manifest
	}
	var result rf3ManifestMember
	value, err := nextRF3Field(&fields, `"member_id"`)
	if err != nil {
		return result, err
	}
	if result.MemberID, err = rf3ManifestPositiveUint64(value); err != nil {
		return rf3ManifestMember{}, err
	}
	value, err = nextRF3Field(&fields, `"node_id"`)
	if err != nil {
		return rf3ManifestMember{}, err
	}
	if result.NodeID, err = rf3ManifestNodeID(value); err != nil {
		return rf3ManifestMember{}, err
	}
	value, err = nextRF3Field(&fields, `"peer_address"`)
	if err != nil {
		return rf3ManifestMember{}, err
	}
	if result.PeerAddress, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestMember{}, err
	}
	key, value, present := fields.Next()
	if present {
		if !bytes.Equal(key.Raw().Bytes(), []byte(`"store_id"`)) {
			return rf3ManifestMember{}, errInvalidRF3Manifest
		}
		encoded, fieldErr := rf3ManifestString(value, maxRF3ManifestStringBytes)
		if fieldErr != nil || !decodeRF3FixedHex(encoded, result.StoreID[:], false) {
			return rf3ManifestMember{}, errInvalidRF3Manifest
		}
		key, value, present = fields.Next()
	}
	if present {
		if !bytes.Equal(key.Raw().Bytes(), []byte(`"native_address"`)) {
			return rf3ManifestMember{}, errInvalidRF3Manifest
		}
		if result.NativeAddress, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return rf3ManifestMember{}, err
		}
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestMember{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestReadAuthority(node vibejson.Node) (*rf3ManifestReadAuthority, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return nil, errInvalidRF3Manifest
	}
	result := new(rf3ManifestReadAuthority)
	value, err := nextRF3Field(&fields, `"enabled"`)
	if err != nil {
		return nil, err
	}
	if result.Enabled, ok = value.Bool(); !ok || !result.Enabled {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"feature_version"`)
	if err != nil {
		return nil, err
	}
	feature, err := rf3ManifestPositiveUint64(value)
	if err != nil || feature > math.MaxUint32 {
		return nil, errInvalidRF3Manifest
	}
	result.FeatureVersion = uint32(feature)
	value, err = nextRF3Field(&fields, `"policy_version"`)
	if err != nil {
		return nil, err
	}
	policyVersion, err := rf3ManifestPositiveUint64(value)
	if err != nil || policyVersion > math.MaxUint32 {
		return nil, errInvalidRF3Manifest
	}
	result.PolicyVersion = uint32(policyVersion)
	value, err = nextRF3Field(&fields, `"max_grant_millis"`)
	if err != nil {
		return nil, err
	}
	if result.MaxGrantMillis, err = rf3ManifestPositiveUint64(value); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"clock_rate_ppm"`)
	if err != nil {
		return nil, err
	}
	clockRate, err := rf3ManifestPositiveUint64(value)
	if err != nil || clockRate > math.MaxUint32 {
		return nil, errInvalidRF3Manifest
	}
	result.ClockRatePPM = uint32(clockRate)
	value, err = nextRF3Field(&fields, `"rounding_margin_millis"`)
	if err != nil {
		return nil, err
	}
	if result.RoundingMarginMillis, err = rf3ManifestPositiveUint64(value); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"voters"`)
	if err != nil {
		return nil, err
	}
	count, ok := value.ArrayLen()
	if !ok || count == 0 || count > rf3ManifestMembers {
		return nil, errInvalidRF3Manifest
	}
	iter, _ := value.ArrayIter()
	result.Voters = make([]uint64, count)
	for index := range result.Voters {
		voter, present := iter.Next()
		if !present {
			return nil, errInvalidRF3Manifest
		}
		result.Voters[index], err = rf3ManifestPositiveUint64(voter)
		if err != nil || index > 0 && result.Voters[index-1] >= result.Voters[index] {
			return nil, errInvalidRF3Manifest
		}
	}
	if _, extra := iter.Next(); extra {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"capabilities"`)
	if err != nil {
		return nil, err
	}
	capCount, ok := value.ArrayLen()
	if !ok || capCount != count {
		return nil, errInvalidRF3Manifest
	}
	capIter, _ := value.ArrayIter()
	result.Capabilities = make([]rf3ManifestVoterCapability, capCount)
	for index := range result.Capabilities {
		capNode, present := capIter.Next()
		if !present {
			return nil, errInvalidRF3Manifest
		}
		capFields, ok := capNode.ObjectIter()
		if !ok {
			return nil, errInvalidRF3Manifest
		}
		memberValue, fieldErr := nextRF3Field(&capFields, `"member_id"`)
		if fieldErr != nil {
			return nil, fieldErr
		}
		memberID, fieldErr := rf3ManifestPositiveUint64(memberValue)
		if fieldErr != nil {
			return nil, fieldErr
		}
		versionValue, fieldErr := nextRF3Field(&capFields, `"policy_version"`)
		if fieldErr != nil {
			return nil, fieldErr
		}
		version, fieldErr := rf3ManifestPositiveUint64(versionValue)
		if fieldErr != nil || version > math.MaxUint32 {
			return nil, errInvalidRF3Manifest
		}
		enabledValue, fieldErr := nextRF3Field(&capFields, `"enabled"`)
		if fieldErr != nil {
			return nil, fieldErr
		}
		enabled, ok := enabledValue.Bool()
		if !ok || !enabled || memberID != result.Voters[index] || uint32(version) != result.PolicyVersion {
			return nil, errInvalidRF3Manifest
		}
		if _, _, extra := capFields.Next(); extra {
			return nil, errInvalidRF3Manifest
		}
		result.Capabilities[index] = rf3ManifestVoterCapability{MemberID: memberID, PolicyVersion: uint32(version), Enabled: enabled}
	}
	if _, extra := capIter.Next(); extra {
		return nil, errInvalidRF3Manifest
	}
	if _, _, extra := fields.Next(); extra {
		return nil, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestEnrolledTarget(
	node vibejson.Node,
	members []rf3ManifestMember,
) (rf3ManifestEnrolledTarget, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
	}
	var result rf3ManifestEnrolledTarget
	value, err := nextRF3Field(&fields, `"member_id"`)
	if err != nil {
		return result, err
	}
	if result.MemberID, err = rf3ManifestPositiveUint64(value); err != nil {
		return rf3ManifestEnrolledTarget{}, err
	}
	value, err = nextRF3Field(&fields, `"node_id"`)
	if err != nil {
		return rf3ManifestEnrolledTarget{}, err
	}
	if result.NodeID, err = rf3ManifestNodeID(value); err != nil {
		return rf3ManifestEnrolledTarget{}, err
	}
	value, err = nextRF3Field(&fields, `"store_id"`)
	if err != nil {
		return result, err
	}
	store, err := rf3ManifestNodeID(value)
	if err != nil {
		return result, err
	}
	result.StoreID = [16]byte(store)
	value, err = nextRF3Field(&fields, `"node_incarnation"`)
	if err != nil {
		return result, err
	}
	if result.NodeIncarnation, err = rf3ManifestPositiveUint64(value); err != nil {
		return result, err
	}
	values := []*string{
		&result.PeerAddress, &result.NativeAddress,
		&result.SnapshotAddress, &result.ControlAddress,
	}
	names := [...]string{
		`"peer_address"`, `"native_address"`, `"snapshot_address"`, `"control_address"`,
	}
	for index := range names {
		value, err = nextRF3Field(&fields, names[index])
		if err != nil {
			return rf3ManifestEnrolledTarget{}, err
		}
		if *values[index], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return rf3ManifestEnrolledTarget{}, err
		}
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
	}
	for _, member := range members {
		if result.MemberID == member.MemberID || result.NodeID == member.NodeID {
			return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
		}
	}
	addresses := [...]string{
		result.PeerAddress, result.NativeAddress, result.SnapshotAddress, result.ControlAddress,
	}
	for index := range addresses {
		for _, member := range members {
			if addresses[index] == member.PeerAddress {
				return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
			}
		}
		for prior := 0; prior < index; prior++ {
			if addresses[index] == addresses[prior] {
				return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
			}
		}
	}
	return result, nil
}

func nextRF3Field(fields *vibejson.ObjectIter, canonical string) (vibejson.Node, error) {
	key, value, ok := fields.Next()
	if !ok || !bytes.Equal(key.Raw().Bytes(), []byte(canonical)) {
		return vibejson.Node{}, errInvalidRF3Manifest
	}
	return value, nil
}

func rf3ManifestString(node vibejson.Node, maximum int) (string, error) {
	value, ok := node.StringBytes()
	if !ok || len(value) == 0 || len(value) > maximum || bytes.IndexByte(value, 0) >= 0 {
		return "", errInvalidRF3Manifest
	}
	return strings.Clone(string(value)), nil
}

func rf3ManifestOptionalString(node vibejson.Node, maximum int) (string, error) {
	value, ok := node.StringBytes()
	if !ok || len(value) > maximum || bytes.IndexByte(value, 0) >= 0 {
		return "", errInvalidRF3Manifest
	}
	return strings.Clone(string(value)), nil
}

func rf3ManifestPositiveUint64(node vibejson.Node) (uint64, error) {
	value, ok := node.Uint64()
	if !ok || value == 0 {
		return 0, errInvalidRF3Manifest
	}
	return value, nil
}

func rf3ManifestPositiveInt64(node vibejson.Node) (int64, error) {
	value, err := rf3ManifestPositiveUint64(node)
	if err != nil || value > math.MaxInt64 {
		return 0, errInvalidRF3Manifest
	}
	return int64(value), nil
}

func rf3ManifestPositiveInt(node vibejson.Node) (int, error) {
	value, err := rf3ManifestPositiveUint64(node)
	if err != nil || value > uint64(math.MaxInt) {
		return 0, errInvalidRF3Manifest
	}
	return int(value), nil
}

func rf3ManifestNodeID(node vibejson.Node) (rafttransport.NodeID, error) {
	var result rafttransport.NodeID
	raw := node.Raw().Bytes()
	if len(raw) != 2+hex.EncodedLen(len(result)) || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return result, errInvalidRF3Manifest
	}
	encoded := raw[1 : len(raw)-1]
	for _, character := range encoded {
		if character >= '0' && character <= '9' || character >= 'a' && character <= 'f' {
			continue
		}
		return rafttransport.NodeID{}, errInvalidRF3Manifest
	}
	if _, err := hex.Decode(result[:], encoded); err != nil || result == (rafttransport.NodeID{}) {
		return rafttransport.NodeID{}, errInvalidRF3Manifest
	}
	return result, nil
}

type rf3ManifestPeerKey struct {
	NodeID    string `json:"node_id"`
	KeyDigest string `json:"key_digest"`
}

func (profile rf3ManifestTLS) equal(other rf3ManifestTLS) bool {
	return profile.Certificate == other.Certificate && profile.Key == other.Key && profile.Roots == other.Roots && profile.IdentityOID == other.IdentityOID && slices.Equal(profile.PeerKeys, other.PeerKeys)
}
