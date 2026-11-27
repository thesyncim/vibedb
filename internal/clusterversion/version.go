// Package clusterversion provides monotonic cluster feature-version negotiation
// and rolling-upgrade compatibility checks across mixed-version nodes.
//
// Every binary declares a supported protocol/storage window [MinSupported,
// Current]. A cluster's active feature level is bounded by the minimum active
// version across all live nodes, and can only advance monotonically once every
// node has upgraded. Incompatible version windows fail closed.
package clusterversion

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	// ErrInvalidVersion is returned for malformed or zero version values.
	ErrInvalidVersion = errors.New("clusterversion: invalid version")
	// ErrIncompatibleClusterVersion is returned when nodes have no common
	// supported protocol/storage version window.
	ErrIncompatibleClusterVersion = errors.New("clusterversion: incompatible cluster version window")
	// ErrVersionDowngrade is returned when attempting to lower the cluster's
	// monotonic active version below its already-committed floor.
	ErrVersionDowngrade = errors.New("clusterversion: cannot downgrade active cluster version")
)

// Version is a monotonic (Major, Minor) cluster protocol/storage version.
type Version struct {
	Major uint16 `json:"major"`
	Minor uint16 `json:"minor"`
}

var (
	// V1_0 is the baseline RF3 cluster protocol and storage version.
	V1_0 = Version{Major: 1, Minor: 0}
	// V1_1 adds incremental/persisted Tin sidecars, gateway latency histograms,
	// remote TLS+SCRAM pgwire, and multi-relation global-index split prune.
	V1_1 = Version{Major: 1, Minor: 1}

	// MinSupported is the oldest cluster active version this binary can join
	// and serve during a rolling upgrade.
	MinSupported = V1_0
	// Current is the newest cluster active version this binary supports.
	Current = V1_1
)

// Valid reports whether v is a non-zero version.
func (v Version) Valid() bool {
	return v.Major > 0 || v.Minor > 0
}

// Compare returns -1 if v < other, 0 if v == other, and +1 if v > other.
func (v Version) Compare(other Version) int {
	if v.Major != other.Major {
		if v.Major < other.Major {
			return -1
		}
		return 1
	}
	if v.Minor != other.Minor {
		if v.Minor < other.Minor {
			return -1
		}
		return 1
	}
	return 0
}

// String formats v as "major.minor".
func (v Version) String() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// ParseVersion parses a canonical "major.minor" version string.
func ParseVersion(s string) (Version, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return Version{}, fmt.Errorf("%w: %q", ErrInvalidVersion, s)
	}
	maj, err := strconv.ParseUint(parts[0], 10, 16)
	if err != nil {
		return Version{}, fmt.Errorf("%w: %q", ErrInvalidVersion, s)
	}
	min, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return Version{}, fmt.Errorf("%w: %q", ErrInvalidVersion, s)
	}
	v := Version{Major: uint16(maj), Minor: uint16(min)}
	if !v.Valid() {
		return Version{}, fmt.Errorf("%w: zero version", ErrInvalidVersion)
	}
	return v, nil
}

// AppendBinary appends v in fixed 4-byte little-endian wire format.
func (v Version) AppendBinary(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, v.Major)
	dst = binary.LittleEndian.AppendUint16(dst, v.Minor)
	return dst
}

// UnmarshalBinary decodes a 4-byte little-endian wire version.
func UnmarshalBinary(b []byte) (Version, error) {
	if len(b) != 4 {
		return Version{}, fmt.Errorf("%w: expected 4 bytes, got %d", ErrInvalidVersion, len(b))
	}
	v := Version{
		Major: binary.LittleEndian.Uint16(b[0:2]),
		Minor: binary.LittleEndian.Uint16(b[2:4]),
	}
	if !v.Valid() {
		return Version{}, fmt.Errorf("%w: zero version", ErrInvalidVersion)
	}
	return v, nil
}

// Feature identifies a cluster-wide capability gated on the active cluster
// version so mixed-version nodes never emit frames or formats older peers
// cannot decode.
type Feature uint32

const (
	FeatureBaseRF3 Feature = 1 << iota
	FeatureTinPersistedSidecar
	FeatureGatewayLatencyHistograms
	FeatureRemoteSCRAMPGWire
	FeatureGlobalIndexSplitPrune
)

// FeatureMinVersion returns the minimum active cluster version required to
// enable f. Unknown features fail closed with an unreachable version.
func FeatureMinVersion(f Feature) Version {
	switch f {
	case FeatureBaseRF3:
		return V1_0
	case FeatureTinPersistedSidecar,
		FeatureGatewayLatencyHistograms,
		FeatureRemoteSCRAMPGWire,
		FeatureGlobalIndexSplitPrune:
		return V1_1
	default:
		return Version{Major: ^uint16(0), Minor: ^uint16(0)}
	}
}

// Enabled reports whether f is active at the given cluster active version.
func (f Feature) Enabled(active Version) bool {
	if !active.Valid() {
		return false
	}
	return active.Compare(FeatureMinVersion(f)) >= 0
}

// NodeVersion advertises one node's supported version window [MinSupported,
// MaxSupported] and its running BinaryVersion.
type NodeVersion struct {
	MinSupported  Version `json:"min_supported"`
	MaxSupported  Version `json:"max_supported"`
	BinaryVersion Version `json:"binary_version"`
}

// LocalNodeVersion returns the version advertisement of the current binary.
func LocalNodeVersion() NodeVersion {
	return NodeVersion{
		MinSupported:  MinSupported,
		MaxSupported:  Current,
		BinaryVersion: Current,
	}
}

// Valid reports whether nv describes a well-formed monotonic window.
func (nv NodeVersion) Valid() bool {
	return nv.MinSupported.Valid() &&
		nv.MaxSupported.Valid() &&
		nv.BinaryVersion.Valid() &&
		nv.MinSupported.Compare(nv.MaxSupported) <= 0 &&
		nv.MinSupported.Compare(nv.BinaryVersion) <= 0 &&
		nv.BinaryVersion.Compare(nv.MaxSupported) <= 0
}

// NegotiatedWindow is the cluster-wide compatible version intersection across
// all participating nodes.
type NegotiatedWindow struct {
	MinCommon Version
	MaxCommon Version
	MinBinary Version
}

// Negotiate computes the intersection of supported version windows across all
// nodes. It fails closed with ErrIncompatibleClusterVersion if any node is
// invalid or if the intersection is empty.
func Negotiate(nodes []NodeVersion) (NegotiatedWindow, error) {
	if len(nodes) == 0 {
		return NegotiatedWindow{}, fmt.Errorf("%w: empty node set", ErrIncompatibleClusterVersion)
	}
	if !nodes[0].Valid() {
		return NegotiatedWindow{}, fmt.Errorf("%w: node 0 invalid", ErrInvalidVersion)
	}
	minCommon := nodes[0].MinSupported
	maxCommon := nodes[0].MaxSupported
	minBinary := nodes[0].BinaryVersion
	for i := 1; i < len(nodes); i++ {
		nv := nodes[i]
		if !nv.Valid() {
			return NegotiatedWindow{}, fmt.Errorf("%w: node %d invalid", ErrInvalidVersion, i)
		}
		if nv.MinSupported.Compare(minCommon) > 0 {
			minCommon = nv.MinSupported
		}
		if nv.MaxSupported.Compare(maxCommon) < 0 {
			maxCommon = nv.MaxSupported
		}
		if nv.BinaryVersion.Compare(minBinary) < 0 {
			minBinary = nv.BinaryVersion
		}
	}
	if minCommon.Compare(maxCommon) > 0 {
		return NegotiatedWindow{}, fmt.Errorf(
			"%w: disjoint support window [%s, %s]",
			ErrIncompatibleClusterVersion, minCommon, maxCommon,
		)
	}
	return NegotiatedWindow{
		MinCommon: minCommon,
		MaxCommon: maxCommon,
		MinBinary: minBinary,
	}, nil
}

// ValidateAdvance checks whether a cluster at committedActive can safely
// advance its active feature version to proposed given the current node set.
// It enforces monotonicity (no downgrade) and requires every live node to
// support proposed.
func ValidateAdvance(committedActive, proposed Version, nodes []NodeVersion) error {
	if !committedActive.Valid() || !proposed.Valid() {
		return ErrInvalidVersion
	}
	if proposed.Compare(committedActive) < 0 {
		return fmt.Errorf("%w: %s -> %s", ErrVersionDowngrade, committedActive, proposed)
	}
	window, err := Negotiate(nodes)
	if err != nil {
		return err
	}
	if proposed.Compare(window.MinCommon) < 0 || proposed.Compare(window.MaxCommon) > 0 {
		return fmt.Errorf(
			"%w: proposed %s outside negotiated window [%s, %s]",
			ErrIncompatibleClusterVersion, proposed, window.MinCommon, window.MaxCommon,
		)
	}
	return nil
}
