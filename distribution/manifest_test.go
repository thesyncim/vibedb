package distribution

// NewManifest is the one gate between an arbitrary caller-supplied shard list
// and an immutable routing generation, so its validation is exhaustively
// enumerated here: every rejection the design requires (gaps, overlaps, order,
// coverage, End.Max placement, duplicate/empty ids, missing leaders) is
// asserted to fail with the specific typed *ManifestError, and a valid
// non-uniform partition is asserted to construct and resolve. Immutability —
// that a published Manifest shares no memory with the caller and exposes no
// mutator — is pinned last, since every concurrency guarantee downstream rests
// on it.
//
// This file also defines the keyspace/manifest builders the rest of the
// package's tests reuse: points live on a uint64 axis so ranges read like the
// hex key ranges of the design (a "-80" split is pt(hb(0x80))).

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

// maxKeyEnd is the exclusive maximum end, legal only on a manifest's final
// range.
var maxKeyEnd = KeyspaceEnd{Max: true}

// pt encodes v as a big-endian KeyspacePoint.
func pt(v uint64) KeyspacePoint {
	var p KeyspacePoint
	binary.BigEndian.PutUint64(p[:], v)
	return p
}

// ptVal decodes a KeyspacePoint back to its big-endian uint64.
func ptVal(p KeyspacePoint) uint64 { return binary.BigEndian.Uint64(p[:]) }

// kend is a concrete exclusive end at v.
func kend(v uint64) KeyspaceEnd { return KeyspaceEnd{Point: pt(v)} }

// hb places byte b in the most significant position, mirroring Vitess hex-
// prefix key ranges: hb(0x80) is the "-80" boundary.
func hb(b byte) uint64 { return uint64(b) << 56 }

// endMinusOne is the last point inside a range: End.Point-1 for a concrete end,
// or the maximum point for a Max end.
func endMinusOne(e KeyspaceEnd) KeyspacePoint {
	if e.Max {
		return pt(math.MaxUint64)
	}
	return pt(ptVal(e.Point) - 1)
}

// shardsFromBounds partitions the keyspace at the given interior boundary
// points (strictly increasing, none zero) into shards s0..sN, each with one
// synthetic leader. The final shard ends at Max.
func shardsFromBounds(bounds ...uint64) []Shard {
	starts := make([]KeyspacePoint, len(bounds)+1)
	for i, b := range bounds {
		starts[i+1] = pt(b)
	}
	shards := make([]Shard, len(starts))
	for i := range shards {
		end := maxKeyEnd
		if i < len(shards)-1 {
			end = KeyspaceEnd{Point: starts[i+1]}
		}
		shards[i] = Shard{
			ID:      ShardID(fmt.Sprintf("s%d", i)),
			Range:   KeyRange{Start: starts[i], End: end},
			Leaders: []EndpointID{EndpointID(fmt.Sprintf("ep-%d", i))},
		}
	}
	return shards
}

// manifestFromBounds builds a validated manifest partitioned at bounds.
func manifestFromBounds(tb testing.TB, bounds ...uint64) *Manifest {
	tb.Helper()
	m, err := NewManifest("test", 1, shardsFromBounds(bounds...))
	if err != nil {
		tb.Fatalf("NewManifest(bounds=%v): %v", bounds, err)
	}
	return m
}

// evenlySpacedBounds returns the shardCount-1 interior boundaries that split
// the keyspace into shardCount near-equal shards; shardCount <= 1 yields none.
func evenlySpacedBounds(shardCount int) []uint64 {
	if shardCount <= 1 {
		return nil
	}
	step := uint64(math.MaxUint64) / uint64(shardCount)
	bounds := make([]uint64, shardCount-1)
	for i := range bounds {
		bounds[i] = step * uint64(i+1)
	}
	return bounds
}

// leader is a one-endpoint leader list for a valid shard record.
func leader(id string) []EndpointID { return []EndpointID{EndpointID("ep-" + id)} }

// TestNewManifestRejects enumerates every documented validation failure and
// asserts each surfaces as a *ManifestError matching ErrInvalidManifest with
// the specific reason, so a future edit that swaps two checks or loosens one is
// caught by the exact-cause assertion rather than a generic "some error".
func TestNewManifestRejects(t *testing.T) {
	cases := []struct {
		name       string
		shards     []Shard
		wantReason string
	}{
		{
			name:       "no shards",
			shards:     nil,
			wantReason: "manifest defines no shards",
		},
		{
			name: "empty shard id",
			shards: []Shard{
				{ID: "", Range: KeyRange{Start: pt(0), End: maxKeyEnd}, Leaders: leader("a")},
			},
			wantReason: "empty shard id",
		},
		{
			name: "duplicate shard id",
			shards: []Shard{
				{ID: "dup", Range: KeyRange{Start: pt(0), End: kend(hb(0x80))}, Leaders: leader("a")},
				{ID: "dup", Range: KeyRange{Start: pt(hb(0x80)), End: maxKeyEnd}, Leaders: leader("b")},
			},
			wantReason: "duplicate shard id",
		},
		{
			name: "shard with no leader",
			shards: []Shard{
				{ID: "s0", Range: KeyRange{Start: pt(0), End: maxKeyEnd}, Leaders: nil},
			},
			wantReason: "has no leader endpoint",
		},
		{
			name: "shard with empty leader endpoint",
			shards: []Shard{
				{ID: "s0", Range: KeyRange{Start: pt(0), End: maxKeyEnd}, Leaders: []EndpointID{""}},
			},
			wantReason: "empty leader endpoint",
		},
		{
			name: "range start not below end",
			shards: []Shard{
				{ID: "s0", Range: KeyRange{Start: pt(hb(0x80)), End: kend(hb(0x40))}, Leaders: leader("a")},
			},
			wantReason: "range start is not below its end",
		},
		{
			name: "first range does not start at zero",
			shards: []Shard{
				{ID: "s0", Range: KeyRange{Start: pt(hb(0x10)), End: maxKeyEnd}, Leaders: leader("a")},
			},
			wantReason: "first range does not start at zero",
		},
		{
			name: "final range does not end at max",
			shards: []Shard{
				{ID: "s0", Range: KeyRange{Start: pt(0), End: kend(hb(0x80))}, Leaders: leader("a")},
			},
			wantReason: "final range does not end at Max",
		},
		{
			name: "non-final range ends at max",
			shards: []Shard{
				{ID: "s0", Range: KeyRange{Start: pt(0), End: maxKeyEnd}, Leaders: leader("a")},
				{ID: "s1", Range: KeyRange{Start: pt(hb(0x80)), End: maxKeyEnd}, Leaders: leader("b")},
			},
			wantReason: "non-final range ends at Max",
		},
		{
			name: "ranges not sorted by start",
			shards: []Shard{
				{ID: "s0", Range: KeyRange{Start: pt(0), End: kend(hb(0x40))}, Leaders: leader("a")},
				{ID: "s1", Range: KeyRange{Start: pt(hb(0x40)), End: kend(hb(0x80))}, Leaders: leader("b")},
				{ID: "s2", Range: KeyRange{Start: pt(hb(0x20)), End: maxKeyEnd}, Leaders: leader("c")},
			},
			wantReason: "ranges are not sorted by start",
		},
		{
			name: "gap between ranges",
			shards: []Shard{
				{ID: "s0", Range: KeyRange{Start: pt(0), End: kend(hb(0x40))}, Leaders: leader("a")},
				{ID: "s1", Range: KeyRange{Start: pt(hb(0x80)), End: maxKeyEnd}, Leaders: leader("b")},
			},
			wantReason: "gap between shard ranges",
		},
		{
			name: "overlapping ranges",
			shards: []Shard{
				{ID: "s0", Range: KeyRange{Start: pt(0), End: kend(hb(0x80))}, Leaders: leader("a")},
				{ID: "s1", Range: KeyRange{Start: pt(hb(0x40)), End: maxKeyEnd}, Leaders: leader("b")},
			},
			wantReason: "overlapping shard ranges",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, err := NewManifest("dist", 1, c.shards)
			if err == nil {
				t.Fatalf("NewManifest accepted invalid input, want rejection %q", c.wantReason)
			}
			if m != nil {
				t.Fatalf("NewManifest returned a non-nil manifest alongside an error")
			}
			if !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("error %v does not match ErrInvalidManifest", err)
			}
			var me *ManifestError
			if !errors.As(err, &me) {
				t.Fatalf("error is %T, want *ManifestError", err)
			}
			if !strings.Contains(me.Reason, c.wantReason) {
				t.Fatalf("reason = %q, want it to contain %q", me.Reason, c.wantReason)
			}
		})
	}
}

// TestNewManifestAcceptsCompleteCoverage constructs the design's non-uniform
// example (-80, 80-c0, c0-e0, e0-f0, f0-) and asserts it builds and that a
// representative point in each shard resolves to that shard, so "complete
// coverage of the 8-byte keyspace" is exercised positively, not only by the
// absence of a rejection.
func TestNewManifestAcceptsCompleteCoverage(t *testing.T) {
	m := manifestFromBounds(t, hb(0x80), hb(0xc0), hb(0xe0), hb(0xf0))
	if m.ShardCount() != 5 {
		t.Fatalf("ShardCount = %d, want 5", m.ShardCount())
	}
	if m.Distribution() != "test" || m.Version() != 1 {
		t.Fatalf("Distribution/Version = %q/%d, want test/1", m.Distribution(), m.Version())
	}

	probes := []struct {
		point   uint64
		wantIdx int
	}{
		{0, 0},
		{hb(0x7f), 0},
		{hb(0x80), 1},
		{hb(0xa0), 1},
		{hb(0xc0), 2},
		{hb(0xd0), 2},
		{hb(0xe0), 3},
		{hb(0xe8), 3},
		{hb(0xf0), 4},
		{math.MaxUint64, 4},
	}
	for _, p := range probes {
		want, _ := m.ShardInfo(p.wantIdx)
		got, ok := m.ResolvePoint(pt(p.point))
		if !ok {
			t.Fatalf("ResolvePoint(%#016x) missed on a complete manifest", p.point)
		}
		if got != want.ID {
			t.Fatalf("ResolvePoint(%#016x) = %q, want %q (shard %d)", p.point, got, want.ID, p.wantIdx)
		}
	}
}

// TestManifestImmutability proves the constructor deep-copies its input: after
// construction the caller mutates the shard slice and both leader backing
// arrays, and the manifest still reports the pre-mutation layout; mutating the
// slice returned by ShardInfo is likewise isolated. Together with the absence
// of exported mutators this is the immutability the atomic publication path
// relies on.
func TestManifestImmutability(t *testing.T) {
	shards := []Shard{
		{ID: "s0", Range: KeyRange{Start: pt(0), End: kend(hb(0x80))}, Leaders: []EndpointID{"ep-a", "ep-b"}},
		{ID: "s1", Range: KeyRange{Start: pt(hb(0x80)), End: maxKeyEnd}, Leaders: []EndpointID{"ep-c"}},
	}
	m, err := NewManifest("dist", 7, shards)
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}

	// Mutate every caller-visible field after construction.
	shards[0].ID = "mutated"
	shards[0].Range.Start = pt(hb(0x12))
	shards[0].Range.End = kend(1)
	shards[0].Leaders[0] = "ep-mutated"
	shards[0].Leaders[1] = "ep-mutated"
	shards[1].Leaders[0] = "ep-mutated"

	s0, _ := m.ShardInfo(0)
	if s0.ID != "s0" {
		t.Fatalf("shard 0 id = %q, want s0 (caller mutation leaked in)", s0.ID)
	}
	if s0.Range.Start != pt(0) || s0.Range.End != kend(hb(0x80)) {
		t.Fatalf("shard 0 range = %+v, want [0, hb(0x80)) (caller mutation leaked in)", s0.Range)
	}
	if len(s0.Leaders) != 2 || s0.Leaders[0] != "ep-a" || s0.Leaders[1] != "ep-b" {
		t.Fatalf("shard 0 leaders = %v, want [ep-a ep-b] (caller mutation leaked in)", s0.Leaders)
	}
	s1, _ := m.ShardInfo(1)
	if len(s1.Leaders) != 1 || s1.Leaders[0] != "ep-c" {
		t.Fatalf("shard 1 leaders = %v, want [ep-c] (caller mutation leaked in)", s1.Leaders)
	}

	// The slice ShardInfo hands back is itself a copy.
	s0.Leaders[0] = "ep-clobber"
	again, _ := m.ShardInfo(0)
	if again.Leaders[0] != "ep-a" {
		t.Fatalf("mutating a ShardInfo result leaked into the manifest: leader = %q", again.Leaders[0])
	}

	if _, ok := m.ShardInfo(-1); ok {
		t.Fatal("ShardInfo(-1) reported ok")
	}
	if _, ok := m.ShardInfo(m.ShardCount()); ok {
		t.Fatal("ShardInfo(ShardCount) reported ok")
	}
}

// TestManifestHasNoExportedFields guards the structural half of immutability:
// with every field unexported and only read-only accessors exported, no caller
// outside this package can reach in and alter a published generation.
func TestManifestHasNoExportedFields(t *testing.T) {
	typ := reflect.TypeFor[Manifest]()
	if typ.NumField() == 0 {
		t.Fatal("Manifest has no fields; test needs updating, not deleting")
	}
	for f := range typ.Fields() {
		if f.IsExported() {
			t.Fatalf("Manifest.%s is exported; a published manifest must expose no mutable state", f.Name)
		}
	}
}
