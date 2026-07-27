package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// Fault injection for durable chunk summaries.
//
// A summary that can disagree with its chunk after a crash is worse than no
// summary: it prunes chunks that match and the query returns fewer rows with
// no error anywhere. The design's answer is that a summary and the chunk
// reference it describes are the same bytes in the same page under the same
// checksum, published by the same commit — so there is no ordering to get
// wrong. This file is the evidence for that claim rather than the argument.
//
// The method is the one the free-set work established: capture the file before
// and after a commit, rebuild every prefix-torn image of that commit's changed
// pages plus every cut inside the superblock write, recover each one, and
// assert the invariant. The invariant here is stronger than "the store opens":
// for every live document in the recovered image, the summary of the chunk that
// document lives in must admit that document's own values. A summary carried
// over from a chunk it no longer describes fails it immediately.

// zoneSnapshotSummaries reads every chunk's summary out of a snapshot's
// directory leaves. It is a test-only accessor because nothing on the query
// path ever needs a chunk-keyed map.
func zoneSnapshotSummaries(s *Snapshot) (map[uint32]store.ZoneSummary, error) {
	out := map[uint32]store.ZoneSummary{}
	state := s.state
	err := storeio.WalkChunkTreeZones(
		s.collection.cache, state.chunkRoot,
		storeio.ChunkTreeBounds{FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID},
		func(chunk uint32, zone storeio.ChunkZone) error {
			var summary store.ZoneSummary
			summary.Decode((*[store.ZoneCompactBytes]byte)(&zone))
			out[chunk] = summary
			return nil
		},
	)
	return out, err
}

// assertZoneSummariesCoverTheirChunks is the invariant. Every live document is
// walked, every top-level member of it is turned into the equality probe a
// query would compile, and the summary of that document's own chunk must keep
// it.
func assertZoneSummariesCoverTheirChunks(t *testing.T, collection *Collection, name string) int {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatalf("%s: Snapshot: %v", name, err)
	}
	defer snapshot.Close()
	summaries, err := zoneSnapshotSummaries(snapshot)
	if err != nil {
		t.Fatalf("%s: summaries: %v", name, err)
	}
	masks := make([]store.Mask, 0, len(summaries))
	live := fileStoreLiveMask(snapshot.state.root.ChunkDocuments)
	for chunk := range summaries {
		masks = append(masks, store.Mask{Chunk: chunk, Bits: live})
	}
	sortMasksByChunk(masks)
	checked := 0
	_, err = snapshot.RangeMasksRawRowsBuffer(masks, nil,
		func(row store.Location, _, value []byte) error {
			summary := summaries[row.Chunk]
			for _, member := range zoneTopLevelMembers(value) {
				probe, ok := zoneEqualityProbe(member.name, member.value)
				if !ok {
					continue
				}
				if !summary.Keep(probe) {
					return fmt.Errorf(
						"%s: chunk %d slot %d: summary prunes its own document %s at member %q",
						name, row.Chunk, row.Slot, value, member.name)
				}
				checked++
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	return checked
}

func sortMasksByChunk(masks []store.Mask) {
	for i := 1; i < len(masks); i++ {
		for j := i; j > 0 && masks[j].Chunk < masks[j-1].Chunk; j-- {
			masks[j], masks[j-1] = masks[j-1], masks[j]
		}
	}
}

type zoneMember struct {
	name  string
	value []byte
}

// zoneTopLevelMembers splits a compact JSON object into its top-level members.
// The corpus this file writes is generated, flat, and escape-free, so a
// deliberately simple splitter is enough and keeps the oracle independent of
// the fold path it is checking.
func zoneTopLevelMembers(src []byte) []zoneMember {
	if len(src) < 2 || src[0] != '{' {
		return nil
	}
	var out []zoneMember
	i := 1
	for i < len(src) && src[i] != '}' {
		if src[i] != '"' {
			return out
		}
		end := i + 1
		for end < len(src) && src[end] != '"' {
			end++
		}
		if end >= len(src) {
			return out
		}
		name := string(src[i+1 : end])
		i = end + 1
		if i >= len(src) || src[i] != ':' {
			return out
		}
		i++
		start := i
		depth := 0
		inString := false
		for i < len(src) {
			c := src[i]
			if inString {
				if c == '\\' {
					i += 2
					continue
				}
				if c == '"' {
					inString = false
				}
				i++
				continue
			}
			switch c {
			case '"':
				inString = true
			case '{', '[':
				depth++
			case '}', ']':
				if depth == 0 {
					goto done
				}
				depth--
			case ',':
				if depth == 0 {
					goto done
				}
			}
			i++
		}
	done:
		out = append(out, zoneMember{name: name, value: src[start:i]})
		if i < len(src) && src[i] == ',' {
			i++
		}
	}
	return out
}

// zoneEqualityProbe compiles the probe a query would compile for `name = value`.
func zoneEqualityProbe(name string, value []byte) (store.ZoneProbe, bool) {
	if len(value) == 0 {
		return store.ZoneProbe{}, false
	}
	var code uint32
	switch value[0] {
	case 't':
		code = store.ZoneCodeBool(true)
	case 'f':
		code = store.ZoneCodeBool(false)
	case '"':
		code = store.ZoneCodeString(string(value[1 : len(value)-1]))
	case 'n', '{', '[':
		// A null cell fails every comparison and a container literal is not a
		// scalar the planner probes with; neither is an oracle for min/max.
		return store.ZoneProbe{}, false
	default:
		c, ok := store.ZoneCodeNumber(value)
		if !ok {
			return store.ZoneProbe{}, false
		}
		code = c
	}
	return store.ZoneProbe{Path: store.ZonePathHash(name), Lo: code, Hi: code, Op: store.ZoneOpEq}, true
}

// Given a durable collection under steady-state churn, when every commit is
// torn at every changed page boundary and at every offset inside the
// superblock write, then every recovered image's chunk summaries still cover
// the documents of the chunks they name.
func TestFileStoreZoneMapsSurviveCrashAtEveryWritePoint(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "zone-crash-source-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = 4
	options.ResidentBytes = 16 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 1024
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	// Enough documents to fill several chunks and several directory leaves, so
	// the commits under test rewrite a real root-to-leaf path rather than the
	// single node a fresh store has.
	const keys = 40
	for i := range keys {
		if _, err := collection.Put(fmt.Sprintf("key-%02d", i),
			[]byte(zoneTestDocument(i))); err != nil {
			t.Fatal(err)
		}
	}

	torn, checked := 0, 0
	for attempt := range 6 {
		before, readErr := os.ReadFile(file.Name())
		if readErr != nil {
			t.Fatal(readErr)
		}
		oldGeneration := collection.Generation()
		// Alternate between replacing a document (which folds into an existing
		// summary) and deleting one (which carries it forward untouched), so
		// both maintenance shapes are torn.
		key := fmt.Sprintf("key-%02d", attempt*3%keys)
		if attempt%3 == 2 {
			if _, err := collection.Delete(key); err != nil {
				t.Fatal(err)
			}
		} else if _, err := collection.Put(key,
			[]byte(zoneTestDocument(1000+attempt))); err != nil {
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		newGeneration := collection.Generation()
		after, readErr := os.ReadFile(file.Name())
		if readErr != nil {
			t.Fatal(readErr)
		}
		n, c := tearCommitAndCheckZones(t, options, before, after,
			oldGeneration, newGeneration, fmt.Sprintf("commit-%d", attempt))
		torn += n
		checked += c
	}
	if torn < 40 {
		t.Fatalf("only %d torn images were exercised, too few to have covered the "+
			"directory pages a commit rewrites", torn)
	}
	if checked == 0 {
		t.Fatal("no recovered image had a document to check: the invariant is vacuous")
	}
	t.Logf("tore %d images, checked %d document-member probes against their own chunk summaries",
		torn, checked)
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
}

func tearCommitAndCheckZones(
	t *testing.T, options Options, before, after []byte,
	oldGeneration, newGeneration uint64, label string,
) (int, int) {
	t.Helper()
	pageSize := options.PageSize
	dataStart := testMutableDataStart(pageSize)
	torn, checked := 0, 0
	for _, cut := range changedPageCrashCuts(before, after, dataStart, pageSize) {
		image := make([]byte, max(len(before), len(after)))
		copy(image, before)
		copy(image[dataStart:dataStart+cut], after[dataStart:dataStart+cut])
		checked += recoverAndCheckZones(t, image, options, oldGeneration, newGeneration,
			fmt.Sprintf("%s/data-cut-%d", label, cut))
		torn++
	}
	rootOffset := int((newGeneration-1)&1) * pageSize
	for _, cut := range []int{0, 1, storeio.InlineSuperblockSize / 2, storeio.InlineSuperblockSize - 1, storeio.InlineSuperblockSize} {
		image := append([]byte(nil), after...)
		copy(image[rootOffset:rootOffset+pageSize], before[rootOffset:rootOffset+pageSize])
		copy(image[rootOffset:rootOffset+cut], after[rootOffset:rootOffset+cut])
		checked += recoverAndCheckZones(t, image, options, oldGeneration, newGeneration,
			fmt.Sprintf("%s/root-cut-%d", label, cut))
		torn++
	}
	return torn, checked
}

func recoverAndCheckZones(
	t *testing.T, image []byte, options Options, oldGeneration, newGeneration uint64, name string,
) int {
	t.Helper()
	path := filepath.Join(t.TempDir(), strings.ReplaceAll(name, "/", "_"))
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Open(file, options)
	if err != nil {
		t.Fatalf("%s recovery: %v", name, err)
	}
	defer collection.Close()
	generation := collection.Generation()
	if generation != oldGeneration && generation != newGeneration {
		t.Fatalf("%s recovered generation %d, want %d or %d",
			name, generation, oldGeneration, newGeneration)
	}
	return assertZoneSummariesCoverTheirChunks(t, collection, name)
}
