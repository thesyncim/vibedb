package durable

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/internal/tin"
	vibejson "github.com/thesyncim/vibejson"
)

// Durable sidecar persistence for Tin full-text builds.
//
// Each durable collection with tin declarations persists its latest build to
// `<collection-primary-path>.tin` via atomic write-temp + fsync + rename.
// The sidecar is keyed by the collection's StoreID, generation, declared
// (name, path) specs, and per-bucket primary leaf PageRefs, and protected by
// a trailing SHA-256 digest.
//
// On reopen, the first tin query loads the sidecar:
//   - If the collection's current generation and every primary leaf PageRef
//     match the persisted build, search serves directly with zero document
//     scans and zero re-tokenization.
//   - If writes occurred since the sidecar was saved, the persisted build
//     feeds straight into buildTinSnapshotIncremental so only changed leaves
//     are read and patched.
//   - Any missing, corrupt, truncated, or schema-mismatched sidecar fails
//     closed cleanly to a full RangeRaw scan.
const (
	tinSidecarSuffix  = ".tin"
	tinSidecarMagic   = "vdbT"
	tinSidecarVersion = 1
)

func tinSidecarPath(primaryPath string) string {
	if primaryPath == "" {
		return ""
	}
	return primaryPath + tinSidecarSuffix
}

func tinLeafRefsMatchRouter(snap *Snapshot, leafRefs map[storeio.BucketID]storeio.PageRef) bool {
	if snap == nil || len(leafRefs) == 0 {
		return false
	}
	router := snap.primaryRouter
	generation := snap.Generation()
	if router == nil || router.Generation() != generation {
		return false
	}
	if router.Len() != len(leafRefs) {
		return false
	}
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			return false
		}
		want, exists := leafRefs[route.Bucket]
		if !exists || want != route.Ref {
			return false
		}
	}
	return router.Generation() == generation
}

func (c *Collection) setTinSidecarPath(path string) {
	c.tinMu.Lock()
	c.tinSidecarPath = path
	c.tinMu.Unlock()
}

func saveTinSnapshotBuild(path string, storeID [16]byte, build *tinSnapshotBuild) error {
	if path == "" || build == nil || build.leafRefs == nil || build.slots == nil || len(build.paths) == 0 {
		return nil
	}
	data, err := marshalTinSnapshotBuild(storeID, build)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tin-persist-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

func loadTinSnapshotBuild(
	path string,
	expectedStoreID [16]byte,
	expectedPaths []tinDefSpec,
) (*tinSnapshotBuild, error) {
	if path == "" || len(expectedPaths) == 0 {
		return nil, fmt.Errorf("vibedb: tin sidecar unavailable")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return unmarshalTinSnapshotBuild(data, expectedStoreID, expectedPaths)
}

func marshalTinSnapshotBuild(storeID [16]byte, build *tinSnapshotBuild) ([]byte, error) {
	out := make([]byte, 0, 256+len(build.keys)*32)
	out = append(out, tinSidecarMagic...)
	out = append(out, tinSidecarVersion)
	out = append(out, storeID[:]...)
	out = binary.LittleEndian.AppendUint64(out, build.generation)
	out = binary.LittleEndian.AppendUint32(out, uint32(build.liveRows))

	// Declared tin paths.
	out = binary.LittleEndian.AppendUint32(out, uint32(len(build.paths)))
	for _, p := range build.paths {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(p.name)))
		out = append(out, p.name...)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(p.path)))
		out = append(out, p.path...)
	}

	// Ordinal keys & slots (nil keys encode presence byte 0).
	out = binary.LittleEndian.AppendUint32(out, uint32(len(build.keys)))
	for i, k := range build.keys {
		if k == nil {
			out = append(out, 0)
			continue
		}
		out = append(out, 1)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(k)))
		out = append(out, k...)
		slot := build.slots.slots[i]
		out = binary.LittleEndian.AppendUint32(out, slot.Chunk)
		out = append(out, slot.Bit)
	}

	// Leaf PageRefs + per-bucket key ordinals in deterministic BucketID order.
	buckets := make([]storeio.BucketID, 0, len(build.leafRefs))
	for b := range build.leafRefs {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	out = binary.LittleEndian.AppendUint32(out, uint32(len(buckets)))
	for _, b := range buckets {
		ref := build.leafRefs[b]
		out = binary.LittleEndian.AppendUint32(out, uint32(b))
		out = binary.LittleEndian.AppendUint64(out, ref.Offset)
		out = binary.LittleEndian.AppendUint64(out, ref.LogicalID)
		out = binary.LittleEndian.AppendUint64(out, ref.Generation)
		out = binary.LittleEndian.AppendUint32(out, ref.Length)
		out = append(out, byte(ref.Kind))

		bKeys := build.bucketKeys[b]
		out = binary.LittleEndian.AppendUint32(out, uint32(len(bKeys)))
		for _, k := range bKeys {
			ord, ok := build.keyToOrdinal[string(k)]
			if !ok {
				return nil, fmt.Errorf("vibedb: tin sidecar missing ordinal for bucket key")
			}
			out = binary.LittleEndian.AppendUint64(out, ord)
		}
	}

	// Per-index tin snapshots in declared path order.
	for _, p := range build.paths {
		ix := build.indexes[p.name]
		if ix == nil {
			return nil, fmt.Errorf("vibedb: tin sidecar missing index %q", p.name)
		}
		snapBytes, err := ix.MarshalSnapshot()
		if err != nil {
			return nil, err
		}
		out = binary.LittleEndian.AppendUint32(out, uint32(len(snapBytes)))
		out = append(out, snapBytes...)
	}

	sum := sha256.Sum256(out)
	out = append(out, sum[:]...)
	return out, nil
}

func unmarshalTinSnapshotBuild(
	data []byte,
	expectedStoreID [16]byte,
	expectedPaths []tinDefSpec,
) (*tinSnapshotBuild, error) {
	if len(data) < 4+1+16+8+4+sha256.Size {
		return nil, fmt.Errorf("vibedb: tin sidecar truncated")
	}
	payload := data[:len(data)-sha256.Size]
	wantSum := data[len(data)-sha256.Size:]
	gotSum := sha256.Sum256(payload)
	if string(gotSum[:]) != string(wantSum) {
		return nil, fmt.Errorf("vibedb: tin sidecar checksum mismatch")
	}
	r := &tinSidecarReader{b: payload}
	if string(r.bytes(4)) != tinSidecarMagic {
		return nil, fmt.Errorf("vibedb: tin sidecar bad magic")
	}
	if ver := r.byte(); ver != tinSidecarVersion {
		return nil, fmt.Errorf("vibedb: tin sidecar unsupported version %d", ver)
	}
	storeIDBytes := r.bytes(16)
	if r.err != nil || string(storeIDBytes) != string(expectedStoreID[:]) {
		return nil, fmt.Errorf("vibedb: tin sidecar storeID mismatch")
	}
	generation := r.u64()
	liveRows := int(r.u32())

	nPaths := int(r.u32())
	if r.err != nil || nPaths != len(expectedPaths) {
		return nil, fmt.Errorf("vibedb: tin sidecar path count mismatch")
	}
	paths := make([]tinDefSpec, nPaths)
	for i := 0; i < nPaths; i++ {
		nLen := int(r.u32())
		if r.err != nil || nLen > len(r.b) {
			return nil, fmt.Errorf("vibedb: tin sidecar bad name len")
		}
		name := string(r.bytes(nLen))
		pLen := int(r.u32())
		if r.err != nil || pLen > len(r.b) {
			return nil, fmt.Errorf("vibedb: tin sidecar bad path len")
		}
		path := string(r.bytes(pLen))
		if name != expectedPaths[i].name || path != expectedPaths[i].path {
			return nil, fmt.Errorf("vibedb: tin sidecar declaration mismatch")
		}
		ptr, err := vibejson.CompilePointer(path)
		if err != nil {
			return nil, err
		}
		paths[i] = tinDefSpec{name: name, path: path, pointer: ptr}
	}

	nKeys := int(r.u32())
	if r.err != nil || nKeys > len(r.b) {
		return nil, fmt.Errorf("vibedb: tin sidecar bad key count %d", nKeys)
	}
	keys := make([][]byte, nKeys)
	slots := make([]TinSlotHit, nKeys)
	keyToOrdinal := make(map[string]uint64, liveRows)
	liveCount := 0
	for i := 0; i < nKeys; i++ {
		present := r.byte()
		if r.err != nil {
			return nil, r.err
		}
		if present == 0 {
			continue
		}
		if present != 1 {
			return nil, fmt.Errorf("vibedb: tin sidecar bad presence flag %d", present)
		}
		kLen := int(r.u32())
		if r.err != nil || kLen > len(r.b) {
			return nil, fmt.Errorf("vibedb: tin sidecar bad key len")
		}
		k := append([]byte(nil), r.bytes(kLen)...)
		chunk := r.u32()
		bit := r.byte()
		if r.err != nil {
			return nil, r.err
		}
		keys[i] = k
		slots[i] = TinSlotHit{Chunk: chunk, Bit: bit}
		keyToOrdinal[string(k)] = uint64(i)
		liveCount++
	}
	if liveCount != liveRows {
		return nil, fmt.Errorf("vibedb: tin sidecar liveRows mismatch %d != %d", liveCount, liveRows)
	}

	nBuckets := int(r.u32())
	if r.err != nil || nBuckets > len(r.b)/37 {
		return nil, fmt.Errorf("vibedb: tin sidecar bad bucket count %d", nBuckets)
	}
	leafRefs := make(map[storeio.BucketID]storeio.PageRef, nBuckets)
	bucketKeys := make(map[storeio.BucketID][][]byte, nBuckets)
	for i := 0; i < nBuckets; i++ {
		b := storeio.BucketID(r.u32())
		ref := storeio.PageRef{
			Offset:     r.u64(),
			LogicalID:  r.u64(),
			Generation: r.u64(),
			Length:     r.u32(),
			Kind:       storeio.PageKind(r.byte()),
		}
		nBKeys := int(r.u32())
		if r.err != nil || nBKeys > len(r.b)/8 {
			return nil, fmt.Errorf("vibedb: tin sidecar bad bucket key count %d", nBKeys)
		}
		bKeys := make([][]byte, nBKeys)
		for j := 0; j < nBKeys; j++ {
			ord := r.u64()
			if r.err != nil || ord >= uint64(len(keys)) || keys[ord] == nil {
				return nil, fmt.Errorf("vibedb: tin sidecar bad bucket key ordinal %d", ord)
			}
			bKeys[j] = keys[ord]
		}
		leafRefs[b] = ref
		bucketKeys[b] = bKeys
	}

	indexes := make(map[string]*tin.Index, len(paths))
	for _, p := range paths {
		sLen := int(r.u32())
		if r.err != nil || sLen > len(r.b) {
			return nil, fmt.Errorf("vibedb: tin sidecar bad index snapshot len")
		}
		ix, err := tin.UnmarshalSnapshot(r.bytes(sLen))
		if err != nil {
			return nil, fmt.Errorf("vibedb: tin sidecar unmarshal %q: %w", p.name, err)
		}
		indexes[p.name] = ix
	}
	if r.err != nil || len(r.b) != 0 {
		return nil, fmt.Errorf("vibedb: tin sidecar trailing bytes")
	}

	return &tinSnapshotBuild{
		generation:   generation,
		indexes:      indexes,
		keys:         keys,
		slots:        &tinSlotMap{slots: slots, rows: liveRows},
		paths:        paths,
		keyToOrdinal: keyToOrdinal,
		leafRefs:     leafRefs,
		bucketKeys:   bucketKeys,
		liveRows:     liveRows,
	}, nil
}

type tinSidecarReader struct {
	b   []byte
	err error
}

func (r *tinSidecarReader) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if len(r.b) < n {
		r.err = fmt.Errorf("vibedb: tin sidecar truncated")
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *tinSidecarReader) byte() byte {
	if v := r.bytes(1); v != nil {
		return v[0]
	}
	return 0
}

func (r *tinSidecarReader) u32() uint32 {
	if v := r.bytes(4); v != nil {
		return binary.LittleEndian.Uint32(v)
	}
	return 0
}

func (r *tinSidecarReader) u64() uint64 {
	if v := r.bytes(8); v != nil {
		return binary.LittleEndian.Uint64(v)
	}
	return 0
}
