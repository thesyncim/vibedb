package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFilePrimaryTemplateLeafEndToEnd(t *testing.T) {
	const count = 4000
	built, keys, values := buildRedundantPrimaryCorpus(t, count)
	options := Options{Backend: BackendPortable, ResidentBytes: 64 << 20}
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "tc.vibe"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		t.Fatal(err)
	}
	primary, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	root := primary.state.Load().root
	counts := filePrimaryLeafClassCounts(t, file, root)
	info, _ := file.Stat()
	t.Logf("class split narrow=%d wide=%d template=%d, file=%d bytes",
		counts[storeio.CommonPrimaryLeafNarrow], counts[storeio.CommonPrimaryLeafWide],
		counts[storeio.CommonPrimaryLeafTemplate], info.Size())
	if counts[storeio.CommonPrimaryLeafTemplate] == 0 {
		t.Fatal("no template leaves selected")
	}
	// point reads: resident, page-walk, snapshot
	buf := make([]byte, 0, 128)
	pw := make([]byte, 0, 128)
	for at, key := range keys {
		v, ok, err := primary.AppendRaw(buf[:0], []byte(key))
		if err != nil || !ok || !bytes.Equal(v, values[at]) {
			t.Fatalf("point %d = %q ok=%v err=%v want %q", at, v, ok, err, values[at])
		}
		pwv, pwok, pwerr := primary.resolvePrimaryGraphPageWalk(pw[:0], primary.state.Load(), []byte(key))
		if pwerr != nil || !pwok || !bytes.Equal(pwv, values[at]) {
			t.Fatalf("pagewalk %d mismatch", at)
		}
		buf, pw = v, pwv
	}
	// absent
	if v, ok, err := primary.AppendRaw(buf[:0], []byte("primary-key-absentzzz")); err != nil || ok || len(v) != 0 {
		t.Fatalf("absent = %q %v %v", v, ok, err)
	}
	// scan differential
	snap, err := primary.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	at := 0
	if err := snap.RangeRaw(func(k, v []byte) error {
		if string(k) != keys[at] || !bytes.Equal(v, values[at]) {
			return fmt.Errorf("scan %d = %q/%q want %q/%q", at, k, v, keys[at], values[at])
		}
		at++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if at != count {
		t.Fatalf("scan count %d", at)
	}
	// Verify walks every template leaf, proving common framing, the image
	// checksums, and cross-leaf key order over the all-template graph.
	report, err := Verify(file)
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK() || !report.Primary || report.Documents != count {
		t.Fatalf("verify on template graph: ok=%v primary=%v documents=%d/%d findings=%+v",
			report.OK(), report.Primary, report.Documents, count, report.Findings)
	}
	// mutations that hit the first (template) leaf
	if created, err := primary.Put([]byte("new"), []byte(`{"v":1}`)); err != nil || !created {
		t.Fatalf("Put new = %v %v", created, err)
	}
	if v, ok, err := primary.AppendRaw(buf[:0], []byte("new")); err != nil || !ok || string(v) != `{"v":1}` {
		t.Fatalf("read new = %q %v %v", v, ok, err)
	}
	if deleted, err := primary.Delete([]byte(keys[0])); err != nil || !deleted {
		t.Fatalf("Delete keys[0] = %v %v", deleted, err)
	}
	if _, ok, err := primary.AppendRaw(buf[:0], []byte(keys[0])); err != nil || ok {
		t.Fatalf("read deleted = %v %v", ok, err)
	}
	// remaining still readable after de-template
	if v, ok, err := primary.AppendRaw(buf[:0], []byte(keys[1])); err != nil || !ok || !bytes.Equal(v, values[1]) {
		t.Fatalf("read keys[1] after mutation = %q %v %v", v, ok, err)
	}
}

func TestFilePrimaryTemplateLeafSalvageRoundTrip(t *testing.T) {
	const count = 3000
	built, keys, values := buildRedundantPrimaryCorpus(t, count)
	expected := make(map[string][]byte, count)
	for at := range keys {
		expected[keys[at]] = values[at]
	}
	options := Options{Backend: BackendPortable, ResidentBytes: 32 << 20}
	oracle := createPrimaryPointFile(t, built, options, "tpl-oracle.vibe")
	if cc := filePrimaryLeafClassCounts(t, oracle, mustPrimaryRoot(t, oracle, options)); cc[storeio.CommonPrimaryLeafTemplate] == 0 {
		t.Fatalf("salvage fixture selected no template leaves: %v", cc)
	}
	// Destroy the routing graph above the self-describing template leaves.
	for _, ref := range verifyScanFile(t, oracle, storeio.PagePrimaryCatalog) {
		verifyBreakChecksum(t, oracle, ref)
	}
	for _, ref := range verifyScanFile(t, oracle, storeio.PageTabletRoute) {
		verifyBreakChecksum(t, oracle, ref)
	}
	out, err := os.OpenFile(filepath.Join(t.TempDir(), "tpl-salvaged.vibe"),
		os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	report, err := Salvage(oracle, out, options)
	if err != nil {
		t.Fatalf("salvage failed: %v", err)
	}
	if report.Documents != count {
		t.Fatalf("salvage recovered %d documents, want %d", report.Documents, count)
	}
	collection, err := Open(out, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	buf := make([]byte, 0, 128)
	for at, key := range keys {
		v, ok, err := collection.AppendRaw(buf[:0], []byte(key))
		if err != nil || !ok || !bytes.Equal(v, values[at]) {
			t.Fatalf("salvaged read %d = %q ok=%v err=%v", at, v, ok, err)
		}
		buf = v
	}
}

func mustPrimaryRoot(t testing.TB, file *os.File, options Options) storeio.StateRoot {
	t.Helper()
	c, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.state.Load().root
}

func TestFilePrimaryTemplateLeafDeterministic(t *testing.T) {
	built, _, _ := buildRedundantPrimaryCorpus(t, 3000)
	// Template-leaf encoding is what this test pins, and it is byte-deterministic.
	// The store root is not under the default DurabilitySync, which mints a
	// per-store-unique recovery journal and stamps its random identity into the
	// root — the same non-determinism buffered-visible+RecoveryJournal has. Use a
	// non-journaled mode so the comparison isolates the graph encoding.
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityAsyncVisible,
	}
	write := func(name string) []byte {
		f, err := os.OpenFile(filepath.Join(t.TempDir(), name), os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := CreateFromPrimary(built, f, options); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a := write("a.vibe")
	b := write("b.vibe")
	if !bytes.Equal(a, b) {
		t.Fatalf("template corpus not deterministic: %d vs %d bytes", len(a), len(b))
	}
}
