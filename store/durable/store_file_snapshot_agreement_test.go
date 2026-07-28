package durable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// A snapshot's point reads and ordered
// scans disagree because point reads consult the live router while scans walk
// the captured PrimaryRoot, and a deferred mutation advances one without the
// other.
func TestFileSnapshotPointScanAgreement(t *testing.T) {
	for _, mode := range []struct {
		name string
		opts Options
	}{
		{"sync-journal", Options{Backend: BackendPortable, ResidentBytes: 32 << 20}},
		{"buffered", Options{
			Backend: BackendPortable, ResidentBytes: 32 << 20,
			Durability:         DurabilityBufferedVisible,
			CheckpointStrength: CheckpointFilesystem,
		}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "repro.db")
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			builder, err := store.NewBuilder(store.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err := builder.Append("k", []byte(`{"v":0}`)); err != nil {
				t.Fatal(err)
			}
			built, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := CreateFromPrimary(built, file, mode.opts); err != nil {
				t.Fatal(err)
			}
			coll, err := Open(file, mode.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer coll.Close()

			if _, err := coll.Put("k", []byte(`{"v":1}`)); err != nil {
				t.Fatal(err)
			}
			snap, err := coll.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()

			point, ok, err := snap.AppendRaw(nil, "k")
			if err != nil || !ok {
				t.Fatalf("snapshot point read: ok=%v err=%v", ok, err)
			}
			var scanned []byte
			if _, err := snap.RangeRawBuffer(nil, func(key, value []byte) error {
				if string(key) == "k" {
					scanned = append([]byte(nil), value...)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if string(point) != string(scanned) {
				t.Errorf("SNAPSHOT SELF-DISAGREEMENT: point=%s scan=%s", point, scanned)
			}
			if string(point) != `{"v":1}` {
				t.Errorf("snapshot point read lost the acknowledged write: %s", point)
			}

			if _, err := coll.Put("k", []byte(`{"v":2}`)); err != nil {
				t.Fatal(err)
			}
			pointAfter, ok, err := snap.AppendRaw(nil, "k")
			if err != nil || !ok {
				t.Fatalf("old-snapshot point read after new put: ok=%v err=%v", ok, err)
			}
			if string(pointAfter) != `{"v":1}` {
				t.Errorf("OLD SNAPSHOT MOVED OR REGRESSED: point after v2 put = %s (want {\"v\":1})", pointAfter)
			}
		})
	}
}
