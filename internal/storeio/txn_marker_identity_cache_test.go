package storeio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func requireTxnMarkerEntryCurrent(t *testing.T, marker *TxnMarker) {
	t.Helper()
	current, err := marker.EntryCurrent()
	if err != nil || !current {
		t.Fatalf("EntryCurrent = %t, %v; want true, nil", current, err)
	}
}

func TestTxnMarkerEntryCurrentAllConstructors(t *testing.T) {
	const capacity = 8 * TxnMarkerMinSectorSize
	t.Run("create-path", func(t *testing.T) {
		marker, err := CreateTxnMarker(
			filepath.Join(t.TempDir(), "txn.vtm"),
			TxnMarkerOptions{Capacity: capacity},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = marker.Close() })
		requireTxnMarkerEntryCurrent(t, marker)
	})

	t.Run("create-at", func(t *testing.T) {
		root, err := os.OpenRoot(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = root.Close() })
		marker, err := CreateTxnMarkerAt(
			root, "txn.vtm", TxnMarkerOptions{Capacity: capacity},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = marker.Close() })
		requireTxnMarkerEntryCurrent(t, marker)
	})

	t.Run("create-recovery-anchor", func(t *testing.T) {
		root, err := os.OpenRoot(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = root.Close() })
		marker, err := CreateTxnMarkerAtRecoveryAnchor(
			root, "txn.vtm", TxnMarkerOptions{Capacity: capacity},
			TxnMarkerRecoveryAnchor{
				MarkerID: [16]byte{1, 2, 3}, Epoch: 2, BaseSequence: 7,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = marker.Close() })
		requireTxnMarkerEntryCurrent(t, marker)
	})

	t.Run("open-path", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "txn.vtm")
		created, err := CreateTxnMarker(path, TxnMarkerOptions{Capacity: capacity})
		if err != nil {
			t.Fatal(err)
		}
		if err := created.Close(); err != nil {
			t.Fatal(err)
		}
		marker, _, err := OpenTxnMarker(path, TxnMarkerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = marker.Close() })
		requireTxnMarkerEntryCurrent(t, marker)
	})

	t.Run("open-at", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "txn.vtm")
		created, err := CreateTxnMarker(path, TxnMarkerOptions{Capacity: capacity})
		if err != nil {
			t.Fatal(err)
		}
		if err := created.Close(); err != nil {
			t.Fatal(err)
		}
		root, err := os.OpenRoot(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = root.Close() })
		marker, _, err := OpenTxnMarkerAt(root, "txn.vtm", TxnMarkerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = marker.Close() })
		requireTxnMarkerEntryCurrent(t, marker)
	})

	t.Run("inspection", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "txn.vtm")
		created, err := CreateTxnMarker(path, TxnMarkerOptions{Capacity: capacity})
		if err != nil {
			t.Fatal(err)
		}
		if err := created.Close(); err != nil {
			t.Fatal(err)
		}
		inspection, _, err := InspectTxnMarker(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = inspection.Close() })
		requireTxnMarkerEntryCurrent(t, inspection.marker)
	})
}

func TestTxnMarkerEntryCurrentSurvivesAppendSyncAndRecycle(t *testing.T) {
	marker, _ := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
	t.Cleanup(func() { _ = marker.Close() })
	requireTxnMarkerEntryCurrent(t, marker)
	if _, err := marker.AppendDecision(1, testTxnCollectionRefs(1)); err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}
	requireTxnMarkerEntryCurrent(t, marker)
	if err := marker.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	requireTxnMarkerEntryCurrent(t, marker)
	if err := marker.Recycle(2); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	requireTxnMarkerEntryCurrent(t, marker)
}

func TestTxnMarkerEntryCurrentRejectsEntryChanges(t *testing.T) {
	for _, test := range []struct {
		name        string
		entry       func(*testing.T, string, string)
		wantErr     bool
		wantSymlink bool
	}{
		{
			name: "renamed-away",
			entry: func(t *testing.T, path, moved string) {
				if err := os.Rename(path, moved); err != nil {
					t.Skipf("cannot rename open marker: %v", err)
				}
			},
			wantErr: true,
		},
		{
			name: "missing",
			entry: func(t *testing.T, path, _ string) {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: true,
		},
		{
			name: "symlink",
			entry: func(t *testing.T, path, moved string) {
				if err := os.Rename(path, moved); err != nil {
					t.Skipf("cannot rename open marker: %v", err)
				}
				if err := os.Symlink(filepath.Base(moved), path); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
			},
			wantSymlink: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "txn.vtm")
			marker, err := CreateTxnMarker(path, TxnMarkerOptions{Capacity: 8 * TxnMarkerMinSectorSize})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = marker.Close() })
			moved := path + ".moved"
			test.entry(t, path, moved)
			current, err := marker.EntryCurrent()
			if current {
				t.Fatal("EntryCurrent accepted a changed marker entry")
			}
			if test.wantErr && err == nil {
				t.Fatal("EntryCurrent error = nil, want missing-entry error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("EntryCurrent error = %v, want false, nil", err)
			}
			if test.wantSymlink {
				info, err := os.Lstat(path)
				if err != nil || info.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("entry after rejection is not the symlink: info=%v err=%v", info, err)
				}
			}
		})
	}
}

func TestTxnMarkerRemoveRejectsReplacementAndPreservesIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "txn.vtm")
	original, err := CreateTxnMarker(path, TxnMarkerOptions{Capacity: 8 * TxnMarkerMinSectorSize})
	if err != nil {
		t.Fatal(err)
	}
	originalMoved := path + ".original"
	if err := os.Rename(path, originalMoved); err != nil {
		_ = original.Close()
		t.Skipf("cannot rename open marker: %v", err)
	}
	replacement, err := CreateTxnMarker(path, TxnMarkerOptions{Capacity: 8 * TxnMarkerMinSectorSize})
	if err != nil {
		_ = original.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replacement.Close() })
	if err := original.Remove(); err == nil {
		t.Fatal("Remove accepted a replacement marker entry")
	}
	if original.file != nil || original.fileIdentity != nil || original.rawConn != nil {
		t.Fatal("rejected Remove retained descriptor identity state")
	}
	requireTxnMarkerEntryCurrent(t, replacement)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("replacement marker was removed: %v", err)
	}
	if _, err := os.Stat(originalMoved); err != nil {
		t.Fatalf("original renamed marker disappeared: %v", err)
	}
}

func TestTxnMarkerEntryCurrentRejectsClosedAndNilManagers(t *testing.T) {
	var nilMarker *TxnMarker
	if current, err := nilMarker.EntryCurrent(); current || !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("nil marker EntryCurrent = %t, %v; want false, ErrInvalidWrite", current, err)
	}

	marker, _ := createTestTxnMarker(t, 8*TxnMarkerMinSectorSize)
	file := marker.file
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if current, err := marker.EntryCurrent(); current || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed descriptor EntryCurrent = %t, %v; want false, os.ErrClosed", current, err)
	}
	if err := marker.Close(); err == nil {
		t.Fatal("Close after private descriptor close returned nil")
	}
	if marker.file != nil || marker.fileIdentity != nil || marker.rawConn != nil || marker.root != nil {
		t.Fatal("Close retained file identity or root state")
	}
	if current, err := marker.EntryCurrent(); current || !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("closed marker EntryCurrent = %t, %v; want false, ErrInvalidWrite", current, err)
	}
}
