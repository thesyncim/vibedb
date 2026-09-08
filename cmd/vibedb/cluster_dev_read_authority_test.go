package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibejson"
)

type devReadAuthorityTestClock struct{ err error }

func (clock devReadAuthorityTestClock) Now() (time.Duration, error) {
	if clock.err != nil {
		return 0, clock.err
	}
	return time.Second, nil
}

func useDevReadAuthorityClock(t *testing.T, factory func() (raftauthority.ElapsedClock, error)) {
	t.Helper()
	previous := newDevQualifiedElapsedClock
	newDevQualifiedElapsedClock = factory
	t.Cleanup(func() { newDevQualifiedElapsedClock = previous })
}

func TestDevReadAuthorityIsExplicitRF3OnlyAndFixed(t *testing.T) {
	if newDevReadAuthority(false) != nil {
		t.Fatal("default development cluster unexpectedly enabled read authority")
	}
	config := newDevReadAuthority(true)
	if config == nil || !validDevReadAuthority(*config) {
		t.Fatalf("fixed RF3 read authority policy is invalid: %+v", config)
	}
	config.Voters[0] = 2
	if validDevReadAuthority(*config) {
		t.Fatal("changed voter roster accepted as the fixed policy")
	}
}

func TestDevReadAuthorityRejectsRF1(t *testing.T) {
	root := t.TempDir()
	if _, err := ensureDevCluster(devClusterOptions{
		root: root, replicas: devClusterRF1, readAuthority: true,
	}); !errors.Is(err, errDevCluster) {
		t.Fatalf("RF1 read authority request error = %v", err)
	}
}

func TestDevReadAuthorityRejectsStandaloneRF3(t *testing.T) {
	root := t.TempDir()
	if _, err := ensureDevCluster(devClusterOptions{
		root: root, replicas: devClusterRF3, readAuthority: true,
	}); !errors.Is(err, errDevCluster) {
		t.Fatalf("standalone RF3 read authority request error = %v", err)
	}
}

func TestDevReadAuthorityFreshDefaultUsesQualifiedClock(t *testing.T) {
	useDevReadAuthorityClock(t, func() (raftauthority.ElapsedClock, error) {
		return devReadAuthorityTestClock{}, nil
	})
	for _, physical := range []int{devClusterPhysicalNodes3, devClusterPhysicalNodes6} {
		resolved, err := resolveDevReadAuthority(devClusterOptions{
			replicas: devClusterRF3, physicalNodes: physical,
		}, nil)
		if err != nil || !resolved.readAuthority {
			t.Fatalf("fresh RF3 physical=%d default = enabled=%t err=%v", physical, resolved.readAuthority, err)
		}
	}
}

func TestDevReadAuthorityFreshUnsupportedClockFallsBackOnlyWhenOmitted(t *testing.T) {
	useDevReadAuthorityClock(t, func() (raftauthority.ElapsedClock, error) {
		return nil, raftauthority.ErrClockUnavailable
	})
	resolved, err := resolveDevReadAuthority(devClusterOptions{
		replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
	}, nil)
	if err != nil || resolved.readAuthority {
		t.Fatalf("omitted unsupported clock = enabled=%t err=%v, want disabled", resolved.readAuthority, err)
	}
	_, err = resolveDevReadAuthority(devClusterOptions{
		replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
		readAuthority: true,
	}, nil)
	if !errors.Is(err, errDevCluster) {
		t.Fatalf("explicit unsupported clock error = %v", err)
	}
}

func TestDevReadAuthorityFreshSelectionPersistsThroughEnsure(t *testing.T) {
	useDevReadAuthorityClock(t, func() (raftauthority.ElapsedClock, error) {
		return devReadAuthorityTestClock{}, nil
	})
	for _, test := range []struct {
		name             string
		readAuthority    bool
		readAuthoritySet bool
		wantEnabled      bool
	}{
		{name: "omitted", wantEnabled: true},
		{name: "explicit false", readAuthoritySet: true, wantEnabled: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			_, err := ensureDevCluster(devClusterOptions{
				root: root, replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
				shardBinary: "/usr/bin/true", readAuthority: test.readAuthority,
				readAuthoritySet: test.readAuthoritySet,
			})
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("ensure fresh cluster error = %v, want missing prepared serve manifest", err)
			}
			raw, err := os.ReadFile(filepath.Join(root, "cluster.vibejson"))
			if err != nil {
				t.Fatal(err)
			}
			var manifest devClusterManifest
			if err := vibejson.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			if !validDevManifest(manifest, root) {
				t.Fatal("persisted fresh manifest failed validation")
			}
			canonical, err := vibejson.Marshal(&manifest)
			if err != nil || !bytes.Equal(raw, canonical) {
				t.Fatalf("persisted fresh manifest is not canonical: %v", err)
			}
			if got := manifest.ReadAuthority != nil; got != test.wantEnabled {
				t.Fatalf("persisted read authority=%t, want %t", got, test.wantEnabled)
			}
		})
	}
}

func TestDevReadAuthorityClockFaultIsNotTreatedAsUnsupported(t *testing.T) {
	clockErr := errors.New("clock fault")
	useDevReadAuthorityClock(t, func() (raftauthority.ElapsedClock, error) {
		return devReadAuthorityTestClock{err: errors.Join(raftauthority.ErrClockFault, clockErr)}, nil
	})
	_, err := resolveDevReadAuthority(devClusterOptions{
		replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
	}, nil)
	if !errors.Is(err, raftauthority.ErrClockFault) || !errors.Is(err, clockErr) {
		t.Fatalf("clock fault error = %v", err)
	}
}

func TestDevReadAuthorityRetainedPolicyIsExactAndFlagMismatchRefuses(t *testing.T) {
	useDevReadAuthorityClock(t, func() (raftauthority.ElapsedClock, error) {
		return devReadAuthorityTestClock{}, nil
	})
	retained := &devClusterManifest{ReadAuthority: newDevReadAuthority(true)}
	resolved, err := resolveDevReadAuthority(devClusterOptions{
		replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
	}, retained)
	if err != nil || !resolved.readAuthority || !devReadAuthorityEqual(retained.ReadAuthority, newDevReadAuthority(true)) {
		t.Fatalf("omitted retained enabled policy = enabled=%t err=%v policy=%+v", resolved.readAuthority, err, retained.ReadAuthority)
	}
	if _, err := resolveDevReadAuthority(devClusterOptions{
		replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
		readAuthoritySet: true, readAuthority: false,
	}, retained); !errors.Is(err, errDevCluster) {
		t.Fatalf("explicit false retained enabled error = %v", err)
	}
	retained.ReadAuthority = nil
	if _, err := resolveDevReadAuthority(devClusterOptions{
		replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
		readAuthoritySet: true, readAuthority: true,
	}, retained); !errors.Is(err, errDevCluster) {
		t.Fatalf("explicit true retained disabled error = %v", err)
	}
}

func TestDevReadAuthorityRetainedDisabledStaysNilWithoutClock(t *testing.T) {
	called := false
	useDevReadAuthorityClock(t, func() (raftauthority.ElapsedClock, error) {
		called = true
		return devReadAuthorityTestClock{}, nil
	})
	resolved, err := resolveDevReadAuthority(devClusterOptions{
		replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
	}, &devClusterManifest{})
	if err != nil || resolved.readAuthority || called {
		t.Fatalf("retained nil policy = enabled=%t called=%t err=%v", resolved.readAuthority, called, err)
	}
}

func snapshotDevRegularFiles(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[relative] = raw
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func assertDevRegularFilesUnchanged(t *testing.T, root string, before map[string][]byte) {
	t.Helper()
	after := snapshotDevRegularFiles(t, root)
	if len(after) != len(before) {
		t.Fatalf("retained file count changed from %d to %d", len(before), len(after))
	}
	for path, original := range before {
		if current, ok := after[path]; !ok || !bytes.Equal(current, original) {
			t.Fatalf("retained file %q changed", path)
		}
	}
}

func TestDevReadAuthorityRetainedFilesRefuseUnavailableOrMismatch(t *testing.T) {
	useDevReadAuthorityClock(t, func() (raftauthority.ElapsedClock, error) {
		return devReadAuthorityTestClock{}, nil
	})
	root := t.TempDir()
	_, err := ensureDevCluster(devClusterOptions{
		root: root, replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
		shardBinary: "/usr/bin/true",
	})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("initial retained fixture error = %v, want missing prepared serve manifest", err)
	}
	manifestRaw, err := os.ReadFile(filepath.Join(root, "cluster.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest devClusterManifest
	if err := vibejson.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.ReadAuthority == nil || !validDevReadAuthority(*manifest.ReadAuthority) {
		t.Fatalf("initial retained policy = %+v", manifest.ReadAuthority)
	}
	before := snapshotDevRegularFiles(t, root)
	useDevReadAuthorityClock(t, func() (raftauthority.ElapsedClock, error) {
		return nil, raftauthority.ErrClockUnavailable
	})
	if _, err := resolveDevReadAuthority(devClusterOptions{
		replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
	}, &manifest); !errors.Is(err, errDevCluster) {
		t.Fatalf("retained enabled unavailable policy error = %v", err)
	}
	if _, err := ensureDevCluster(devClusterOptions{
		root: root, replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
		shardBinary: "/usr/bin/true",
	}); !errors.Is(err, errDevCluster) {
		t.Fatalf("retained enabled unavailable ensure error = %v", err)
	}
	assertDevRegularFilesUnchanged(t, root, before)
	if _, err := ensureDevCluster(devClusterOptions{
		root: root, replicas: devClusterRF3, physicalNodes: devClusterPhysicalNodes3,
		shardBinary: "/usr/bin/true", readAuthoritySet: true, readAuthority: false,
	}); !errors.Is(err, errDevCluster) {
		t.Fatalf("retained enabled explicit false ensure error = %v", err)
	}
	assertDevRegularFilesUnchanged(t, root, before)
}

func TestDevPhysicalReadAuthorityManifestReconciliationSupportsMultipleTables(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			root := t.TempDir()
			nodeRoot := filepath.Join(root, "node-1")
			groupRoot := filepath.Join(nodeRoot, "group-1")
			expected := newDevReadAuthority(enabled)
			if err := os.MkdirAll(groupRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			serveManifest := filepath.Join(nodeRoot, "serve-rf3.vibejson")
			groupManifest := filepath.Join(groupRoot, "serve-rf3.vibejson")
			groupSource := map[string]any{
				"wal":           map[string]any{},
				"sql":           map[string]any{},
				"route":         map[string]any{},
				"split_control": map[string]any{"child_registry": map[string]any{}},
				"members":       []any{},
			}
			if enabled {
				groupSource["read_authority"] = expected
			}
			groupRaw, err := json.Marshal(groupSource)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(groupManifest, groupRaw, 0o600); err != nil {
				t.Fatal(err)
			}
			source := map[string]any{
				"node_log":             map[string]any{},
				"listeners":            map[string]any{},
				"tls":                  map[string]any{},
				"authorization_policy": "policy",
				"replica_control":      map[string]any{},
				"split_control":        map[string]any{},
				"gateway":              map[string]any{},
				"groups":               []any{map[string]any{}},
			}
			if enabled {
				source["read_authority"] = expected
			}
			nodeRaw, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(serveManifest, nodeRaw, 0o600); err != nil {
				t.Fatal(err)
			}

			member := devClusterMember{GroupRoot: groupRoot, ServeManifest: serveManifest}
			if err := reconcileDevPhysicalNodeGroup(member, true, expected); err != nil {
				t.Fatalf("reconcile enabled=%t: %v", enabled, err)
			}
			updated, err := os.ReadFile(serveManifest)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(updated, &fields); err != nil {
				t.Fatal(err)
			}
			var groups []json.RawMessage
			if err := json.Unmarshal(fields["groups"], &groups); err != nil || len(groups) != 2 {
				t.Fatalf("reconciled groups=%d err=%v", len(groups), err)
			}
			if enabled != (len(fields["read_authority"]) != 0) {
				t.Fatalf("read authority field presence=%t, want %t", len(fields["read_authority"]) != 0, enabled)
			}
		})
	}
}
