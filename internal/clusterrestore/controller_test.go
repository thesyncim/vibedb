package clusterrestore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
)

type testActivationSource struct {
	operation Operation
	opens     int
}

func (source *testActivationSource) permit() clusterbackup.RestoreStagingPermit {
	return source.operation.Permit
}
func (source *testActivationSource) certificate(digest [32]byte) (clusterbackup.Certificate, error) {
	if digest != source.operation.Certificate.Digest {
		return clusterbackup.Certificate{}, ErrActivation
	}
	return source.operation.Certificate, nil
}
func (source *testActivationSource) openArtifact(digest [32]byte, ordinal int) (io.ReadCloser, error) {
	if digest != source.operation.Certificate.Digest || ordinal < 0 || ordinal >= len(source.operation.Targets) {
		return nil, ErrActivation
	}
	source.opens++
	return io.NopCloser(bytes.NewReader([]byte{byte(ordinal)})), nil
}

type testInstaller struct{ calls int }

func (installer *testInstaller) Install(_ context.Context, operation Operation, ordinal uint32,
	artifact io.Reader,
) (RootWitness, error) {
	if _, err := io.Copy(io.Discard, artifact); err != nil {
		return RootWitness{}, err
	}
	installer.calls++
	cut := operation.Certificate.Groups[ordinal]
	witness := RootWitness{ArtifactManifest: cut.ArtifactManifestDigest,
		SanitizedImageDigest: filled32(byte(180 + ordinal)), GenesisProof: filled32(byte(190 + ordinal)),
		SnapshotIndex: cut.SnapshotIndex, SnapshotTerm: cut.SnapshotTerm}
	appendGroupKey(witness.TargetGroup[:], operation.Targets[ordinal].Group)
	for replica := range witness.ReplicaRoots {
		witness.ReplicaRoots[replica] = filled32(byte(200 + int(ordinal)*3 + replica))
	}
	return witness, nil
}

type testCatalog struct {
	calls   int
	witness CatalogWitness
}

func (catalog *testCatalog) Publish(_ context.Context, witness CatalogWitness) error {
	catalog.calls++
	if catalog.witness.CatalogDigest == ([32]byte{}) {
		catalog.witness = witness
		return nil
	}
	if catalog.witness != witness {
		return ErrActivation
	}
	return nil
}

func privateRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "activation")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestActivationResumesEveryGroupCatalogAndServingPublicationCut(t *testing.T) {
	operation := restoreOperationFixture(t, 3)
	events := len(operation.Targets) + 3
	for stop := 1; stop <= events; stop++ {
		t.Run(string(rune('a'+stop-1)), func(t *testing.T) {
			source := &testActivationSource{operation: operation}
			installer, catalog := &testInstaller{}, &testCatalog{}
			seen := 0
			options := Options{Root: privateRoot(t), Operation: operation, Installer: installer, Catalog: catalog,
				Fault: func(point FaultPoint) error {
					if point == FaultAfterOperation || point == FaultAfterGroup || point == FaultAfterCatalog || point == FaultAfterServingPermit {
						seen++
						if seen == stop {
							return errors.New("crash")
						}
					}
					return nil
				}}
			if _, err := activateFromSource(t.Context(), options, source); err == nil {
				t.Fatal("fault did not stop activation")
			}
			if stop <= len(operation.Targets)+2 {
				if _, err := os.Stat(filepath.Join(options.Root, "serving.permit")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("serving authority escaped before terminal cut: %v", err)
				}
			}
			options.Fault = nil
			permit, err := activateFromSource(t.Context(), options, source)
			if err != nil || permit.Operation != operation.Digest || permit.Groups != 3 {
				t.Fatalf("permit=%+v err=%v", permit, err)
			}
			if installer.calls != 3 || source.opens != 3 || catalog.calls != 1 {
				t.Fatalf("calls installer=%d opens=%d catalog=%d", installer.calls, source.opens, catalog.calls)
			}
			again, err := activateFromSource(t.Context(), options, source)
			if err != nil || again != permit || installer.calls != 3 || catalog.calls != 1 {
				t.Fatalf("duplicate permit=%+v calls=%d/%d err=%v", again, installer.calls, catalog.calls, err)
			}
		})
	}
}

func TestActivationRejectsCorruptionPartialAndUnsafeRoot(t *testing.T) {
	operation := restoreOperationFixture(t, 2)
	source, installer, catalog := &testActivationSource{operation: operation}, &testInstaller{}, &testCatalog{}
	root := privateRoot(t)
	options := Options{Root: root, Operation: operation, Installer: installer, Catalog: catalog,
		Fault: func(point FaultPoint) error {
			if point == FaultAfterGroup {
				return errors.New("crash")
			}
			return nil
		}}
	if _, err := activateFromSource(t.Context(), options, source); err == nil {
		t.Fatal("fault did not stop")
	}
	progress := filepath.Join(root, "progress")
	raw, err := os.ReadFile(progress)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 1
	if err = os.WriteFile(progress, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	options.Fault = nil
	if _, err = activateFromSource(t.Context(), options, source); !errors.Is(err, ErrActivation) {
		t.Fatalf("corruption err=%v", err)
	}
	unsafe := privateRoot(t)
	if err = os.Chmod(unsafe, 0o755); err != nil {
		t.Fatal(err)
	}
	options.Root = unsafe
	if _, err = activateFromSource(t.Context(), options, source); !errors.Is(err, ErrActivation) {
		t.Fatalf("unsafe err=%v", err)
	}
}

func TestServingPermitCanonicalCorruptionRejection(t *testing.T) {
	operation := restoreOperationFixture(t, 1)
	catalog := makeCatalogWitness(operation, []RootWitness{func() RootWitness {
		installer := &testInstaller{}
		witness, _ := installer.Install(t.Context(), operation, 0, bytes.NewReader(nil))
		return witness
	}()})
	permit := makeServingPermit(operation, catalog)
	raw := appendServingPermit(nil, permit)
	opened, err := OpenServingPermit(raw)
	if err != nil || opened != permit {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	raw[len(raw)-1] ^= 1
	if _, err = OpenServingPermit(raw); err == nil {
		t.Fatal("accepted corrupt serving permit")
	}
}
