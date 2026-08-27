//go:build linux

package clusterrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

const restoreProcessChild = "VIBEDB_RESTORE_ACTIVATION_CHILD"

type processInstaller struct{ root string }

func (installer processInstaller) Install(_ context.Context, operation Operation, ordinal uint32,
	artifact io.Reader,
) (RootWitness, error) {
	payload, err := io.ReadAll(io.LimitReader(artifact, 2))
	if err != nil || len(payload) != 1 || payload[0] != byte(ordinal) {
		return RootWitness{}, ErrActivation
	}
	cut := operation.Certificate.Groups[ordinal]
	witness := RootWitness{ArtifactManifest: cut.ArtifactManifestDigest,
		SanitizedImageDigest: filled32(byte(180 + ordinal)), GenesisProof: filled32(byte(190 + ordinal)),
		SnapshotIndex: cut.SnapshotIndex, SnapshotTerm: cut.SnapshotTerm}
	appendGroupKey(witness.TargetGroup[:], operation.Targets[ordinal].Group)
	for replica := range witness.ReplicaRoots {
		content := []byte(fmt.Sprintf("fresh-root/%x/%d/%d", operation.Digest, ordinal, replica))
		digest := sha256.Sum256(content)
		witness.ReplicaRoots[replica] = digest
		path := filepath.Join(installer.root, fmt.Sprintf("group-%04d-replica-%d", ordinal, replica+1))
		if existing, readErr := os.ReadFile(path); readErr == nil {
			if !bytes.Equal(existing, content) {
				return RootWitness{}, ErrActivation
			}
			continue
		} else if !errors.Is(readErr, os.ErrNotExist) {
			return RootWitness{}, readErr
		}
		file, createErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if createErr != nil {
			return RootWitness{}, createErr
		}
		_, writeErr := file.Write(content)
		syncErr := file.Sync()
		closeErr := file.Close()
		if writeErr != nil || syncErr != nil || closeErr != nil {
			return RootWitness{}, errors.Join(writeErr, syncErr, closeErr)
		}
	}
	directory, err := os.Open(installer.root)
	if err != nil {
		return RootWitness{}, err
	}
	if syncErr, closeErr := directory.Sync(), directory.Close(); syncErr != nil || closeErr != nil {
		return RootWitness{}, errors.Join(syncErr, closeErr)
	}
	return witness, nil
}

type processCatalog struct{ path string }

func (catalog processCatalog) Publish(_ context.Context, witness CatalogWitness) error {
	if existing, err := os.ReadFile(catalog.path); err == nil {
		if bytes.Equal(existing, witness.CatalogDigest[:]) {
			return nil
		}
		return ErrActivation
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(catalog.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(witness.CatalogDigest[:])
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(catalog.path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func TestActivationExternalProcessRecoversEveryPublicationCutWithinBounds(t *testing.T) {
	if child := os.Getenv(restoreProcessChild); child != "" {
		runRestoreActivationChild(t, child)
		return
	}
	if os.Getenv("VIBEDB_RESTORE_ACTIVATION_E2E") != "1" {
		t.Skip("mandatory Linux qualification has a dedicated CI invocation")
	}
	started := time.Now()
	maximumRSS := int64(0)
	maximumStorage := int64(0)
	for cut := 1; cut <= 6; cut++ {
		root := filepath.Join(t.TempDir(), "activation")
		groups := root + "-groups"
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(groups, 0o700); err != nil {
			t.Fatal(err)
		}
		run := func(stop int, wantExit int) {
			command := exec.Command(os.Args[0], "-test.run=^TestActivationExternalProcessRecoversEveryPublicationCutWithinBounds$")
			command.Env = append(os.Environ(), restoreProcessChild+"="+root+"|"+groups+"|"+strconv.Itoa(stop))
			output, err := command.CombinedOutput()
			exit := 0
			if err != nil {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatal(err)
				}
				exit = exitErr.ExitCode()
			}
			if exit != wantExit {
				t.Fatalf("cut=%d stop=%d exit=%d want=%d output=%s", cut, stop, exit, wantExit, output)
			}
			if usage, ok := command.ProcessState.SysUsage().(*syscall.Rusage); ok && int64(usage.Maxrss) > maximumRSS {
				maximumRSS = int64(usage.Maxrss)
			}
		}
		run(cut, 91)
		run(0, 0)
		run(0, 0)
		bytesUsed := int64(0)
		if err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				bytesUsed += info.Size()
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := filepath.Walk(groups, func(_ string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				bytesUsed += info.Size()
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if bytesUsed > 1<<20 {
			t.Fatalf("cut=%d durable bytes=%d", cut, bytesUsed)
		}
		if bytesUsed > maximumStorage {
			maximumStorage = bytesUsed
		}
	}
	if elapsed := time.Since(started); elapsed > 15*time.Second {
		t.Fatalf("elapsed=%s", elapsed)
	}
	if maximumRSS > 256<<10 {
		t.Fatalf("maximum RSS=%d KiB", maximumRSS)
	}
	t.Logf("restore_activation_evidence cuts=6 elapsed_micros=%d max_rss_kib=%d max_storage_bytes=%d",
		time.Since(started).Microseconds(), maximumRSS, maximumStorage)
}

func runRestoreActivationChild(t *testing.T, encoded string) {
	parts := bytes.Split([]byte(encoded), []byte{'|'})
	if len(parts) != 3 {
		t.Fatal("invalid child input")
	}
	root, groups := string(parts[0]), string(parts[1])
	stop, err := strconv.Atoi(string(parts[2]))
	if err != nil {
		t.Fatal(err)
	}
	operation := restoreOperationFixture(t, 3)
	source := &testActivationSource{operation: operation}
	seen := 0
	options := Options{Root: root, Operation: operation, Installer: processInstaller{root: groups},
		Catalog: processCatalog{path: groups + "/catalog-witness"}}
	if stop != 0 {
		options.Fault = func(point FaultPoint) error {
			if point == FaultAfterOperation || point == FaultAfterGroup || point == FaultAfterCatalog || point == FaultAfterServingPermit {
				seen++
				if seen == stop {
					os.Exit(91)
				}
			}
			return nil
		}
	}
	permit, err := activateFromSource(t.Context(), options, source)
	if err != nil || permit.Operation != operation.Digest {
		t.Fatalf("permit=%+v err=%v", permit, err)
	}
}
