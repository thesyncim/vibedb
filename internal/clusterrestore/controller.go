package clusterrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
)

var ErrActivation = errors.New("clusterrestore: activation failed")

const rootWitnessBytes = 280

type RootWitness struct {
	TargetGroup          [72]byte
	ArtifactManifest     [sha256.Size]byte
	SanitizedImageDigest [sha256.Size]byte
	GenesisProof         [sha256.Size]byte
	ReplicaRoots         [3][sha256.Size]byte
	SnapshotIndex        uint64
	SnapshotTerm         uint64
}

type GroupInstaller interface {
	// Install constructs or reopens one fresh, non-serving root. It must verify
	// the complete artifact, discard source system/capture authority, and be
	// exactly idempotent when a crash precedes witness publication.
	Install(context.Context, Operation, uint32, io.Reader) (RootWitness, error)
}

type CatalogPublisher interface {
	// Publish conditionally installs the generation-one catalog witness. Exact
	// replay is idempotent; a different witness for the fresh cluster must fail.
	Publish(context.Context, CatalogWitness) error
}

type CatalogWitness struct {
	Operation           [sha256.Size]byte
	CatalogGroup        [72]byte
	GroupsDigest        [sha256.Size]byte
	TargetPolicyDigest  [sha256.Size]byte
	TargetCatalogDigest [sha256.Size]byte
	CatalogDigest       [sha256.Size]byte
}

type ServingPermit struct {
	Operation      [sha256.Size]byte
	CatalogWitness [sha256.Size]byte
	Groups         uint32
	Digest         [sha256.Size]byte
}

type FaultPoint uint8

const (
	FaultAfterOperation FaultPoint = iota + 1
	FaultAfterGroup
	FaultAfterCatalog
	FaultAfterServingPermit
)

type Options struct {
	Root      string
	Staging   *clusterbackup.RestoreStagingRoot
	Operation Operation
	Installer GroupInstaller
	Catalog   CatalogPublisher
	Fault     func(FaultPoint) error
}

type activationSource interface {
	permit() clusterbackup.RestoreStagingPermit
	certificate([sha256.Size]byte) (clusterbackup.Certificate, error)
	openArtifact([sha256.Size]byte, int) (io.ReadCloser, error)
}

type stagingActivationSource struct {
	root *clusterbackup.RestoreStagingRoot
}

func (source stagingActivationSource) permit() clusterbackup.RestoreStagingPermit {
	return source.root.Permit
}
func (source stagingActivationSource) certificate(digest [sha256.Size]byte) (clusterbackup.Certificate, error) {
	return source.root.Repository.Certificate(digest)
}
func (source stagingActivationSource) openArtifact(digest [sha256.Size]byte, ordinal int) (io.ReadCloser, error) {
	return source.root.Repository.OpenArtifact(digest, ordinal)
}

// Activate resumes the sole all-group activation. The only returned serving
// authority is the terminal permit published after every root and catalog CAS.
func Activate(ctx context.Context, options Options) (ServingPermit, error) {
	if options.Staging == nil || options.Staging.Repository == nil {
		return ServingPermit{}, ErrActivation
	}
	return activateFromSource(ctx, options, stagingActivationSource{options.Staging})
}

func activateFromSource(ctx context.Context, options Options, source activationSource) (ServingPermit, error) {
	if ctx == nil || source == nil || options.Installer == nil || options.Catalog == nil ||
		!privateActivationRoot(options.Root) {
		return ServingPermit{}, ErrActivation
	}
	rawOperation, err := AppendOperation(nil, options.Operation)
	if err != nil {
		return ServingPermit{}, err
	}
	opened, err := OpenOperation(rawOperation)
	if err != nil || opened.Digest != options.Operation.Digest || opened.Permit != source.permit() {
		return ServingPermit{}, errors.Join(ErrActivation, err)
	}
	certificate, err := source.certificate(opened.Certificate.Digest)
	if err != nil || !equalCertificate(certificate, opened.Certificate) {
		return ServingPermit{}, errors.Join(ErrActivation, err)
	}
	root, err := os.OpenRoot(options.Root)
	if err != nil {
		return ServingPermit{}, err
	}
	defer root.Close()
	if err = validateActivationEntries(root); err != nil {
		return ServingPermit{}, err
	}
	if err = publishExact(root, "operation", rawOperation); err != nil {
		return ServingPermit{}, err
	}
	if err = inject(options.Fault, FaultAfterOperation); err != nil {
		return ServingPermit{}, err
	}
	progress, err := readProgress(root, opened)
	if err != nil {
		return ServingPermit{}, err
	}
	for ordinal := len(progress.Roots); ordinal < len(opened.Targets); ordinal++ {
		if cause := context.Cause(ctx); cause != nil {
			return ServingPermit{}, cause
		}
		artifact, openErr := source.openArtifact(opened.Certificate.Digest, ordinal)
		if openErr != nil {
			return ServingPermit{}, openErr
		}
		witness, installErr := options.Installer.Install(ctx, opened, uint32(ordinal), artifact)
		closeErr := artifact.Close()
		if installErr != nil || closeErr != nil || !validRootWitness(opened, ordinal, witness) {
			return ServingPermit{}, errors.Join(ErrActivation, installErr, closeErr)
		}
		progress.Roots = append(progress.Roots, witness)
		if err = writeProgress(root, progress); err != nil {
			return ServingPermit{}, err
		}
		if err = inject(options.Fault, FaultAfterGroup); err != nil {
			return ServingPermit{}, err
		}
	}
	catalog := makeCatalogWitness(opened, progress.Roots)
	if progress.Catalog == ([sha256.Size]byte{}) {
		if err = options.Catalog.Publish(ctx, catalog); err != nil {
			return ServingPermit{}, err
		}
		progress.Catalog = catalog.CatalogDigest
		if err = writeProgress(root, progress); err != nil {
			return ServingPermit{}, err
		}
	} else if progress.Catalog != catalog.CatalogDigest {
		return ServingPermit{}, ErrActivation
	}
	if err = inject(options.Fault, FaultAfterCatalog); err != nil {
		return ServingPermit{}, err
	}
	permit := makeServingPermit(opened, catalog)
	permitRaw := appendServingPermit(nil, permit)
	if err = publishExact(root, "serving.permit", permitRaw); err != nil {
		return ServingPermit{}, err
	}
	if err = inject(options.Fault, FaultAfterServingPermit); err != nil {
		return ServingPermit{}, err
	}
	return permit, nil
}

func privateActivationRoot(path string) bool {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm()&0o077 == 0
}

func equalCertificate(a, b clusterbackup.Certificate) bool {
	left, leftErr := clusterbackup.AppendCertificate(nil, a)
	right, rightErr := clusterbackup.AppendCertificate(nil, b)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}

func validRootWitness(operation Operation, ordinal int, witness RootWitness) bool {
	var target [72]byte
	appendGroupKey(target[:], operation.Targets[ordinal].Group)
	cut := operation.Certificate.Groups[ordinal]
	if witness.TargetGroup != target || witness.ArtifactManifest != cut.ArtifactManifestDigest ||
		witness.SanitizedImageDigest == ([sha256.Size]byte{}) || witness.GenesisProof == ([sha256.Size]byte{}) ||
		witness.SnapshotIndex != cut.SnapshotIndex || witness.SnapshotTerm != cut.SnapshotTerm {
		return false
	}
	for ordinal, root := range witness.ReplicaRoots {
		if root == ([sha256.Size]byte{}) {
			return false
		}
		for prior := 0; prior < ordinal; prior++ {
			if root == witness.ReplicaRoots[prior] {
				return false
			}
		}
	}
	return true
}

func makeCatalogWitness(operation Operation, roots []RootWitness) CatalogWitness {
	hasher := sha256.New()
	hasher.Write([]byte("vibedb/restore/groups-witness/format-1\x00"))
	for _, root := range roots {
		hasher.Write(appendRootWitness(nil, root))
	}
	var groups [sha256.Size]byte
	copy(groups[:], hasher.Sum(nil))
	var catalogGroup [72]byte
	appendGroupKey(catalogGroup[:], operation.Targets[operation.CatalogOrdinal].Group)
	witness := CatalogWitness{Operation: operation.Digest, CatalogGroup: catalogGroup,
		GroupsDigest: groups, TargetPolicyDigest: operation.TargetPolicyDigest,
		TargetCatalogDigest: operation.TargetCatalogDigest}
	hasher.Reset()
	hasher.Write([]byte("vibedb/restore/catalog-witness/format-1\x00"))
	hasher.Write(witness.Operation[:])
	hasher.Write(witness.CatalogGroup[:])
	hasher.Write(witness.GroupsDigest[:])
	hasher.Write(witness.TargetPolicyDigest[:])
	hasher.Write(witness.TargetCatalogDigest[:])
	copy(witness.CatalogDigest[:], hasher.Sum(nil))
	return witness
}

func makeServingPermit(operation Operation, catalog CatalogWitness) ServingPermit {
	permit := ServingPermit{Operation: operation.Digest, CatalogWitness: catalog.CatalogDigest,
		Groups: uint32(len(operation.Targets))}
	raw := appendServingPermit(nil, permit)
	permit.Digest = sha256.Sum256(raw[:len(raw)-sha256.Size])
	return permit
}

func appendServingPermit(dst []byte, permit ServingPermit) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, 128)...)
	raw := dst[start:]
	copy(raw[:8], []byte("VBRSSERV"))
	binary.BigEndian.PutUint16(raw[8:10], 1)
	copy(raw[16:48], permit.Operation[:])
	copy(raw[48:80], permit.CatalogWitness[:])
	binary.BigEndian.PutUint32(raw[80:84], permit.Groups)
	digest := sha256.Sum256(raw[:80+16])
	copy(raw[80+16:], digest[:])
	return dst
}

func OpenServingPermit(raw []byte) (ServingPermit, error) {
	if len(raw) != 128 || !bytes.Equal(raw[:8], []byte("VBRSSERV")) ||
		binary.BigEndian.Uint16(raw[8:10]) != 1 || !allZero(raw[10:16]) || !allZero(raw[84:96]) {
		return ServingPermit{}, ErrActivation
	}
	digest := sha256.Sum256(raw[:96])
	if digest != [sha256.Size]byte(raw[96:]) {
		return ServingPermit{}, ErrActivation
	}
	permit := ServingPermit{Groups: binary.BigEndian.Uint32(raw[80:84]), Digest: digest}
	copy(permit.Operation[:], raw[16:48])
	copy(permit.CatalogWitness[:], raw[48:80])
	if permit.Operation == ([sha256.Size]byte{}) || permit.CatalogWitness == ([sha256.Size]byte{}) || permit.Groups == 0 {
		return ServingPermit{}, ErrActivation
	}
	return permit, nil
}

func inject(fault func(FaultPoint) error, point FaultPoint) error {
	if fault == nil {
		return nil
	}
	return fault(point)
}

func allZero(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}

func validateActivationEntries(root *os.Root) error {
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Type().IsRegular() && (entry.Name() == "operation" || entry.Name() == "progress" ||
			entry.Name() == "serving.permit" || entry.Name() == ".write") {
			continue
		}
		return ErrActivation
	}
	return nil
}
