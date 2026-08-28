package snapshottransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"sort"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const AbandonmentWitnessBytes = 432

var abandonmentWitnessMagic = [8]byte{'V', 'B', 'A', 'B', 'N', 'D', 0, 0}

var (
	ErrAbandonment         = errors.New("snapshottransfer: invalid replicated abandonment witness")
	ErrRetainedBytes       = errors.New("snapshottransfer: retained artifact bytes exceed gate")
	ErrCollectorIncomplete = errors.New("snapshottransfer: abandonment collection requires another bounded pass")
)

// ArtifactAbandonmentWitness is the exact record an implementation backed by
// a replicated control plane must return. OwnerEpoch and LeaseRevision bind
// the former owner; LeaseAppliedThrough is the last replicated index at which
// that lease could authorize transfer progress. AbandonedAppliedThrough must
// be strictly later, preventing a slow or partitioned transfer from being
// mistaken for abandoned work.
type ArtifactAbandonmentWitness struct {
	Operation               [sha256.Size]byte
	Step                    [sha256.Size]byte
	Artifact                [sha256.Size]byte
	TargetStore             [16]byte
	TargetIncarnation       uint64
	SchemaGeneration        uint64
	ReplicaSetVersion       uint64
	Owner                   rafttransport.NodeID
	OwnerEpoch              uint64
	LeaseRevision           uint64
	LeaseAppliedThrough     uint64
	AbandonedAppliedThrough uint64
	AuthorityRevision       uint64
	Descriptor              Descriptor
}

func (w ArtifactAbandonmentWitness) Valid() bool {
	d := w.Descriptor
	return w.Operation != ([sha256.Size]byte{}) && w.Step != ([sha256.Size]byte{}) &&
		w.Artifact != ([sha256.Size]byte{}) &&
		w.TargetStore != ([16]byte{}) && w.TargetIncarnation != 0 &&
		w.SchemaGeneration != 0 && w.ReplicaSetVersion != 0 &&
		w.Owner != (rafttransport.NodeID{}) && w.OwnerEpoch != 0 && w.LeaseRevision != 0 &&
		w.LeaseAppliedThrough != 0 && w.AbandonedAppliedThrough > w.LeaseAppliedThrough &&
		w.AuthorityRevision != 0 && d.Valid() && w.Artifact == d.ArtifactHash &&
		w.TargetStore == d.TargetStore && w.TargetIncarnation == d.TargetIncarnation &&
		w.SchemaGeneration == d.SchemaGeneration && w.ReplicaSetVersion == d.ReplicaSetVersion
}

// AppendAbandonmentWitness appends the fixed, canonical replicated grammar.
// It is deliberately independent of JSON encoders so byte identity is stable
// across the catalog authority, source transport, and crash journal.
func AppendAbandonmentWitness(dst []byte, witness ArtifactAbandonmentWitness) ([]byte, error) {
	if !witness.Valid() || len(dst) > math.MaxInt-AbandonmentWitnessBytes {
		return dst, ErrAbandonment
	}
	start := len(dst)
	dst = append(dst, make([]byte, AbandonmentWitnessBytes)...)
	out := dst[start:]
	copy(out[:8], abandonmentWitnessMagic[:])
	copy(out[8:40], witness.Operation[:])
	copy(out[40:72], witness.Step[:])
	copy(out[72:104], witness.Artifact[:])
	copy(out[104:120], witness.TargetStore[:])
	binary.BigEndian.PutUint64(out[120:128], witness.TargetIncarnation)
	binary.BigEndian.PutUint64(out[128:136], witness.SchemaGeneration)
	binary.BigEndian.PutUint64(out[136:144], witness.ReplicaSetVersion)
	copy(out[144:160], witness.Owner[:])
	binary.BigEndian.PutUint64(out[160:168], witness.OwnerEpoch)
	binary.BigEndian.PutUint64(out[168:176], witness.LeaseRevision)
	binary.BigEndian.PutUint64(out[176:184], witness.LeaseAppliedThrough)
	binary.BigEndian.PutUint64(out[184:192], witness.AbandonedAppliedThrough)
	binary.BigEndian.PutUint64(out[192:200], witness.AuthorityRevision)
	encoded, err := AppendDescriptor(out[:200], witness.Descriptor)
	if err != nil || len(encoded) != AbandonmentWitnessBytes {
		return dst[:start], errors.Join(ErrAbandonment, err)
	}
	return dst, nil
}

func OpenAbandonmentWitness(raw []byte) (ArtifactAbandonmentWitness, error) {
	if len(raw) != AbandonmentWitnessBytes || !bytes.Equal(raw[:8], abandonmentWitnessMagic[:]) {
		return ArtifactAbandonmentWitness{}, ErrAbandonment
	}
	var witness ArtifactAbandonmentWitness
	copy(witness.Operation[:], raw[8:40])
	copy(witness.Step[:], raw[40:72])
	copy(witness.Artifact[:], raw[72:104])
	copy(witness.TargetStore[:], raw[104:120])
	witness.TargetIncarnation = binary.BigEndian.Uint64(raw[120:128])
	witness.SchemaGeneration = binary.BigEndian.Uint64(raw[128:136])
	witness.ReplicaSetVersion = binary.BigEndian.Uint64(raw[136:144])
	copy(witness.Owner[:], raw[144:160])
	witness.OwnerEpoch = binary.BigEndian.Uint64(raw[160:168])
	witness.LeaseRevision = binary.BigEndian.Uint64(raw[168:176])
	witness.LeaseAppliedThrough = binary.BigEndian.Uint64(raw[176:184])
	witness.AbandonedAppliedThrough = binary.BigEndian.Uint64(raw[184:192])
	witness.AuthorityRevision = binary.BigEndian.Uint64(raw[192:200])
	var err error
	witness.Descriptor, err = OpenDescriptor(raw[200:])
	if err != nil || !witness.Valid() {
		return ArtifactAbandonmentWitness{}, errors.Join(ErrAbandonment, err)
	}
	return witness, nil
}

// ArtifactAbandonmentAuthority must read from the replicated owner/lease
// authority. Local clocks, directory age, and absence of a process are not
// sufficient evidence and deliberately cannot satisfy this interface.
type ArtifactAbandonmentAuthority interface {
	ReadArtifactAbandonment(context.Context, [sha256.Size]byte) (ArtifactAbandonmentWitness, bool, error)
}

// SourceExportCursor is a deterministic, resumable position in the durable
// source journal. AfterOperation is exclusive. Done means the caller reached
// the end of this cut and may begin a later sweep from the zero cursor.
type SourceExportCursor struct {
	AfterOperation [sha256.Size]byte
	Done           bool
}

// ScanSourceExports returns at most len(dst) records in operation order. The
// fixed repository limit bounds its sort workspace even after a crash/reopen.
func (journal *SourceFileJournal) ScanSourceExports(
	ctx context.Context, cursor SourceExportCursor, dst []SourceControlRecord,
) ([]SourceControlRecord, SourceExportCursor, error) {
	if journal == nil || ctx == nil || cursor.Done || cap(dst) == 0 || cap(dst) > AbsoluteMaxSourceRecords {
		return dst[:0], cursor, ErrBound
	}
	if cause := context.Cause(ctx); cause != nil {
		return dst[:0], cursor, cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return dst[:0], cursor, ErrSourceControl
	}
	keys := make([][sha256.Size]byte, 0, len(journal.records))
	for operation := range journal.records {
		if bytes.Compare(operation[:], cursor.AfterOperation[:]) > 0 {
			keys = append(keys, operation)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	count := min(len(keys), cap(dst))
	dst = dst[:count]
	for i := 0; i < count; i++ {
		dst[i] = journal.records[keys[i]]
		cursor.AfterOperation = keys[i]
	}
	cursor.Done = count == len(keys)
	return dst, cursor, nil
}

type AbandonmentCollectorOptions struct {
	Journal           *SourceFileJournal
	Repository        *Repository
	Authority         ArtifactAbandonmentAuthority
	MaxRecords        int
	MaxReclaimedBytes uint64
	MaxRetainedBytes  uint64
}

type AbandonmentCollector struct{ options AbandonmentCollectorOptions }

type AbandonmentPass struct {
	Cursor         SourceExportCursor
	Scanned        int
	Witnessed      int
	Deleted        int
	ReclaimedBytes uint64
	RetainedBytes  uint64
}

func NewAbandonmentCollector(options AbandonmentCollectorOptions) (*AbandonmentCollector, error) {
	if options.Journal == nil || options.Repository == nil || options.Authority == nil ||
		options.MaxRecords <= 0 || options.MaxRecords > AbsoluteMaxSourceRecords ||
		options.MaxReclaimedBytes < options.Repository.limits.MaxArtifactBytes+DescriptorBytes+cursorBytes ||
		options.MaxRetainedBytes > options.Repository.limits.MaxDiskBytes {
		return nil, ErrBound
	}
	return &AbandonmentCollector{options: options}, nil
}

// RunPass examines a bounded journal slice, accepts only exact replicated
// witnesses, durably deletes the artifact, then tombstones the local operation
// as Released. If the delete or journal publication has an unknown outcome,
// replay uses the same witness and both sides are idempotent.
func (collector *AbandonmentCollector) RunPass(
	ctx context.Context, cursor SourceExportCursor,
) (AbandonmentPass, error) {
	if collector == nil || ctx == nil || cursor.Done {
		return AbandonmentPass{}, ErrBound
	}
	options := collector.options
	workspace := make([]SourceControlRecord, 0, options.MaxRecords)
	records, next, err := options.Journal.ScanSourceExports(ctx, cursor, workspace)
	pass := AbandonmentPass{Cursor: cursor, Scanned: len(records)}
	if err != nil {
		return pass, err
	}
	for _, record := range records {
		if record.State == SourceControlReleased {
			pass.Cursor.AfterOperation = record.Request.Operation
			continue
		}
		witness, found, readErr := options.Authority.ReadArtifactAbandonment(ctx, record.Request.Operation)
		if readErr != nil {
			return pass, readErr
		}
		if !found {
			pass.Cursor.AfterOperation = record.Request.Operation
			continue
		}
		pass.Witnessed++
		if !witness.Valid() || witness.Operation != record.Request.Operation ||
			witness.Step != record.Request.Step || witness.Owner != record.Request.SourceNode ||
			!descriptorMatchesSourceRequest(witness.Descriptor, record.Request) {
			return pass, ErrAbandonment
		}
		owned, ownedErr := options.Repository.AbandonedArtifactBytes(witness)
		if ownedErr != nil {
			return pass, ownedErr
		}
		if owned > options.MaxReclaimedBytes-pass.ReclaimedBytes {
			pass.Cursor = cursor
			return pass, ErrCollectorIncomplete
		}
		reclaimed, abandonErr := options.Repository.AbandonArtifact(witness)
		if abandonErr != nil {
			return pass, abandonErr
		}
		pass.ReclaimedBytes += reclaimed
		retired := record
		retired.Revision++
		retired.State = SourceControlReleased
		retired.Descriptor = witness.Descriptor
		if publishErr := options.Journal.PublishSourceExport(ctx, record.Revision, retired); publishErr != nil {
			settled, settleErr := options.Journal.ReadSourceExport(ctx, record.Request.Operation)
			if settleErr != nil || settled != retired {
				return pass, errors.Join(ErrSourceOutcomeUnknown, publishErr, settleErr)
			}
		}
		pass.Deleted++
		pass.Cursor.AfterOperation = record.Request.Operation
	}
	pass.Cursor.Done = next.Done
	pass.RetainedBytes = options.Repository.Stats().DiskBytes
	if pass.Cursor.Done && pass.RetainedBytes > options.MaxRetainedBytes {
		return pass, ErrRetainedBytes
	}
	return pass, nil
}

// AbandonedArtifactBytes reports the exact live bytes a matching witness may
// reclaim. Absence is an idempotent zero result.
func (r *Repository) AbandonedArtifactBytes(w ArtifactAbandonmentWitness) (uint64, error) {
	if r == nil || !w.Valid() {
		return 0, ErrAbandonment
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return 0, ErrRepository
	}
	rec := r.records[w.Artifact]
	if rec == nil {
		return 0, nil
	}
	if rec.descriptor != w.Descriptor {
		return 0, ErrStaleFence
	}
	owned := uint64(DescriptorBytes) + rec.stageBytes
	if rec.complete {
		owned = uint64(DescriptorBytes) + rec.descriptor.ArtifactBytes
	}
	if rec.cursorLive {
		owned += cursorBytes
	}
	return owned, nil
}
