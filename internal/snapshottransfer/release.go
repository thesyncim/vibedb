package snapshottransfer

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

func (r *Repository) ReleaseInstalledArtifact(
	ctx context.Context,
	request BootstrapRequest,
	identity raftmember.RuntimeIdentity,
) error {
	if ctx == nil || context.Cause(ctx) != nil || !validBootstrapRequest(request) ||
		!runtimeMatchesDescriptor(identity, request.Descriptor) {
		return ErrBootstrapConflict
	}
	return r.ReleasePublished(ArtifactReleaseRequest{
		Operation: request.Operation, Step: request.Step, Descriptor: request.Descriptor,
	})
}

// ArtifactReleaseRequest binds reclamation to the exact orchestrated replica
// operation and step which certified installation. Authentication and
// authorization remain the control plane's responsibility; the repository
// rejects anonymous or descriptor-ambiguous release requests.
type ArtifactReleaseRequest struct {
	Operation  [sha256.Size]byte
	Step       [sha256.Size]byte
	Descriptor Descriptor
}

func (q ArtifactReleaseRequest) Valid() bool {
	return q.Operation != ([sha256.Size]byte{}) && q.Step != ([sha256.Size]byte{}) && q.Descriptor.Valid()
}

// ReleasePublished durably retires one immutable published artifact. Rename
// to the deleting namespace is the commit point. A crash after that point is
// completed during repository recovery; retries before or after recovery are
// idempotent. Staged artifacts are never released by this API.
func (r *Repository) ReleasePublished(q ArtifactReleaseRequest) error {
	if r == nil || !q.Valid() {
		return ErrDescriptor
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepository
	}
	d := q.Descriptor
	rec := r.records[d.ArtifactHash]
	deleting := deletingArtifactName(d.ArtifactHash)
	if rec == nil {
		// An exact retry after recovery observes no live namespace.
		if _, err := r.root.Lstat(deleting); err == nil {
			return r.finishRelease(nil, d, deleting)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if rec.descriptor != d {
		return ErrStaleFence
	}
	if !rec.complete {
		return ErrStaleFence
	}
	if rec.readers != 0 {
		return ErrArtifactBusy
	}
	if rec.file != nil {
		if err := rec.file.Close(); err != nil {
			return err
		}
		rec.file = nil
	}
	if _, err := r.root.Lstat(deleting); errors.Is(err, os.ErrNotExist) {
		if err = r.root.Rename(rec.published, deleting); errors.Is(err, os.ErrNotExist) {
			// The unlink may have completed while its result was unknown. The
			// following directory sync makes that absence authoritative.
			return r.finishRelease(rec, d, deleting)
		} else if err != nil {
			return err
		}
		if err = r.inject(faultAfterReleaseRename); err != nil {
			return errors.Join(ErrOutcomeUnknown, err)
		}
	} else if err != nil {
		return err
	}
	return r.finishRelease(rec, d, deleting)
}

func (r *Repository) finishRelease(rec *record, d Descriptor, deleting string) error {
	if err := syncRoot(r.root); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if err := r.root.Remove(deleting); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := r.inject(faultAfterReleaseUnlink); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if err := syncRoot(r.root); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if err := r.inject(faultAfterReleaseSync); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if rec != nil {
		r.subtractDisk(uint64(DescriptorBytes) + d.ArtifactBytes)
		delete(r.records, d.ArtifactHash)
	}
	return nil
}
