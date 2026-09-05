package snapshottransfer

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
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
	if rec.finishing {
		return ErrArtifactBusy
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

// AbandonArtifact durably retires either a staged or published artifact after
// the caller has obtained an exact replicated abandonment witness. Rename to
// the abandoning namespace is the commit point. Recovery completes partial
// deletion without treating an incomplete stage as a valid publication.
func (r *Repository) AbandonArtifact(w ArtifactAbandonmentWitness) (uint64, error) {
	if r == nil || !w.Valid() {
		return 0, ErrAbandonment
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, ErrRepository
	}
	d := w.Descriptor
	rec := r.records[w.Artifact]
	abandoning := abandoningArtifactName(w.Artifact)
	if rec == nil {
		if _, err := r.root.Lstat(abandoning); err == nil {
			if err = r.validateAbandoning(abandoning, d); err != nil {
				return 0, err
			}
			return 0, r.finishAbandon(nil, d, abandoning, 0)
		} else if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		return 0, nil
	}
	if rec.descriptor != d {
		return 0, ErrStaleFence
	}
	if rec.finishing {
		return 0, ErrArtifactBusy
	}
	if rec.readers != 0 {
		return 0, ErrArtifactBusy
	}
	if rec.file != nil {
		if err := rec.file.Close(); err != nil {
			return 0, err
		}
		rec.file = nil
	}
	owned := uint64(DescriptorBytes) + rec.stageBytes
	live := rec.stage
	if rec.complete {
		owned = uint64(DescriptorBytes) + d.ArtifactBytes
		live = rec.published
	}
	if rec.cursorLive {
		owned += cursorBytes
	}
	if _, err := r.root.Lstat(abandoning); errors.Is(err, os.ErrNotExist) {
		if err = r.root.Rename(live, abandoning); errors.Is(err, os.ErrNotExist) {
			return 0, r.finishAbandon(rec, d, abandoning, owned)
		} else if err != nil {
			return 0, err
		}
		if err = r.inject(faultAfterAbandonRename); err != nil {
			return 0, errors.Join(ErrOutcomeUnknown, err)
		}
	} else if err != nil {
		return 0, err
	} else if err = r.validateAbandoning(abandoning, d); err != nil {
		return 0, err
	}
	if err := r.finishAbandon(rec, d, abandoning, owned); err != nil {
		return 0, err
	}
	return owned, nil
}

func (r *Repository) validateAbandoning(name string, expected Descriptor) error {
	file, err := openRegular(r.root, name, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	var raw [DescriptorBytes]byte
	_, readErr := io.ReadFull(file, raw[:])
	actual, descriptorErr := OpenDescriptor(raw[:])
	info, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil || descriptorErr != nil || statErr != nil || closeErr != nil ||
		actual != expected || info.Size() < DescriptorBytes ||
		uint64(info.Size()-DescriptorBytes) > actual.ArtifactBytes {
		return errors.Join(ErrRepository, readErr, descriptorErr, statErr, closeErr)
	}
	return nil
}

func (r *Repository) finishAbandon(rec *record, d Descriptor, abandoning string, owned uint64) error {
	if err := syncRoot(r.root); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	_, cursor, _, _ := artifactNames(d.ArtifactHash)
	if err := r.root.Remove(cursor); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := r.root.Remove(abandoning); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := r.inject(faultAfterAbandonUnlink); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if err := syncRoot(r.root); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if err := r.inject(faultAfterAbandonSync); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if rec != nil {
		r.subtractDisk(owned)
		delete(r.records, d.ArtifactHash)
	}
	return nil
}
