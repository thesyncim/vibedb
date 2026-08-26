package splitcontroller

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
)

// PersistSourceCapture records only the bounded identity and published head of
// a source capture. Transition payloads remain in the opaque collection that
// replicated apply updates atomically with source publication.
func (s *DurableRuntimeStore) PersistSourceCapture(
	revision uint64,
	capture *rangesplit.SourceCapture,
) error {
	descriptor, err := capture.Descriptor()
	if err != nil {
		return errors.Join(ErrRuntimeStore, err)
	}
	raw, err := rangesplit.AppendSourceCaptureDescriptor(nil, descriptor)
	if err != nil || len(raw) > MaxCaptureControlBytes {
		return errors.Join(ErrRuntimeStore, err)
	}
	return s.Persist(RuntimeStateCapture, 0, revision, raw)
}

// LoadSourceCaptureDescriptor decodes and binds a recovered capture record to
// the exact immutable partition plan. It does not grant transition authority;
// RecoverSourceCapture performs the collection and replicated-state proof.
func (s *DurableRuntimeStore) LoadSourceCaptureDescriptor(
	partitioner *rangesplit.Partitioner,
) (rangesplit.SourceCaptureDescriptor, uint64, bool, error) {
	state, ok, err := s.Load(RuntimeStateCapture, 0)
	if err != nil || !ok {
		return rangesplit.SourceCaptureDescriptor{}, 0, ok, err
	}
	descriptor, err := rangesplit.OpenSourceCaptureDescriptor(state.Payload)
	if err != nil || partitioner == nil {
		return rangesplit.SourceCaptureDescriptor{}, 0, false,
			errors.Join(ErrRuntimeStore, err)
	}
	if err = partitioner.ValidateSourceCaptureDescriptor(descriptor); err != nil {
		return rangesplit.SourceCaptureDescriptor{}, 0, false,
			errors.Join(ErrRuntimeStore, err)
	}
	canonical, err := rangesplit.AppendSourceCaptureDescriptor(nil, descriptor)
	if err != nil || !bytes.Equal(canonical, state.Payload) {
		return rangesplit.SourceCaptureDescriptor{}, 0, false,
			errors.Join(ErrRuntimeStore, err)
	}
	return descriptor, state.Revision, true, nil
}

// RecoverSourceCapture reconstructs the live transition authority after a
// restart. Begin scans the opaque capture collection with bounded workspace
// and proves every retained entry against sourceState. The final canonical
// descriptor comparison prevents a newer, older, or copied collection from
// being accepted under this operation's manifest-bound control record.
func (s *DurableRuntimeStore) RecoverSourceCapture(
	partitioner *rangesplit.Partitioner,
	targetName string,
	collection *durable.Collection,
	sourceState replicatedstate.State,
) (*rangesplit.SourceCapture, uint64, bool, error) {
	descriptor, revision, ok, err := s.LoadSourceCaptureDescriptor(partitioner)
	if err != nil || !ok {
		return nil, revision, ok, err
	}
	capture, err := rangesplit.NewSourceCapture(partitioner, targetName, collection)
	if err != nil {
		return nil, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	// A recovered collection must already contain its header. A nil publisher
	// deliberately prevents this path from manufacturing fresh authority.
	if err = capture.Begin(sourceState, nil); err != nil {
		return nil, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	recovered, err := capture.Descriptor()
	if err != nil || recovered != descriptor {
		return nil, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	return capture, revision, true, nil
}

// PersistChildArtifacts stores the complete bounded manifest set, not the
// artifact payloads themselves.
func (s *DurableRuntimeStore) PersistChildArtifacts(
	revision uint64,
	set rangesplit.ChildArtifactSet,
) error {
	raw, err := rangesplit.AppendChildArtifactSet(nil, set)
	if err != nil || len(raw) > MaxArtifactControlBytes {
		return errors.Join(ErrRuntimeStore, err)
	}
	return s.Persist(RuntimeStateArtifacts, 0, revision, raw)
}

// LoadChildArtifacts reconstructs the manifest authority and binds it to the
// exact partition plan. Artifact bytes remain independently authenticated by
// the descriptors when their repositories are opened.
func (s *DurableRuntimeStore) LoadChildArtifacts(
	partitioner *rangesplit.Partitioner,
) (rangesplit.ChildArtifactSet, uint64, bool, error) {
	state, ok, err := s.Load(RuntimeStateArtifacts, 0)
	if err != nil || !ok {
		return rangesplit.ChildArtifactSet{}, 0, ok, err
	}
	set, err := rangesplit.OpenChildArtifactSet(state.Payload)
	if err != nil || partitioner == nil {
		return rangesplit.ChildArtifactSet{}, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	if err = partitioner.ValidateChildArtifactSet(set); err != nil {
		return rangesplit.ChildArtifactSet{}, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	canonical, err := rangesplit.AppendChildArtifactSet(nil, set)
	if err != nil || !bytes.Equal(canonical, state.Payload) {
		return rangesplit.ChildArtifactSet{}, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	return set, state.Revision, true, nil
}

func (s *DurableRuntimeStore) PersistTailCursor(
	revision uint64,
	cursor rangesplit.TailCursor,
) error {
	raw, err := rangesplit.AppendTailCursor(nil, cursor)
	if err != nil || len(raw) > MaxTailControlBytes {
		return errors.Join(ErrRuntimeStore, err)
	}
	return s.Persist(RuntimeStateTail, 0, revision, raw)
}

func (s *DurableRuntimeStore) LoadTailCursor(
	partitioner *rangesplit.Partitioner,
) (rangesplit.TailCursor, uint64, bool, error) {
	state, ok, err := s.Load(RuntimeStateTail, 0)
	if err != nil || !ok {
		return rangesplit.TailCursor{}, 0, ok, err
	}
	cursor, err := rangesplit.OpenTailCursor(state.Payload)
	if err != nil || partitioner == nil {
		return rangesplit.TailCursor{}, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	if err = partitioner.ValidateTailCursor(cursor); err != nil {
		return rangesplit.TailCursor{}, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	// Preserve canonical uniqueness as a defense against a future decoder that
	// accepts multiple encodings for the same cursor.
	canonical, err := rangesplit.AppendTailCursor(nil, cursor)
	if err != nil || !bytes.Equal(canonical, state.Payload) {
		return rangesplit.TailCursor{}, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	return cursor, state.Revision, true, nil
}

// PersistChildStage stores only the constant-size authenticated destination
// cursor. Child rows remain in their independently durable collection.
func (s *DurableRuntimeStore) PersistChildStage(
	child uint8,
	revision uint64,
	cursor rangesplit.ChildStageCursor,
) error {
	if cursor.Child() != child {
		return ErrRuntimeStore
	}
	raw, err := rangesplit.AppendChildStageCursor(nil, &cursor)
	if err != nil || len(raw) > MaxTailControlBytes {
		return errors.Join(ErrRuntimeStore, err)
	}
	return s.Persist(RuntimeStateStage, child, revision, raw)
}

// LoadChildStage reconstructs one canonical destination cursor and binds it
// to the exact child manifest and partition plan.
func (s *DurableRuntimeStore) LoadChildStage(
	partitioner *rangesplit.Partitioner,
	expected rangesplit.ChildArtifactManifest,
	child uint8,
) (rangesplit.ChildStageCursor, uint64, bool, error) {
	state, ok, err := s.Load(RuntimeStateStage, child)
	if err != nil || !ok {
		return rangesplit.ChildStageCursor{}, 0, ok, err
	}
	cursor, err := rangesplit.OpenChildStageCursor(state.Payload)
	if err != nil || partitioner == nil || cursor == nil || cursor.Child() != child {
		return rangesplit.ChildStageCursor{}, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	if err = partitioner.ValidateChildStageCursor(expected, *cursor); err != nil {
		return rangesplit.ChildStageCursor{}, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	canonical, err := rangesplit.AppendChildStageCursor(nil, cursor)
	if err != nil || !bytes.Equal(canonical, state.Payload) {
		return rangesplit.ChildStageCursor{}, 0, false, errors.Join(ErrRuntimeStore, err)
	}
	return *cursor, state.Revision, true, nil
}

// PersistCutoverCertificate publishes the bounded immutable proof only after
// the source fence and every non-retained child seal have settled durably.
// The certificate is topology evidence, never catalog publication authority.
func (s *DurableRuntimeStore) PersistCutoverCertificate(
	revision uint64,
	certificate rangesplit.CutoverCertificate,
) error {
	raw, err := rangesplit.AppendCutoverCertificate(nil, &certificate)
	if err != nil || len(raw) > MaxCertificateBytes {
		return errors.Join(ErrRuntimeStore, err)
	}
	return s.Persist(RuntimeStateCertificate, 0, revision, raw)
}

// LoadCutoverCertificate returns an exact canonical certificate bound to the
// supplied immutable split plan. It performs no source or child data scan.
func (s *DurableRuntimeStore) LoadCutoverCertificate(
	partitioner *rangesplit.Partitioner,
) (rangesplit.CutoverCertificate, uint64, bool, error) {
	state, ok, err := s.Load(RuntimeStateCertificate, 0)
	if err != nil || !ok {
		return rangesplit.CutoverCertificate{}, 0, ok, err
	}
	certificate, err := rangesplit.OpenCutoverCertificate(state.Payload)
	if err != nil || certificate == nil || partitioner == nil {
		return rangesplit.CutoverCertificate{}, 0, false,
			errors.Join(ErrRuntimeStore, err)
	}
	if err = partitioner.VerifyCutoverCertificate(*certificate); err != nil {
		return rangesplit.CutoverCertificate{}, 0, false,
			errors.Join(ErrRuntimeStore, err)
	}
	canonical, err := rangesplit.AppendCutoverCertificate(nil, certificate)
	if err != nil || !bytes.Equal(canonical, state.Payload) {
		return rangesplit.CutoverCertificate{}, 0, false,
			errors.Join(ErrRuntimeStore, err)
	}
	return *certificate, state.Revision, true, nil
}
