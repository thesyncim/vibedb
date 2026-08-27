package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

// verifyTailPreimages performs one indexed lookup per touched key, never an
// image scan. The one reusable decode buffer is bounded by the collection's
// frozen maximum value size; no old value is retained per operation or copied
// into the tail frame. A durable receipt permits an already-written after
// image, but a first attempt must match every exact before image.
func (s *ChildStage) verifyTailPreimages(batch TailBatch, resuming bool) error {
	if batch.Operations == 0 {
		return nil
	}
	if err := s.collection.SnapshotInto(&s.image.snapshot); err != nil {
		return err
	}
	defer s.image.snapshot.Close()
	iterator := batch.Iterator()
	for iterator.Next() {
		operation := iterator.Operation()
		if operation.Relation != 1 {
			return ErrChildStage
		}
		if operation.Kind == replication.MutationPut {
			if len(operation.Value) > s.collection.MaxDocumentBytes() {
				return ErrChildStage
			}
			// Source capture must supply the exact stored logical spelling, not
			// merely equivalent JSON: inline encoding canonicalizes documents.
			needed, err := vibejson.RequiredIndexEntries(operation.Value)
			if err != nil {
				return ErrChildStage
			}
			if cap(s.image.canonicalEntries) < needed {
				s.image.canonicalEntries = make([]vibejson.IndexEntry, needed)
			}
			index, err := vibejson.BuildIndex(operation.Value, s.image.canonicalEntries[:needed])
			if err != nil || !storeio.IndexIsCanonical(index, &s.image.canonical) {
				return fmt.Errorf("%w: noncanonical tail after image", ErrChildStage)
			}
		}
		value, found, err := s.image.snapshot.AppendRaw(s.image.scratch[:0], operation.Key)
		s.image.scratch = value
		if err != nil {
			return err
		}
		if len(value) > s.collection.MaxDocumentBytes() {
			return ErrChildStage
		}
		before := operation.BeforeWitness
		matches := !found && !before.Present
		if found && before.Present && len(value) == int(before.DocumentBytes) && sha256.Sum256(value) == before.Digest {
			point, err := s.partitioner.RelationPoint(operation.Relation, operation.Key, value, &s.image.document)
			matches = err == nil && point == before.Point
		}
		if !matches && resuming {
			matches = !found && operation.Kind == replication.MutationDelete ||
				found && operation.Kind == replication.MutationPut && bytes.Equal(value, operation.Value)
		}
		if !matches {
			return fmt.Errorf("%w: tail preimage relation %d, present=%t, bytes=%d, receipt=%t", ErrChildStage, operation.Relation, found, len(value), resuming)
		}
	}
	if iterator.wireInvalid {
		return ErrChildStage
	}
	return nil
}
