package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"slices"

	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

const deterministicApplySemantics = "vibejson-strict;last-mutation-per-key-wins;" +
	"validate-final-against-snapshot;delete-absent-and-put-equal-are-noops;" +
	"mutation-validation-result-map;bytewise-changed-key-order;" +
	"ordered-client-session-sequences;cumulative-ack-through;" +
	"fixed-retry-ring;explicit-session-open;raft-index-session-epoch;" +
	"shard-epoch-high-water;explicit-session-retirement;terminal-retire-only;" +
	"exact-retired-session-release;terminal-stale-retire-unstored;" +
	"absolute-session-lease;lease-deadline-cas;sequenced-session-revoke;" +
	"stable-logical-command-digest;data-chain-value-descriptor-sha256"

var (
	canonicalImageDigestDomain = []byte("vibedb/replicated-state/logical-image\x00")
	dataChainSeedDigestDomain  = []byte("vibedb/replicated-state/data-chain-seed\x00")
	dataChainDigestDomain      = []byte("vibedb/replicated-state/data-chain/value-descriptor-sha256\x00")
	applyContractDigestDomain  = []byte("vibedb/replicated-state/apply-contract\x00")
	dataChainMarkers           = [2][1]byte{{0}, {1}}
	applySemanticsDigest       = sha256.Sum256([]byte(deterministicApplySemantics))
)

type finalMutation struct {
	key             []byte
	value           []byte
	before          []byte
	descriptorIndex uint16
	delete          bool
	beforeFound     bool
	described       bool
}

// mutationValueDescriptor is transient batch workspace, never per-Machine
// state. Singleton apply keeps the raw before/after slices it already owns.
// batched apply stores fixed descriptors here so finalMutation stays compact.
type mutationValueDescriptor struct {
	beforeDigest [sha256.Size]byte
	afterDigest  [sha256.Size]byte
	beforeLength uint64
	afterLength  uint64
}

type canonicalImageHasher struct {
	h         hash.Hash
	validator MutationValidator
}

// dataChainHasher owns the small values that cross hash.Hash's interface
// boundary. Keeping them with the reusable SHA state prevents per-command
// escape allocations without retaining command-sized payloads.
type dataChainHasher struct {
	h        hash.Hash
	length   [8]byte
	previous [32]byte
	contract [32]byte
	result   [32]byte
}

func newDataChainHasher() *dataChainHasher {
	return &dataChainHasher{h: sha256.New()}
}

func (h *dataChainHasher) writeFrame(value []byte) {
	binary.LittleEndian.PutUint64(h.length[:], uint64(len(value)))
	_, _ = h.h.Write(h.length[:])
	_, _ = h.h.Write(value)
}

func (h *dataChainHasher) writeValueDescriptor(
	found bool,
	length uint64,
	digest *[sha256.Size]byte,
) {
	marker := 0
	if found {
		marker = 1
	}
	_, _ = h.h.Write(dataChainMarkers[marker][:])
	binary.LittleEndian.PutUint64(h.length[:], length)
	_, _ = h.h.Write(h.length[:])
	if digest == nil {
		h.result = [32]byte{}
		digest = &h.result
	}
	_, _ = h.h.Write(digest[:])
}

func newCanonicalImageHasher(
	name string,
	validation ValidationProfile,
	validationDigest [32]byte,
	validator MutationValidator,
) (*canonicalImageHasher, error) {
	if validation != ValidationDeterministicMutation ||
		validationDigest == ([32]byte{}) || validator == nil {
		return nil, ErrInvalidCollection
	}
	h := sha256.New()
	_, _ = h.Write(canonicalImageDigestDomain)
	_, _ = h.Write([]byte{byte(validation)})
	_, _ = h.Write(validationDigest[:])
	writeHashFrame(h, []byte(name))
	return &canonicalImageHasher{h: h, validator: validator}, nil
}

func (h *canonicalImageHasher) add(key, value []byte) error {
	if h == nil || h.h == nil || h.validator == nil {
		return ErrInvalidCollection
	}
	if err := vibejson.Validate(value); err != nil {
		return fmt.Errorf("%w: malformed JSON in logical image", ErrSchemaProfile)
	}
	switch result := h.validator.ValidatePut(key, value); result {
	case MutationValidationAccept:
	case MutationValidationInvalid:
		return fmt.Errorf("%w: logical image contains an invalid row", ErrSchemaProfile)
	case MutationValidationTargetBound:
		return fmt.Errorf("%w: logical image row exceeds the validator target", ErrSchemaProfile)
	case MutationValidationWrongShard:
		return fmt.Errorf("%w: logical image row belongs to another shard", ErrSchemaProfile)
	default:
		return fmt.Errorf("%w: mutation validator returned %d", ErrInvalidCollection, result)
	}
	_, _ = h.h.Write([]byte{1})
	writeHashFrame(h.h, key)
	writeHashFrame(h.h, value)
	return nil
}

func (h *canonicalImageHasher) sum() [32]byte {
	if h == nil || h.h == nil {
		return [32]byte{}
	}
	_, _ = h.h.Write([]byte{0})
	var result [32]byte
	_ = h.h.Sum(result[:0])
	return result
}

// canonicalImageDigest performs the intentionally cold, complete logical
// image audit used at reopen, snapshot certification, and explicit audit
// boundaries. Normal command planning must use dataChainTransitionDigest.
func canonicalImageDigest(
	name string,
	validation ValidationProfile,
	validationDigest [32]byte,
	validator MutationValidator,
	snapshot *durable.Snapshot,
	overlay []finalMutation,
) ([32]byte, error) {
	h, err := newCanonicalImageHasher(name, validation, validationDigest, validator)
	if err != nil {
		return [32]byte{}, err
	}

	ordered := slices.Clone(overlay)
	slices.SortFunc(ordered, func(a, b finalMutation) int {
		return bytes.Compare(a.key, b.key)
	})
	next := 0
	if snapshot != nil {
		err := snapshot.RangeRaw(func(key, value []byte) error {
			for next < len(ordered) && bytes.Compare(ordered[next].key, key) < 0 {
				if !ordered[next].delete {
					if err := h.add(ordered[next].key, ordered[next].value); err != nil {
						return err
					}
				}
				next++
			}
			if next < len(ordered) && bytes.Equal(ordered[next].key, key) {
				if !ordered[next].delete {
					if err := h.add(ordered[next].key, ordered[next].value); err != nil {
						return err
					}
				}
				next++
				return nil
			}
			return h.add(key, value)
		})
		if err != nil {
			return [32]byte{}, err
		}
	}
	for next < len(ordered) {
		if !ordered[next].delete {
			if err := h.add(ordered[next].key, ordered[next].value); err != nil {
				return [32]byte{}, err
			}
		}
		next++
	}
	return h.sum(), nil
}

// dataChainSeedDigest starts a transition history from a certified image and
// its frozen result contract. Domain separation keeps the history fence
// distinct from the canonical image identity even before the first mutation.
func dataChainSeedDigest(
	applyContractDigest [32]byte,
	imageDigest [32]byte,
) ([32]byte, error) {
	if applyContractDigest == ([32]byte{}) || imageDigest == ([32]byte{}) {
		return [32]byte{}, ErrInvalidCollection
	}
	var frame [128]byte
	n := copy(frame[:], dataChainSeedDigestDomain)
	n += copy(frame[n:], applyContractDigest[:])
	n += copy(frame[n:], imageDigest[:])
	return sha256.Sum256(frame[:n]), nil
}

// dataChainTransitionDigest advances the replicated logical identity from only
// the exact changed rows. changes must be unique and key ordered. The chain is
// deterministic and independent of shard cardinality. Unlike the certified
// image digest it intentionally commits to transition history as well as the
// resulting values.
func dataChainTransitionDigest(
	workspace *dataChainHasher,
	previous [32]byte,
	applyContractDigest [32]byte,
	changes []finalMutation,
	descriptors []mutationValueDescriptor,
) ([32]byte, error) {
	if previous == ([32]byte{}) || applyContractDigest == ([32]byte{}) ||
		len(changes) == 0 {
		return [32]byte{}, ErrInvalidCollection
	}
	if workspace == nil {
		workspace = newDataChainHasher()
	} else if workspace.h == nil {
		return [32]byte{}, ErrInvalidCollection
	}
	workspace.h.Reset()
	workspace.previous = previous
	workspace.contract = applyContractDigest
	h := workspace.h
	_, _ = h.Write(dataChainDigestDomain)
	_, _ = h.Write(workspace.previous[:])
	_, _ = h.Write(workspace.contract[:])
	binary.LittleEndian.PutUint64(workspace.length[:], uint64(len(changes)))
	_, _ = h.Write(workspace.length[:])
	var prior []byte
	for index := range changes {
		change := &changes[index]
		var descriptor *mutationValueDescriptor
		if change.described {
			if int(change.descriptorIndex) >= len(descriptors) {
				return [32]byte{}, ErrInvalidCollection
			}
			descriptor = &descriptors[change.descriptorIndex]
		} else if change.descriptorIndex != 0 {
			return [32]byte{}, ErrInvalidCollection
		}
		if len(change.key) == 0 || prior != nil && bytes.Compare(prior, change.key) >= 0 ||
			change.delete && !change.beforeFound || descriptor != nil &&
			((!change.beforeFound && (descriptor.beforeLength != 0 ||
				descriptor.beforeDigest != ([sha256.Size]byte{}))) ||
				(change.delete && (descriptor.afterLength != 0 ||
					descriptor.afterDigest != ([sha256.Size]byte{})))) {
			return [32]byte{}, ErrInvalidCollection
		}
		if change.beforeFound {
			if descriptor != nil {
				workspace.writeValueDescriptor(
					true, descriptor.beforeLength, &descriptor.beforeDigest,
				)
			} else {
				if change.before == nil {
					return [32]byte{}, ErrInvalidCollection
				}
				workspace.result = sha256.Sum256(change.before)
				workspace.writeValueDescriptor(
					true, uint64(len(change.before)), &workspace.result,
				)
			}
		} else {
			workspace.writeValueDescriptor(false, 0, nil)
		}
		if change.delete {
			workspace.writeValueDescriptor(false, 0, nil)
		} else if descriptor != nil {
			workspace.writeValueDescriptor(
				true, descriptor.afterLength, &descriptor.afterDigest,
			)
		} else {
			if change.value == nil {
				return [32]byte{}, ErrInvalidCollection
			}
			workspace.result = sha256.Sum256(change.value)
			workspace.writeValueDescriptor(
				true, uint64(len(change.value)), &workspace.result,
			)
		}
		workspace.writeFrame(change.key)
		prior = change.key
	}
	_ = h.Sum(workspace.result[:0])
	return workspace.result, nil
}

// applyContractDigest binds every frozen input that can change deterministic
// command results. Local capture and checkpoint plumbing is deliberately
// excluded because replicas may configure it independently.
func applyContractDigest(
	name string,
	target CollectionTarget,
	maxSessions uint64,
	retryWindow uint16,
) ([32]byte, error) {
	if name == "" || target.Validation != ValidationDeterministicMutation ||
		target.ValidationDigest == ([32]byte{}) || maxSessions == 0 || retryWindow == 0 ||
		target.Limits.MaxKeyBytes <= 0 || target.Limits.MaxDocumentBytes <= 0 ||
		target.Limits.MaxDistinctMutations <= 0 || target.Limits.MaxBatchBytes <= 0 {
		return [32]byte{}, ErrInvalidCollection
	}
	h := sha256.New()
	_, _ = h.Write(applyContractDigestDomain)
	writeHashFrame(h, []byte(name))
	_, _ = h.Write([]byte{byte(target.Validation)})
	_, _ = h.Write(target.ValidationDigest[:])
	_, _ = h.Write(applySemanticsDigest[:])
	var grammar [2 + 11*4]byte
	binary.LittleEndian.PutUint16(grammar[0:2], ResultFormatMutation)
	for index, code := range [...]uint32{
		ResultApplied,
		ResultStaleFence,
		ResultUnknownCollection,
		ResultInvalidDocument,
		ResultTargetBound,
		ResultWrongShard,
		ResultSessionRetired,
		ResultSessionOpened,
		ResultSessionRenewed,
		ResultSessionRevoked,
		MaxDistinctMutations,
	} {
		binary.LittleEndian.PutUint32(grammar[2+index*4:2+(index+1)*4], code)
	}
	_, _ = h.Write(grammar[:])
	var fixed [50]byte
	binary.LittleEndian.PutUint64(fixed[0:8], uint64(target.Limits.MaxKeyBytes))
	binary.LittleEndian.PutUint64(fixed[8:16], uint64(target.Limits.MaxDocumentBytes))
	binary.LittleEndian.PutUint64(fixed[16:24], uint64(target.Limits.MaxDistinctMutations))
	binary.LittleEndian.PutUint64(fixed[24:32], uint64(target.Limits.MaxBatchBytes))
	binary.LittleEndian.PutUint64(fixed[32:40], maxSessions)
	binary.LittleEndian.PutUint16(fixed[40:42], retryWindow)
	binary.LittleEndian.PutUint64(fixed[42:50], MaxSessionRetryWindow)
	_, _ = h.Write(fixed[:])
	var result [32]byte
	_ = h.Sum(result[:0])
	return result, nil
}
