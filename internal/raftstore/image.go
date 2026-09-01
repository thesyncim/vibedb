package raftstore

import (
	"bytes"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type logImage struct {
	hard      *pb.HardState
	first     uint64
	last      uint64
	baseTerm  uint64
	entries   []*pb.Entry
	liveBytes int64
}

type imageDelta struct {
	prefixLength int
	replace      bool
	entries      []*pb.Entry
	hard         *pb.HardState
	last         uint64
	liveBytes    int64
}

func bootstrapImage(snapshot *pb.Snapshot) logImage {
	index := snapshot.GetMetadata().GetIndex()
	term := snapshot.GetMetadata().GetTerm()
	return logImage{
		hard:  &pb.HardState{Term: uint64Pointer(term), Vote: uint64Pointer(0), Commit: uint64Pointer(index)},
		first: index + 1, last: index, baseTerm: term,
	}
}

// planReadyPayload performs all validation and prepares only the new suffix.
// It never copies the retained prefix or mutates image, keeping sequential
// appends O(total appended entries), not O(total live entries per Ready).
func planReadyPayload(image *logImage, payload readyPayload, options normalizedOptions) (imageDelta, error) {
	if image == nil || image.hard == nil || image.first == 0 || image.last == math.MaxUint64 || image.first-1 > image.last ||
		uint64(len(image.entries)) != image.last-(image.first-1) {
		return imageDelta{}, fmt.Errorf("%w: invalid durable image", ErrCorrupt)
	}
	if len(payload.entries) > MaxReadyEntries || uint64(len(payload.entries)) > options.maxEntries {
		return imageDelta{}, fmt.Errorf("%w: Ready entry count", ErrBounds)
	}
	if payload.hard != nil && len(payload.hard.ProtoReflect().GetUnknown()) != 0 {
		return imageDelta{}, fmt.Errorf("%w: HardState unknown fields", ErrInvalid)
	}
	var previous uint64
	var previousTerm uint64
	var batchBytes int64
	for ordinal, entry := range payload.entries {
		if entry == nil || entry.GetIndex() == 0 || entry.GetIndex() == math.MaxUint64 || entry.GetTerm() == 0 || entry.GetTerm() == math.MaxUint64 ||
			len(entry.ProtoReflect().GetUnknown()) != 0 ||
			entry.GetType() < pb.EntryNormal || entry.GetType() > pb.EntryConfChangeV2 ||
			(ordinal != 0 && (entry.GetIndex() != previous+1 || entry.GetTerm() < previousTerm)) || len(entry.GetData()) > raftmodel.MaxProposalBytes ||
			batchBytes > int64(raftmodel.MaxUncommittedEntriesSize)-int64(len(entry.GetData())) {
			return imageDelta{}, fmt.Errorf("%w: malformed entry ordinal %d", ErrInvalid, ordinal)
		}
		previous = entry.GetIndex()
		previousTerm = entry.GetTerm()
		batchBytes += int64(len(entry.GetData()))
	}
	delta := imageDelta{hard: image.hard, last: image.last, liveBytes: image.liveBytes}
	if len(payload.entries) != 0 {
		start := payload.entries[0].GetIndex()
		if start < image.first || start > image.last+1 {
			return imageDelta{}, fmt.Errorf("%w: entries start %d outside [%d,%d]", ErrInvalid, start, image.first, image.last+1)
		}
		delta.replace = true
		delta.prefixLength = int(start - image.first)
		boundaryTerm := image.baseTerm
		if delta.prefixLength != 0 {
			boundaryTerm = image.entries[delta.prefixLength-1].GetTerm()
		}
		if payload.entries[0].GetTerm() < boundaryTerm {
			return imageDelta{}, fmt.Errorf("%w: entry term decreases across retained boundary", ErrInvalid)
		}
		for ordinal, replacement := range payload.entries {
			index := replacement.GetIndex()
			if index > image.last {
				break
			}
			existing := image.entries[index-image.first]
			if existing.GetTerm() == replacement.GetTerm() && !entriesSemanticallyEqual(existing, replacement) {
				return imageDelta{}, fmt.Errorf("%w: entry %d changes bytes within term %d", ErrCorrupt, index, replacement.GetTerm())
			}
			if index <= image.hard.GetCommit() && !entriesSemanticallyEqual(existing, replacement) {
				return imageDelta{}, fmt.Errorf("%w: entry %d overwrites committed prefix", ErrInvalid, index)
			}
			_ = ordinal
		}
		newCount := delta.prefixLength + len(payload.entries)
		if uint64(newCount) > options.maxEntries {
			return imageDelta{}, fmt.Errorf("%w: live entry count", ErrBounds)
		}
		removedBytes := entriesFootprint(image.entries[delta.prefixLength:])
		newBytes := entriesFootprint(payload.entries)
		delta.liveBytes = image.liveBytes - removedBytes + newBytes
		if delta.liveBytes < 0 || delta.liveBytes > options.maxLiveBytes {
			return imageDelta{}, fmt.Errorf("%w: live log bytes %d", ErrBounds, delta.liveBytes)
		}
		if payload.owned {
			delta.entries = payload.entries
		} else {
			delta.entries = cloneEntries(payload.entries)
		}
		delta.last = payload.entries[len(payload.entries)-1].GetIndex()
	}
	if !isEmptyHardState(payload.hard) {
		candidate := payload.hard
		if candidate.GetTerm() == math.MaxUint64 || candidate.GetTerm() < image.hard.GetTerm() || candidate.GetCommit() < image.hard.GetCommit() ||
			(candidate.GetTerm() == image.hard.GetTerm() && image.hard.GetVote() != 0 && candidate.GetVote() != image.hard.GetVote()) ||
			candidate.GetCommit() < image.first-1 || candidate.GetCommit() > delta.last ||
			(candidate.GetTerm() == 0 && candidate.GetVote() != 0) || (candidate.GetVote() != 0 && raft.IsLocalMsgTarget(candidate.GetVote())) {
			return imageDelta{}, fmt.Errorf("%w: HardState regression or impossible commit", ErrInvalid)
		}
		if payload.owned {
			delta.hard = candidate
		} else {
			delta.hard = cloneHardState(candidate)
		}
	}
	lastTerm := image.baseTerm
	switch {
	case len(delta.entries) != 0:
		lastTerm = delta.entries[len(delta.entries)-1].GetTerm()
	case delta.replace && delta.prefixLength != 0:
		lastTerm = image.entries[delta.prefixLength-1].GetTerm()
	case !delta.replace && len(image.entries) != 0:
		lastTerm = image.entries[len(image.entries)-1].GetTerm()
	}
	if delta.hard.GetTerm() < lastTerm || delta.hard.GetCommit() > delta.last {
		return imageDelta{}, fmt.Errorf("%w: HardState does not cover durable log", ErrInvalid)
	}
	return delta, nil
}

func commitImageDelta(image *logImage, delta imageDelta) {
	if delta.replace {
		if delta.prefixLength < len(image.entries) {
			// Entries exposes immutable borrowed slices to Raft. A conflicting
			// suffix replacement therefore publishes a new pointer vector instead
			// of mutating any position a concurrent reader may still hold. Ordinary
			// sequential append may reuse capacity beyond the old visible length.
			next := make([]*pb.Entry, delta.prefixLength, delta.prefixLength+len(delta.entries))
			copy(next, image.entries[:delta.prefixLength])
			image.entries = append(next, delta.entries...)
		} else {
			image.entries = append(image.entries, delta.entries...)
		}
		image.last = delta.last
		image.liveBytes = delta.liveBytes
	}
	image.hard = delta.hard
}

func applyReadyPayload(image *logImage, payload readyPayload, options normalizedOptions) error {
	payload.owned = true
	delta, err := planReadyPayload(image, payload, options)
	if err != nil {
		return err
	}
	commitImageDelta(image, delta)
	return nil
}

func entriesFootprint(entries []*pb.Entry) int64 {
	var result int64
	for _, entry := range entries {
		result += 32 + int64(len(entry.GetData()))
	}
	return result
}

func entriesSemanticallyEqual(left, right *pb.Entry) bool {
	return left != nil && right != nil && left.GetIndex() == right.GetIndex() && left.GetTerm() == right.GetTerm() &&
		left.GetType() == right.GetType() && bytes.Equal(left.GetData(), right.GetData())
}
