package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

func retainedPruneRelationCursor(relation replication.RelationID) RetainedPruneCursor {
	return RetainedPruneCursor{
		phase: RetainedPruneScan, relation: relation, relationCount: 2,
		applied: 1, term: 1, ownershipEpoch: 1, routingVersion: 1, routeGeneration: 1,
		plan: [32]byte{1}, operation: [32]byte{2}, placement: [32]byte{3},
		cutover: [32]byte{4}, dataChain: [32]byte{5}, base: [32]byte{6}, entry: [32]byte{7},
	}
}

func TestRetainedPruneRelationFramesAndDigestAreExact(t *testing.T) {
	key, value := []byte("same-key"), []byte{0, 0xff, 0x80, 1}
	var digests [2][sha256.Size]byte
	for ordinal := range 2 {
		relation := replication.RelationID(ordinal + 1)
		cursor := retainedPruneRelationCursor(relation)
		cursor.phase, cursor.pendingCount, cursor.pendingKeyBytes = RetainedPruneAwaitingApply, 1, uint64(len(key))
		cursor.pendingApplied, cursor.pendingEntry = cursor.applied, cursor.entry
		cursor.resumeAfter = key
		cursor.pendingKeys = appendPendingPruneRows(nil, relation, [][]byte{key}, [][]byte{value})
		pruner := &RetainedPruner{cursor: cursor}
		var workspace RetainedPruneWorkspace
		batch, err := pruner.openPendingBatch(&workspace)
		if err != nil || batch.Relation() != relation || batch.Count != 1 || batch.DocumentBytes != uint64(len(value)) {
			t.Fatalf("relation %d batch=%+v err=%v", relation, batch, err)
		}
		iterator := batch.Iterator()
		if iterator.Relation() != 0 || !iterator.Next() || iterator.Relation() != relation ||
			!bytes.Equal(iterator.Key(), key) || !bytes.Equal(iterator.Document(), value) {
			t.Fatal("opaque relation row was relabeled or interpreted as JSON")
		}
		digests[ordinal] = batch.Digest
		cursor.pending = batch.Digest
		raw, err := AppendRetainedPruneCursor(nil, &cursor)
		if err != nil {
			t.Fatal(err)
		}
		reopened, err := OpenRetainedPruneCursor(raw)
		if err != nil || reopened.relation != relation || reopened.relationCount != 2 {
			t.Fatal("relation cursor", err)
		}
		reencoded, err := AppendRetainedPruneCursor(nil, reopened)
		if err != nil || !bytes.Equal(raw, reencoded) {
			t.Fatal("noncanonical relation cursor", err)
		}
		pruner.cursor = *reopened
		retry, err := pruner.openPendingBatch(&workspace)
		if err != nil || retry.Relation() != relation || retry.Digest != batch.Digest {
			t.Fatal("restart relabeled pending batch", err)
		}
		wrong := *reopened
		wrong.relation = replication.RelationID(3 - int(relation))
		if validPendingPruneKeys(&wrong) || validRetainedPruneCursor(&wrong) {
			t.Fatal("pending rows accepted under another relation")
		}
	}
	if digests[0] == digests[1] {
		t.Fatal("equal keys and values in different relations shared a prune digest")
	}
}

func TestRetainedPruneCursorRejectsNonRelationCoordinates(t *testing.T) {
	cursor := retainedPruneRelationCursor(2)
	cursor.scanAfter = []byte("last-key")
	raw, err := AppendRetainedPruneCursor(nil, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		edit func([]byte)
	}{
		{"zero relation", func(raw []byte) { binary.LittleEndian.PutUint16(raw[18:20], 0) }},
		{"zero count", func(raw []byte) { binary.LittleEndian.PutUint16(raw[20:22], 0) }},
		{"outside bundle", func(raw []byte) { binary.LittleEndian.PutUint16(raw[18:20], 3) }},
		{"oversized bundle", func(raw []byte) { binary.LittleEndian.PutUint16(raw[20:22], replication.MaxRelationsPerBundle+1) }},
		{"reserved bytes", func(raw []byte) { raw[22] = 1 }},
		{"32-bit length cancellation", func(raw []byte) {
			binary.LittleEndian.PutUint32(raw[464:468], ^uint32(0)-7)
			binary.LittleEndian.PutUint32(raw[468:472], 16)
		}},
		{"premature terminal", func(raw []byte) { raw[16] = byte(RetainedPruneComplete) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := bytes.Clone(raw)
			test.edit(changed)
			var codec RetainedPruneCursorWorkspace
			retainedPruneCursorDigestInto(&codec, changed[:len(changed)-sha256.Size])
			copy(changed[len(changed)-sha256.Size:], codec.digest[:])
			if _, err := OpenRetainedPruneCursor(changed); err == nil {
				t.Fatal("resealed noncanonical relation cursor accepted")
			}
		})
	}
	for _, keys := range [][][]byte{{[]byte("a"), []byte("a")}, {[]byte("z"), []byte("a")}} {
		cursor.pendingCount, cursor.pendingKeyBytes = 2, 2
		cursor.pendingKeys = appendPendingPruneRows(nil, 2, keys, [][]byte{{1}, {2}})
		if validPendingPruneKeys(&cursor) {
			t.Fatal("duplicate/reordered relation keys accepted")
		}
	}
	cursor.pendingCount, cursor.pendingKeyBytes = 1, 1
	validPending := appendPendingPruneRows(nil, 2, [][]byte{{'a'}}, [][]byte{{1}})
	for _, offset := range []int{2, 7} {
		cursor.pendingKeys = bytes.Clone(validPending)
		binary.LittleEndian.PutUint32(cursor.pendingKeys[offset:offset+4], ^uint32(0))
		if validPendingPruneKeys(&cursor) {
			t.Fatal("unbounded row length accepted before int conversion")
		}
	}
}

func TestRetainedPruneGlobalRowUsesOwnStoredKeyMapper(t *testing.T) {
	partitioner, profile := testRelationPartitioner(t)
	partitioner, err := partitioner.BindRelations(profile)
	if err != nil {
		t.Fatal(err)
	}
	var moved, retained []byte
	locator, err := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString("base-row")})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 1024 && (moved == nil || retained == nil); index++ {
		tuple, err := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString(fmt.Sprintf("index-%d", index))})
		if err != nil {
			t.Fatal(err)
		}
		key := append(tuple, locator...)
		point, err := partitioner.RelationPoint(2, key, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if partitioner.childFor(point) == int(partitioner.retained) {
			retained = key
		} else {
			moved = key
		}
	}
	if moved == nil || retained == nil {
		t.Fatal("fixture did not cover both index placements")
	}
	value := []byte{0xff, 0, 0x80, 'x'}
	var workspace RetainedPruneWorkspace
	scan := retainedPruneScan{partitioner: partitioner, relation: 2, workspace: &workspace,
		limits: RetainedPruneLimits{MaxKeys: 4, MaxKeyBytes: 4096, MaxBatchBytes: 8192, MaxScanRows: 8}}
	if err := scan.visitRow(retained, value); err != nil || len(workspace.keys) != 0 {
		t.Fatal("retained index row selected", err)
	}
	if err := scan.visitRow(moved, value); err != nil || len(workspace.keys) != 1 ||
		!bytes.Equal(workspace.keys[0], moved) || !bytes.Equal(workspace.documents[0], value) {
		t.Fatal("moved index was not captured byte-exactly", err)
	}
	if _, err := partitioner.RelationPoint(1, moved, value, &workspace.document); err == nil {
		t.Fatal("fixture accidentally supplied a base JSON document")
	}
	if err := scan.visitRow(append(bytes.Clone(moved), 0), value); err == nil {
		t.Fatal("malformed global storage key accepted")
	}
	if len(workspace.keys) != 1 {
		t.Fatal("failed mapping changed the selected batch")
	}
}
