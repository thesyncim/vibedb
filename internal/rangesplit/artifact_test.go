package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibejson"
)

type childArtifactTestRow struct {
	key   []byte
	value []byte
}

func TestChildArtifactsRoundTripIsDeterministicAndSkipsRetainedCopy(t *testing.T) {
	partitioner := testChildArtifactPartitioner(t)
	left := documentForChild(t, partitioner, 0)
	right := documentsForChild(t, partitioner, 1, 2)
	right[0] = childArtifactDocumentPayload(right[0], 3<<10)
	right[1] = childArtifactDocumentPayload(right[1], 3<<10)
	rows := []childArtifactTestRow{
		{key: []byte("a"), value: left},
		{key: []byte("b"), value: right[0]},
		{key: []byte("c"), value: right[1]},
	}
	state := testSourceState(testSplitPlan(t, "node-b"))
	first, set, scans := writeChildArtifactRows(t, partitioner, state, rows)
	if scans != 1 {
		t.Fatalf("source scans = %d, want 1", scans)
	}
	if set.Children[0].Present || !set.Children[1].Present ||
		set.Partition.Rows != [autosplit.MaxSplitChildren]uint64{1, 2, 0} ||
		set.Children[1].Rows != 2 || set.Children[1].RowBytes != set.Partition.Bytes[1] ||
		set.Children[1].Source.Applied != state.Applied ||
		set.Children[1].Source.Term != state.LastTerm ||
		set.Children[1].Source.RouteGeneration != state.Binding.RouteGeneration ||
		set.Children[1].PlanDigest != partitioner.Digest() ||
		set.Children[1].PlacementDigest != partitioner.program.Digest() ||
		set.Children[1].Digest == ([sha256.Size]byte{}) {
		t.Fatalf("artifact set = %+v", set)
	}
	second, again, _ := writeChildArtifactRows(t, partitioner, state, rows)
	if !bytes.Equal(first, second) || set.Children[1].Digest != again.Children[1].Digest {
		t.Fatal("same source cut produced different child artifact bytes")
	}

	var got []childArtifactTestRow
	checkpoints := 0
	verified, err := partitioner.VerifyChildArtifact(
		bytes.NewReader(first), 1,
		ChildArtifactCallbacks{Rows: func(
			checkpoint ChildArtifactCheckpoint,
			chunk ChildArtifactRows,
		) error {
			if checkpoint.Child != 1 || checkpoint.Sequence != uint64(checkpoints) ||
				checkpoint.Rows != chunk.Len() || checkpoint.Digest == ([sha256.Size]byte{}) {
				t.Fatalf("checkpoint = %+v rows=%d", checkpoint, chunk.Len())
			}
			iterator := chunk.Iterator()
			for {
				key, value, ok := iterator.Next()
				if !ok {
					break
				}
				got = append(got, childArtifactTestRow{
					key: bytes.Clone(key), value: bytes.Clone(value),
				})
			}
			checkpoints++
			return nil
		}},
		&ChildArtifactVerifyWorkspace{
			PayloadBuffer: make([]byte, 0, MaxChildArtifactChunkBytes),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if verified != set.Children[1] || verified.EncodedBytes != uint64(len(first)) ||
		checkpoints != 2 || len(got) != 2 {
		t.Fatalf("verified=%+v checkpoints=%d rows=%d", verified, checkpoints, len(got))
	}
	for ordinal := range got {
		if !bytes.Equal(got[ordinal].key, rows[ordinal+1].key) ||
			!bytes.Equal(got[ordinal].value, rows[ordinal+1].value) {
			t.Fatalf("verified row %d = %q %q", ordinal, got[ordinal].key, got[ordinal].value)
		}
	}
}

func TestChildArtifactEmptyChildIsCertifiedWithoutChunk(t *testing.T) {
	partitioner := testChildArtifactPartitioner(t)
	rows := []childArtifactTestRow{{
		key: []byte("only-retained"), value: documentForChild(t, partitioner, 0),
	}}
	state := testSourceState(testSplitPlan(t, "node-b"))
	artifact, set, _ := writeChildArtifactRows(t, partitioner, state, rows)
	if set.Children[1].Chunks != 0 || set.Children[1].Rows != 0 ||
		len(artifact) <= childArtifactHeaderFixedBytes+childArtifactFooterBytes {
		t.Fatalf("empty child manifest=%+v bytes=%d", set.Children[1], len(artifact))
	}
	verified, err := partitioner.VerifyChildArtifact(
		bytes.NewReader(artifact), 1, ChildArtifactCallbacks{}, nil,
	)
	if err != nil || verified.Chunks != 0 || verified.Rows != 0 ||
		verified.Digest != set.Children[1].Digest {
		t.Fatalf("verified=%+v error=%v", verified, err)
	}
}

func TestChildArtifactRejectsCorruptionTruncationAndTrailingBytes(t *testing.T) {
	partitioner := testChildArtifactPartitioner(t)
	rows := []childArtifactTestRow{{
		key: []byte("right"), value: documentForChild(t, partitioner, 1),
	}}
	state := testSourceState(testSplitPlan(t, "node-b"))
	artifact, _, _ := writeChildArtifactRows(t, partitioner, state, rows)
	headerBytes := int(binary.LittleEndian.Uint32(artifact[12:16]))
	payloadOffset := headerBytes + childArtifactChunkHeaderBytes
	cases := []struct {
		name string
		data []byte
	}{
		{name: "header", data: corruptChildArtifactByte(artifact, 40)},
		{name: "chunk-header", data: corruptChildArtifactByte(artifact, headerBytes+20)},
		{name: "payload", data: corruptChildArtifactByte(artifact, payloadOffset)},
		{name: "footer", data: corruptChildArtifactByte(artifact, len(artifact)-20)},
		{name: "short-header", data: bytes.Clone(artifact[:20])},
		{name: "short-chunk", data: bytes.Clone(artifact[:payloadOffset+1])},
		{name: "short-footer", data: bytes.Clone(artifact[:len(artifact)-1])},
		{name: "trailing", data: append(bytes.Clone(artifact), 1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := partitioner.VerifyChildArtifact(
				bytes.NewReader(test.data), 1, ChildArtifactCallbacks{}, nil,
			); !errors.Is(err, ErrChildArtifact) {
				t.Fatalf("VerifyChildArtifact error = %v", err)
			}
		})
	}
}

func TestChildArtifactRejectsValidlyChecksummedWrongPlacementBeforeCallback(t *testing.T) {
	partitioner := testChildArtifactPartitioner(t)
	left, right := equalLengthDocumentsForChildren(t, partitioner)
	rows := []childArtifactTestRow{{key: []byte("row"), value: right}}
	state := testSourceState(testSplitPlan(t, "node-b"))
	artifact, _, _ := writeChildArtifactRows(t, partitioner, state, rows)
	headerBytes := int(binary.LittleEndian.Uint32(artifact[12:16]))
	chunkStart := headerBytes
	payloadStart := chunkStart + childArtifactChunkHeaderBytes
	keyBytes := int(binary.LittleEndian.Uint32(artifact[payloadStart : payloadStart+4]))
	valueBytes := int(binary.LittleEndian.Uint32(artifact[payloadStart+4 : payloadStart+8]))
	valueStart := payloadStart + childArtifactRowHeaderBytes + keyBytes
	if valueBytes != len(left) {
		t.Fatalf("replacement bytes = %d, want %d", len(left), valueBytes)
	}
	copy(artifact[valueStart:valueStart+valueBytes], left)
	payloadBytes := int(binary.LittleEndian.Uint32(artifact[chunkStart+32 : chunkStart+36]))
	chunkDigest := childArtifactDigestParts(
		childArtifactChunkDomain,
		artifact[chunkStart:payloadStart],
		artifact[payloadStart:payloadStart+payloadBytes],
	)
	chunkDigestStart := payloadStart + payloadBytes
	copy(artifact[chunkDigestStart:chunkDigestStart+sha256.Size], chunkDigest[:])
	footerStart := chunkDigestStart + sha256.Size
	copy(artifact[footerStart+64:footerStart+96], chunkDigest[:])
	footerDigest := childArtifactDigest(
		childArtifactFooterDomain, artifact[footerStart:footerStart+128],
	)
	copy(artifact[footerStart+128:footerStart+160], footerDigest[:])
	callbacks := 0
	if _, err := partitioner.VerifyChildArtifact(
		bytes.NewReader(artifact), 1,
		ChildArtifactCallbacks{Rows: func(ChildArtifactCheckpoint, ChildArtifactRows) error {
			callbacks++
			return nil
		}}, nil,
	); !errors.Is(err, ErrChildArtifact) || callbacks != 0 {
		t.Fatalf("wrong placement error=%v callbacks=%d", err, callbacks)
	}
}

func TestChildArtifactsRejectOrderingBoundsAndCallbackFailure(t *testing.T) {
	partitioner := testChildArtifactPartitioner(t)
	right := documentsForChild(t, partitioner, 1, 2)
	state := testSourceState(testSplitPlan(t, "node-b"))
	var output bytes.Buffer
	options := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	options.Writers[1] = &output
	unsorted := []childArtifactTestRow{
		{key: []byte("z"), value: right[0]},
		{key: []byte("a"), value: right[1]},
	}
	var workspace ChildArtifactWorkspace
	if _, err := partitioner.writeChildArtifacts(
		state, rangeChildArtifactRows(unsorted, nil), options, &workspace,
	); !errors.Is(err, ErrChildArtifact) {
		t.Fatalf("unsorted source error = %v", err)
	}

	options.PayloadBuffers[1] = make([]byte, 0, MinChildArtifactChunkBytes-1)
	if _, err := partitioner.writeChildArtifacts(
		state, rangeChildArtifactRows(unsorted[:1], nil), options, &workspace,
	); !errors.Is(err, ErrChildArtifactBound) {
		t.Fatalf("short buffer error = %v", err)
	}
	options.PayloadBuffers[1] = nil
	artifact, _, _ := writeChildArtifactRows(t, partitioner, state, unsorted[:1])
	want := errors.New("durable receiver stopped")
	if _, err := partitioner.VerifyChildArtifact(
		bytes.NewReader(artifact), 1,
		ChildArtifactCallbacks{Rows: func(ChildArtifactCheckpoint, ChildArtifactRows) error {
			return want
		}}, nil,
	); !errors.Is(err, want) {
		t.Fatalf("callback error = %v", err)
	}

	badOptions := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	badOptions.Writers[0] = io.Discard
	badOptions.Writers[1] = io.Discard
	if _, err := partitioner.writeChildArtifacts(
		state, rangeChildArtifactRows(unsorted[:1], nil), badOptions, &workspace,
	); !errors.Is(err, ErrInvalidPartition) {
		t.Fatalf("retained writer error = %v", err)
	}
}

func TestWriteChildArtifactsAllocatesZeroWhenWarm(t *testing.T) {
	partitioner := testChildArtifactPartitioner(t)
	document := documentForChild(t, partitioner, 1)
	rows := []childArtifactTestRow{{key: []byte("row"), value: document}}
	state := testSourceState(testSplitPlan(t, "node-b"))
	options := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	options.Writers[1] = io.Discard
	options.PayloadBuffers[1] = make([]byte, 0, MaxChildArtifactChunkBytes)
	var workspace ChildArtifactWorkspace
	rangeRows := rangeChildArtifactRows(rows, nil)
	if _, err := partitioner.writeChildArtifacts(state, rangeRows, options, &workspace); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		if _, err := partitioner.writeChildArtifacts(state, rangeRows, options, &workspace); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm child artifact write allocations = %v, want 0", allocs)
	}
}

func TestVerifyChildArtifactAllocatesZeroWhenWarm(t *testing.T) {
	partitioner := testChildArtifactPartitioner(t)
	rows := []childArtifactTestRow{{
		key: []byte("row"), value: documentForChild(t, partitioner, 1),
	}}
	state := testSourceState(testSplitPlan(t, "node-b"))
	artifact, _, _ := writeChildArtifactRows(t, partitioner, state, rows)
	workspace := ChildArtifactVerifyWorkspace{
		HeaderBuffer:  make([]byte, 0, MaxChildArtifactHeaderBytes),
		PayloadBuffer: make([]byte, 0, MaxChildArtifactChunkBytes),
	}
	reader := bytes.NewReader(artifact)
	if _, err := partitioner.VerifyChildArtifact(
		reader, 1, ChildArtifactCallbacks{}, &workspace,
	); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		reader.Reset(artifact)
		if _, err := partitioner.VerifyChildArtifact(
			reader, 1, ChildArtifactCallbacks{}, &workspace,
		); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm child artifact verify allocations = %v, want 0", allocs)
	}
}

func BenchmarkVerifyChildArtifact(b *testing.B) {
	partitioner := testChildArtifactPartitioner(b)
	right := documentsForChild(b, partitioner, 1, 128)
	rows := make([]childArtifactTestRow, len(right))
	for ordinal := range right {
		rows[ordinal] = childArtifactTestRow{
			key: []byte(fmt.Sprintf("key-%04d", ordinal)), value: right[ordinal],
		}
	}
	state := testSourceState(testSplitPlan(b, "node-b"))
	artifact, _, _ := writeChildArtifactRows(b, partitioner, state, rows)
	workspace := ChildArtifactVerifyWorkspace{
		HeaderBuffer:  make([]byte, 0, MaxChildArtifactHeaderBytes),
		PayloadBuffer: make([]byte, 0, MaxChildArtifactChunkBytes),
	}
	reader := bytes.NewReader(nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(artifact)))
	b.ResetTimer()
	for range b.N {
		reader.Reset(artifact)
		if _, err := partitioner.VerifyChildArtifact(
			reader, 1, ChildArtifactCallbacks{}, &workspace,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteChildArtifactOnePass(b *testing.B) {
	partitioner := testChildArtifactPartitioner(b)
	document := documentForChild(b, partitioner, 1)
	rows := []childArtifactTestRow{{key: []byte("row"), value: document}}
	state := testSourceState(testSplitPlan(b, "node-b"))
	options := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	options.Writers[1] = io.Discard
	options.PayloadBuffers[1] = make([]byte, 0, MaxChildArtifactChunkBytes)
	var workspace ChildArtifactWorkspace
	rangeRows := rangeChildArtifactRows(rows, nil)
	if _, err := partitioner.writeChildArtifacts(state, rangeRows, options, &workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(document)))
	b.ResetTimer()
	for range b.N {
		if _, err := partitioner.writeChildArtifacts(state, rangeRows, options, &workspace); err != nil {
			b.Fatal(err)
		}
	}
}

func testChildArtifactPartitioner(t testing.TB) *Partitioner {
	t.Helper()
	partitioner, err := NewPartitioner(
		testSplitPlan(t, "node-b"), "docs", []string{"/tenant", "/sequence"},
		distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	return partitioner
}

func writeChildArtifactRows(
	t testing.TB,
	partitioner *Partitioner,
	state replicatedstate.State,
	rows []childArtifactTestRow,
) ([]byte, ChildArtifactSet, int) {
	t.Helper()
	var output bytes.Buffer
	options := ChildArtifactOptions{TargetChunkBytes: MinChildArtifactChunkBytes}
	options.Writers[1] = &output
	options.PayloadBuffers[1] = make([]byte, 0, MaxChildArtifactChunkBytes)
	scans := 0
	var workspace ChildArtifactWorkspace
	set, err := partitioner.writeChildArtifacts(
		state,
		rangeChildArtifactRows(rows, &scans),
		options,
		&workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(output.Bytes()), set, scans
}

func rangeChildArtifactRows(
	rows []childArtifactTestRow,
	scans *int,
) func(func(key, value []byte) error) error {
	return func(visit func(key, value []byte) error) error {
		if scans != nil {
			(*scans)++
		}
		for _, row := range rows {
			if err := visit(row.key, row.value); err != nil {
				return err
			}
		}
		return nil
	}
}

func documentsForChild(t testing.TB, partitioner *Partitioner, child, count int) [][]byte {
	t.Helper()
	documents := make([][]byte, 0, count)
	var workspace distribution.DocumentPointWorkspace
	for sequence := 0; sequence < 1_000_000 && len(documents) < count; sequence++ {
		document := []byte(fmt.Sprintf(`{"sequence":%d,"tenant":"acme"}`, sequence))
		point, err := partitioner.program.Point(document, &workspace)
		if err != nil {
			t.Fatal(err)
		}
		if partitioner.childFor(point) == child {
			documents = append(documents, document)
		}
	}
	if len(documents) != count {
		t.Fatalf("found %d documents for child %d, want %d", len(documents), child, count)
	}
	return documents
}

func equalLengthDocumentsForChildren(t testing.TB, partitioner *Partitioner) ([]byte, []byte) {
	t.Helper()
	leftByLength := make(map[int][]byte)
	var workspace distribution.DocumentPointWorkspace
	for sequence := 0; sequence < 1_000_000; sequence++ {
		document := []byte(fmt.Sprintf(`{"tenant":"acme","sequence":%d}`, sequence))
		point, err := partitioner.program.Point(document, &workspace)
		if err != nil {
			t.Fatal(err)
		}
		switch partitioner.childFor(point) {
		case 0:
			leftByLength[len(document)] = document
		case 1:
			if left := leftByLength[len(document)]; left != nil {
				return left, document
			}
		}
	}
	t.Fatal("no equal-length documents for both children")
	return nil, nil
}

func corruptChildArtifactByte(src []byte, offset int) []byte {
	dst := bytes.Clone(src)
	dst[offset] ^= 0x80
	return dst
}

func childArtifactDocumentPayload(document []byte, payloadBytes int) []byte {
	result := make([]byte, 0, len(document)+payloadBytes+13)
	result = append(result, document[:len(document)-1]...)
	result = append(result, `,"payload":"`...)
	result = append(result, bytes.Repeat([]byte{'x'}, payloadBytes)...)
	result = append(result, '"', '}')
	canonical, err := vibejson.Canonicalize(result)
	if err != nil {
		panic(err)
	}
	return canonical
}
