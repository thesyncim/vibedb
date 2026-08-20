package shardservice

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

func TestMutationBatchRoundTripAndBorrowing(t *testing.T) {
	statements := []MutationStatement{
		{SQL: "INSERT INTO docs (id, n) VALUES (?, ?)", Params: []Param{
			StringBytesParam([]byte("first")), NumberBytesParam([]byte("7")),
		}},
		{SQL: "UPDATE docs SET doc = ? WHERE id = ?", Params: []Param{
			DocumentBytesParam([]byte(`{"id":"first","n":8}`)), StringBytesParam([]byte("first")),
		}},
		{
			Kind: MutationGlobalIndexPut, Relation: "docs_by_email",
			IndexID: 17, Incarnation: 3,
			EntryKey: []byte{1, 3, 'a', '@', 'b'},
			Value:    []byte(`["tenant",7.0]`), LocatorCount: 2, Unique: true,
		},
	}
	raw, err := AppendMutationBatch([]byte("prefix"), statements)
	if err != nil {
		t.Fatal(err)
	}
	raw = raw[len("prefix"):]
	batch, err := OpenMutationBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	first, ok, err := batch.Next()
	if err != nil || !ok || first.SQL != statements[0].SQL || len(first.Params) != 2 ||
		!bytes.Equal(first.Params[0].Bytes, []byte("first")) {
		t.Fatalf("first = %+v,%v,%v", first, ok, err)
	}
	offset := bytes.Index(raw, []byte("first"))
	if offset < 0 || &first.Params[0].Bytes[0] != &raw[offset] {
		t.Fatal("parameter payload did not borrow the durable batch")
	}
	second, ok, err := batch.Next()
	if err != nil || !ok || second.SQL != statements[1].SQL || len(second.Params) != 2 {
		t.Fatalf("second = %+v,%v,%v", second, ok, err)
	}
	third, ok, err := batch.Next()
	if err != nil || !ok || third.Kind != MutationGlobalIndexPut ||
		third.Relation != "docs_by_email" || third.IndexID != 17 ||
		third.Incarnation != 3 || third.LocatorCount != 2 || !third.Unique ||
		!bytes.Equal(third.EntryKey, statements[2].EntryKey) ||
		!bytes.Equal(third.Value, statements[2].Value) {
		t.Fatalf("third = %+v,%v,%v", third, ok, err)
	}
	entryOffset := bytes.Index(raw, statements[2].EntryKey)
	if entryOffset < 0 || &third.EntryKey[0] != &raw[entryOffset] {
		t.Fatal("global index key did not borrow the durable batch")
	}
	if _, ok, err := batch.Next(); err != nil || ok {
		t.Fatalf("batch end = %v,%v", ok, err)
	}
}

func TestMutationBatchRejectsCorruptionAndOversize(t *testing.T) {
	raw, err := AppendMutationBatch(nil, []MutationStatement{{SQL: "DELETE FROM docs WHERE id = ?", Params: []Param{StringParam("x")}}})
	if err != nil {
		t.Fatal(err)
	}
	raw = append([]byte(nil), raw...)
	raw[len(raw)-1] = 0xff
	if _, err := OpenMutationBatch(raw); !errors.Is(err, ErrMutationBatch) {
		t.Fatalf("corrupt batch = %v", err)
	}
	tooLarge := MutationStatement{SQL: string(make([]byte, distributedtxn.MaxMutationBytes))}
	if _, err := AppendMutationBatch(nil, []MutationStatement{tooLarge}); !errors.Is(err, distributedtxn.ErrTooLarge) {
		t.Fatalf("oversize batch = %v", err)
	}
	for _, invalid := range []MutationStatement{
		{Kind: MutationGlobalIndexPut, Relation: "idx", IndexID: 1, Incarnation: 1, EntryKey: []byte{0}, Value: []byte(`[]`), LocatorCount: 1},
		{Kind: MutationGlobalIndexPut, Relation: "idx", IndexID: 1, Incarnation: 1, EntryKey: []byte{1}, Value: []byte(`{}`), LocatorCount: 1},
		{Kind: MutationGlobalIndexDelete, Relation: "idx", IndexID: 1, Incarnation: 1, EntryKey: []byte{1}, Value: []byte(`["x"]`), LocatorCount: 1, Unique: true},
	} {
		if _, err := AppendMutationBatch(nil, []MutationStatement{invalid}); !errors.Is(err, ErrMutationBatch) {
			t.Fatalf("invalid typed mutation %+v err = %v", invalid, err)
		}
	}
}

func TestMutationBatchPrimaryPreconditionRoundTrip(t *testing.T) {
	digests := [][sha256.Size]byte{
		sha256.Sum256([]byte(`{"id":"a"}`)),
		sha256.Sum256([]byte(`{"id":"b"}`)),
	}
	raw, err := AppendMutationBatch(nil, []MutationStatement{{
		Kind: MutationPrimaryPrecondition, Relation: "docs", PrimaryPath: []byte("/id"),
		ExpectedKeys: [][]byte{{1, 'a'}, {1, 'b'}}, ExpectedDigests: digests,
	}})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := OpenMutationBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	condition, ok, err := batch.Next()
	if err != nil || !ok || condition.Kind != MutationPrimaryPrecondition ||
		condition.Relation != "docs" || string(condition.PrimaryPath) != "/id" ||
		len(condition.ExpectedKeys) != 2 || condition.ExpectedDigests[1] != digests[1] {
		t.Fatalf("condition = %+v,%v,%v", condition, ok, err)
	}
	keyOffset := bytes.Index(raw, []byte{1, 'a'})
	if keyOffset < 0 || &condition.ExpectedKeys[0][0] != &raw[keyOffset] {
		t.Fatal("precondition key did not borrow the durable batch")
	}

	if _, err := AppendMutationBatch(nil, []MutationStatement{{
		Kind: MutationPrimaryPrecondition, Relation: "docs", PrimaryPath: []byte("/id"),
		ExpectedKeys: [][]byte{{1, 'b'}, {1, 'a'}}, ExpectedDigests: digests,
	}}); !errors.Is(err, ErrMutationBatch) {
		t.Fatalf("unsorted precondition err = %v", err)
	}
	if _, err := AppendMutationBatch(nil, []MutationStatement{{
		Kind: MutationPrimaryPrecondition, Relation: "docs", PrimaryPath: []byte("/id"),
	}}); err != nil {
		t.Fatalf("empty-set precondition: %v", err)
	}
	checkRaw, err := AppendMutationBatch(nil, []MutationStatement{{
		Kind: MutationPrimaryCheck, Relation: "docs", PrimaryPath: []byte("/id"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	checkBatch, err := OpenMutationBatch(checkRaw)
	if err != nil {
		t.Fatal(err)
	}
	check, ok, err := checkBatch.Next()
	if err != nil || !ok || check.Kind != MutationPrimaryCheck {
		t.Fatalf("primary check = %+v,%v,%v", check, ok, err)
	}
}

func BenchmarkMutationBatchOpen(b *testing.B) {
	raw, err := AppendMutationBatch(nil, []MutationStatement{
		{SQL: "INSERT INTO docs (id, n) VALUES (?, ?)", Params: []Param{StringParam("first"), NumberParam("7")}},
		{SQL: "DELETE FROM docs WHERE id = ?", Params: []Param{StringParam("second")}},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for range b.N {
		if _, err := OpenMutationBatch(raw); err != nil {
			b.Fatal(err)
		}
	}
}
