package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson/x/byteview"
)

func driverTransactionID(seed byte) distributedtxn.ID {
	var id distributedtxn.ID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func TestCommitDistributedParticipantPublishesDataAndStateAtomically(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "shard.vdb")
	binding := ShardStoreBinding{
		Distribution: distribution.DistributionName("tenant_data"),
		Shard:        distribution.ShardID("-80"), AllocationGeneration: 1,
	}
	db, err := InitializeShardStore(path, binding)
	if err != nil {
		t.Fatalf("InitializeShardStore: %v", err)
	}
	journal, err := db.OpenDistributedTransactionJournal()
	if err != nil {
		t.Fatalf("OpenDistributedTransactionJournal: %v", err)
	}
	_ = journal.Close()
	session, err := db.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	create, err := session.Prepare(ctx, `CREATE TABLE docs (id STRING PRIMARY KEY, n INTEGER NOT NULL)`)
	if err != nil {
		t.Fatalf("prepare CREATE: %v", err)
	}
	if _, err := create.Exec(ctx, nil); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	_ = create.Close()
	insert, err := session.Prepare(ctx, `INSERT INTO docs (id, n) VALUES (?, ?)`)
	if err != nil {
		t.Fatalf("prepare INSERT: %v", err)
	}
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	result, err := insert.Exec(ctx, []any{"a", int64(1)})
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	id := driverTransactionID(1)
	rows, err := session.CommitDistributedParticipant(ctx, id, 1, result.RowsAffected)
	if err != nil || rows != 1 {
		t.Fatalf("CommitDistributedParticipant = %d,%v, want 1,nil", rows, err)
	}
	revision, rows, found, err := db.DistributedParticipantStatus(id)
	if err != nil || !found || revision != 2 || rows != 1 {
		t.Fatalf("status = revision %d rows %d found %v err %v", revision, rows, found, err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		revision, rows, found, err = db.DistributedParticipantStatus(id)
	}); allocations != 0 {
		t.Fatalf("warm participant status allocations = %.2f, want 0", allocations)
	}
	if err != nil || !found || revision != 2 || rows != 1 {
		t.Fatalf("allocated status result = revision %d rows %d found %v err %v",
			revision, rows, found, err)
	}
	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatalf("retry Begin: %v", err)
	}
	if _, err := insert.Exec(ctx, []any{"retry-must-not-publish", int64(2)}); err != nil {
		t.Fatalf("retry INSERT: %v", err)
	}
	rows, err = session.CommitDistributedParticipant(ctx, id, 1, 9)
	if err != nil || rows != 1 {
		t.Fatalf("exact retry = %d,%v, want retained 1,nil", rows, err)
	}
	count := distributedParticipantPrepare(t, session,
		`SELECT COUNT(*) FROM docs WHERE id = ?`)
	assertDistributedParticipantCount(t, count, "retry-must-not-publish", 0)

	if err := session.Begin(ctx, TxOptions{}); err != nil {
		t.Fatalf("conflicting Begin: %v", err)
	}
	if _, err := insert.Exec(ctx, []any{"conflict-must-not-publish", int64(3)}); err != nil {
		t.Fatalf("conflicting INSERT: %v", err)
	}
	if _, err := session.CommitDistributedParticipant(ctx, id, 2, 1); !errors.Is(err, ErrDistributedTransactionConflict) {
		t.Fatalf("revision mismatch = %v, want ErrDistributedTransactionConflict", err)
	}
	assertDistributedParticipantCount(t, count, "conflict-must-not-publish", 0)
	_ = count.Close()
	_ = insert.Close()
	_ = session.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = OpenShardStore(path, binding)
	if err != nil {
		t.Fatalf("OpenShardStore: %v", err)
	}
	defer db.Close()
	revision, rows, found, err = db.DistributedParticipantStatus(id)
	if err != nil || !found || revision != 2 || rows != 1 {
		t.Fatalf("reopened status = revision %d rows %d found %v err %v", revision, rows, found, err)
	}
	assertDistributedTransactionStateProfile(t, db.connector.db.distributedTxnCollection)
}

func TestDistributedParticipantStateCodecGoldenBoundaries(t *testing.T) {
	if distributedtxn.ParticipantApplied != 3 {
		t.Fatalf("ParticipantApplied = %d, want wire state 3", distributedtxn.ParticipantApplied)
	}
	if distributedParticipantStateHeaderBytes != 8 ||
		distributedParticipantStateMaxBytes != 27 ||
		distributedTransactionStateKeyBytes != 16 ||
		distributedTransactionStateBatchBytes != 43 {
		t.Fatalf("participant geometry = header %d value %d key %d batch %d, want 8/27/16/43",
			distributedParticipantStateHeaderBytes,
			distributedParticipantStateMaxBytes,
			distributedTransactionStateKeyBytes,
			distributedTransactionStateBatchBytes,
		)
	}
	cases := []struct {
		name     string
		revision uint64
		rows     int64
		want     []byte
	}{
		{
			name: "minimum", revision: 1, rows: 0,
			want: []byte{'V', 'D', 'P', 'A', 0, 3, 0, 0, 1, 0},
		},
		{
			name: "single-byte maxima", revision: 127, rows: 127,
			want: []byte{'V', 'D', 'P', 'A', 0, 3, 0, 0, 0x7f, 0x7f},
		},
		{
			name: "first two-byte values", revision: 128, rows: 128,
			want: []byte{'V', 'D', 'P', 'A', 0, 3, 0, 0, 0x80, 0x01, 0x80, 0x01},
		},
		{
			name: "exact maximum", revision: math.MaxUint64, rows: math.MaxInt64,
			want: []byte{
				'V', 'D', 'P', 'A', 0, 3, 0, 0,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var storage [distributedParticipantStateMaxBytes]byte
			got := appendDistributedParticipantState(storage[:0], test.revision, test.rows)
			if !bytes.Equal(got, test.want) {
				t.Fatalf("encoded = %x, want %x", got, test.want)
			}
			revision, rows, err := openDistributedParticipantState(got)
			if err != nil || revision != test.revision || rows != test.rows {
				t.Fatalf("open = (%d,%d,%v), want (%d,%d,nil)",
					revision, rows, err, test.revision, test.rows)
			}
		})
	}
}

func TestOpenDistributedParticipantStateRejectsCorruption(t *testing.T) {
	valid := appendDistributedParticipantState(nil, 300, 700)
	for length := 0; length < len(valid); length++ {
		if revision, rows, err := openDistributedParticipantState(valid[:length]); !errors.Is(err, ErrDistributedTransactionConflict) || revision != 0 || rows != 0 {
			t.Fatalf("truncation %d = (%d,%d,%v), want zero conflict", length, revision, rows, err)
		}
	}
	for offset := 0; offset < distributedParticipantStateHeaderBytes; offset++ {
		corrupt := append([]byte(nil), valid...)
		corrupt[offset] ^= 0x80
		if _, _, err := openDistributedParticipantState(corrupt); !errors.Is(err, ErrDistributedTransactionConflict) {
			t.Fatalf("header corruption at %d = %v, want conflict", offset, err)
		}
	}
	cases := [][]byte{
		append(append([]byte(nil), valid...), 0),
		append(append([]byte(nil), valid...), make([]byte, distributedParticipantStateMaxBytes-len(valid)+1)...),
		[]byte(`[3,300,700]`),
	}
	for i, corrupt := range cases {
		if _, _, err := openDistributedParticipantState(corrupt); !errors.Is(err, ErrDistributedTransactionConflict) {
			t.Fatalf("corruption %d (%x) = %v, want conflict", i, corrupt, err)
		}
	}
}

func TestOpenDistributedParticipantStateRejectsNoncanonicalValues(t *testing.T) {
	header := []byte{'V', 'D', 'P', 'A', 0, 3, 0, 0}
	appendValue := func(revision, rows uint64) []byte {
		out := append([]byte(nil), header...)
		out = binary.AppendUvarint(out, revision)
		return binary.AppendUvarint(out, rows)
	}
	cases := []struct {
		name string
		raw  []byte
	}{
		{"zero revision", appendValue(0, 0)},
		{"overlong revision", append(append([]byte(nil), header...), 0x81, 0x00, 0x00)},
		{"overlong rows", append(append([]byte(nil), header...), 0x01, 0x80, 0x00)},
		{"rows above MaxInt64", appendValue(1, uint64(math.MaxInt64)+1)},
		{"revision overflow", append(append([]byte(nil), header...), 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00)},
		{"rows overflow", append(append([]byte(nil), header...), 0x01, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if revision, rows, err := openDistributedParticipantState(test.raw); !errors.Is(err, ErrDistributedTransactionConflict) || revision != 0 || rows != 0 {
				t.Fatalf("open %x = (%d,%d,%v), want zero conflict", test.raw, revision, rows, err)
			}
		})
	}
}

func TestDistributedParticipantStateProfileReopensAndRejectsMismatches(t *testing.T) {
	options := distributedTransactionStateOptions()
	if !options.OpaqueValues || options.Durability != durable.DurabilitySync ||
		options.MaxKeyBytes != 16 || options.InlineValueBytes != 27 ||
		options.MaxDocumentBytes != 27 || options.MaxBatchDocuments != 1 ||
		options.MaxBatchBytes != 43 {
		t.Fatalf("distributed state options = %+v, want opaque sync 16/27/27/1/43", options)
	}

	path := filepath.Join(t.TempDir(), "participant-state.vjc")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	assertDistributedTransactionStateProfile(t, collection)
	id := driverTransactionID(41)
	var storage [distributedParticipantStateMaxBytes]byte
	want := appendDistributedParticipantState(storage[:0], math.MaxUint64, math.MaxInt64)
	if _, err := collection.Put(id[:], want); err != nil {
		t.Fatalf("Put max state: %v", err)
	}
	if err := collection.Close(); err != nil {
		t.Fatalf("close created collection: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close created file: %v", err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	collection, err = durable.Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("reopen participant state: %v", err)
	}
	t.Cleanup(func() { _ = collection.Close(); _ = file.Close() })
	assertDistributedTransactionStateProfile(t, collection)
	var rawBuf [distributedParticipantStateMaxBytes]byte
	raw, found, err := collection.AppendRaw(rawBuf[:0], id[:])
	if err != nil || !found || !bytes.Equal(raw, want) {
		t.Fatalf("reopened raw = (%x,%v,%v), want %x,true,nil", raw, found, err, want)
	}

	mismatches := []struct {
		name   string
		mutate func(*durable.Options)
	}{
		{"JSON values", func(o *durable.Options) { o.OpaqueValues = false }},
		{"key bound", func(o *durable.Options) { o.MaxKeyBytes, o.MaxBatchBytes = 15, 42 }},
		{"value bound", func(o *durable.Options) { o.InlineValueBytes, o.MaxDocumentBytes, o.MaxBatchBytes = 28, 28, 44 }},
		{"batch count", func(o *durable.Options) { o.MaxBatchDocuments, o.MaxBatchBytes = 2, 59 }},
		{"batch bytes", func(o *durable.Options) { o.MaxBatchBytes = 44 }},
		{"durability", func(o *durable.Options) { o.Durability = durable.DurabilityAsyncVisible }},
	}
	for _, test := range mismatches {
		t.Run(test.name, func(t *testing.T) {
			wrong := options
			test.mutate(&wrong)
			wrongFile, err := os.OpenFile(
				filepath.Join(t.TempDir(), "wrong.vjc"),
				os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
			)
			if err != nil {
				t.Fatal(err)
			}
			wrongCollection, err := durable.Create(wrongFile, wrong)
			if err != nil {
				_ = wrongFile.Close()
				t.Fatal(err)
			}
			defer wrongCollection.Close()
			defer wrongFile.Close()
			if err := validateDistributedTransactionStateCollection(wrongCollection); err == nil {
				t.Fatal("mismatched participant profile was accepted")
			}
		})
	}

	t.Run("development JSON profile cannot reopen", func(t *testing.T) {
		legacyPath := filepath.Join(t.TempDir(), "legacy-json-state.vjc")
		legacyFile, err := os.OpenFile(
			legacyPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		legacyOptions := durable.Options{
			Durability:        durable.DurabilitySync,
			MaxKeyBytes:       16,
			InlineValueBytes:  64,
			MaxDocumentBytes:  64,
			MaxBatchDocuments: 1,
			MaxBatchBytes:     80,
		}
		legacyCollection, err := durable.Create(legacyFile, legacyOptions)
		if err != nil {
			_ = legacyFile.Close()
			t.Fatal(err)
		}
		legacyID := driverTransactionID(91)
		if _, err := legacyCollection.Put(legacyID[:], []byte(`[3,2,1]`)); err != nil {
			t.Fatalf("write legacy JSON state: %v", err)
		}
		if err := legacyCollection.Close(); err != nil {
			t.Fatalf("close legacy JSON collection: %v", err)
		}
		if err := legacyFile.Close(); err != nil {
			t.Fatalf("close legacy JSON file: %v", err)
		}

		legacyFile, err = os.OpenFile(legacyPath, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer legacyFile.Close()
		reopened, err := durable.Open(legacyFile, options)
		if reopened != nil {
			_ = reopened.Close()
		}
		if err == nil {
			t.Fatal("development JSON-profile state reopened as the opaque binary profile")
		}
	})
}

func TestDistributedParticipantStateCodecAllocations(t *testing.T) {
	var storage [distributedParticipantStateMaxBytes]byte
	encoded := appendDistributedParticipantState(storage[:0], math.MaxUint64, math.MaxInt64)
	if allocations := testing.AllocsPerRun(1000, func() {
		out := appendDistributedParticipantState(storage[:0], math.MaxUint64, math.MaxInt64)
		if len(out) != distributedParticipantStateMaxBytes {
			panic("wrong encoded length")
		}
	}); allocations != 0 {
		t.Fatalf("encode allocations = %.2f, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		revision, rows, err := openDistributedParticipantState(encoded)
		if err != nil || revision != math.MaxUint64 || rows != math.MaxInt64 {
			panic("wrong decoded state")
		}
	}); allocations != 0 {
		t.Fatalf("decode allocations = %.2f, want 0", allocations)
	}
}

func FuzzOpenDistributedParticipantState(f *testing.F) {
	seeds := [][]byte{
		appendDistributedParticipantState(nil, 1, 0),
		appendDistributedParticipantState(nil, math.MaxUint64, math.MaxInt64),
		[]byte(`[3,1,0]`),
		{'V', 'D', 'P', 'A', 0, 3, 0, 0, 0x81, 0, 0},
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		revision, rows, err := openDistributedParticipantState(raw)
		if err != nil {
			if !errors.Is(err, ErrDistributedTransactionConflict) || revision != 0 || rows != 0 {
				t.Fatalf("rejected %x as (%d,%d,%v)", raw, revision, rows, err)
			}
			return
		}
		if revision == 0 || rows < 0 {
			t.Fatalf("accepted invalid semantics revision=%d rows=%d", revision, rows)
		}
		canonical := appendDistributedParticipantState(nil, revision, rows)
		if !bytes.Equal(raw, canonical) {
			t.Fatalf("accepted noncanonical %x; canonical is %x", raw, canonical)
		}
	})
}

func BenchmarkDistributedParticipantStateEncode(b *testing.B) {
	b.Run("binary_format0", func(b *testing.B) {
		var storage [distributedParticipantStateMaxBytes]byte
		b.ReportAllocs()
		b.SetBytes(distributedParticipantStateMaxBytes)
		for range b.N {
			out := appendDistributedParticipantState(storage[:0], math.MaxUint64, math.MaxInt64)
			if len(out) != distributedParticipantStateMaxBytes {
				b.Fatal("wrong encoded length")
			}
		}
	})
	b.Run("legacy_decimal_JSON", func(b *testing.B) {
		var storage [64]byte
		b.ReportAllocs()
		for range b.N {
			out := appendLegacyDistributedParticipantState(storage[:0], math.MaxUint64, math.MaxInt64)
			if len(out) == 0 {
				b.Fatal("empty legacy encoding")
			}
		}
	})
}

func BenchmarkDistributedParticipantStateDecode(b *testing.B) {
	b.Run("binary_format0", func(b *testing.B) {
		encoded := appendDistributedParticipantState(nil, math.MaxUint64, math.MaxInt64)
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		for range b.N {
			revision, rows, err := openDistributedParticipantState(encoded)
			if err != nil || revision != math.MaxUint64 || rows != math.MaxInt64 {
				b.Fatal("wrong decoded state")
			}
		}
	})
	b.Run("legacy_decimal_JSON", func(b *testing.B) {
		encoded := appendLegacyDistributedParticipantState(nil, math.MaxUint64, math.MaxInt64)
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		for range b.N {
			revision, rows, err := openLegacyDistributedParticipantState(encoded)
			if err != nil || revision != math.MaxUint64 || rows != math.MaxInt64 {
				b.Fatal("wrong decoded legacy state")
			}
		}
	})
}

func appendLegacyDistributedParticipantState(dst []byte, revision uint64, rowsAffected int64) []byte {
	dst = append(dst, '[', byte('0'+distributedtxn.ParticipantApplied), ',')
	dst = strconv.AppendUint(dst, revision, 10)
	dst = append(dst, ',')
	dst = strconv.AppendInt(dst, rowsAffected, 10)
	return append(dst, ']')
}

func openLegacyDistributedParticipantState(src []byte) (revision uint64, rowsAffected int64, err error) {
	if len(src) < 7 || src[0] != '[' ||
		src[1] != byte('0'+distributedtxn.ParticipantApplied) ||
		src[2] != ',' || src[len(src)-1] != ']' {
		return 0, 0, ErrDistributedTransactionConflict
	}
	comma := -1
	for i := 3; i < len(src)-1; i++ {
		if src[i] == ',' {
			comma = i
			break
		}
	}
	if comma < 0 {
		return 0, 0, ErrDistributedTransactionConflict
	}
	revision, err = strconv.ParseUint(byteview.String(src[3:comma]), 10, 64)
	if err != nil || revision == 0 {
		return 0, 0, ErrDistributedTransactionConflict
	}
	rowsAffected, err = strconv.ParseInt(byteview.String(src[comma+1:len(src)-1]), 10, 64)
	if err != nil || rowsAffected < 0 {
		return 0, 0, ErrDistributedTransactionConflict
	}
	return revision, rowsAffected, nil
}

func distributedParticipantPrepare(t testing.TB, session *Session, statement string) *Prepared {
	t.Helper()
	prepared, err := session.Prepare(context.Background(), statement)
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

func assertDistributedParticipantCount(t testing.TB, prepared *Prepared, key string, want int64) {
	t.Helper()
	cursor, err := prepared.Query(context.Background(), []any{key})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("COUNT returned no row")
	}
	got, ok := cursor.Cell(0).Int64()
	if !ok || got != want {
		t.Fatalf("COUNT = (%d,%v), want %d", got, ok, want)
	}
	if cursor.Next() {
		t.Fatal("COUNT returned more than one row")
	}
}

func assertDistributedTransactionStateProfile(t testing.TB, collection *durable.Collection) {
	t.Helper()
	if err := validateDistributedTransactionStateCollection(collection); err != nil {
		t.Fatalf("distributed participant collection profile: %v", err)
	}
	if !collection.HasOpaqueValues() || collection.HasSchema() || collection.HasIndexes() ||
		!collection.HasSynchronousDurability() || !collection.SupportsUpdate() ||
		collection.MaxKeyBytes() != 16 || collection.MaxDocumentBytes() != 27 ||
		collection.MaxBatchDocuments() != 1 || collection.MaxBatchBytes() != 43 {
		t.Fatal("distributed participant collection does not expose its exact opaque profile")
	}
}
