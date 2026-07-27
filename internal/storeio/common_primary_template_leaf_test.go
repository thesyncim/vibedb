package storeio

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func commonPrimaryTemplateLeafRecords(count int) []CommonPrimaryLeafRecord {
	records := make([]CommonPrimaryLeafRecord, count)
	for at := range records {
		records[at] = CommonPrimaryLeafRecord{
			Key: []byte(fmt.Sprintf("primary-key-%09d", at)),
			Value: CommonPrimaryLeafValue{Inline: []byte(fmt.Sprintf(
				`{"id":%d,"kind":"document","group":%d,"active":%t,`+
					`"tier":"standard","region":"eu-west-1","name":"row %d"}`,
				at, at%997, at%3 == 0, at))},
		}
	}
	return records
}

func commonPrimaryTemplateLeafFixture(
	t testing.TB, count int,
) (
	[]byte, []CommonPrimaryLeafRecord,
	[16]byte, BucketID, PageRef, uint64, CommonPrimaryLeafBounds,
) {
	t.Helper()
	records := commonPrimaryTemplateLeafRecords(count)
	seed := format0StoreID
	bucket := BucketID(7)
	logicalID, _ := CommonPrimaryLeafLogicalID(bucket)
	const generation = uint64(3)
	bounds := CommonPrimaryLeafBounds{
		FileEnd:           1 << 20,
		NextLogicalID:     PrimaryFirstDynamicLogicalID + 1,
		AllocationQuantum: format0PageSize,
	}
	page := make([]byte, CommonPrimaryLeafNarrowBytes)
	if _, err := EncodeCommonPrimaryTemplateLeaf(
		page,
		CommonPrimaryLeafHeader{
			StoreID: seed, Generation: generation, Bucket: bucket,
			PageSize: CommonPrimaryLeafNarrowBytes,
		},
		records, bounds,
	); err != nil {
		t.Fatalf("encode: %v", err)
	}
	ref := PageRef{
		Offset: uint64(format0PageSize) * 4, LogicalID: logicalID,
		Generation: generation, Length: CommonPrimaryLeafNarrowBytes,
		Kind: PagePrimaryLeaf,
	}
	return page, records, seed, bucket, ref, generation, bounds
}

func TestCommonPrimaryTemplateLeafRoundTrip(t *testing.T) {
	page, records, seed, bucket, ref, generation, bounds :=
		commonPrimaryTemplateLeafFixture(t, 30)
	if PrimaryLeafClass(page) != CommonPrimaryLeafTemplate {
		t.Fatalf("class = %d", PrimaryLeafClass(page))
	}
	if _, err := OpenCommonPrimaryTemplateLeaf(
		page, seed, bucket, ref, generation, bounds,
	); err != nil {
		t.Fatalf("open: %v", err)
	}
	view, ok := AdmittedCommonPrimaryTemplateLeaf(page, bucket, bounds)
	if !ok || view.Len() != len(records) {
		t.Fatalf("admit ok=%v len=%d", ok, view.Len())
	}
	for at := range records {
		out, found := view.AppendRawByKey(nil, records[at].Key)
		if !found || !bytes.Equal(out, records[at].Value.Inline) {
			t.Fatalf("lookup %d found=%v got=%q want=%q",
				at, found, out, records[at].Value.Inline)
		}
	}
	if _, found := view.AppendRawByKey(nil, []byte("primary-key-999999999")); found {
		t.Fatal("absent key found")
	}
	// Ordered reconstruction from FirstRankFrom.
	rank := view.FirstRankFrom(records[10].Key)
	got := 10
	for r := rank; r < view.Len(); r++ {
		key, ti, rowOK := view.RowAt(r)
		if !rowOK || !bytes.Equal(key, records[got].Key) {
			t.Fatalf("scan rank %d key=%q", r, key)
		}
		doc := view.AppendRawRank(nil, r, ti)
		if !bytes.Equal(doc, records[got].Value.Inline) {
			t.Fatalf("scan splice %d = %q", r, doc)
		}
		got++
	}
	if got != len(records) {
		t.Fatalf("scanned %d, want %d", got, len(records))
	}
}

func TestCommonPrimaryTemplateLeafDetemplate(t *testing.T) {
	page, records, seed, bucket, _, _, bounds :=
		commonPrimaryTemplateLeafFixture(t, 40)
	leaf, detemplated, err := AdmittedPrimaryLeafForMutation(
		page, seed, bucket, bounds,
	)
	if err != nil || !detemplated {
		t.Fatalf("detemplate = %v, %v", detemplated, err)
	}
	if leaf.Class() == CommonPrimaryLeafTemplate {
		t.Fatalf("de-templated leaf still template class")
	}
	if leaf.Len() != len(records) {
		t.Fatalf("de-templated len = %d, want %d", leaf.Len(), len(records))
	}
	for at := range records {
		hash := commonPrimaryLeafHash(seed, records[at].Key)
		_, raw, overflow, ok := leaf.LookupRawHashed(hash, records[at].Key)
		if !ok || overflow || !bytes.Equal(raw, records[at].Value.Inline) {
			t.Fatalf("de-templated lookup %d ok=%v raw=%q", at, ok, raw)
		}
	}
}

func TestCommonPrimaryTemplateLeafDeterministic(t *testing.T) {
	records := commonPrimaryTemplateLeafRecords(30)
	bounds := CommonPrimaryLeafBounds{
		FileEnd: 1 << 20, NextLogicalID: PrimaryFirstDynamicLogicalID + 1,
		AllocationQuantum: format0PageSize,
	}
	header := CommonPrimaryLeafHeader{
		StoreID: format0StoreID, Generation: 3, Bucket: BucketID(7),
		PageSize: CommonPrimaryLeafNarrowBytes,
	}
	a := make([]byte, CommonPrimaryLeafNarrowBytes)
	b := make([]byte, CommonPrimaryLeafNarrowBytes)
	if _, err := EncodeCommonPrimaryTemplateLeaf(a, header, records, bounds); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeCommonPrimaryTemplateLeaf(b, header, records, bounds); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("template leaf encoding not deterministic")
	}
}

func TestCommonPrimaryTemplateLeafCorruptionFailsClosed(t *testing.T) {
	page, _, seed, bucket, ref, generation, bounds :=
		commonPrimaryTemplateLeafFixture(t, 24)
	// Class byte flip: the raw decoder must reject an unknown class.
	classFlip := append([]byte(nil), page...)
	classFlip[PageHeaderSize+2] ^= 0x40
	if _, err := SealPage(classFlip); err != nil {
		t.Fatal(err)
	}
	if PrimaryLeafClass(classFlip) == CommonPrimaryLeafTemplate {
		t.Fatal("class flip still reads as template")
	}
	if _, err := OpenCommonPrimaryLeaf(
		classFlip, seed, bucket, ref, generation, bounds,
	); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("class flip accepted: %v", err)
	}
	// Image-region flip inside the template payload.
	imageFlip := append([]byte(nil), page...)
	imageFlip[PageHeaderSize+commonPrimaryTemplateLeafImageOffset+40] ^= 0x20
	if _, err := SealPage(imageFlip); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCommonPrimaryTemplateLeaf(
		imageFlip, seed, bucket, ref, generation, bounds,
	); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("image corruption accepted: %v", err)
	}
}
