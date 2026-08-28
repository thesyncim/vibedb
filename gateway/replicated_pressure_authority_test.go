package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestReplicatedPressureRecordCanonicalRoundTripAndBounds(t *testing.T) {
	record := pressureRecord(7, 11, []byte(`{"nodes":[],"reports":[]}`))
	raw, err := appendReplicatedPressureRecord([]byte("prefix"), record)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openReplicatedPressureRecord(raw[len("prefix"):])
	if err != nil || opened.CatalogGeneration != record.CatalogGeneration ||
		opened.AuthorityRevision != record.AuthorityRevision ||
		opened.PayloadDigest != record.PayloadDigest || !bytes.Equal(opened.Payload, record.Payload) {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	if _, err = openReplicatedPressureRecord(append(raw[len("prefix"):], ' ')); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	noncanonical := pressureRecord(7, 11, []byte(`{ "nodes": [], "reports": [] }`))
	if _, err = appendReplicatedPressureRecord(nil, noncanonical); err == nil {
		t.Fatal("noncanonical payload accepted")
	}
}

func TestReplicatedPressureAuthorityCASAndExactRetry(t *testing.T) {
	authority, _, _ := newCatalogAuthorityFixture(t)
	ctx := context.Background()
	if _, err := authority.ReadPressureRecord(ctx); !errors.Is(err, ErrReplicatedPressureMissing) {
		t.Fatalf("missing pressure read=%v", err)
	}
	first := pressureRecord(10, 1, []byte(`{"nodes":[],"reports":[]}`))
	if err := authority.PublishPressureRecord(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	if err := authority.PublishPressureRecord(ctx, 0, first); err != nil {
		t.Fatalf("exact retry=%v", err)
	}
	conflict := pressureRecord(10, 1, []byte(`{"nodes":[1],"reports":[]}`))
	if err := authority.PublishPressureRecord(ctx, 0, conflict); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("same revision conflict=%v", err)
	}
	second := pressureRecord(10, 2, []byte(`{"nodes":[],"reports":[1]}`))
	if err := authority.PublishPressureRecord(ctx, 1, second); err != nil {
		t.Fatal(err)
	}
	opened, err := authority.ReadPressureRecord(ctx)
	if err != nil || opened.AuthorityRevision != 2 || !bytes.Equal(opened.Payload, second.Payload) {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	if err := authority.PublishPressureRecord(ctx, 0, first); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("regressed revision=%v", err)
	}
}

func pressureRecord(generation, revision uint64, payload []byte) ReplicatedPressureRecord {
	return ReplicatedPressureRecord{CatalogGeneration: generation,
		AuthorityRevision: revision, PayloadDigest: sha256.Sum256(payload), Payload: payload}
}
