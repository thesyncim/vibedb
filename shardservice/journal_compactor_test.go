package shardservice

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

func TestServerJournalCompactorReclaimsRecommendedGeneration(t *testing.T) {
	const targetCount = 50_000
	var pages [][]byte
	builder, err := distributedtxn.NewManifestBuilder(
		make([]byte, distributedtxn.ManifestSegmentBytes),
		func(segment distributedtxn.ManifestSegment) error {
			pages = append(pages, bytes.Clone(segment.Raw))
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint64(0); index < targetCount; index++ {
		var digest distributedtxn.Digest
		binary.LittleEndian.PutUint64(digest[:8], index+1)
		for at := 8; at < len(digest); at++ {
			digest[at] = byte(at)
		}
		if err = builder.Append(distributedtxn.TransactionTargetRef{
			Distribution: []byte("docs"), Shard: []byte(fmt.Sprintf("%016x", index)),
			RoutingVersion: 7, AllocationGeneration: 11, OwnershipEpoch: 13,
			AuthorityWitness: distributedtxn.AuthorityWitness(digest[:16]),
			MutationDigest:   digest, State: distributedtxn.TargetStaged,
		}); err != nil {
			t.Fatal(err)
		}
	}
	descriptor, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	journal, err := distributedtxn.OpenJournal(filepath.Join(t.TempDir(), "transactions.vtj"))
	if err != nil {
		t.Fatal(err)
	}
	var id distributedtxn.ID
	copy(id[:], []byte("compact-driver01"))
	raw, err := distributedtxn.AppendManifestCoordinator(nil, distributedtxn.ManifestCoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, RecoveryDeadline: 1, Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = journal.StageManifestCoordinator(raw); err != nil {
		t.Fatal(err)
	}
	targets := make([]distributedtxn.TransactionTargetRef, distributedtxn.MaxManifestPageTargets)
	identities := make([]byte, distributedtxn.MaxManifestPageTargets*distributedtxn.MaxShardIdentityBytes*2)
	for _, page := range pages {
		if _, err = journal.StageManifestSegment(id, page, targets, identities); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = journal.SealManifestCoordinator(id, 1, distributedtxn.CoordinatorCommitted); err != nil {
		t.Fatal(err)
	}
	if _, err = journal.TransitionCoordinator(id, 2, distributedtxn.CoordinatorRetired); err != nil {
		t.Fatal(err)
	}
	before := journal.Usage().RetainedBytes
	if opportunity := journal.CompactionOpportunity(); !opportunity.Recommended {
		t.Fatalf("fixture is not a recommended compaction: %+v", opportunity)
	}

	baseCtx, cancel := context.WithCancel(context.Background())
	server := &Server{
		journal: journal, baseCtx: baseCtx, cancel: cancel,
		journalCompact: make(chan struct{}, 1),
	}
	server.maintenanceWG.Add(1)
	go server.runJournalCompactor()
	server.scheduleJournalCompaction()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for journal.Usage().RetainedBytes == before {
		select {
		case <-deadline.C:
			t.Fatal("background journal compaction did not complete")
		case <-ticker.C:
		}
	}
	after := journal.Usage().RetainedBytes
	if after*32 >= before {
		t.Fatalf("background compaction ratio = %d/%d, want <1/32", after, before)
	}
	cancel()
	server.maintenanceWG.Wait()
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
}
