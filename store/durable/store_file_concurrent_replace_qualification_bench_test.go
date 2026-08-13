package durable

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

const (
	// Compact stripes hold substantially more rows than the retired class-5
	// leaves. Keep enough source rows for the 32-client disjoint-bucket arm to
	// select 32 independently locked stripes after bulk packing.
	concurrentReplaceCorpusSize  = 65_536
	concurrentReplaceKeysPerLane = 4
)

type concurrentReplaceLane struct {
	keys   [][]byte
	first  [][]byte
	second [][]byte
	bucket storeio.BucketID
	stripe uint32
}

type concurrentReplaceBucket struct {
	indices []int
	id      storeio.BucketID
	compact bool
	stripe  uint32
}

// BenchmarkConcurrentExistingKeyReplace qualifies buffered-visible replacement
// scaling independently of insert/split work. Every worker owns four existing
// keys and alternates two equal-sized canonical values, so the timed operation
// is deterministic and cannot turn into a no-op or an insert.
//
// DisjointBuckets gives each worker a separate routed leaf and lock stripe, so
// an accidental stripe-hash collision cannot turn the parallel qualification
// into hidden contention. SameBucket keeps keys disjoint but places every
// worker in one leaf, exposing contention that a same-key benchmark would hide
// through last-write-wins folding.
func BenchmarkConcurrentExistingKeyReplace(b *testing.B) {
	keys, docs := unifiedPrimaryCorpus(concurrentReplaceCorpusSize, true)
	keyBytes := make([][]byte, len(keys))
	canonical := make([][]byte, len(keys))
	replacement := make([][]byte, len(keys))
	for i := range keys {
		keyBytes[i] = []byte(keys[i])
		var err error
		canonical[i], err = vibejson.AppendCanonicalize(nil, docs[i])
		if err != nil {
			b.Fatalf("canonicalize document %d: %v", i, err)
		}
		replacement[i] = append([]byte(nil), canonical[i]...)
		const countryPrefix = `"country":"`
		country := bytes.Index(replacement[i], []byte(countryPrefix))
		if country < 0 || country+len(countryPrefix)+2 > len(replacement[i]) {
			b.Fatalf("canonical document %d has no two-byte country", i)
		}
		country += len(countryPrefix)
		alternate := "ZZ"
		if string(replacement[i][country:country+2]) == alternate {
			alternate = "YY"
		}
		copy(replacement[i][country:country+2], alternate)
		if len(replacement[i]) != len(canonical[i]) ||
			bytes.Equal(replacement[i], canonical[i]) {
			b.Fatalf("replacement %d is not distinct and equal-sized", i)
		}
		checked, err := vibejson.AppendCanonicalize(nil, replacement[i])
		if err != nil || !bytes.Equal(checked, replacement[i]) {
			b.Fatalf("replacement %d is not canonical: %v", i, err)
		}
	}

	options := Options{
		ResidentBytes: 64 << 20,
		Backend:       BackendPortable,
		Durability:    DurabilityBufferedVisible,
	}
	layouts := []struct {
		name       string
		sameBucket bool
	}{
		{name: "DisjointBuckets"},
		{name: "SameBucket", sameBucket: true},
	}
	for _, layout := range layouts {
		for _, clients := range []int{1, 8, 32} {
			b.Run(fmt.Sprintf("%s/clients=%d", layout.name, clients), func(b *testing.B) {
				benchmarkConcurrentExistingKeyReplaceCase(
					b, keys, docs, keyBytes, canonical, replacement,
					options, clients, layout.sameBucket, false,
				)
			})
		}
	}

	journalOptions := options
	journalOptions.RecoveryJournal = true
	journalOptions.CheckpointStrength = CheckpointFilesystem
	b.Run("RecoveryJournal", func(b *testing.B) {
		for _, layout := range layouts {
			for _, clients := range []int{1, 8, 32} {
				b.Run(fmt.Sprintf("%s/clients=%d", layout.name, clients), func(b *testing.B) {
					benchmarkConcurrentExistingKeyReplaceCase(
						b, keys, docs, keyBytes, canonical, replacement,
						journalOptions, clients, layout.sameBucket, true,
					)
				})
			}
		}
	})
}

func benchmarkConcurrentExistingKeyReplaceCase(
	b *testing.B,
	keys []string,
	docs, keyBytes, canonical, replacement [][]byte,
	options Options,
	clients int,
	sameBucket, recoveryJournal bool,
) {
	b.Helper()
	b.StopTimer()
	collection := unifiedBenchStoreWith(b, keys, docs, options, options)
	lanes := concurrentReplaceLanes(
		b, collection, keyBytes, canonical, replacement,
		clients, sameBucket,
	)

	// Establish each alternate spelling before flushing, leaving the measured
	// run at a clean checkpoint with every selected key known to remain on the
	// equal-size existing-row path. Flush may replace the physical router, so
	// certify the selected buckets and lock stripes again afterwards rather than
	// trusting the pre-checkpoint route.
	for _, lane := range lanes {
		for i := range lane.keys {
			if created, err := collection.Put(lane.keys[i], lane.second[i]); err != nil {
				b.Fatalf("warm replacement: %v", err)
			} else if created {
				b.Fatal("warm replacement unexpectedly inserted a key")
			}
			if created, err := collection.Put(lane.keys[i], lane.first[i]); err != nil {
				b.Fatalf("warm restore: %v", err)
			} else if created {
				b.Fatal("warm restore unexpectedly inserted a key")
			}
		}
	}
	if err := collection.Flush(); err != nil {
		b.Fatalf("warm flush: %v", err)
	}
	if recoveryJournal {
		certifyConcurrentReplaceLanes(b, collection, lanes, sameBucket)
	}
	before := collection.Stats()

	start := make(chan struct{})
	errCh := make(chan error, clients)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(clients)
	done.Add(clients)
	perClient := b.N / clients
	extra := b.N % clients
	for client := 0; client < clients; client++ {
		operations := perClient
		if client < extra {
			operations++
		}
		lane := lanes[client]
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			for operation := 0; operation < operations; operation++ {
				at := operation % len(lane.keys)
				value := lane.second[at]
				if operation/len(lane.keys)&1 != 0 {
					value = lane.first[at]
				}
				created, err := collection.Put(lane.keys[at], value)
				if err != nil {
					errCh <- err
					return
				}
				if created {
					errCh <- errors.New("replacement unexpectedly inserted a key")
					return
				}
			}
		}()
	}
	ready.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	close(errCh)
	for err := range errCh {
		b.Error(err)
	}

	after := collection.Stats()
	journalCohortReplaces := uint64(0)
	if recoveryJournal {
		journalCohortReplaces = after.JournalCohortReplaces -
			before.JournalCohortReplaces
	}
	reportConcurrentReplaceMetrics(
		b, before, after, !recoveryJournal, journalCohortReplaces,
	)
	fastReplaces := after.ConcurrentPrimaryReplaces -
		before.ConcurrentPrimaryReplaces
	pressureFallbacks := after.ConcurrentPrimaryFallbacks -
		before.ConcurrentPrimaryFallbacks
	if recoveryJournal {
		reportConcurrentReplaceJournalMetrics(b, before, after, clients)
		return
	}
	if !sameBucket {
		if accounted := fastReplaces + pressureFallbacks; accounted != uint64(b.N) {
			b.Errorf(
				"concurrent replacements=%d fast + %d pressure fallback = %d, want all %d operations",
				fastReplaces, pressureFallbacks, accounted, b.N,
			)
		}
		folds := after.PrimaryOverlayFolds - before.PrimaryOverlayFolds
		automaticCheckpoints := after.AutomaticCheckpoints -
			before.AutomaticCheckpoints
		journalFallbacks := after.JournalDeltaFullFallbacks -
			before.JournalDeltaFullFallbacks
		if pressureFallbacks >
			folds+automaticCheckpoints+journalFallbacks+1 {
			b.Errorf(
				"pressure fallbacks=%d exceed %d overlay folds + %d automatic checkpoints + %d journal fallbacks + one initial recovery",
				pressureFallbacks, folds, automaticCheckpoints, journalFallbacks,
			)
		}
	}
}

func concurrentReplaceLanes(
	b *testing.B,
	collection *Collection,
	keys, first, second [][]byte,
	clients int,
	sameBucket bool,
) []concurrentReplaceLane {
	b.Helper()
	router := collection.primaryRouter.Load()
	if router == nil {
		b.Fatal("primary router is unavailable")
	}

	byID := make(map[storeio.BucketID]int)
	buckets := make([]concurrentReplaceBucket, 0, router.Len())
	for i, key := range keys {
		route, ok := router.Route(key)
		if !ok {
			b.Fatalf("route existing key %q", key)
		}
		bucketAt, exists := byID[route.Bucket]
		if !exists {
			lease, err := router.AcquireLeaf(collection.cache, route)
			if err != nil {
				b.Fatalf("acquire bucket %d: %v", route.Bucket, err)
			}
			compact := storeio.PrimaryLeafClass(lease.Page()) ==
				storeio.CommonPrimaryLeafCompact
			lease.Release()
			bucketAt = len(buckets)
			byID[route.Bucket] = bucketAt
			buckets = append(buckets, concurrentReplaceBucket{
				id:      route.Bucket,
				compact: compact,
				stripe:  primaryConcurrentStripeIndex(route.Bucket),
			})
		}
		buckets[bucketAt].indices = append(buckets[bucketAt].indices, i)
	}

	lanes := make([]concurrentReplaceLane, 0, clients)
	if sameBucket {
		needed := clients * concurrentReplaceKeysPerLane
		for _, bucket := range buckets {
			if !bucket.compact || len(bucket.indices) < needed {
				continue
			}
			for client := 0; client < clients; client++ {
				from := client * concurrentReplaceKeysPerLane
				lanes = append(lanes, concurrentReplaceLaneFromIndices(
					bucket.indices[from:from+concurrentReplaceKeysPerLane],
					keys, first, second, bucket.id, bucket.stripe,
				))
			}
			break
		}
	} else {
		usedStripes := make(map[uint32]struct{}, clients)
		for _, bucket := range buckets {
			if !bucket.compact ||
				len(bucket.indices) < concurrentReplaceKeysPerLane {
				continue
			}
			if _, collision := usedStripes[bucket.stripe]; collision {
				continue
			}
			usedStripes[bucket.stripe] = struct{}{}
			lanes = append(lanes, concurrentReplaceLaneFromIndices(
				bucket.indices[:concurrentReplaceKeysPerLane],
				keys, first, second, bucket.id, bucket.stripe,
			))
			if len(lanes) == clients {
				break
			}
		}
	}
	if len(lanes) != clients {
		b.Fatalf(
			"qualification corpus supplied %d/%d %s lanes across %d buckets",
			len(lanes), clients, map[bool]string{
				false: "disjoint-bucket", true: "same-bucket",
			}[sameBucket], len(buckets),
		)
	}
	return lanes
}

func concurrentReplaceLaneFromIndices(
	indices []int,
	keys, first, second [][]byte,
	bucket storeio.BucketID,
	stripe uint32,
) concurrentReplaceLane {
	lane := concurrentReplaceLane{
		keys:   make([][]byte, len(indices)),
		first:  make([][]byte, len(indices)),
		second: make([][]byte, len(indices)),
		bucket: bucket,
		stripe: stripe,
	}
	for i, at := range indices {
		lane.keys[i] = keys[at]
		lane.first[i] = first[at]
		lane.second[i] = second[at]
	}
	return lane
}

func certifyConcurrentReplaceLanes(
	b *testing.B,
	collection *Collection,
	lanes []concurrentReplaceLane,
	sameBucket bool,
) {
	b.Helper()
	router := collection.primaryRouter.Load()
	if router == nil {
		b.Fatal("primary router is unavailable after warm flush")
	}
	seenBuckets := make(map[storeio.BucketID]struct{}, len(lanes))
	seenStripes := make(map[uint32]struct{}, len(lanes))
	for laneIndex, lane := range lanes {
		if len(lane.keys) != concurrentReplaceKeysPerLane {
			b.Fatalf(
				"lane %d has %d keys after warm flush, want %d",
				laneIndex, len(lane.keys), concurrentReplaceKeysPerLane,
			)
		}
		if got := primaryConcurrentStripeIndex(lane.bucket); got != lane.stripe {
			b.Fatalf(
				"lane %d pre-flush bucket %d now hashes to stripe %d, want certificate %d",
				laneIndex, lane.bucket, got, lane.stripe,
			)
		}
		for _, key := range lane.keys {
			route, ok := router.Route(key)
			if !ok || route.Ref == (storeio.PageRef{}) {
				b.Fatalf("route existing key %q after warm flush", key)
			}
			stripe := primaryConcurrentStripeIndex(route.Bucket)
			if route.Bucket != lane.bucket || stripe != lane.stripe {
				b.Fatalf(
					"key %q route after warm flush = bucket %d stripe %d, want certified bucket %d stripe %d",
					key, route.Bucket, stripe, lane.bucket, lane.stripe,
				)
			}
			lease, err := router.AcquireLeaf(collection.cache, route)
			if err != nil {
				b.Fatalf("acquire bucket %d after warm flush: %v", route.Bucket, err)
			}
			page := lease.Page()
			class := storeio.PrimaryLeafClass(page)
			leaf, admitted := storeio.AdmittedCompactPrimaryStripe(
				page, collection.storeID, route.Bucket,
			)
			found := false
			if admitted {
				_, found = leaf.FindKey(key)
			}
			lease.Release()
			if class != storeio.CommonPrimaryLeafCompact || !admitted || !found {
				b.Fatalf(
					"key %q lost compact existing-row certificate after warm flush: class=%d admitted=%t found=%t",
					key, class, admitted, found,
				)
			}
		}

		if sameBucket {
			if laneIndex != 0 {
				if _, ok := seenBuckets[lane.bucket]; !ok {
					b.Fatalf("same-bucket lane %d moved to bucket %d", laneIndex, lane.bucket)
				}
				if _, ok := seenStripes[lane.stripe]; !ok {
					b.Fatalf("same-bucket lane %d moved to stripe %d", laneIndex, lane.stripe)
				}
			}
		} else {
			if _, duplicate := seenBuckets[lane.bucket]; duplicate {
				b.Fatalf("disjoint lane %d reuses bucket %d", laneIndex, lane.bucket)
			}
			if _, collision := seenStripes[lane.stripe]; collision {
				b.Fatalf("disjoint lane %d collides on stripe %d", laneIndex, lane.stripe)
			}
		}
		seenBuckets[lane.bucket] = struct{}{}
		seenStripes[lane.stripe] = struct{}{}
	}
}

func concurrentReplaceHistogramDelta(
	b *testing.B,
	name string,
	before, after StatsHistogram,
) (count, sum uint64) {
	b.Helper()
	if after.Count < before.Count || after.Sum < before.Sum {
		b.Fatalf("%s histogram regressed: before=%+v after=%+v", name, before, after)
	}
	count = after.Count - before.Count
	sum = after.Sum - before.Sum
	var bucketCount uint64
	for index := range after.Buckets {
		if after.Buckets[index] < before.Buckets[index] {
			b.Fatalf(
				"%s histogram bucket %d regressed: before=%d after=%d",
				name, index, before.Buckets[index], after.Buckets[index],
			)
		}
		bucketCount += after.Buckets[index] - before.Buckets[index]
	}
	if bucketCount != count {
		b.Errorf(
			"%s histogram delta has %d bucket samples, want count %d",
			name, bucketCount, count,
		)
	}
	return count, sum
}

func reportConcurrentReplaceJournalMetrics(
	b *testing.B,
	before, after Stats,
	clients int,
) {
	b.Helper()
	if b.N == 0 {
		return
	}
	ops := float64(b.N)
	journalAcks := after.JournalAcks - before.JournalAcks
	chainAcks := after.ChainAcks - before.ChainAcks
	journalSyncs := after.JournalSyncs - before.JournalSyncs
	recordGroups, syncedRecords := concurrentReplaceHistogramDelta(
		b, "journal records", before.JournalGroupRecords, after.JournalGroupRecords,
	)
	mutationGroups, syncedMutations := concurrentReplaceHistogramDelta(
		b, "journal mutations", before.JournalGroupMutations, after.JournalGroupMutations,
	)
	byteGroups, syncedBytes := concurrentReplaceHistogramDelta(
		b, "journal bytes", before.JournalGroupBytes, after.JournalGroupBytes,
	)
	syncSamples, syncNS := concurrentReplaceHistogramDelta(
		b, "journal sync latency", before.JournalGroupSyncNS, after.JournalGroupSyncNS,
	)
	cohortReplaces := after.JournalCohortReplaces - before.JournalCohortReplaces
	cohortGroups := after.JournalCohortPublishGroups -
		before.JournalCohortPublishGroups
	cohortSamples, cohortPublished := concurrentReplaceHistogramDelta(
		b, "journal cohort publish group", before.JournalCohortPublishGroupSize,
		after.JournalCohortPublishGroupSize,
	)
	ordinaryPublishSamples, ordinaryPublished := concurrentReplaceHistogramDelta(
		b, "ordinary concurrent publish group",
		before.ConcurrentPrimaryPublishGroupSize,
		after.ConcurrentPrimaryPublishGroupSize,
	)
	ordinaryStripeWaits, ordinaryStripeWaitNS := concurrentReplaceHistogramDelta(
		b, "ordinary concurrent stripe wait", before.ConcurrentPrimaryStripeWaitNS,
		after.ConcurrentPrimaryStripeWaitNS,
	)
	ordinaryReplaces := after.ConcurrentPrimaryReplaces -
		before.ConcurrentPrimaryReplaces
	ordinaryFallbacks := after.ConcurrentPrimaryFallbacks -
		before.ConcurrentPrimaryFallbacks
	ordinaryPublishGroups := after.ConcurrentPrimaryPublishGroups -
		before.ConcurrentPrimaryPublishGroups

	b.ReportMetric(float64(journalAcks)/ops, "journal-acks/op")
	b.ReportMetric(float64(chainAcks)/ops, "chain-acks/op")
	b.ReportMetric(float64(journalSyncs)/ops, "journal-syncs/op")
	b.ReportMetric(float64(syncedRecords)/ops, "synced-journal-records/op")
	b.ReportMetric(float64(syncedBytes)/ops, "synced-journal-B/op")
	b.ReportMetric(float64(cohortReplaces)/ops, "journal-cohort-replaces/op")
	b.ReportMetric(float64(cohortGroups)/ops, "journal-cohort-publish-groups/op")
	if cohortGroups != 0 {
		b.ReportMetric(
			float64(cohortPublished)/float64(cohortGroups),
			"mean-journal-cohort-publish-group",
		)
	}
	if cohortReplaces <= uint64(b.N) {
		b.ReportMetric(
			float64(uint64(b.N)-cohortReplaces)/ops,
			"journal-baseline-replaces/op",
		)
	}
	checkpointCovered := uint64(0)
	if syncedRecords <= journalAcks {
		checkpointCovered = journalAcks - syncedRecords
	}
	b.ReportMetric(
		float64(checkpointCovered)/ops,
		"checkpoint-covered-journal-acks/op",
	)
	if journalSyncs != 0 {
		b.ReportMetric(
			float64(syncedRecords)/float64(journalSyncs),
			"journal-records/sync",
		)
		b.ReportMetric(
			float64(syncNS)/float64(journalSyncs),
			"journal-sync-ns/sync",
		)
	}
	if durableAcks := journalAcks + chainAcks; durableAcks != uint64(b.N) {
		b.Errorf(
			"recovery-journal durable acknowledgements=%d journal + %d chain = %d, want all %d operations",
			journalAcks, chainAcks, durableAcks, b.N,
		)
	}
	if cohortReplaces > uint64(b.N) {
		b.Errorf(
			"journal cohort replacements=%d exceed all %d operations",
			cohortReplaces, b.N,
		)
	}
	if cohortSamples != cohortGroups || cohortPublished != cohortReplaces {
		b.Errorf(
			"journal cohort samples=%d sum=%d, want groups=%d replacements=%d",
			cohortSamples, cohortPublished, cohortGroups, cohortReplaces,
		)
	}
	if cohortGroups == 0 && cohortReplaces != 0 {
		b.Errorf(
			"journal cohort replacements=%d have no publish group",
			cohortReplaces,
		)
	} else if cohortGroups != 0 &&
		(cohortReplaces < 2*cohortGroups ||
			cohortReplaces > primaryJournalAdmissionLimit*cohortGroups) {
		b.Errorf(
			"journal cohort replacements=%d outside bounded group range [%d,%d] for %d groups",
			cohortReplaces, 2*cohortGroups,
			primaryJournalAdmissionLimit*cohortGroups, cohortGroups,
		)
	}
	if clients > 1 && b.N >= clients && cohortReplaces == 0 {
		b.Errorf(
			"%d-client recovery-journal qualification published no journal cohort across %d operations",
			clients, b.N,
		)
	}
	if ordinaryReplaces != 0 || ordinaryFallbacks != 0 ||
		ordinaryPublishGroups != 0 || ordinaryPublishSamples != 0 ||
		ordinaryPublished != 0 || ordinaryStripeWaits != 0 ||
		ordinaryStripeWaitNS != 0 {
		b.Errorf(
			"recovery-journal cohort used ordinary concurrent stripe publisher: replaces=%d fallbacks=%d groups=%d group-samples=%d group-sum=%d stripe-waits=%d stripe-wait-ns=%d",
			ordinaryReplaces, ordinaryFallbacks, ordinaryPublishGroups,
			ordinaryPublishSamples, ordinaryPublished, ordinaryStripeWaits,
			ordinaryStripeWaitNS,
		)
	}
	if journalSyncs > journalAcks {
		b.Errorf("journal syncs=%d exceed journal acknowledgements=%d", journalSyncs, journalAcks)
	}
	if recordGroups != journalSyncs || mutationGroups != journalSyncs ||
		byteGroups != journalSyncs || syncSamples != journalSyncs {
		b.Errorf(
			"journal group sample counts records=%d mutations=%d bytes=%d sync-ns=%d, want syncs=%d",
			recordGroups, mutationGroups, byteGroups, syncSamples, journalSyncs,
		)
	}
	if syncedRecords > journalAcks {
		b.Errorf(
			"journal group records=%d exceed journal acknowledgements=%d",
			syncedRecords, journalAcks,
		)
	}
	if syncedMutations != syncedRecords {
		b.Errorf(
			"journal group mutations=%d, want one per synced record=%d",
			syncedMutations, syncedRecords,
		)
	}
	if syncedRecords != 0 && syncedBytes == 0 {
		b.Error("journal group bytes are zero for non-empty synced records")
	}
	automaticCheckpoints := after.AutomaticCheckpoints - before.AutomaticCheckpoints
	retirementCheckpoints := after.RetirementPressureCheckpoints -
		before.RetirementPressureCheckpoints
	checkpointEvidence := automaticCheckpoints + retirementCheckpoints
	if checkpointCovered != 0 && checkpointEvidence == 0 {
		b.Errorf(
			"%d journal acknowledgements lack a sync record without an automatic or retirement-pressure checkpoint",
			checkpointCovered,
		)
	}
	if chainAcks > checkpointEvidence {
		b.Errorf(
			"chain acknowledgements=%d exceed automatic=%d + retirement-pressure=%d checkpoints",
			chainAcks, automaticCheckpoints, retirementCheckpoints,
		)
	}
	strictSyncs := after.JournalStrictSyncs - before.JournalStrictSyncs
	strictRecords := after.JournalStrictRecords - before.JournalStrictRecords
	strictMutations := after.JournalStrictMutations - before.JournalStrictMutations
	strictBytes := after.JournalStrictBytes - before.JournalStrictBytes
	strictSyncSamples := after.JournalStrictSyncNS.Count - before.JournalStrictSyncNS.Count
	if strictSyncs != 0 || strictRecords != 0 || strictMutations != 0 ||
		strictBytes != 0 || strictSyncSamples != 0 {
		b.Errorf(
			"buffered journal used strict lane: syncs=%d records=%d mutations=%d bytes=%d samples=%d",
			strictSyncs, strictRecords, strictMutations, strictBytes, strictSyncSamples,
		)
	}
	deltaCheckpoints := after.JournalDeltaCheckpoints - before.JournalDeltaCheckpoints
	deltaRecords := after.JournalDeltaRecords - before.JournalDeltaRecords
	deltaBytes := after.JournalDeltaBytes - before.JournalDeltaBytes
	deltaFallbacks := after.JournalDeltaFullFallbacks - before.JournalDeltaFullFallbacks
	if deltaCheckpoints != 0 || deltaRecords != 0 || deltaBytes != 0 || deltaFallbacks != 0 {
		b.Errorf(
			"explicit recovery journal used ordinary delta lane: checkpoints=%d records=%d bytes=%d fallbacks=%d",
			deltaCheckpoints, deltaRecords, deltaBytes, deltaFallbacks,
		)
	}
}

func reportConcurrentReplaceMetrics(
	b *testing.B,
	before, after Stats,
	reportLargestPublishGroup bool,
	additionalAccounted uint64,
) {
	if b.N == 0 {
		return
	}
	ops := float64(b.N)
	b.ReportMetric(ops/b.Elapsed().Seconds(), "ops/s")
	b.ReportMetric(
		float64(after.DeviceBytes-before.DeviceBytes)/ops,
		"device-B/op",
	)
	b.ReportMetric(
		float64(after.AutomaticCheckpoints-before.AutomaticCheckpoints)/ops,
		"auto-checkpoints/op",
	)
	b.ReportMetric(
		float64(after.PrimaryOverlayFolds-before.PrimaryOverlayFolds)/ops,
		"overlay-folds/op",
	)
	b.ReportMetric(
		float64(after.ConcurrentPrimaryReplaces-before.ConcurrentPrimaryReplaces)/ops,
		"fast-replaces/op",
	)
	b.ReportMetric(
		float64(after.ConcurrentPrimaryFallbacks-before.ConcurrentPrimaryFallbacks)/ops,
		"pressure-fallbacks/op",
	)
	publishGroups := after.ConcurrentPrimaryPublishGroups -
		before.ConcurrentPrimaryPublishGroups
	b.ReportMetric(float64(publishGroups)/ops, "publish-groups/op")
	if publishGroups != 0 {
		fast := after.ConcurrentPrimaryReplaces - before.ConcurrentPrimaryReplaces
		b.ReportMetric(float64(fast)/float64(publishGroups), "mean-publish-group")
	}
	if reportLargestPublishGroup {
		b.ReportMetric(
			float64(after.ConcurrentPrimaryLargestPublishGroup),
			"largest-publish-group",
		)
	}
	accounted := after.ConcurrentPrimaryReplaces - before.ConcurrentPrimaryReplaces +
		after.ConcurrentPrimaryFallbacks - before.ConcurrentPrimaryFallbacks +
		additionalAccounted
	if accounted <= uint64(b.N) {
		b.ReportMetric(float64(uint64(b.N)-accounted)/ops, "exclusive-fallbacks/op")
	}
	b.ReportMetric(
		float64(after.JournalDeltaFullFallbacks-before.JournalDeltaFullFallbacks)/ops,
		"journal-full-fallbacks/op",
	)
	b.ReportMetric(
		float64(after.MaterializationFallbacks-before.MaterializationFallbacks)/ops,
		"materialization-fallbacks/op",
	)
}
