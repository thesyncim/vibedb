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
}

type concurrentReplaceBucket struct {
	indices []int
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
	for _, layout := range []struct {
		name       string
		sameBucket bool
	}{
		{name: "DisjointBuckets"},
		{name: "SameBucket", sameBucket: true},
	} {
		for _, clients := range []int{1, 8, 32} {
			b.Run(fmt.Sprintf("%s/clients=%d", layout.name, clients), func(b *testing.B) {
				collection := unifiedBenchStoreWith(b, keys, docs, options, options)
				lanes := concurrentReplaceLanes(
					b, collection, keyBytes, canonical, replacement,
					clients, layout.sameBucket,
				)

				// Establish each alternate spelling before flushing, leaving the
				// measured run at a clean checkpoint with every selected key known
				// to remain on the equal-size existing-row path.
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
				close(start)
				done.Wait()
				b.StopTimer()
				close(errCh)
				for err := range errCh {
					b.Error(err)
				}

				after := collection.Stats()
				reportConcurrentReplaceMetrics(b, before, after)
				fastReplaces := after.ConcurrentPrimaryReplaces -
					before.ConcurrentPrimaryReplaces
				pressureFallbacks := after.ConcurrentPrimaryFallbacks -
					before.ConcurrentPrimaryFallbacks
				if !layout.sameBucket {
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
			})
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
					keys, first, second,
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
				keys, first, second,
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
) concurrentReplaceLane {
	lane := concurrentReplaceLane{
		keys:   make([][]byte, len(indices)),
		first:  make([][]byte, len(indices)),
		second: make([][]byte, len(indices)),
	}
	for i, at := range indices {
		lane.keys[i] = keys[at]
		lane.first[i] = first[at]
		lane.second[i] = second[at]
	}
	return lane
}

func reportConcurrentReplaceMetrics(b *testing.B, before, after Stats) {
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
	b.ReportMetric(
		float64(after.ConcurrentPrimaryLargestPublishGroup),
		"largest-publish-group",
	)
	accounted := after.ConcurrentPrimaryReplaces - before.ConcurrentPrimaryReplaces +
		after.ConcurrentPrimaryFallbacks - before.ConcurrentPrimaryFallbacks
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
