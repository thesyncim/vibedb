// Package benchcorpus owns the deterministic JSON corpus shared by root
// regression gates and the nested competitive-benchmark module. Keeping one
// generator prevents a local micro-gate from silently measuring different
// documents than the cross-engine harness it claims to model.
package benchcorpus

import (
	"fmt"
	"math/rand/v2"
	"strconv"
)

// Document is one deterministic key and JSON body.
type Document struct {
	Key  string
	JSON []byte
}

var (
	countries = buildCountries()
	tiers     = []string{"free", "pro", "team", "enterprise"}
	regions   = []string{"eu-west-1", "eu-central-1", "us-east-1", "us-west-2", "ap-south-1"}
	tagPool   = []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	notes     = []string{
		"steady state, no anomalies observed in the last reporting window",
		"processed by the current pipeline during the maintenance window",
		"flagged for review after a threshold breach on the ingest path",
		"nominal; retention policy applied and checkpoint acknowledged",
	}
)

func buildCountries() []string {
	const letters = "ABCDEFGHIJ"
	out := make([]string, 0, 100)
	for i := range len(letters) {
		for j := range len(letters) {
			out = append(out, string(letters[i])+string(letters[j]))
		}
	}
	out[26] = "PT"
	return out
}

// Key returns the fixed-width lexical key for ordinal i.
func Key(i int) string { return fmt.Sprintf("doc:%08d", i) }

// Corpus returns n deterministic documents. highCardinality replaces every
// pool-drawn string except country and date with a per-document random value of
// exactly the same length, preserving document shape and byte length.
func Corpus(n int, highCardinality bool) []Document {
	rng := rand.New(rand.NewPCG(0x5DEECE66D, 0xB16B00B5))
	fill := rand.New(rand.NewPCG(0xC0FFEE, 0x1234567))
	uniq := func(sample string) string {
		if !highCardinality {
			return sample
		}
		return randomLower(fill, len(sample))
	}
	documents := make([]Document, n)
	buf := make([]byte, 0, 512)
	for i := range n {
		country := countries[rng.IntN(len(countries))]
		tier := uniq(tiers[rng.IntN(len(tiers))])
		region := uniq(regions[rng.IntN(len(regions))])
		note := uniq(notes[rng.IntN(len(notes))])
		score := rng.IntN(1000)
		joinedY := 2018 + rng.IntN(7)
		joinedM := 1 + rng.IntN(12)
		joinedD := 1 + rng.IntN(28)
		nTags := 2 + rng.IntN(3)

		buf = buf[:0]
		buf = append(buf, `{"id":`...)
		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, `,"name":"user-`...)
		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, `","country":"`...)
		buf = append(buf, country...)
		buf = append(buf, `","score":`...)
		buf = strconv.AppendInt(buf, int64(score), 10)
		buf = append(buf, `,"active":`...)
		if i%3 == 0 {
			buf = append(buf, "false"...)
		} else {
			buf = append(buf, "true"...)
		}
		buf = append(buf, `,"profile":{"tier":"`...)
		buf = append(buf, tier...)
		buf = append(buf, `","region":"`...)
		buf = append(buf, region...)
		buf = append(buf, `","joined":"`...)
		buf = append(buf, fmt.Sprintf("%04d-%02d-%02d", joinedY, joinedM, joinedD)...)
		buf = append(buf, `"},"tags":[`...)
		for tag := range nTags {
			if tag > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, '"')
			buf = append(buf, uniq(tagPool[(i+tag*3)%len(tagPool)])...)
			buf = append(buf, '"')
		}
		buf = append(buf, `],"note":"`...)
		buf = append(buf, note...)
		buf = append(buf, `"}`...)

		documents[i] = Document{
			Key: Key(i), JSON: append([]byte(nil), buf...),
		}
	}
	return documents
}

func randomLower(rng *rand.Rand, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('a' + rng.IntN(26))
	}
	return string(out)
}
