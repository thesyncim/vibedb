// Command clickhousefixture emits the shared competitive corpus as
// ClickHouse JSONEachRow records. It deliberately exposes the same documents
// in a typed-column shape so a ClickHouse comparison can use its native
// columnar representation rather than being forced to store opaque JSON.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
)

type document struct {
	ID      uint64 `json:"id"`
	Name    string `json:"name"`
	Country string `json:"country"`
	Score   uint16 `json:"score"`
	Active  bool   `json:"active"`
	Profile struct {
		Tier   string `json:"tier"`
		Region string `json:"region"`
		Joined string `json:"joined"`
	} `json:"profile"`
	Tags []string `json:"tags"`
	Note string   `json:"note"`
}

type row struct {
	Key     string   `json:"key"`
	ID      uint64   `json:"id"`
	Name    string   `json:"name"`
	Country string   `json:"country"`
	Score   uint16   `json:"score"`
	Active  bool     `json:"active"`
	Tier    string   `json:"tier"`
	Region  string   `json:"region"`
	Joined  string   `json:"joined"`
	Tags    []string `json:"tags"`
	Note    string   `json:"note"`
}

func main() {
	n := flag.Int("corpus", competitive.CorpusSize, "number of documents")
	card := flag.String("cardinality", "low", "low or high")
	shape := flag.String("shape", "typed", "typed or raw")
	flag.Parse()

	cardinality, err := competitive.ParseCardinality(*card)
	if err != nil {
		fail(err)
	}
	if *n < 1 {
		fail(fmt.Errorf("-corpus must be positive"))
	}
	if *shape != "typed" && *shape != "raw" {
		fail(fmt.Errorf("-shape must be typed or raw"))
	}

	enc := json.NewEncoder(os.Stdout)
	for _, source := range competitive.CorpusOf(*n, cardinality) {
		if *shape == "raw" {
			var doc struct {
				ID uint64 `json:"id"`
			}
			if err := json.Unmarshal(source.JSON, &doc); err != nil {
				fail(fmt.Errorf("decode %s: %w", source.Key, err))
			}
			if err := enc.Encode(struct {
				Key string `json:"key"`
				Raw string `json:"raw"`
			}{Key: source.Key, Raw: string(source.JSON)}); err != nil {
				fail(err)
			}
			continue
		}
		var doc document
		if err := json.Unmarshal(source.JSON, &doc); err != nil {
			fail(fmt.Errorf("decode %s: %w", source.Key, err))
		}
		if err := enc.Encode(row{
			Key:     source.Key,
			ID:      doc.ID,
			Name:    doc.Name,
			Country: doc.Country,
			Score:   doc.Score,
			Active:  doc.Active,
			Tier:    doc.Profile.Tier,
			Region:  doc.Profile.Region,
			Joined:  doc.Profile.Joined,
			Tags:    doc.Tags,
			Note:    doc.Note,
		}); err != nil {
			fail(err)
		}
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
