// Command clickhousefixture emits the shared competitive corpus as
// ClickHouse JSONEachRow records. It deliberately exposes the same documents
// in a typed-column shape so a ClickHouse comparison can use its native
// columnar representation rather than being forced to store opaque JSON.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
	vibejson "github.com/thesyncim/vibejson"
)

const maxFixtureDocumentBytes = 1 << 20

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

var (
	documentDecoder = mustCompileDocumentDecoder()
	rowEncoder      = mustCompileRowEncoder()
)

func mustCompileDocumentDecoder() vibejson.Decoder[document] {
	decoder, err := vibejson.CompileDecoder[document](vibejson.DecoderOptions{
		MaxDepth:              4,
		ZeroCopy:              true,
		DisallowUnknownFields: true,
		CaseSensitive:         true,
	})
	if err != nil {
		panic(err)
	}
	return decoder
}

func mustCompileRowEncoder() vibejson.Encoder[row] {
	encoder, err := vibejson.CompileEncoder[row](vibejson.EncoderOptions{})
	if err != nil {
		panic(err)
	}
	return encoder
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

	if err := emitFixture(os.Stdout, *n, cardinality, *shape); err != nil {
		fail(err)
	}
}

func emitFixture(out io.Writer, n int, cardinality competitive.Cardinality, shape string) error {
	return emitDocuments(out, competitive.CorpusOf(n, cardinality), shape)
}

func emitDocuments(out io.Writer, documents []competitive.Doc, shape string) error {
	if shape != "typed" && shape != "raw" {
		return fmt.Errorf("shape must be typed or raw")
	}
	w := vibejson.NewWriter(out)
	var doc document
	for _, source := range documents {
		if len(source.JSON) > maxFixtureDocumentBytes {
			return fmt.Errorf(
				"decode %s: document is %d bytes, limit %d",
				source.Key, len(source.JSON), maxFixtureDocumentBytes,
			)
		}
		if err := documentDecoder.Decode(source.JSON, &doc); err != nil {
			return fmt.Errorf("decode %s: %w", source.Key, err)
		}
		if shape == "raw" {
			if err := w.BeginObject(); err != nil {
				return err
			}
			if err := w.Key("key"); err != nil {
				return err
			}
			if err := w.String(source.Key); err != nil {
				return err
			}
			if err := w.Key("raw"); err != nil {
				return err
			}
			if err := w.String(string(source.JSON)); err != nil {
				return err
			}
			if err := w.EndObject(); err != nil {
				return err
			}
		} else {
			encoded := row{
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
			}
			if err := vibejson.EncodeTo(w, rowEncoder, &encoded); err != nil {
				return err
			}
		}
		if err := w.Newline(); err != nil {
			return err
		}
	}
	return w.Close()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
