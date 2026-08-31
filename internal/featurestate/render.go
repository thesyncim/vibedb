package featurestate

import (
	"bytes"
	"fmt"
	"strings"
)

const markdownWrapWidth = 96

func RenderMarkdown() []byte {
	var out bytes.Buffer
	evidenceIDs, evidence := collectEvidence()
	out.WriteString("# Distributed feature ledger\n\n")
	out.WriteString("> [!CAUTION]\n")
	writeWrapped(&out, "> ", "> ", "VibeDB is unreleased development software. Any commit may break APIs, commands, wire protocols, or disk formats. Use documentation and binaries from the exact same commit, and store only disposable or independently recoverable data. A **Yes** below does not mean supported, production-ready, or compatible with another commit. Consult [current status](status.md) for known failing tests and defects.")
	out.WriteByte('\n')
	out.WriteString("> [!NOTE]\n")
	writeWrapped(&out, "> ", "> ", "Generated from `internal/featurestate.Distributed`. Change the manifest, not this file.")
	out.WriteByte('\n')
	out.WriteString("## How to read this ledger\n\n")
	writeWrapped(&out, "", "", "Every feature has four independent stages. The adjacent contract is the claim; the label alone is not. This ledger describes only the exact commit that generated it.")
	out.WriteByte('\n')
	writeWrapped(&out, "- **Yes** — ", "  ", "The manifest makes the complete stage claim written in its contract and cites evidence.")
	writeWrapped(&out, "- **Partial** — ", "  ", "Only the subset stated in the contract is present or qualified; the contract names material gaps.")
	writeWrapped(&out, "- **No** — ", "  ", "The manifest makes no positive claim for that stage; the contract states the current boundary.")
	out.WriteByte('\n')
	out.WriteString("| Stage | Question answered |\n")
	out.WriteString("| --- | --- |\n")
	out.WriteString("| Primitive | Does the underlying code, codec, or protocol exist? |\n")
	out.WriteString("| Integrated | Does a repository path compose and use it? |\n")
	out.WriteString("| Development command | Does a checked-in command or CLI construct the path? |\n")
	out.WriteString("| Qualification | Has the stated contract passed the cited fault or performance gate? |\n\n")
	writeWrapped(&out, "", "", "A qualification **Yes** requires the fault or benchmark gate named by its contract; ordinary correctness tests remain **Partial**. A development-command **Yes** means a checked-in command constructs the path, not that the feature is released.")
	out.WriteByte('\n')

	out.WriteString("## Features\n\n")
	for _, feature := range Distributed {
		renderFeature(&out, feature, evidenceIDs)
	}

	out.WriteString("## Evidence index\n\n")
	writeWrapped(&out, "", "", "Evidence is deduplicated across features. Links point to source or executable tests in this repository.")
	out.WriteByte('\n')
	fmt.Fprintf(&out, "<details>\n<summary>%d unique source and test references</summary>\n\n", len(evidence))
	for index, reference := range evidence {
		fmt.Fprintf(&out, "%d. <a id=\"evidence-%d\"></a>[%s](../%s) — `%s`\n",
			index+1, index+1, escape(reference.Path), escape(reference.Path),
			escape(reference.Symbol))
	}
	out.WriteString("\n</details>\n")

	out.WriteString("\n## Regenerate\n\n")
	writeWrapped(&out, "1. ", "   ", "Change `internal/featurestate/manifest.go` and cite production code or an executable test.")
	writeWrapped(&out, "2. ", "   ", "Run `go generate ./internal/featurestate`.")
	writeWrapped(&out, "3. ", "   ", "Run `go test ./internal/featurestate`. The test rejects stale output, invalid state transitions, duplicate rows, and missing evidence files.")
	out.WriteByte('\n')
	writeWrapped(&out, "", "", "See the [embedded capability matrix](capabilities.md) for local entry points and [distributed operations](operations/distributed.md) for runtime behavior.")
	return out.Bytes()
}

func renderFeature(out *bytes.Buffer, feature Feature, evidenceIDs map[Reference]int) {
	fmt.Fprintf(out,
		"<details>\n<summary><strong>%s</strong> — Primitive:%s · Integrated:%s · Command:%s · Qualification:%s</summary>\n\n",
		escape(feature.Name), feature.Primitive.Status.label(),
		feature.Integrated.Status.label(), feature.Shipped.Status.label(),
		feature.Qualification.Status.label())
	renderStage(out, "Primitive", feature.Primitive, evidenceIDs)
	renderStage(out, "Integrated", feature.Integrated, evidenceIDs)
	renderStage(out, "Development command", feature.Shipped, evidenceIDs)
	renderStage(out, "Qualification", feature.Qualification, evidenceIDs)
	out.WriteString("\n</details>\n\n")
}

func collectEvidence() (map[Reference]int, []Reference) {
	ids := make(map[Reference]int)
	var ordered []Reference
	for _, feature := range Distributed {
		stages := [...]Stage{
			feature.Primitive,
			feature.Integrated,
			feature.Shipped,
			feature.Qualification,
		}
		for _, stage := range stages {
			for _, reference := range stage.Evidence {
				if _, exists := ids[reference]; exists {
					continue
				}
				ordered = append(ordered, reference)
				ids[reference] = len(ordered)
			}
		}
	}
	return ids, ordered
}

func renderStage(out *bytes.Buffer, title string, stage Stage, evidenceIDs map[Reference]int) {
	fmt.Fprintf(out, "**%s — %s**\n\n", title, stage.Status.label())
	writeWrapped(out, "", "", escape(stage.Contract))
	if len(stage.Evidence) != 0 {
		out.WriteString("\n_Evidence:_ ")
		for index, reference := range stage.Evidence {
			if index != 0 {
				out.WriteString(", ")
			}
			fmt.Fprintf(out, "[E%d](#evidence-%d)", evidenceIDs[reference], evidenceIDs[reference])
		}
		out.WriteByte('\n')
	}
	out.WriteByte('\n')
}

func writeWrapped(out *bytes.Buffer, prefix, continuation, value string) {
	words := strings.Fields(value)
	out.WriteString(prefix)
	column := len(prefix)
	for index, word := range words {
		separator := 0
		if index != 0 {
			separator = 1
		}
		if index != 0 && column+separator+len(word) > markdownWrapWidth {
			out.WriteByte('\n')
			out.WriteString(continuation)
			column = len(continuation)
			separator = 0
		}
		if separator != 0 {
			out.WriteByte(' ')
			column++
		}
		out.WriteString(word)
		column += len(word)
	}
	out.WriteByte('\n')
}

func escape(value string) string {
	value = strings.ReplaceAll(value, "|", "&#124;")
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}
